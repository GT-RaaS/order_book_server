use crate::{
    listeners::order_book::{L2SnapshotParams, L2Snapshots},
    order_book::{
        Oid, Side, Snapshot,
        multi_book::{OrderBooks, Snapshots},
        types::InnerOrder,
    },
    prelude::*,
    types::{
        inner::InnerLevel,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use reqwest::Client;
use serde_json::json;
use std::collections::VecDeque;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

pub(super) async fn process_rmp_file(dir: &Path) -> Result<PathBuf> {
    let output_path = dir.join("out.json");
    let payload = json!({
        "type": "fileSnapshot",
        "request": {
            "type": "l4Snapshots",
            "includeUsers": true,
            "includeTriggerOrders": false
        },
        "outPath": output_path,
        "includeHeightInOutput": true
    });

    let client = Client::new();
    client
        .post("http://localhost:3001/info")
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await?
        .error_for_status()?;
    Ok(output_path)
}

pub(super) fn validate_snapshot_consistency<O: InnerOrder + PartialEq + Debug>(
    snapshot: &Snapshots<O>,
    expected: &Snapshots<O>,
    ignore_spot: bool,
) -> Result<()> {
    let mut snapshot_map: HashMap<_, _> = expected
        .as_ref()
        .iter()
        .filter(|(c, _)| !c.is_spot() || !ignore_spot)
        .map(|(coin, book)| (coin.clone(), book))
        .collect();

    for (coin, book) in snapshot.as_ref() {
        if ignore_spot && coin.is_spot() {
            continue;
        }
        let book1 = book.as_ref();
        if let Some(book2) = snapshot_map.remove(coin) {
            for (side, (orders1, orders2)) in [Side::Bid, Side::Ask].into_iter().zip(book1.iter().zip(book2.as_ref())) {
                // zip alone would miss a trailing order present on only one side.
                for index in 0..orders1.len().max(orders2.len()) {
                    let received = orders1.get(index);
                    let expected = orders2.get(index);
                    if received != expected {
                        let expected_oid_in_received_book = expected.and_then(|order| find_order(book, order.oid()));
                        let received_oid_in_expected_book = received.and_then(|order| find_order(book2, order.oid()));
                        return Err(format!(
                            "Orders do not match, coin: {}, side: {side:?}, index: {index}, \
                             expected_count: {}, received_count: {}, expected: {expected:?}, received: {received:?}, \
                             expected_oid_in_received_book: {expected_oid_in_received_book:?}, \
                             received_oid_in_expected_book: {received_oid_in_expected_book:?}",
                            coin.value(),
                            orders2.len(),
                            orders1.len(),
                        )
                        .into());
                    }
                }
            }
        } else if !book1[0].is_empty() || !book1[1].is_empty() {
            return Err(format!("Missing {} book", coin.value()).into());
        }
    }
    if !snapshot_map.is_empty() {
        return Err("Extra orderbooks detected".to_string().into());
    }
    Ok(())
}

// Only scan by order ID after a mismatch; keep successful validation linear.
fn find_order<O: InnerOrder>(book: &Snapshot<O>, oid: Oid) -> Option<(Side, usize, &O)> {
    for (side, orders) in [Side::Bid, Side::Ask].into_iter().zip(book.as_ref()) {
        if let Some((index, order)) = orders.iter().enumerate().find(|(_, order)| order.oid() == oid) {
            return Some((side, index, order));
        }
    }
    None
}

impl L2SnapshotParams {
    pub(crate) const fn new(n_sig_figs: Option<u32>, mantissa: Option<u64>) -> Self {
        Self { n_sig_figs, mantissa }
    }
}

pub(super) fn compute_l2_snapshots<O: InnerOrder + Send + Sync>(order_books: &OrderBooks<O>) -> L2Snapshots {
    L2Snapshots(
        order_books
            .as_ref()
            .par_iter()
            .map(|(coin, order_book)| {
                let mut entries = Vec::new();
                let snapshot = order_book.to_l2_snapshot(None, None, None);
                entries.push((L2SnapshotParams { n_sig_figs: None, mantissa: None }, snapshot));
                let mut add_new_snapshot = |n_sig_figs: Option<u32>, mantissa: Option<u64>, idx: usize| {
                    if let Some((_, last_snapshot)) = &entries.get(entries.len() - idx) {
                        let snapshot = last_snapshot.to_l2_snapshot(None, n_sig_figs, mantissa);
                        entries.push((L2SnapshotParams { n_sig_figs, mantissa }, snapshot));
                    }
                };
                for n_sig_figs in (2..=5).rev() {
                    if n_sig_figs == 5 {
                        for mantissa in [None, Some(2), Some(5)] {
                            if mantissa == Some(5) {
                                // Some(2) is NOT a superset of this info!
                                add_new_snapshot(Some(n_sig_figs), mantissa, 2);
                            } else {
                                add_new_snapshot(Some(n_sig_figs), mantissa, 1);
                            }
                        }
                    } else {
                        add_new_snapshot(Some(n_sig_figs), None, 1);
                    }
                }
                (coin.clone(), entries.into_iter().collect::<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>())
            })
            .collect(),
    )
}

pub(super) enum EventBatch {
    Orders(Batch<NodeDataOrderStatus>),
    BookDiffs(Batch<NodeDataOrderDiff>),
    Fills(Batch<NodeDataFill>),
}

pub(super) struct BatchQueue<T> {
    deque: VecDeque<Batch<T>>,
    last_ts: Option<u64>,
}

impl<T> BatchQueue<T> {
    pub(super) const fn new() -> Self {
        Self { deque: VecDeque::new(), last_ts: None }
    }

    pub(super) fn push(&mut self, block: Batch<T>) -> bool {
        if let Some(last_ts) = self.last_ts {
            if last_ts >= block.block_number() {
                return false;
            }
        }
        self.last_ts = Some(block.block_number());
        self.deque.push_back(block);
        true
    }

    pub(super) fn pop_front(&mut self) -> Option<Batch<T>> {
        self.deque.pop_front()
    }

    pub(super) fn front(&self) -> Option<&Batch<T>> {
        self.deque.front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        order_book::{Coin, OrderBook, Px, Sz},
        types::inner::InnerL4Order,
    };
    use alloy::primitives::Address;

    fn order(oid: u64, side: Side) -> InnerL4Order {
        InnerL4Order {
            user: Address::ZERO,
            coin: Coin::new("ETH"),
            side,
            limit_px: Px::new(100),
            sz: Sz::new(1),
            oid,
            timestamp: 100,
            trigger_condition: "N/A".into(),
            is_trigger: false,
            trigger_px: "0.0".into(),
            is_position_tpsl: false,
            reduce_only: false,
            order_type: "Limit".into(),
            tif: Some("Gtc".into()),
            cloid: None,
        }
    }

    fn snapshot(orders: Vec<InnerL4Order>) -> Snapshots<InnerL4Order> {
        let mut book = OrderBook::new();
        for order in orders {
            book.add_order(order);
        }
        Snapshots::new(HashMap::from([(Coin::new("ETH"), book.to_snapshot())]))
    }

    #[test]
    fn matching_snapshots_pass() {
        let orders = vec![order(1, Side::Bid), order(2, Side::Bid)];
        assert!(validate_snapshot_consistency(&snapshot(orders.clone()), &snapshot(orders), false).is_ok());
    }

    #[test]
    fn trailing_missing_and_extra_orders_fail_on_both_sides() {
        for side in [Side::Bid, Side::Ask] {
            for prefix_len in [0, 1] {
                let orders = vec![order(1, side), order(2, side)];
                let prefix = orders[..prefix_len].to_vec();
                let missing =
                    validate_snapshot_consistency(&snapshot(prefix.clone()), &snapshot(orders.clone()), false)
                        .unwrap_err()
                        .to_string();
                assert!(missing.contains(&format!("side: {side:?}, index: {prefix_len}")), "{missing}");
                assert!(missing.contains(&format!("expected_count: 2, received_count: {prefix_len}")), "{missing}");
                assert!(missing.contains("received: None"), "{missing}");
                let extra =
                    validate_snapshot_consistency(&snapshot(orders), &snapshot(prefix), false).unwrap_err().to_string();
                assert!(extra.contains(&format!("expected_count: {prefix_len}, received_count: 2")), "{extra}");
                assert!(extra.contains("expected: None"), "{extra}");
            }
        }
    }

    #[test]
    fn missing_order_diagnostic_looks_up_the_other_order_by_id() {
        let expected = snapshot(vec![order(1, Side::Bid), order(2, Side::Bid)]);
        let received = snapshot(vec![order(2, Side::Bid)]);
        let error = validate_snapshot_consistency(&received, &expected, false).unwrap_err().to_string();
        assert!(error.contains("expected_oid_in_received_book: None"), "{error}");
        assert!(error.contains("received_oid_in_expected_book: Some((Bid, 1,"), "{error}");
    }

    #[test]
    fn misplaced_order_diagnostic_shows_its_actual_position() {
        let expected = snapshot(vec![order(1, Side::Bid), order(2, Side::Bid)]);
        let mut misplaced = order(1, Side::Bid);
        misplaced.limit_px = Px::new(99);
        let received = snapshot(vec![misplaced, order(2, Side::Bid)]);
        let error = validate_snapshot_consistency(&received, &expected, false).unwrap_err().to_string();
        assert!(error.contains("expected_oid_in_received_book: Some((Bid, 1,"), "{error}");
        assert!(error.contains("received_oid_in_expected_book: Some((Bid, 1,"), "{error}");
    }

    #[test]
    fn metadata_mismatch_still_fails() {
        let expected = snapshot(vec![order(1, Side::Bid)]);
        let mut changed = order(1, Side::Bid);
        changed.timestamp += 1;
        let error = validate_snapshot_consistency(&snapshot(vec![changed]), &expected, false).unwrap_err().to_string();
        assert!(error.contains("Orders do not match"), "{error}");
        assert!(error.contains("expected_oid_in_received_book: Some((Bid, 0,"), "{error}");
    }
}
