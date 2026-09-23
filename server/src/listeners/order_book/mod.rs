use crate::{
    HL_NODE,
    listeners::{
        directory::{DirectoryListener, seek_to_last_line_boundary},
        order_book::state::OrderBookState,
    },
    order_book::{
        Coin, Snapshot,
        multi_book::{Snapshots, load_snapshots_from_json},
    },
    prelude::*,
    types::{
        L4Order,
        inner::{InnerL4Order, InnerLevel},
        node_data::{Batch, EventSource, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use alloy::primitives::Address;
use fs::File;
use log::{error, info};
use notify::{Event, RecursiveMode, Watcher, recommended_watcher};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    io::Seek,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{
        Mutex,
        broadcast::Sender,
        mpsc::{UnboundedSender, unbounded_channel},
    },
    time::{Instant, interval_at, sleep},
};
use utils::{BatchQueue, EventBatch, process_rmp_file, validate_snapshot_consistency};

mod state;
mod utils;

// WARNING - this code assumes no other file system operations are occurring in the watched directories
// if there are scripts running, this may not work as intended
pub(crate) async fn hl_listen(listener: Arc<Mutex<OrderBookListener>>, dir: PathBuf) -> Result<()> {
    let order_statuses_dir = EventSource::OrderStatuses.event_source_dir(&dir).canonicalize()?;
    let fills_dir = EventSource::Fills.event_source_dir(&dir).canonicalize()?;
    let order_diffs_dir = EventSource::OrderDiffs.event_source_dir(&dir).canonicalize()?;
    info!("Monitoring order status directory: {}", order_statuses_dir.display());
    info!("Monitoring order diffs directory: {}", order_diffs_dir.display());
    info!("Monitoring fills directory: {}", fills_dir.display());

    // monitoring the directory via the notify crate (gives file system events)
    let (fs_event_tx, mut fs_event_rx) = unbounded_channel();
    let mut watcher = recommended_watcher(move |res| {
        let fs_event_tx = fs_event_tx.clone();
        if let Err(err) = fs_event_tx.send(res) {
            error!("Error sending fs event to processor via channel: {err}");
        }
    })?;

    let ignore_spot = {
        let listener = listener.lock().await;
        listener.ignore_spot
    };

    // every so often, we fetch a new snapshot and the snapshot_fetch_task starts running.
    // Result is sent back along this channel (if error, we want to return to top level)
    let (snapshot_fetch_task_tx, mut snapshot_fetch_task_rx) = unbounded_channel::<Result<()>>();

    watcher.watch(&order_statuses_dir, RecursiveMode::Recursive)?;
    watcher.watch(&fills_dir, RecursiveMode::Recursive)?;
    watcher.watch(&order_diffs_dir, RecursiveMode::Recursive)?;
    let start = Instant::now() + Duration::from_secs(5);
    let mut ticker = interval_at(start, Duration::from_secs(10));
    let mut snapshot_fetch_in_progress = false;
    loop {
        tokio::select! {
            event = fs_event_rx.recv() =>  match event {
                Some(Ok(event)) => {
                    if event.kind.is_create() || event.kind.is_modify() {
                        let new_path = &event.paths[0];
                        if new_path.starts_with(&order_statuses_dir) && new_path.is_file() {
                            listener
                                .lock()
                                .await
                                .process_update(&event, new_path, EventSource::OrderStatuses)
                                .map_err(|err| format!("Order status processing error: {err}"))?;
                        } else if new_path.starts_with(&fills_dir) && new_path.is_file() {
                            listener
                                .lock()
                                .await
                                .process_update(&event, new_path, EventSource::Fills)
                                .map_err(|err| format!("Fill update processing error: {err}"))?;
                        } else if new_path.starts_with(&order_diffs_dir) && new_path.is_file() {
                            listener
                                .lock()
                                .await
                                .process_update(&event, new_path, EventSource::OrderDiffs)
                                .map_err(|err| format!("Book diff processing error: {err}"))?;
                        }
                    }
                }
                Some(Err(err)) => {
                    error!("Watcher error: {err}");
                    return Err(format!("Watcher error: {err}").into());
                }
                None => {
                    error!("Channel closed. Listener exiting");
                    return Err("Channel closed.".into());
                }
            },
            snapshot_fetch_res = snapshot_fetch_task_rx.recv() => {
                snapshot_fetch_in_progress = false;
                match snapshot_fetch_res {
                    None => {
                        return Err("Snapshot fetch task sender dropped".into());
                    }
                    Some(Err(err)) => {
                        return Err(format!("Abci state reading error: {err}").into());
                    }
                    Some(Ok(())) => {}
                }
            }
            _ = ticker.tick() => {
                // The snapshot output file and replay cache are shared by all fetches.
                if !snapshot_fetch_in_progress {
                    snapshot_fetch_in_progress = true;
                    fetch_snapshot(dir.clone(), listener.clone(), snapshot_fetch_task_tx.clone(), ignore_spot);
                }
            }
            () = sleep(Duration::from_secs(5)) => {
                let listener = listener.lock().await;
                if listener.is_ready() {
                    return Err(format!("Stream has fallen behind ({HL_NODE} failed?)").into());
                }
            }
        }
    }
}

fn fetch_snapshot(
    dir: PathBuf,
    listener: Arc<Mutex<OrderBookListener>>,
    tx: UnboundedSender<Result<()>>,
    ignore_spot: bool,
) {
    tokio::spawn(async move {
        let res = fetch_and_validate_snapshot(&dir, &listener, ignore_spot).await;
        // Also stop caching on an early fetch/alignment error.
        listener.lock().await.take_cache();
        let _unused = tx.send(res);
    });
}

async fn fetch_and_validate_snapshot(
    dir: &std::path::Path,
    listener: &Arc<Mutex<OrderBookListener>>,
    ignore_spot: bool,
) -> Result<()> {
    // Start before requesting the snapshot so updates applied while the node writes it
    // can be replayed both for validation and for replacing a divergent live book.
    let state = {
        let mut listener = listener.lock().await;
        listener.begin_caching();
        listener.clone_state()
    };
    let output_fln = process_rmp_file(dir).await?;
    let (height, expected_snapshot) = load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(&output_fln).await?;
    info!("Snapshot fetched at height {height}");
    sleep(Duration::from_secs(1)).await;

    if let Some(mut state) = state {
        let mut cache = listener.lock().await.drain_cache();
        while state.height() < height {
            if let Some((order_statuses, order_diffs)) = cache.pop_front() {
                state.apply_updates(order_statuses, order_diffs)?;
            } else {
                error!("Not enough cached updates to validate snapshot {height}; retrying with the next snapshot");
                return Ok(());
            }
        }
        if state.height() > height {
            error!("Snapshot {height} lags stored state {}; retrying with the next snapshot", state.height());
            return Ok(());
        }
        validate_or_replace_snapshot(listener, state, expected_snapshot, cache, ignore_spot).await;
    } else {
        listener.lock().await.init_from_snapshot(expected_snapshot, height);
    }
    Ok(())
}

async fn validate_or_replace_snapshot(
    listener: &Arc<Mutex<OrderBookListener>>,
    state: OrderBookState,
    expected_snapshot: Snapshots<InnerL4Order>,
    mut cache: VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)>,
    ignore_spot: bool,
) {
    let stored_snapshot = state.compute_snapshot();
    let height = stored_snapshot.height;
    info!("Validating snapshot at height {height}");
    if let Err(err) = validate_snapshot_consistency(&stored_snapshot.snapshot, &expected_snapshot, ignore_spot) {
        error!("Snapshot mismatch at height {height}: {err}; discarding the old order book and rebuilding");
        let mut listener = listener.lock().await;
        // Validation runs without the lock; include every update that arrived meanwhile.
        cache.extend(listener.take_cache());
        listener.replace_from_snapshot(expected_snapshot, height, stored_snapshot.time, cache);
    }
}

pub(crate) struct OrderBookListener {
    ignore_spot: bool,
    fill_status_file: Option<File>,
    order_status_file: Option<File>,
    order_diff_file: Option<File>,
    // None if we haven't seen a valid snapshot yet
    order_book_state: Option<OrderBookState>,
    last_fill: Option<u64>,
    order_diff_cache: BatchQueue<NodeDataOrderDiff>,
    order_status_cache: BatchQueue<NodeDataOrderStatus>,
    // Only Some when we want it to collect updates
    fetched_snapshot_cache: Option<VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)>>,
    internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
}

impl OrderBookListener {
    pub(crate) const fn new(internal_message_tx: Option<Sender<Arc<InternalMessage>>>, ignore_spot: bool) -> Self {
        Self {
            ignore_spot,
            fill_status_file: None,
            order_status_file: None,
            order_diff_file: None,
            order_book_state: None,
            last_fill: None,
            fetched_snapshot_cache: None,
            internal_message_tx,
            order_diff_cache: BatchQueue::new(),
            order_status_cache: BatchQueue::new(),
        }
    }

    fn clone_state(&self) -> Option<OrderBookState> {
        self.order_book_state.clone()
    }

    pub(crate) const fn is_ready(&self) -> bool {
        self.order_book_state.is_some()
    }

    pub(crate) fn universe(&self) -> HashSet<Coin> {
        self.order_book_state.as_ref().map_or_else(HashSet::new, OrderBookState::compute_universe)
    }

    #[allow(clippy::type_complexity)]
    // pops earliest pair of cached updates that have the same timestamp if possible
    fn pop_cache(&mut self) -> Option<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        // synchronize to same block
        while let Some(t) = self.order_diff_cache.front() {
            if let Some(s) = self.order_status_cache.front() {
                match t.block_number().cmp(&s.block_number()) {
                    Ordering::Less => {
                        self.order_diff_cache.pop_front();
                    }
                    Ordering::Equal => {
                        return self
                            .order_status_cache
                            .pop_front()
                            .and_then(|t| self.order_diff_cache.pop_front().map(|s| (t, s)));
                    }
                    Ordering::Greater => {
                        self.order_status_cache.pop_front();
                    }
                }
            } else {
                break;
            }
        }
        None
    }

    fn receive_batch(&mut self, updates: EventBatch) -> Result<()> {
        match updates {
            EventBatch::Orders(batch) => {
                self.order_status_cache.push(batch);
            }
            EventBatch::BookDiffs(batch) => {
                self.order_diff_cache.push(batch);
            }
            EventBatch::Fills(batch) => {
                if self.last_fill.is_none_or(|height| height < batch.block_number()) {
                    // send fill updates if we received a new update
                    if let Some(tx) = &self.internal_message_tx {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            let snapshot = Arc::new(InternalMessage::Fills { batch });
                            let _unused = tx.send(snapshot);
                        });
                    }
                }
            }
        }
        if self.is_ready() {
            if let Some((order_statuses, order_diffs)) = self.pop_cache() {
                self.order_book_state
                    .as_mut()
                    .map(|book| book.apply_updates(order_statuses.clone(), order_diffs.clone()))
                    .transpose()?;
                if let Some(cache) = &mut self.fetched_snapshot_cache {
                    cache.push_back((order_statuses.clone(), order_diffs.clone()));
                }
                if let Some(tx) = &self.internal_message_tx {
                    let updates = Arc::new(InternalMessage::L4BookUpdates {
                        diff_batch: order_diffs,
                        status_batch: order_statuses,
                    });
                    let _unused = tx.send(updates);
                }
            }
        }
        Ok(())
    }

    fn begin_caching(&mut self) {
        self.fetched_snapshot_cache = Some(VecDeque::new());
    }

    // Drain a prefix while continuing to collect updates during validation.
    fn drain_cache(&mut self) -> VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        self.fetched_snapshot_cache.as_mut().map(std::mem::take).unwrap_or_default()
    }

    // Take the cached updates and stop collecting updates.
    fn take_cache(&mut self) -> VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        self.fetched_snapshot_cache.take().unwrap_or_default()
    }

    fn init_from_snapshot(&mut self, snapshot: Snapshots<InnerL4Order>, height: u64) {
        info!("No existing snapshot");
        self.replace_from_snapshot(snapshot, height, 0, VecDeque::new());
    }

    fn replace_from_snapshot(
        &mut self,
        snapshot: Snapshots<InnerL4Order>,
        height: u64,
        time: u64,
        mut cache: VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)>,
    ) {
        let previous_height = self.order_book_state.take().map(|state| state.height());
        let mut new_order_book = OrderBookState::from_snapshot(snapshot, height, time, true, self.ignore_spot);
        // Include paired updates not yet applied to the live book; leave unpaired rows
        // queued until their counterpart arrives.
        while let Some((order_statuses, order_diffs)) = self.pop_cache() {
            cache.push_back((order_statuses, order_diffs));
        }
        for (order_statuses, order_diffs) in cache {
            if let Err(err) = new_order_book.apply_updates(order_statuses, order_diffs) {
                error!(
                    "Failed to replay updates after snapshot {height}: {err}; old order book cleared, waiting for next snapshot"
                );
                return;
            }
        }
        if previous_height.is_some_and(|previous| new_order_book.height() < previous) {
            error!(
                "Snapshot {height} cannot catch up to {previous_height:?}; old order book cleared, waiting for next snapshot"
            );
            return;
        }
        self.order_book_state = Some(new_order_book);
        info!("Order book ready after snapshot {height}");
        self.send_book_reset();
    }

    fn send_book_reset(&mut self) {
        if let Some(tx) = &self.internal_message_tx {
            if let Some(book) = &mut self.order_book_state {
                let snapshot = book.compute_snapshot();
                if let Some((_, _, l2_snapshots)) = book.l2_snapshots(true) {
                    // Broadcast synchronously under the listener lock so older updates
                    // cannot overtake this reset and newer updates always follow it.
                    let _unused = tx.send(Arc::new(InternalMessage::BookReset { snapshot, l2_snapshots }));
                }
            }
        }
    }

    // forcibly grab current snapshot
    pub(crate) fn compute_snapshot(&mut self) -> Option<TimedSnapshots> {
        self.order_book_state.as_mut().map(|o| o.compute_snapshot())
    }

    // prevent snapshotting mutiple times at the same height
    fn l2_snapshots(&mut self, prevent_future_snaps: bool) -> Option<(u64, u64, L2Snapshots)> {
        self.order_book_state.as_mut().and_then(|o| o.l2_snapshots(prevent_future_snaps))
    }
}

impl OrderBookListener {
    fn process_update(&mut self, event: &Event, new_path: &PathBuf, event_source: EventSource) -> Result<()> {
        if event.kind.is_create() {
            info!("-- Event: {} created --", new_path.display());
            self.on_file_creation(new_path.clone(), event_source)?;
        }
        // Check for `Modify` event (only if the file is already initialized)
        else {
            // If we are not tracking anything right now, we treat a file update as declaring that it has been created.
            // Skip complete history, but keep any trailing line that is still being written.
            if self.is_reading(event_source) {
                self.on_file_modification(event_source)?;
            } else {
                info!("-- Event: {} modified, tracking it now --", new_path.display());
                let file = self.file_mut(event_source);
                let mut new_file = File::open(new_path)?;
                seek_to_last_line_boundary(&mut new_file)?;
                *file = Some(new_file);
            }
        }
        Ok(())
    }
}

impl DirectoryListener for OrderBookListener {
    fn is_reading(&self, event_source: EventSource) -> bool {
        match event_source {
            EventSource::Fills => self.fill_status_file.is_some(),
            EventSource::OrderStatuses => self.order_status_file.is_some(),
            EventSource::OrderDiffs => self.order_diff_file.is_some(),
        }
    }

    fn file_mut(&mut self, event_source: EventSource) -> &mut Option<File> {
        match event_source {
            EventSource::Fills => &mut self.fill_status_file,
            EventSource::OrderStatuses => &mut self.order_status_file,
            EventSource::OrderDiffs => &mut self.order_diff_file,
        }
    }

    fn on_file_creation(&mut self, new_file: PathBuf, event_source: EventSource) -> Result<()> {
        if self.is_reading(event_source) {
            self.on_file_modification(event_source)?;
        }
        if let Some(file) = self.file_mut(event_source).as_mut() {
            if file.stream_position()? < file.metadata()?.len() {
                return Err(format!("Incomplete {event_source} line when rotating to {}", new_file.display()).into());
            }
        }
        *self.file_mut(event_source) = Some(File::open(new_file)?);
        self.on_file_modification(event_source)
    }

    fn process_data(&mut self, data: String, event_source: EventSource) -> Result<()> {
        let lines = data.lines();
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let res = match event_source {
                EventSource::Fills => serde_json::from_str::<Batch<NodeDataFill>>(line).map(|batch| {
                    let height = batch.block_number();
                    (height, EventBatch::Fills(batch))
                }),
                EventSource::OrderStatuses => serde_json::from_str(line)
                    .map(|batch: Batch<NodeDataOrderStatus>| (batch.block_number(), EventBatch::Orders(batch))),
                EventSource::OrderDiffs => serde_json::from_str(line)
                    .map(|batch: Batch<NodeDataOrderDiff>| (batch.block_number(), EventBatch::BookDiffs(batch))),
            };
            // Only newline-terminated records reach this parser. An invalid complete record is
            // corruption, not a partial write; stop instead of repeatedly replaying earlier rows.
            let (height, event_batch) = res.map_err(|err| {
                format!(
                    "{event_source} deserialization error {err}, height: {:?}, line: {:?}",
                    self.order_book_state.as_ref().map(OrderBookState::height),
                    line.chars().take(100).collect::<String>(),
                )
            })?;
            if height % 100 == 0 {
                info!("{event_source} block: {height}");
            }
            if let Err(err) = self.receive_batch(event_batch) {
                self.order_book_state = None;
                return Err(err);
            }
        }
        let snapshot = self.l2_snapshots(true);
        if let Some((time, height, l2_snapshots)) = snapshot {
            if let Some(tx) = &self.internal_message_tx {
                let snapshot = Arc::new(InternalMessage::Snapshot { l2_snapshots, time, height });
                let _unused = tx.send(snapshot);
            }
        }
        Ok(())
    }
}

pub(crate) struct L2Snapshots(HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>);

impl L2Snapshots {
    pub(crate) const fn as_ref(&self) -> &HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>> {
        &self.0
    }
}

pub(crate) struct TimedSnapshots {
    pub(crate) time: u64,
    pub(crate) height: u64,
    pub(crate) snapshot: Snapshots<InnerL4Order>,
}

// Messages sent from node data listener to websocket dispatch to support streaming
pub(crate) enum InternalMessage {
    Snapshot { l2_snapshots: L2Snapshots, time: u64, height: u64 },
    BookReset { snapshot: TimedSnapshots, l2_snapshots: L2Snapshots },
    Fills { batch: Batch<NodeDataFill> },
    L4BookUpdates { diff_batch: Batch<NodeDataOrderDiff>, status_batch: Batch<NodeDataOrderStatus> },
}

#[derive(Eq, PartialEq, Hash)]
pub(crate) struct L2SnapshotParams {
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order_book::multi_book::load_snapshots_from_str;
    use serde::de::DeserializeOwned;
    use serde_json::{Value, json};
    use std::{fs::OpenOptions, io::Write};

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("order-book-lines-{}", rand::random::<u64>()));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _unused = fs::remove_dir_all(&self.0);
        }
    }

    fn empty_batch(height: u64) -> String {
        format!(
            "{{\"local_time\":\"2026-09-09T01:26:47\",\"block_time\":\"2026-09-09T01:26:46\",\"block_number\":{height},\"events\":[]}}\n"
        )
    }

    #[test]
    fn append_and_rotation_preserve_order_batches() {
        let directory = TestDirectory::new();
        let mut listener = OrderBookListener::new(None, true);
        let first = empty_batch(100);
        let second = empty_batch(101);
        let third = empty_batch(102);

        for source in [EventSource::OrderStatuses, EventSource::OrderDiffs] {
            let path = directory.0.join(format!("{source}-1"));
            fs::write(&path, format!("{first}{}", &second[..30])).unwrap();
            listener.on_file_creation(path.clone(), source).unwrap();
            assert_eq!(listener.file_mut(source).as_mut().unwrap().stream_position().unwrap(), first.len() as u64);

            // Repeated notifications before the writer finishes must not consume the partial row.
            listener.on_file_modification(source).unwrap();
            let mut writer = OpenOptions::new().append(true).open(&path).unwrap();
            writer.write_all(second[30..].as_bytes()).unwrap();
            listener.on_file_modification(source).unwrap();

            let next_path = directory.0.join(format!("{source}-2"));
            fs::write(&next_path, &third).unwrap();
            listener.on_file_creation(next_path, source).unwrap();
        }

        for expected_height in 100..=102 {
            let (statuses, diffs) = listener.pop_cache().unwrap();
            assert_eq!(statuses.block_number(), expected_height);
            assert_eq!(diffs.block_number(), expected_height);
        }
        assert!(listener.pop_cache().is_none());
    }

    #[test]
    fn rotation_rejects_unfinished_record() {
        let directory = TestDirectory::new();
        let mut listener = OrderBookListener::new(None, true);
        let path = directory.0.join("old");
        fs::write(&path, "{\"block_number\":").unwrap();
        listener.on_file_creation(path, EventSource::OrderStatuses).unwrap();

        let next_path = directory.0.join("new");
        fs::write(&next_path, empty_batch(102)).unwrap();
        let err = listener.on_file_creation(next_path, EventSource::OrderStatuses).unwrap_err();
        assert!(err.to_string().contains("Incomplete OrderStatuses line"));
    }

    #[test]
    fn malformed_complete_records_return_errors_without_panicking() {
        let mut listener = OrderBookListener::new(None, true);
        for data in ["{\n".to_string(), format!("{}中\n", "x".repeat(99))] {
            let err = listener.process_data(data, EventSource::OrderStatuses).unwrap_err();
            assert!(err.to_string().contains("deserialization error"));
        }
    }

    fn order_json(oid: u64) -> Value {
        json!({
            "coin": "ETH", "side": "B", "limitPx": "2153.6", "sz": "0.1139",
            "oid": oid, "timestamp": 1788919049596_u64, "triggerCondition": "N/A",
            "isTrigger": false, "triggerPx": "0.0", "isPositionTpsl": false,
            "reduceOnly": false, "orderType": "Limit", "tif": "Gtc", "cloid": null
        })
    }

    fn book_snapshot(oids: &[u64]) -> Snapshots<InnerL4Order> {
        let orders: Vec<_> = oids.iter().map(|&oid| json!([Address::ZERO, order_json(oid)])).collect();
        let text = json!([100, [["ETH", [orders, []]]]]).to_string();
        load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(&text).unwrap().1
    }

    fn batch<E: DeserializeOwned>(height: u64, events: Vec<Value>) -> Batch<E> {
        serde_json::from_value(json!({
            "local_time": "2026-09-09T01:57:30", "block_time": "2026-09-09T01:57:29",
            "block_number": height, "events": events
        }))
        .unwrap()
    }

    fn new_order_batches(height: u64, oid: u64) -> (Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>) {
        let statuses = batch(
            height,
            vec![json!({
                "time": "2026-09-09T01:57:29", "user": Address::ZERO, "status": "open", "order": order_json(oid)
            })],
        );
        let diffs = batch(
            height,
            vec![json!({
                "user": Address::ZERO, "oid": oid, "coin": "ETH", "px": "2153.6",
                "raw_book_diff": {"new": {"sz": "0.1139"}}
            })],
        );
        (statuses, diffs)
    }

    fn receive_new_order(listener: &mut OrderBookListener, height: u64, oid: u64) {
        let (statuses, diffs) = new_order_batches(height, oid);
        listener.receive_batch(EventBatch::Orders(statuses)).unwrap();
        listener.receive_batch(EventBatch::BookDiffs(diffs)).unwrap();
    }

    fn assert_orders(snapshot: &TimedSnapshots, height: u64, oids: &[u64]) {
        assert_eq!(snapshot.height, height);
        let orders = snapshot.snapshot.as_ref()[&Coin::new("ETH")].as_ref();
        assert_eq!(orders[0].iter().map(|order| order.oid).collect::<Vec<_>>(), oids);
        assert!(orders[1].is_empty());
    }

    #[tokio::test]
    async fn mismatch_replaces_old_orders_and_replays_updates_across_validation() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(16);
        let mut listener = OrderBookListener::new(Some(tx), true);
        listener.order_book_state = Some(OrderBookState::from_snapshot(book_snapshot(&[1, 2]), 100, 1, true, true));
        listener.begin_caching();
        let state_at_snapshot = listener.clone_state().unwrap();
        receive_new_order(&mut listener, 101, 3);
        let cache = listener.drain_cache();
        // This update arrives after the validation worker has drained its initial cache.
        receive_new_order(&mut listener, 102, 4);
        // Preserve an unpaired row until its diff arrives after the reset.
        let (statuses, diffs) = new_order_batches(103, 5);
        listener.receive_batch(EventBatch::Orders(statuses)).unwrap();
        let listener = Arc::new(Mutex::new(listener));

        validate_or_replace_snapshot(&listener, state_at_snapshot, book_snapshot(&[2]), cache, true).await;

        let mut listener = listener.lock().await;
        assert_orders(&listener.compute_snapshot().unwrap(), 102, &[2, 3, 4]);
        listener.receive_batch(EventBatch::BookDiffs(diffs)).unwrap();
        assert_orders(&listener.compute_snapshot().unwrap(), 103, &[2, 3, 4, 5]);
        for expected_height in [101, 102] {
            let msg = rx.try_recv().unwrap();
            assert!(matches!(msg.as_ref(), InternalMessage::L4BookUpdates { diff_batch, .. }
                if diff_batch.block_number() == expected_height));
        }
        let reset = rx.try_recv().unwrap();
        let InternalMessage::BookReset { snapshot, l2_snapshots } = reset.as_ref() else {
            panic!("Expected a full snapshot between the old and new updates");
        };
        assert_orders(snapshot, 102, &[2, 3, 4]);
        assert!(l2_snapshots.as_ref().contains_key(&Coin::new("ETH")));
        let msg = rx.try_recv().unwrap();
        assert!(matches!(msg.as_ref(), InternalMessage::L4BookUpdates { diff_batch, .. }
            if diff_batch.block_number() == 103));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn matching_snapshot_leaves_newer_live_orders_intact() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(16);
        let mut listener = OrderBookListener::new(Some(tx), true);
        listener.order_book_state = Some(OrderBookState::from_snapshot(book_snapshot(&[1]), 100, 1, true, true));
        listener.begin_caching();
        let state_at_snapshot = listener.clone_state().unwrap();
        receive_new_order(&mut listener, 101, 2);
        let cache = listener.drain_cache();
        let listener = Arc::new(Mutex::new(listener));

        validate_or_replace_snapshot(&listener, state_at_snapshot, book_snapshot(&[1]), cache, true).await;

        assert_orders(&listener.lock().await.compute_snapshot().unwrap(), 101, &[1, 2]);
        assert!(matches!(rx.try_recv().unwrap().as_ref(), InternalMessage::L4BookUpdates { .. }));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn failed_replay_discards_old_book_and_recovers_on_a_later_snapshot() {
        let mut listener = OrderBookListener::new(None, true);
        listener.order_book_state = Some(OrderBookState::from_snapshot(book_snapshot(&[1]), 102, 1, true, true));
        let cache = VecDeque::from([new_order_batches(102, 3)]); // Block 101 is missing.
        listener.replace_from_snapshot(book_snapshot(&[2]), 100, 1, cache);
        assert!(!listener.is_ready());
        assert!(listener.compute_snapshot().is_none());

        receive_new_order(&mut listener, 103, 4);
        listener.init_from_snapshot(book_snapshot(&[2, 3]), 102);
        assert_orders(&listener.compute_snapshot().unwrap(), 103, &[2, 3, 4]);
    }

    #[test]
    fn replacement_cannot_silently_roll_back_the_live_height() {
        let mut listener = OrderBookListener::new(None, true);
        listener.order_book_state = Some(OrderBookState::from_snapshot(book_snapshot(&[1]), 102, 1, true, true));
        listener.replace_from_snapshot(book_snapshot(&[2]), 100, 1, VecDeque::new());
        assert!(!listener.is_ready());
    }
}
