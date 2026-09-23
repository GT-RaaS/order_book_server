use crate::{
    listeners::order_book::{
        InternalMessage, L2SnapshotParams, L2Snapshots, OrderBookListener, TimedSnapshots, hl_listen,
    },
    order_book::{Coin, Snapshot},
    prelude::*,
    types::{
        L2Book, L4Book, L4BookUpdates, L4Order, Trade,
        inner::InnerLevel,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
        subscription::{ClientMessage, DEFAULT_LEVELS, ServerResponse, Subscription, SubscriptionManager},
    },
};
use axum::{Router, response::IntoResponse, routing::get};
use futures_util::{SinkExt, StreamExt};
use log::{error, info};
use std::{
    collections::{HashMap, HashSet},
    env::home_dir,
    sync::Arc,
};
use tokio::select;
use tokio::{
    net::TcpListener,
    sync::{
        Mutex,
        broadcast::{Sender, channel},
    },
};
use yawc::{FrameView, OpCode, WebSocket};

pub async fn run_websocket_server(address: &str, ignore_spot: bool, compression_level: u32) -> Result<()> {
    let (internal_message_tx, _) = channel::<Arc<InternalMessage>>(100);

    // Central task: listen to messages and forward them for distribution
    let home_dir = home_dir().ok_or("Could not find home directory")?;
    let listener = {
        let internal_message_tx = internal_message_tx.clone();
        OrderBookListener::new(Some(internal_message_tx), ignore_spot)
    };
    let listener = Arc::new(Mutex::new(listener));
    {
        let listener = listener.clone();
        tokio::spawn(async move {
            if let Err(err) = hl_listen(listener, home_dir).await {
                error!("Listener fatal error: {err}");
                std::process::exit(1);
            }
        });
    }

    let websocket_opts =
        yawc::Options::default().with_compression_level(yawc::CompressionLevel::new(compression_level));
    let app = Router::new().route(
        "/ws",
        get({
            let internal_message_tx = internal_message_tx.clone();
            async move |ws_upgrade| {
                ws_handler(ws_upgrade, internal_message_tx.clone(), listener.clone(), ignore_spot, websocket_opts)
            }
        }),
    );

    let listener = TcpListener::bind(address).await?;
    info!("WebSocket server running at ws://{address}");

    if let Err(err) = axum::serve(listener, app.into_make_service()).await {
        error!("Server fatal error: {err}");
        std::process::exit(2);
    }

    Ok(())
}

fn ws_handler(
    incoming: yawc::IncomingUpgrade,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    ignore_spot: bool,
    websocket_opts: yawc::Options,
) -> impl IntoResponse {
    let (resp, fut) = incoming.upgrade(websocket_opts).unwrap();
    tokio::spawn(async move {
        let ws = match fut.await {
            Ok(ok) => ok,
            Err(err) => {
                log::error!("failed to upgrade websocket connection: {err}");
                return;
            }
        };

        handle_socket(ws, internal_message_tx, listener, ignore_spot).await
    });

    resp
}

async fn handle_socket(
    mut socket: WebSocket,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    ignore_spot: bool,
) {
    let mut internal_message_rx = internal_message_tx.subscribe();
    let is_ready = listener.lock().await.is_ready();
    let mut manager = SubscriptionManager::default();
    let mut universe = listener.lock().await.universe().into_iter().map(|c| c.value()).collect();
    if !is_ready {
        let msg = ServerResponse::Error("Order book not ready for streaming (waiting for snapshot)".to_string());
        send_socket_message(&mut socket, msg).await;
        return;
    }
    loop {
        select! {
            recv_result = internal_message_rx.recv() => {
                match recv_result {
                    Ok(msg) => {
                        match msg.as_ref() {
                            InternalMessage::Snapshot{ l2_snapshots, time, height } => {
                                universe = new_universe(l2_snapshots, ignore_spot);
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_snapshot(&mut socket, sub, l2_snapshots.as_ref(), *time, *height).await;
                                }
                            },
                            InternalMessage::BookReset { snapshot, l2_snapshots } => {
                                universe = new_universe(l2_snapshots, ignore_spot);
                                for sub in manager.subscriptions() {
                                    match sub {
                                        Subscription::L4Book { coin } => {
                                            let msg = l4_snapshot_response(coin, snapshot);
                                            send_socket_message(&mut socket, msg).await;
                                        }
                                        Subscription::L2Book { coin, .. } if !universe.contains(coin) => {
                                            let book = L2Book::from_l2_snapshot(coin.clone(), [vec![], vec![]], snapshot.time, snapshot.height);
                                            send_socket_message(&mut socket, ServerResponse::L2Book(book)).await;
                                        }
                                        _ => {
                                            send_ws_data_from_snapshot(&mut socket, sub, l2_snapshots.as_ref(), snapshot.time, snapshot.height).await;
                                        }
                                    }
                                }
                            },
                            InternalMessage::Fills{ batch } => {
                                let coins = manager.subscriptions().iter().filter_map(|sub| match sub {
                                    Subscription::Trades { coin } => Some(coin.as_str()),
                                    _ => None,
                                }).collect::<HashSet<_>>();
                                if !coins.is_empty() {
                                    let mut trades = coin_to_trades(batch, &coins);
                                    for sub in manager.subscriptions() {
                                        send_ws_data_from_trades(&mut socket, sub, &mut trades, batch.block_number()).await;
                                    }
                                }
                            },
                            InternalMessage::L4BookUpdates{ diff_batch, status_batch } => {
                                let mut book_updates = coin_to_book_updates(diff_batch, status_batch);
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_book_updates(&mut socket, sub, &mut book_updates).await;
                                }
                            },
                        }

                    }
                    Err(err) => {
                        error!("Receiver error: {err}");
                        return;
                    }
                }
            }

            msg = socket.next() => {
                if let Some(frame) = msg {
                    match frame.opcode {
                        OpCode::Text => {
                            let text = match std::str::from_utf8(&frame.payload) {
                                Ok(text) => text,
                                Err(err) => {
                                    log::warn!("unable to parse websocket content: {err}: {:?}", frame.payload.as_ref());
                                    // deserves to close the connection because the payload is not a valid utf8 string.
                                    return;
                                }
                            };

                            info!("Client message: {text}");

                            if let Ok(value) = serde_json::from_str::<ClientMessage>(text) {
                                receive_client_message(&mut socket, &mut manager, value, &universe, listener.clone()).await;
                            }
                            else {
                                let msg = ServerResponse::Error(format!("Error parsing JSON into valid websocket request: {text}"));
                                send_socket_message(&mut socket, msg).await;
                            }
                        }
                        OpCode::Close => {
                            info!("Client disconnected");
                            return;
                        }
                        _ => {}
                    }
                } else {
                    info!("Client connection closed");
                    return;
                }
            }
        }
    }
}

async fn receive_client_message(
    socket: &mut WebSocket,
    manager: &mut SubscriptionManager,
    client_message: ClientMessage,
    universe: &HashSet<String>,
    listener: Arc<Mutex<OrderBookListener>>,
) {
    let subscription = match &client_message {
        ClientMessage::Unsubscribe { subscription } | ClientMessage::Subscribe { subscription } => subscription.clone(),
    };
    // this is used for display purposes only, hence unwrap_or_default. It also shouldn't fail
    let sub = serde_json::to_string(&subscription).unwrap_or_default();
    if !subscription.validate(universe) {
        let msg = ServerResponse::Error(format!("Invalid subscription: {sub}"));
        send_socket_message(socket, msg).await;
        return;
    }
    let (word, success) = match &client_message {
        ClientMessage::Subscribe { .. } => ("", manager.subscribe(subscription)),
        ClientMessage::Unsubscribe { .. } => ("un", manager.unsubscribe(subscription)),
    };
    if success {
        let snapshot_msg = if let ClientMessage::Subscribe { subscription } = &client_message {
            let msg = subscription.handle_immediate_snapshot(listener).await;
            match msg {
                Ok(msg) => msg,
                Err(err) => {
                    manager.unsubscribe(subscription.clone());
                    let msg = ServerResponse::Error(format!("Unable to grab order book snapshot: {err}"));
                    send_socket_message(socket, msg).await;
                    return;
                }
            }
        } else {
            None
        };
        let msg = ServerResponse::SubscriptionResponse(client_message);
        send_socket_message(socket, msg).await;
        if let Some(snapshot_msg) = snapshot_msg {
            send_socket_message(socket, snapshot_msg).await;
        }
    } else {
        let msg = ServerResponse::Error(format!("Already {word}subscribed: {sub}"));
        send_socket_message(socket, msg).await;
    }
}

async fn send_socket_message(socket: &mut WebSocket, msg: ServerResponse) {
    let msg = serde_json::to_string(&msg);
    match msg {
        Ok(msg) => {
            if let Err(err) = socket.send(FrameView::text(msg)).await {
                error!("Failed to send: {err}");
            }
        }
        Err(err) => {
            error!("Server response serialization error: {err}");
        }
    }
}

fn l4_snapshot_response(coin: &str, snapshot: &TimedSnapshots) -> ServerResponse {
    let levels = snapshot.snapshot.as_ref().get(&Coin::new(coin)).map_or_else(
        || [vec![], vec![]],
        |book| book.as_ref().clone().map(|orders| orders.into_iter().map(L4Order::from).collect()),
    );
    ServerResponse::L4Book(L4Book::Snapshot {
        coin: coin.to_string(),
        time: snapshot.time,
        height: snapshot.height,
        levels,
    })
}

// derive it from l2_snapshots because thats convenient
fn new_universe(l2_snapshots: &L2Snapshots, ignore_spot: bool) -> HashSet<String> {
    l2_snapshots
        .as_ref()
        .iter()
        .filter_map(|(c, _)| if !c.is_spot() || !ignore_spot { Some(c.clone().value()) } else { None })
        .collect()
}

async fn send_ws_data_from_snapshot(
    socket: &mut WebSocket,
    subscription: &Subscription,
    snapshot: &HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>,
    time: u64,
    height: u64,
) {
    if let Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } = subscription {
        let snapshot = snapshot.get(&Coin::new(coin));
        if let Some(snapshot) =
            snapshot.and_then(|snapshot| snapshot.get(&L2SnapshotParams::new(*n_sig_figs, *mantissa)))
        {
            let n_levels = n_levels.unwrap_or(DEFAULT_LEVELS);
            let snapshot = snapshot.truncate(n_levels);
            let snapshot = snapshot.export_inner_snapshot();
            let l2_book = L2Book::from_l2_snapshot(coin.clone(), snapshot, time, height);
            let msg = ServerResponse::L2Book(l2_book);
            send_socket_message(socket, msg).await;
        } else {
            error!("Coin {coin} not found");
        }
    }
}

fn coin_to_trades(batch: &Batch<NodeDataFill>, coins: &HashSet<&str>) -> HashMap<String, Vec<Trade>> {
    let mut group_indices = HashMap::new();
    let mut groups: Vec<Vec<NodeDataFill>> = Vec::new();
    // The two sides of a trade need not be adjacent. Preserve first-seen trade order.
    for fill in batch.clone().events() {
        if fill.1.coin.starts_with('#') || !coins.contains(fill.1.coin.as_str()) {
            continue;
        }
        let key = (fill.1.coin.clone(), fill.1.tid);
        let index = *group_indices.entry(key).or_insert_with(|| {
            groups.push(Vec::new());
            groups.len() - 1
        });
        groups[index].push(fill);
    }
    let mut trades = HashMap::new();
    for fills in groups {
        if let Some(trade) = Trade::from_fills(&fills) {
            trades.entry(trade.coin.clone()).or_insert_with(Vec::new).push(trade);
        } else {
            error!("Unable to pair trade fills at height {}: {fills:?}", batch.block_number());
        }
    }
    trades
}

fn coin_to_book_updates(
    diff_batch: &Batch<NodeDataOrderDiff>,
    status_batch: &Batch<NodeDataOrderStatus>,
) -> HashMap<String, L4BookUpdates> {
    let diffs = diff_batch.clone().events();
    let statuses = status_batch.clone().events();
    let time = diff_batch.block_time();
    let height = diff_batch.block_number();
    let mut updates = HashMap::new();
    for diff in diffs {
        let coin = diff.coin().value();
        updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).book_diffs.push(diff);
    }
    for status in statuses {
        let coin = status.order.coin.clone();
        updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).order_statuses.push(status);
    }
    updates
}

async fn send_ws_data_from_book_updates(
    socket: &mut WebSocket,
    subscription: &Subscription,
    book_updates: &mut HashMap<String, L4BookUpdates>,
) {
    if let Subscription::L4Book { coin } = subscription {
        if let Some(updates) = book_updates.remove(coin) {
            let msg = ServerResponse::L4Book(L4Book::Updates(updates));
            send_socket_message(socket, msg).await;
        }
    }
}

async fn send_ws_data_from_trades(
    socket: &mut WebSocket,
    subscription: &Subscription,
    trades: &mut HashMap<String, Vec<Trade>>,
    block_number: u64,
) {
    if let Subscription::Trades { coin } = subscription {
        if let Some(trades) = trades.remove(coin) {
            let msg = ServerResponse::Trades { block_number, fills: trades };
            send_socket_message(socket, msg).await;
        }
    }
}

impl Subscription {
    // snapshots that begin a stream
    async fn handle_immediate_snapshot(
        &self,
        listener: Arc<Mutex<OrderBookListener>>,
    ) -> Result<Option<ServerResponse>> {
        if let Self::L4Book { coin } = self {
            let snapshot = listener.lock().await.compute_snapshot();
            if let Some(TimedSnapshots { time, height, snapshot }) = snapshot {
                let snapshot =
                    snapshot.value().into_iter().filter(|(c, _)| *c == Coin::new(coin)).collect::<Vec<_>>().pop();
                if let Some((coin, snapshot)) = snapshot {
                    let snapshot =
                        snapshot.as_ref().clone().map(|orders| orders.into_iter().map(L4Order::from).collect());
                    return Ok(Some(ServerResponse::L4Book(L4Book::Snapshot {
                        coin: coin.value(),
                        time,
                        height,
                        levels: snapshot,
                    })));
                }
            }
            return Err("Snapshot Failed".into());
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order_book::multi_book::{Snapshots, load_snapshots_from_str};
    use alloy::primitives::Address;
    use serde_json::json;

    fn fill(coin: &str, tid: u64, side: &str, crossed: bool, user: Address) -> NodeDataFill {
        serde_json::from_value(json!([user, {
            "coin": coin, "tid": tid, "side": side, "crossed": crossed,
            "px": "2000.0", "sz": "1.0", "time": 1788919049596_u64,
            "startPosition": "0.0", "dir": "Open Long", "closedPnl": "0.0",
            "hash": "0x123", "oid": tid, "fee": "0.0", "feeToken": "USDC"
        }]))
        .unwrap()
    }

    fn fill_batch(fills: Vec<NodeDataFill>) -> Batch<NodeDataFill> {
        serde_json::from_value(json!({
            "local_time": "2026-09-09T01:57:30", "block_time": "2026-09-09T01:57:29",
            "block_number": 100, "events": fills
        }))
        .unwrap()
    }

    #[test]
    fn trades_pair_interleaved_fills_and_preserve_first_seen_order() {
        let buyer = Address::repeat_byte(1);
        let seller = Address::repeat_byte(2);
        let batch = fill_batch(vec![
            fill("ETH", 1, "B", true, buyer),
            fill("ETH", 2, "B", false, buyer),
            fill("ETH", 2, "A", true, seller),
            fill("ETH", 1, "A", false, seller),
        ]);
        let trades = coin_to_trades(&batch, &HashSet::from(["ETH"]));
        let trades = serde_json::to_value(&trades["ETH"]).unwrap();
        assert_eq!(
            trades,
            json!([
                {"coin": "ETH", "tid": 1, "side": "B", "px": "2000.0", "sz": "1.0",
                 "time": 1788919049596_u64, "hash": "0x123", "users": [buyer, seller]},
                {"coin": "ETH", "tid": 2, "side": "A", "px": "2000.0", "sz": "1.0",
                 "time": 1788919049596_u64, "hash": "0x123", "users": [buyer, seller]}
            ])
        );
    }

    #[test]
    fn trades_with_the_same_id_on_different_coins_are_separate() {
        let buyer = Address::repeat_byte(1);
        let seller = Address::repeat_byte(2);
        let batch = fill_batch(vec![
            fill("ETH", 1, "A", false, seller),
            fill("BTC", 1, "B", false, buyer),
            fill("ETH", 1, "B", true, buyer),
            fill("BTC", 1, "A", true, seller),
        ]);
        let trades = coin_to_trades(&batch, &HashSet::from(["ETH", "BTC"]));
        assert_eq!(trades.len(), 2);
        for coin in ["ETH", "BTC"] {
            assert_eq!(trades[coin].len(), 1);
            let trade = serde_json::to_value(&trades[coin][0]).unwrap();
            assert_eq!(trade["coin"], coin);
            assert_eq!(trade["users"], json!([buyer, seller]));
        }
    }

    #[test]
    fn invalid_fill_groups_do_not_drop_valid_trades_or_panic() {
        let user = Address::ZERO;
        let batch = fill_batch(vec![
            fill("ETH", 1, "A", true, user),
            fill("ETH", 2, "B", true, user),
            fill("ETH", 3, "A", true, user),
            fill("ETH", 3, "A", false, user),
            fill("ETH", 4, "B", true, user),
            fill("ETH", 4, "B", false, user),
            fill("ETH", 5, "A", true, user),
            fill("ETH", 5, "B", false, user),
            fill("ETH", 5, "A", true, user),
            fill("ETH", 6, "A", true, user),
            fill("ETH", 6, "B", false, user),
        ]);
        let trades = coin_to_trades(&batch, &HashSet::from(["ETH"]));
        assert_eq!(trades["ETH"].len(), 1);
        assert_eq!(serde_json::to_value(&trades["ETH"][0]).unwrap()["tid"], 6);
        assert!(coin_to_trades(&fill_batch(vec![]), &HashSet::from(["ETH"])).is_empty());
    }

    #[test]
    fn trades_only_process_subscribed_non_outcome_coins() {
        let user = Address::ZERO;
        let mut fills = vec![
            fill("#33290", 481212882673789, "A", true, user),
            fill("#33291", 583392906092948, "A", true, user),
            fill("#44690", 532792333879334, "A", true, user),
            fill("#44691", 771181182710302, "A", false, user),
        ];
        for fill in &mut fills[..2] {
            fill.1.dir = "Merge Outcome".to_string();
        }
        for fill in &mut fills[2..] {
            fill.1.dir = "Sell".to_string();
        }
        for coin in ["BTC", "ETH", "@123", "PURR/USDC", "#10"] {
            fills.push(fill(coin, 1, "B", true, user));
            fills.push(fill(coin, 1, "A", false, user));
        }
        let batch = fill_batch(fills);
        let coins = HashSet::from(["BTC", "@123", "PURR/USDC", "#10", "#33290", "#33291", "#44690", "#44691"]);
        let trades = coin_to_trades(&batch, &coins);
        assert_eq!(trades.len(), 3);
        for coin in ["BTC", "@123", "PURR/USDC"] {
            assert_eq!(trades[coin].len(), 1);
        }
        assert!(coin_to_trades(&batch, &HashSet::new()).is_empty());
    }

    #[test]
    fn l4_reset_sends_a_full_snapshot_and_clears_removed_coins() {
        let text = json!([102, [["ETH", [[[
            Address::ZERO,
            {
                "coin": "ETH", "side": "B", "limitPx": "2153.6", "sz": "0.1139",
                "oid": 2, "timestamp": 1788919049596_u64, "triggerCondition": "N/A",
                "isTrigger": false, "triggerPx": "0.0", "isPositionTpsl": false,
                "reduceOnly": false, "orderType": "Limit", "tif": "Gtc", "cloid": null
            }
        ]], []]]]])
        .to_string();
        let (_, orders) = load_snapshots_from_str::<_, (Address, L4Order)>(&text).unwrap();
        let snapshot = TimedSnapshots { height: 102, time: 1788919049596, snapshot: orders };
        let response = serde_json::to_value(l4_snapshot_response("ETH", &snapshot)).unwrap();
        assert_eq!(response["channel"], "l4Book");
        assert_eq!(response["data"]["Snapshot"]["height"], 102);
        assert_eq!(response["data"]["Snapshot"]["time"], snapshot.time);
        assert_eq!(response["data"]["Snapshot"]["levels"][0][0]["oid"], 2);

        let removed = TimedSnapshots { snapshot: Snapshots::new(HashMap::new()), ..snapshot };
        let response = serde_json::to_value(l4_snapshot_response("ETH", &removed)).unwrap();
        assert_eq!(response["data"]["Snapshot"]["coin"], "ETH");
        assert_eq!(response["data"]["Snapshot"]["levels"], json!([[], []]));
    }
}
