# Local WebSocket Server

## Disclaimer

This was a standalone project, not written by the Hyperliquid Labs core team. It is made available "as is", without warranty of any kind, express or implied, including but not limited to warranties of merchantability, fitness for a particular purpose, or noninfringement. Use at your own risk. It is intended for educational or illustrative purposes only and may be incomplete, insecure, or incompatible with future systems. No commitment is made to maintain, update, or fix any issues in this repository.

## Functionality

This server provides the `l2book` and `trades` endpoints from [Hyperliquid’s official API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions), with roughly the same API.

- The `l2book` subscription now includes an optional field:
  `n_levels`, which can be up to `100` and defaults to `20`.
- This server also introduces a new endpoint: `l4book`.

The `l4book` subscription first sends a snapshot of the entire book and then forwards order diffs by block. A new full snapshot is also sent after the server rebuilds its order book. Clients must replace their local book whenever they receive an `L4Book::Snapshot`, including clearing it when both sides are empty, and then apply subsequent updates. The subscription format is:

```json
{
  "method": "subscribe",
  "subscription": {
    "type": "l4Book",
    "coin": "<coin_symbol>"
  }
}
```

## Setup

1. Run a non-validating node (from [`hyperliquid-dex/node`](https://github.com/hyperliquid-dex/node)). Requires batching by block. Requires recording fills, order statuses, and raw book diffs. Requires handling info requests. 

2. Then run this local server:

```bash
cargo run --release --bin websocket_server -- --address 0.0.0.0 --port 8000
```

If this local server does not detect the node writing down any new events, it will automatically exit after some amount of time (currently set to 5 seconds).
In addition, the local server periodically fetches order book snapshots from the node and compares them to its own internal state at the same block height. If a difference is detected, it logs an error, discards the old order book, rebuilds from the node snapshot, and replays cached updates before resuming. Connected L2 and L4 subscribers receive fresh snapshots after recovery. If replay fails or cannot catch up, the old book stays cleared and order book streaming waits for the next snapshot; a consistency mismatch does not terminate the server.

If you want logging, prepend the command with `RUST_LOG=info`.

The WebSocket server comes with compression built-in. The compression ratio can be tuned using the `--websocket-compression-level` flag.

## Caveats

- This server does **not** show untriggered trigger orders.
- It currently **does not** support spot order books.
- The current implementation batches node outputs by block, making the order book a few milliseconds slower than a streaming implementation.
