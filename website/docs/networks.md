---
sidebar_position: 5
title: EVM networks
---

# EVM networks

Each EVM network entry in `config.json` names the endpoints the orchestrator talks to and how hard it may push them. Two decisions matter: **how many endpoints** and **how fast**.

## Use two or more independent endpoints

The orchestrator only discards an unwrap event, or removes a stored record during a [backfill](operations/backfill.md), when **every configured endpoint agrees** that the block the event was seen in is no longer canonical. With a single URL that is one provider's word. Behind a load balancer the same URL can answer from a lagging or forked backend, and three samples through one balancer are not independent.

List two providers operated by different parties. The first reachable one becomes the connected endpoint; the others are consulted only for canonical-block agreement, so their request volume is small. Listing the same URL twice adds nothing and is warned about at startup.

For historical sync every endpoint must serve full history: headers at any height and complete `eth_getLogs` results. A provider that returns an empty result for pruned ranges cannot be detected and would silently skip events.

## Rate limits

`RpcRequestsPerSecond` and `RpcBurst` cap requests **per endpoint** with a token bucket. The cap covers every read the orchestrator makes to that endpoint: the sync's `eth_getLogs` and head reads, receipts and headers during event confirmation, canonical-block checks, and the bridge contract reads. It does not cover the log subscription, which is server-pushed, or the rare transaction sends.

### Sizing against a free tier

The limits below are quoted from one free-tier provider's published terms as of September 2026, in that provider's compute units (CU). Providers differ and change their terms; check yours and redo the arithmetic.

| Limit | Value |
| --- | --- |
| Normal throughput | 120,000 CU/min per IP, about 2,000 CU/s |
| Degraded floor | 50,400 CU/min, about 840 CU/s |
| Monthly quota | 210M CU per 30 days |
| Minimum per call | 10 CU |
| Request timeout | 2 s |
| `eth_getLogs` response cap | 10,000 entries |

At about 75 CU per `eth_getLogs`, the default cap of 3 requests per second with a burst of 5 is roughly 225 CU/s: a quarter of the degraded floor and a tenth of the normal budget, leaving room for other clients on the same IP. A full first sync from contract deployment is a few thousand `eth_getLogs` calls, far below the monthly quota.

```json
"Ethereum": {
    "Urls": ["wss://eth-a.example/v3/KEY", "wss://eth-b.example/KEY"],
    "FilterQuerySize": 2000,
    "RpcRequestsPerSecond": 3,
    "RpcBurst": 5
}
```

Tuning:

- **Provider rejects on frequency**: lower `RpcRequestsPerSecond`.
- **Provider times out or rejects on size**: lower `FilterQuerySize` to `1000`. The retry loop handles the occasional timeout, but a range that always times out never completes.
- **Several networks or URLs share one API key**: the caps add up per endpoint. Set each low enough that the sum fits the account.

## What the endpoint must support

- WebSocket (`ws://` or `wss://`) for the log subscription. HTTP endpoints connect lazily and cannot subscribe.
- `eth_getLogs` by block range and by block hash, `eth_getTransactionReceipt`, `eth_getBlockByNumber`, `eth_blockNumber`, `eth_syncing`, and `eth_call` for the bridge contract.
- Full history back to the bridge contract's deployment block.
