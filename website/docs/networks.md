---
sidebar_position: 5
title: EVM networks
description: "Choosing EVM endpoints for a signer: why two independent providers matter, running your own node, and sizing request rate caps against provider limits."
---

# EVM networks

Each EVM network entry in `config.json` names the endpoints the orchestrator talks to and how hard it may push them. Two decisions matter: **how many endpoints** and **how fast**.

## Use two or more independent endpoints

The orchestrator only discards an unwrap event, or removes a stored record during a [backfill](operations/backfill.md), when **every configured endpoint agrees** that the block the event was seen in is no longer canonical. With a single URL that is one provider's word. Behind a load balancer the same URL can answer from a lagging or forked backend, and three samples through one balancer are not independent.

List two providers operated by different parties. The first reachable endpoint supplies subscriptions and range scans. Every configured endpoint serves canonical block checks and source-event evidence during sync, queue confirmation, and signing. A signer configured with one endpoint trusts that provider for the transfer evidence. Listing the same URL twice adds nothing and is warned about at startup.

For historical sync every endpoint must serve full history: headers at any height and complete `eth_getLogs` results. A provider that returns an empty result for pruned ranges cannot be detected and would silently skip events.

## Prefer your own node

The strongest configuration is a full node you run yourself for each bridged network, listed first, with a second independent endpoint for agreement. Your own node gives an exact view of chain state, detects reorganisations directly, and cannot be censored or rate-limited by a third party. Remote providers, whether centralised (Infura, Alchemy, QuickNode and the like) or decentralised (Ankr, Pocket), are appropriate as the second endpoint and as a fallback, and they are what the [rate limits](#rate-limits) below are for.

## Rate limits

`RpcRequestsPerSecond` and `RpcBurst` cap requests **per endpoint** with a token bucket. The cap covers every read the orchestrator makes to that endpoint: the sync's `eth_getLogs` and head reads, receipts and headers during event confirmation, canonical-block checks, and the bridge contract reads. It does not cover the log subscription, which is server-pushed, or the rare transaction sends.

### Low-rate configuration

Set these fields in the existing `Networks.Ethereum` entry of `config.json` and restart the orchestrator. For example, for a plan that permits 10 blocks per log query and one request every two seconds, keep your provider URLs and use these values.

```json
{
    "FilterQuerySize": 10,
    "RpcRequestsPerSecond": 0.5,
    "RpcBurst": 1
}
```

`RpcBurst: 1` spaces requests even after idle time. Lowering the rate also slows retries and other reads that share the endpoint. Local waits happen before the RPC timeout starts. A missing or zero `RpcRequestsPerSecond` leaves an existing network entry uncapped.

Choose a rate that fits both throughput and total quota. For example, a continuously busy endpoint at 0.5 requests per second can use about 43,200 requests per day. If the provider bills compute units, allow for the most expensive calls the sync makes. Divide shared account allowances among all active endpoints, networks and other clients using the same key. The orchestrator does not track the provider's remaining credits.

`FilterQuerySize` counts both ends of the query. With a ceiling of 10, a query starting at block 100 ends no later than block 109, even near the chain head. If your plan publishes a maximum range, setting it here avoids probing above it.

### Sizing against a free tier

The limits below are [dRPC's](https://drpc.org/docs/howitworks/ratelimiting) free tier as published in September 2026, in its compute units (CU). Providers differ and change their terms; check yours and redo the arithmetic.

| Limit | Value |
| --- | --- |
| Normal throughput | 120,000 CU/min per IP, about 2,000 CU/s |
| Degraded floor | 50,400 CU/min, about 840 CU/s |
| Monthly quota | 210M CU per 30 days |
| Minimum per call | 10 CU |
| Request timeout | 2 s |
| `eth_getLogs` response cap | 10,000 entries |

The cost of the cap in CU per second is `RpcRequestsPerSecond × CU per call`, so look up `eth_getLogs` and `eth_blockNumber` in the provider's [compute-unit table](https://drpc.org/docs/pricing/compute-units), which changes over time. At 20 CU per call the default cap of 3 requests per second is 60 CU/s, about 7% of the degraded floor; at 75 CU per call it is 225 CU/s, about 27%. Either leaves room for other clients on the same IP. A full first sync from contract deployment is a few thousand `eth_getLogs` calls plus as many head reads, far below the monthly quota.

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
- **Provider rejects the window width or result size**: the sync narrows its `eth_getLogs` window automatically. When the provider's message names the width it accepts, as Alchemy's does, the window drops straight to it; otherwise it halves on each rejection down to 8 blocks. It widens again after 50 consecutive successes, and logs each change. `FilterQuerySize` is the ceiling it starts from.
- **Provider times out**: lower `FilterQuerySize`. A range that always times out never completes.

:::caution Historical access and first-sync time on free plans
Measured in September 2026: **Alchemy's** free tier limits `eth_getLogs` to a 10-block window, and **dRPC's** free plan rejects windows wider than roughly 100 blocks (its message quotes 10,000) and often cannot route queries far behind the head at all. The sync narrows its window and keeps going, but a first scan of Ethereum from the bridge's deployment block at 10-block windows is close to a million queries, days at any sane request rate.

For a signer with a known-good [local checkpoint](operations/sync-checkpoints.md), restore its event stores and pending queues together so only the tail needs scanning. An events-only copy can carry a cursor past an unwrap that is still waiting in the donor's queue. A new signer without a checkpoint needs an endpoint that serves the bridge's historical blocks within its rate and range limits. Once the cursor is near the head, the three-minute catch-ups usually cover only a handful of blocks.
:::
- **Several networks or URLs share one API key**: the caps add up per endpoint. Set each low enough that the sum fits the account.

## What the endpoint must support

- WebSocket (`ws://` or `wss://`) for the log subscription. HTTP endpoints connect lazily and cannot subscribe.
- `eth_getLogs` by block range and by block hash, `eth_getTransactionReceipt`, `eth_getBlockByNumber`, `eth_blockNumber`, `eth_syncing`, and `eth_call` for the bridge contract.
- Full history back to the bridge contract's deployment block.
