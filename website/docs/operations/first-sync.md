---
sidebar_position: 2
title: First sync
---

# First sync

On first start, and after a [backfill](backfill.md), the orchestrator scans every EVM network from the bridge contract's deployment block to the head, in `FilterQuerySize` steps. On Ethereum mainnet that is a few thousand `eth_getLogs` queries, which is where free-tier providers push back.

## Before you start

1. Configure two or more independent, full-history endpoints per network. See [EVM networks](../networks.md).
2. Set `RpcRequestsPerSecond` and `RpcBurst` under the provider's degraded limit. The sizing table in [EVM networks](../networks.md#rate-limits) covers a typical free tier.
3. Make sure the Zenon node is synced. The orchestrator waits for it before doing anything else.

## What you will see

```
Sync progress for chainId 1: block 18004000 of 21003400 (24.9%), 500 ranges in 3m20s
```

A progress line every 25 ranges, then a completion line. Each range costs two throttled requests, a head read and the `eth_getLogs` query, so the lower bound is:

```
ranges  = blocks to scan / FilterQuerySize
minutes = ranges × 2 / RpcRequestsPerSecond / 60
```

Ethereum mainnet from an 18M-block deployment to a 26M head, at 2,000 blocks per query and 3 requests per second, is 4,000 ranges and about 45 minutes before any retries or historical-event validation. Halving `FilterQuerySize` doubles it.

When the provider rejects or times out a query:

```
Sync for chainId 1: eth_getLogs for blocks [18004000, 18006000] failed at block 18004000 (attempt 3/12), retrying in 8s: 429 Too Many Requests
```

The same range is retried with backoff from 2 seconds to 2 minutes. After 12 consecutive failures the pass stops. What happens next depends on when it failed:

- **During the initial sync at startup**, the orchestrator exits. The supervisor restarts it (the systemd unit in [Install](../install.md) waits 10 seconds) and the sync resumes from the persisted cursor.
- **On a running node**, the next 3-minute refresh resumes from the same cursor.

The cursor **never** advances past a range that did not complete, so nothing is skipped by a failure.

## Historical events are validated

Every unwrap log found during the scan is checked against the canonical block before it is stored: all configured endpoints must agree on the block hash. A log from a block the endpoints agree was reorged out is skipped with a warning. If the endpoints disagree or one is unreachable, the range is retried; that is the message to look for if a sync seems stuck:

```
cannot validate historical unwrap 0x…/3 against the canonical chain: canonical block 18004123 inconclusive, 1 of 2 endpoints did not answer
```

## After the scan

The orchestrator subscribes to new logs and refreshes the subscription every 3 minutes with a short catch-up sync. `latestUpdateHeight` in [`getStatus`](../health-api.md) should then follow the head.

Live events wait in `queues/` until they have `confirmationsToFinality` confirmations. A live event is confirmed by its transaction receipt from the connected endpoint: a successful receipt in the block the event was observed in is enough. The other endpoints are consulted only when the receipt is missing, reverted, or in a different block, and an event is discarded only when every endpoint agrees its block was reorged out, across three checks 30 seconds apart. Confirmed events then enter the signing pool.
