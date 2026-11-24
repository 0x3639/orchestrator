---
sidebar_position: 2
title: First sync
description: "What happens during an orchestrator's first sync of an EVM network, how long it takes, how rate limits and retries behave, and the log lines to expect."
---

# First sync

On first start, and after a [backfill](backfill.md), the orchestrator scans every EVM network from the bridge contract's deployment block to the head, in `FilterQuerySize` steps. On Ethereum mainnet that is a few thousand `eth_getLogs` queries, which is where free-tier providers push back.

## Before you start

1. Configure two or more independent, full-history endpoints per network. See [EVM networks](../networks.md).
2. Set `RpcRequestsPerSecond` and `RpcBurst` under the provider's degraded limit. See the [free-tier configuration example](../networks.md#low-rate-configuration) for fractional request rates and per-query block limits.
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

Ethereum mainnet from an 18M-block deployment to a 26M head, at 2,000 blocks per query and 3 requests per second, is 4,000 ranges and about 45 minutes before any retries or historical-event validation. Halving the window doubles it. If the provider rejects the window's width the sync narrows it automatically, down to 8 blocks; at that width the same scan is a million queries, which is the sign to switch providers for the initial scan. See [EVM networks](../networks.md#rate-limits).

When the provider rejects or times out a query:

```
Sync for chainId 1: eth_getLogs for blocks [18004000, 18005999] failed at block 18004000 (attempt 3/12), retrying in 8s: 429 Too Many Requests
```

The same range is retried with backoff from 2 seconds to 2 minutes. After 12 consecutive failures the pass stops. What happens next depends on when it failed:

- **During the initial sync at startup**, the orchestrator exits. The supervisor restarts it (the systemd unit in [Install](../install.md) waits 10 seconds) and the sync resumes from the persisted cursor. A signer in `EmergencyState` or `HaltedState` is kept running instead and starts the network again after 30 seconds; see [A held startup](states.md#a-held-startup).
- **On a running node**, the next 3-minute refresh resumes from the same cursor.

The cursor **never** advances past a range that did not complete, so nothing is skipped by a failure.

## Historical events are validated

For each mature unwrap, the signer checks the canonical block hash and confirmation depth across the configured endpoints. Each endpoint must also return the same bridge event from its receipt or a single-height log query. The signer skips an agreed orphan block. Missing or conflicting evidence causes the range to retry. A disagreement can appear in the log as follows.

```
cannot validate historical unwrap 0x…/3 against the canonical chain: canonical block 18004123 inconclusive, 1 of 2 endpoints did not answer
```

## After the scan

The orchestrator subscribes to new logs and refreshes the subscription every 3 minutes with a short catch-up sync. `latestUpdateHeight` in [`getStatus`](../health-api.md) should then follow the head.

Live unwraps wait in `queues/` until the same confirmation and transfer checks pass. The signer discards a queued event only after all endpoints report the same replacement block across three checks 30 seconds apart. Before a signing ceremony, the signer rechecks the selected unsigned records against the source event, including the recipient, token, amount and any affiliate split. This covers records created by an earlier binary or restored from a checkpoint. A batch with unresolved source evidence waits for a later ceremony.
