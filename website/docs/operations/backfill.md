---
sidebar_position: 4
title: Backfill
---

# Backfill

`--evm.backfill-blocks N` is the bounded repair for a signer that is missing events or holding records from a fork. It replaces deleting `events/` and `queues/` in all but emergency cases.

## What it does

At startup, once, for every EVM network:

1. Removes **unsigned, unredeemed** records at or above `cursor − N` whose block every configured endpoint agrees is no longer canonical. Records that are signed, sent, or whose block is still canonical are never touched. A block that is canonical but whose logs the endpoint does not return is left alone with a warning.
2. Rewinds the sync cursor to `cursor − N`, bounded by the deployment block.
3. Runs a normal [sync](first-sync.md) from there. Events already stored are kept; missing ones are added.

The flag is never persisted. It applies once per process start, so leaving it in the service arguments repeats the rewind on every restart.

## Runbook

1. **Stop the signer** and back up `~/.orchestrator/events` and `~/.orchestrator/queues`.
2. **Verify the endpoints.** Two or more independent, full-history providers per network, all healthy and agreeing. Backfill refuses to delete on any disagreement and fails the range on any unreachable endpoint, so a flaky provider makes it stall rather than repair. See [EVM networks](../networks.md).
3. **Choose `N`** so that `cursor − N` precedes the earliest event the signer may have missed, plus a margin of one confirmation window. Blocks per day: Ethereum about 7,200, BSC about 28,800. A week on Ethereum is `50000`.
4. **Start once with the flag**:
   ```bash
   orchestrator --evm.backfill-blocks 50000
   ```
   or add it to `ExecStart` for one start and remove it afterwards.
5. **Watch the log** for the rewind line, the prune summary and the sync completion:
   ```
   Backfill requested: rewinding sync cursor for chainId 1 from 21003400 to 20953400 (50000 blocks); …
   Backfill: checked 12 unsigned unwrap records from block 20953400, removed 1, inconclusive 0
   Sync complete for chainId 1: blocks 20953400 to 21003512 in 26 ranges, 2m41s
   ```
   Endpoint disagreement, inconclusive validation or repeated range retries mean the repair is incomplete; fix the provider and run again. If startup fails before the rewind line, rerun with the same flag.
6. **Remove the flag** and restart normally.
7. **Compare `unwrapsHash`** across signers at the next ceremony window. It should match.

## When backfill is not enough

- The provider cannot serve history back to the range you need. Switch providers rather than wiping; a wipe needs the same history.
- The divergence is in `LocalPubKeys`, not in events. See [Signing stalls](signing-stalls.md).
- The store or queue is corrupt. Use the [emergency reset](emergency-reset.md).
