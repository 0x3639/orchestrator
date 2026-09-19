---
sidebar_position: 3
title: Signing stalls
---

# Signing stalls

**Symptom.** Unwraps (EVM to Zenon) stop being signed. `getStatus` shows `unwrapsToSign` above zero on every signer, the bridge is not halted, and nothing moves. Historically the fix was to delete `events/` and `queues/` on each signer and resync from the deployment block. This page explains why that worked, why it is no longer needed, and what to do instead.

## Why signers stall

A TSS signing ceremony is identified by a hash of **the exact set of messages** being signed plus the signer keys. Every signer builds that set locally: the first 50 unsigned unwrap events in its own `events/` store, in key order. Signers whose sets differ compute different ceremony ids, join different rooms, and no room reaches the two-thirds threshold. Nothing signs, and nothing changes the sets, so it stays that way.

Wiping the stores forced every signer to rebuild its set from the same chain history, so the sets converged. It also re-signed all of history, 50 events per ceremony, each rejected by Zenon as already existing, which is where the provider load came from.

## How the fork keeps sets converged

| Divergence path in the old code | Fix |
| --- | --- |
| A queued event was dropped after two RPC failures, so an overloaded provider lost events on some signers | Events are only dropped when every endpoint agrees the block was reorged out, confirmed across three checks 30 s apart. Transport errors retry with backoff. |
| Duplicate deliveries reset a signed record to unsigned | Adding a record is idempotent; the ceremony writes only the signature. |
| The sync cursor was rewound by live subscription logs, re-fetching and re-queueing ranges | Only the sync loop advances the cursor, and only forward. |
| An event this signer missed the ceremony for stayed in its pool forever | Before each ceremony the pool is reconciled against Zenon; events Zenon already has are marked with Zenon's status and excluded. |
| Historical logs from a forked backend were stored as-is | Historical logs are stored only when all endpoints agree their block is canonical. |

A signer that missed a ceremony now **self-heals** at its next window through reconciliation. A signer that lost an event before the upgrade needs a [backfill](backfill.md).

## Diagnosing a stall

Collect from **every** signer at the same time:

1. `getStatus`: `unwrapsToSign`, `unwrapsHash`, `latestUpdateHeight`, `stateName`, `frontierMomentum`.
2. The `LocalPubKeys` list from `config.json` under `TssConfig`, sorted.
3. The most recent `reconcile:` and `Event … not confirmable yet` log lines.

Then:

- **`unwrapsHash` differs between signers** and `latestUpdateHeight` values are all near the head: the event sets diverged. Identify the signer whose count is off. If its count is higher, reconciliation will clear stale extras at the next ceremony. If lower, it is missing events: run a [backfill](backfill.md) on that signer.
- **`latestUpdateHeight` is far behind on one signer**: that signer's sync is failing. See [First sync](first-sync.md) for the log lines and [Troubleshooting](troubleshooting.md).
- **`LocalPubKeys` differ**: a configuration-level split that no store repair fixes. The signers disagree about who is in the group; that is resolved by the next key generation.
- **All hashes match but nothing signs**: the problem is the ceremony itself, not the sets. Check TSS peer connectivity on port `55055` and `peersLen` in `getStatus`.

## What reconciliation costs

Before each ceremony a signer looks up its unsigned events on the local Zenon node, in order, until it has enough to fill the pool, with at most 200 lookups per ceremony. A signer with a large stale backlog works it off over successive windows and skips signing until its lookups complete, so that it never proposes a pool its peers do not share. If the Zenon RPC fails mid-way the signer sits out that ceremony rather than guess.
