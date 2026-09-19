---
sidebar_position: 3
title: Signing stalls
description: "Why unwrap signing stalls when signers' event sets diverge, how the orchestrator keeps them converged, and how to diagnose a stalled bridge."
---

# Signing stalls

**Symptom.** Unwraps (EVM to Zenon) stop being signed. `getStatus` shows `unwrapsToSign` above zero on every signer, the bridge is not halted, and nothing moves.

## How a ceremony forms

A TSS signing ceremony is identified by a hash of **the exact set of messages** being signed plus the signer keys. Every signer builds that set locally: the first unsigned unwrap events in its own `events/` store, in key order, up to the ceremony pool size (50 by default, adjustable by the bridge administrator through [bridge metadata](../bridge-parameters.md)). Signers whose sets are identical join the same ceremony; a ceremony succeeds when at least two thirds of the group, rounded up (`ceil(2N/3)` of `N` signers), join it.

A signer whose set differs from its peers', even by one event, computes a different ceremony id and stays out. If the group fragments into sets that none reaches the threshold, nothing signs.

## How signers keep their sets converged

The orchestrator treats its local event set as something that must match its peers', and guards it in five ways:

- **Events are never dropped on transport errors.** A queued unwrap event is discarded only when every configured endpoint agrees the block it was seen in was reorged out, confirmed across three checks 30 seconds apart. RPC errors, missing receipts and lagging backends retry with backoff instead.
- **Stored records are never reset by duplicates.** The same event arrives more than once (subscription and periodic sync); adding it is idempotent, and a ceremony writes only the signature.
- **The sync cursor only moves forward**, and only after a whole block range has been processed. A failed range is retried; it is never skipped.
- **Historical events are validated** against the canonical block, agreed by every endpoint, before they are stored.
- **The pool is reconciled against Zenon before each ceremony.** Events Zenon already knows about, because other signers reached the threshold without this one, are marked with Zenon's status and excluded. A signer that missed a ceremony therefore rejoins at its next window without intervention.

What these guards cannot fix is a signer that has genuinely lost events from its store, or holds records from a fork. For that, [backfill](backfill.md) re-scans a bounded range.

## Diagnosing a stall

Collect from **every** signer at the same time:

1. `getStatus`: `unwrapsToSign`, `unwrapsHash`, `latestUpdateHeight`, `stateName`, `frontierMomentum`.
2. The `LocalPubKeys` list from `config.json` under `TssConfig`, sorted.
3. The most recent `reconcile:` and `Event … not confirmable yet` log lines.

Then:

- **`unwrapsHash` differs between signers** and `latestUpdateHeight` values are all near the head: the event sets diverged. Identify the signer whose count is off. If its count is higher and the extras are events Zenon already has, reconciliation clears them at the next ceremony; if the extras are records nobody else holds (from a forked backend), only a [backfill](backfill.md) removes them. If its count is lower, it is missing events: run a backfill on that signer.
- **`latestUpdateHeight` is far behind on one signer**: that signer's sync is failing. See [First sync](first-sync.md) for the log lines and [Troubleshooting](troubleshooting.md).
- **`LocalPubKeys` differ**: a configuration-level split that no store repair fixes. The signers disagree about who is in the group; that is resolved by the next key generation.
- **All hashes match but nothing signs**: the problem is the ceremony itself, not the sets. Check TSS peer connectivity on port `55055` and `peersLen` in `getStatus`.

## What reconciliation costs

Before each ceremony a signer looks up its unsigned events on the local Zenon node, in order, until it has enough to fill the pool, with at most four times the pool size in lookups per ceremony (200 at the default pool size). A signer with a large stale backlog works it off over successive windows and skips signing until its lookups complete, so that it never proposes a pool its peers do not share. If the Zenon RPC fails mid-way the signer sits out that ceremony rather than guess.
