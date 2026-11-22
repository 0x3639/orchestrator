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

## What happens on the upstream release

On upstream `v0.0.9a-alphanet`, a receipt lookup can fail while the confirmation worker is reading a queued unwrap. The first error schedules a dequeue. The next iteration removes the event **before** checking its receipt again. If that lookup also fails, the queue has already lost the event. The persisted scan position can be ahead of the event, so an ordinary restart never requests its block again. The upstream scan can also advance past an event whose interpretation returned an error, and a later WebSocket log can move the scan position beyond intervening blocks. [Upstream confirmation loop](https://github.com/HyperCore-Team/orchestrator/blob/v0.0.9a-alphanet/network/evm.go#L595-L645) · [Range checkpoint](https://github.com/HyperCore-Team/orchestrator/blob/v0.0.9a-alphanet/network/evm.go#L143-L158) · [Per-log checkpoint](https://github.com/HyperCore-Team/orchestrator/blob/v0.0.9a-alphanet/network/evm.go#L523-L526)

Each signer builds its own unwrap batch from its local unsigned records. The signing library derives the ceremony ID from the sorted messages and signer keys. A single missing event in the selected batch can therefore put two online signers in different ceremonies. A split stalls signing when neither group reaches threshold. [Upstream batch selection](https://github.com/HyperCore-Team/orchestrator/blob/v0.0.9a-alphanet/node/node.go#L1127-L1169) · [Ceremony ID construction](https://github.com/HyperCore-Team/go-tss/blob/a8efe6a2e98e/tss/tss.go#L196-L218)

An offline replay of that release started with one unsigned unwrap at block 100 and queued a second from block 105. Two consecutive temporary receipt errors dropped the second event. After restart the signer reached block 150 with one candidate. Restoring the block-100 checkpoint and replaying to the same block 150 produced both candidates. This reproduces one way for healthy-looking signers to disagree. Confirm an individual incident by comparing the actual signing attempts.

During a stall, capture `getBuildInfo` and `getStatus` from each signer within the same window, allowing for the health cache. Compare the chain ID, contract address, scan height and per-network pending hashes. The upstream `unwrapsHash` covers **all unsigned transaction hashes and log indexes** for the network. Equal hashes can still hide a difference in message contents or in the selected batch. Compare the sorted `encoded msg:` lines for a single matching unwrap signing attempt across signers. Upstream logs these values just before `before sending sign request`. If the message sets agree, check signer membership and TSS connectivity before resetting any data. [Health digest](https://github.com/HyperCore-Team/orchestrator/blob/v0.0.9a-alphanet/health/server.go#L99-L138) · [Signing request log](https://github.com/HyperCore-Team/orchestrator/blob/v0.0.9a-alphanet/tss/manager.go#L128-L139)

A known-good [local sync checkpoint](sync-checkpoints.md) works with the upstream binary. The bounded backfill flag and the retry and reconciliation behavior described below belong to the development branch.

## How signers keep their sets converged

On `dev`, each signer sorts its candidates by chain ID, transaction hash and log index before applying the batch limit. It requests bridge signatures only after a quorum certifies the same ordered messages for the same key and signing window. Two matching signers in a group of three can proceed. A split with no quorum still stalls that window. Preserve the signer's current `unwrap-pool-votes` alongside its key shares during recovery. [Development branch implementation](https://github.com/0x3639/orchestrator/blob/41e05ffd83a591d4b796f08915b7fcb3740de0e9/tss/unwrap_agreement.go#L54-L89)

The development branch also checks the event set locally.

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
- **All hashes match but nothing signs.** Compare the proposed messages, signing window and signer membership. If those agree, check peer connectivity on port `55055` and `peersLen` in `getStatus`.

## What reconciliation costs

Before each ceremony a signer looks up its unsigned events on the local Zenon node, in order, until it has enough to fill the pool, with at most four times the pool size in lookups per ceremony (200 at the default pool size). A signer with a large stale backlog works it off over successive windows and skips signing until its lookups complete, so that it never proposes a pool its peers do not share. If the Zenon RPC fails mid-way the signer sits out that ceremony rather than guess.
