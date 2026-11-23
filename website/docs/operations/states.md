---
sidebar_position: 1
title: Node states
description: "The orchestrator's node states, their numeric values, what triggers each one and what the signer does in it."
---

# Node states

`getStatus` reports `state` and `stateName`. The state is persisted in `config.json` as `GlobalState` and drives what the signer does.

| Value | State | Meaning | What the signer does |
| --- | --- | --- | --- |
| `0` | `LiveState` | Normal operation | Alternates wrap and unwrap signing ceremonies on the momentum window, sends signatures, syncs both chains |
| `1` | `KeyGenState` | The bridge's `allowKeyGen` flag is set | Waits for the key-gen window, runs the TSS key generation with the other Pillars, then returns to live |
| `2` | `HaltedState` | The bridge is halted on Zenon or an EVM chain | Signs nothing; polls until the halt is lifted and the unhalt duration has passed |
| `3` | `EmergencyState` | This signer observed a redeem registration on one network (an EVM `RegisteredRedeem` or a NoM unwrap request) with no matching request on the source network | Signs halt messages with the other signers, submits them when selected, and moves to halted |
| `4` | `ReSignState` | The bridge administrator activated a re-sign for one network through bridge metadata | Walks every wrap request for that network, re-signs the ones whose signatures are no longer valid, then returns to live |

The flags and parameters behind these transitions are described in [Bridge parameters](../bridge-parameters.md), and the halting design in [Security model](../security-model.md).

The administrator can halt a network unilaterally. Ordinary signers halt only through the threshold-signed emergency ceremony, and reach `HaltedState` when the bridge's halt flags are set, however they were set.

The signer also asks to be restarted, rather than continuing, when a subscription or storage operation fails in a way it cannot recover from. Run it under a supervisor with automatic restart.

## The bridge-account listener

The listener reads the bridge contract's account blocks in order and stops at the first one it cannot process: a block the Zenon node has not confirmed, a node that reports it is still syncing, or an unwrap request whose source event the EVM endpoint cannot be asked about. It retries every four seconds and reads nothing after that height in the meantime, including a halt. The hold is visible as `zenon.receiveHold` in the [health snapshot](../health-api.md) and, after a minute, as an error line that repeats every minute. After ten minutes at the same height the signer stops rather than keep signing without its listener. A supervisor restart retries the same block during startup and exits again until the cause is fixed.

Once a signer's database has followed the bridge account to its tip, or has met a request there that it had to check on the way, the signer checks the source event of every unwrap request it reads from then on, whenever it reads it: live, after a hold, or after a restart. Following a long stop or a [checkpoint restore](sync-checkpoints.md), start it against EVM endpoints that still serve the receipts and logs for the period it was away, since a request whose transaction the endpoint does not have is treated as having no source.

A database reading the bridge account for the first time has no such record yet. There the signer checks a receive that is still within finality and a request the bridge still withholds, one inside its redeem delay, in the same way. It records a request that is past its delay, redeemed or revoked without looking up its source, because the transaction may be older than the endpoint's history.
