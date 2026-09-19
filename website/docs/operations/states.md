---
sidebar_position: 1
title: Node states
---

# Node states

`getStatus` reports `state` and `stateName`. The state is persisted in `config.json` as `GlobalState` and drives what the signer does.

| Value | State | Meaning | What the signer does |
| --- | --- | --- | --- |
| `0` | `LiveState` | Normal operation | Alternates wrap and unwrap signing ceremonies on the momentum window, sends signatures, syncs both chains |
| `1` | `KeyGenState` | The bridge's `allowKeyGen` flag is set | Waits for the key-gen window, runs the TSS key generation with the other Pillars, then returns to live |
| `2` | `HaltedState` | The bridge is halted on Zenon or an EVM chain | Signs nothing; polls until the halt is lifted and the unhalt duration has passed |
| `3` | `EmergencyState` | This signer observed a redeem on an EVM chain with no matching request on Zenon | Signs a halt message with the other signers, submits it on each network, and moves to halted |
| `4` | `ReSignState` | The bridge administrator activated a re-sign for one network through bridge metadata | Walks every wrap request for that network, re-signs the ones whose signatures are no longer valid, then returns to live |

The flags and parameters behind these transitions are described in [Bridge parameters](../bridge-parameters.md), and the halting design in [Security model](../security-model.md).

Only the administrator account can halt networks directly; ordinary signers reach `HaltedState` through the bridge's own halt flags.

The signer also asks to be restarted, rather than continuing, when a subscription or storage operation fails in a way it cannot recover from. Run it under a supervisor with automatic restart.
