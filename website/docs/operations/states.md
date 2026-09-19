---
sidebar_position: 1
title: Node states
---

# Node states

`getStatus` reports `stateName`. The state is persisted in `config.json` as `GlobalState` and drives what the signer does.

| State | Meaning | What the signer does |
| --- | --- | --- |
| `LiveState` | Normal operation | Alternates wrap and unwrap signing ceremonies on a momentum-based window, sends signatures, syncs both chains |
| `KeyGenState` | The bridge allows key generation | Waits for the key-gen window, runs the TSS key generation with the other pillars, then returns to live |
| `HaltedState` | The bridge is halted on Zenon or an EVM chain | Signs nothing; polls until the halt is lifted |
| `EmergencyState` | This signer observed an inconsistency it treats as an attack, for example a redeem on an EVM chain with no matching wrap on Zenon | Signs a halt message with the other signers and moves to halted |
| `ReSignState` | The bridge administrator activated a re-sign for one network through bridge metadata | Walks every wrap request for that network, re-signs the ones whose signatures are no longer valid, then returns to live |

Only the administrator account can halt networks directly; ordinary signers reach `HaltedState` through the bridge's own halt flags.

The signer also asks to be restarted, rather than continuing, when a subscription or storage operation fails in a way it cannot recover from. Run it under a supervisor with automatic restart.
