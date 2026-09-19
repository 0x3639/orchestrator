---
sidebar_position: 8
title: Bridge parameters
description: "The bridge contract parameters the orchestrator reads from Zenon and each EVM contract, and the behaviour each one drives."
---

# Bridge parameters

The orchestrator takes most of its operating parameters from the bridge embedded contract on NoM, not from `config.json`. It reads them at startup and refreshes most of them as the chain changes; the exceptions are noted below. Knowing which parameter drives which behaviour makes the log lines and state changes easier to read. They are available over the Zenon node's JSON-RPC under `embedded.bridge`.

## `getOrchestratorInfo`

| Field | Drives |
| --- | --- |
| `windowSize` | Length in momentums of one signing window. Even windows sign wraps, odd windows unwraps; one ceremony per window per signer. |
| `keyGenThreshold` | Minimum number of participating Pillars for a key generation ceremony to start. |
| `confirmationsToFinality` | Momentums a wrap request must age on NoM before the orchestrator will sign it. The EVM-side equivalent is read from each EVM contract. |
| `estimatedMomentumTime` | Read and stored, but not used to schedule anything; the waits between ceremony checks are fixed in the code. |
| `allowKeyGenHeight` | Reference height; the orchestrator looks back 24 hours of momentums from it to find the Pillars that produced momentums and are therefore eligible for key generation. |

## `getBridgeInfo`

| Field | Drives |
| --- | --- |
| `administrator` | If this signer's producer address matches **at startup**, it runs in administrator mode and does not join TSS ceremonies. A later administrator change is tracked for verifying on-chain actions but does not switch a running node's role; restart affected nodes. |
| `compressedTssECDSAPubKey` | The group key the orchestrator must hold a share of. If it differs from the key in `tss/`, the signer waits for a key generation. |
| `allowKeyGen` | When true the signer enters `KeyGenState` and joins the next key generation window. |
| `halted`, `unhaltedAt`, `unhaltDurationInMomentums` | Halt flags. The signer enters `HaltedState` while any network is halted and leaves it only after the unhalt duration has elapsed. |
| `tssNonce` | Nonce included in the NoM messages for changing the TSS key and for halting, so those signatures cannot be replayed. Ordinary transfers carry their own identifiers instead; EVM administrative actions use the EVM contract's `actionsNonce`. |
| `metadata` | JSON with runtime overrides, below. |

### Metadata

The administrator can change these without a release:

| Key | Effect |
| --- | --- |
| `signCeremonyPoolSize` | Maximum events signed in one ceremony. Default 50. |
| `partyTimeout`, `keyGenTimeout`, `keySignTimeout`, `preParamTimeout` | TSS ceremony timeouts, in seconds. |
| `joinPartyVersion` | Selects the TSS party-formation protocol version all signers must share. |
| `resignState` | Activates `ReSignState` for one network so its wraps are signed again, for example after a contract upgrade. |
| Affiliate program settings | Per network and token: whether unwraps carry an affiliate share and from which block. |

## Per-network parameters from the EVM contract

Read from each EVM bridge contract at startup and shown in the [health API](health-api.md) under `networks`:

| Field | Drives |
| --- | --- |
| `contractDeploymentHeight` | Where the first sync starts. |
| `estimatedBlockTime` | Whole seconds; used to pace confirmation waits. |
| `confirmationsToFinality` | Blocks an unwrap event must age before it is confirmed and enters the signing pool. |

## Where to read more

The full JSON-RPC surface of the bridge embedded contract, its constants and its error strings are documented in the HyperCore Team's [embedded bridge API reference](https://hypercore-team.github.io/decentralized_bridge/core_contracts/embedded_bridge/api.html).

---

*Adapted from the HyperCore Team's [NoM multi-chain infrastructure documentation](https://hypercore-team.github.io/) (MIT licence, copyright 2023 HyperCore Team; see [Attribution](attribution.md)), which describes [ZIP:sumamu-0001](https://forum.zenon.org/t/zip-sumamu-0001-final/1327). Source pages: [Embedded bridge API](https://hypercore-team.github.io/decentralized_bridge/core_contracts/embedded_bridge/api.html).*
