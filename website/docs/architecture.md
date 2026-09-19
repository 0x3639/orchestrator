---
sidebar_position: 2
title: How the bridge works
description: "How the Zenon bridge moves assets between NoM and EVM networks: lock-and-release, request-then-redeem, TSS super-majority signing, ceremony windows and finality."
---

# How the bridge works

The Zenon bridge moves assets between the Network of Momentum (NoM) and EVM networks. It has three parts:

- **Core contracts**: the bridge embedded contract on NoM and a Solidity contract on each EVM network. They hold or mint assets and verify the signatures that release them.
- **The orchestrator layer**: the off-chain signer nodes this site documents. They watch the core contracts on every network and produce the threshold signatures the contracts require.
- **Protocol-level liquidity**: the embedded contract behind the Orbital Program, which rewards liquidity providers. It is separate from the orchestrator and not covered here.

## Lock-and-release, mint-and-burn

Assets are never copied. Native assets are locked or burned on the source network and released or minted on the destination:

| Direction | Source | Destination |
| --- | --- | --- |
| Wrap ZNN | ZNN locked in the bridge embedded contract on NoM | wZNN minted as ERC-20 by the EVM contract |
| Unwrap wZNN | wZNN burned by the EVM contract | ZNN released by the bridge embedded contract |
| Wrap an ERC-20 | Token locked in the EVM contract | Its ZTS representation minted on NoM |
| Unwrap that ZTS | ZTS burned by the embedded contract | Token released by the EVM contract |

Every transfer is a two-step, **request then redeem**. The request records the user's intent on one network; the redeem, once the orchestrators have signed, delivers the assets on the other. Between the two sits a time window during which the orchestrators verify that the source-side lock or burn really happened. See [Security model](security-model.md).

## The orchestrator's role

Each orchestrator node runs alongside a Pillar and holds one share of a threshold-signature (TSS) key. The contracts verify the group signature and their own local rules; they cannot see the other chain. Verifying that a transfer has a genuine source-side cause is the orchestrators' job, done twice: before signing, and again during the redeem delay, when every signer checks each registered redeem against the source network and halts the bridge if the cause is missing. See [Security model](security-model.md).

There is no extra consensus round between orchestrators. Each node independently watches every network, waits for the transfer to reach that network's finality threshold, and then proposes it for signing in the next ceremony. A ceremony succeeds when at least two thirds of the group, rounded up (`ceil(2N/3)` of `N` signers), propose the identical set of transfers. That successful signature **is** the off-chain consensus on which transfers are valid.

Two consequences shape everything else in this guide:

- Every signer must see the same events. A signer that missed an event, or holds one the others do not, proposes a different set and stays out of the ceremony. [Signing stalls](operations/signing-stalls.md) covers how the orchestrator keeps its view converged with its peers.
- Every signer must judge finality from the chain, not from one provider. [EVM networks](networks.md) covers endpoint choice.

## Ceremony windows

Signing alternates between the two directions on a fixed schedule measured in momentums. The bridge's `windowSize` parameter (see [Bridge parameters](bridge-parameters.md)) divides momentum height into windows; even windows sign wraps, odd windows sign unwraps, and each signer runs at most one ceremony per window. If there is nothing to sign in the scheduled direction, the signer tries the other one.

## Networks and finality

Only EVM networks are bridged today: Ethereum-compatible execution, secp256k1 ECDSA signatures and the Ethereum JSON-RPC API. Each network is identified by a network class and chain id:

| Network | Class | Chain id | Consensus finality, for background |
| --- | --- | --- | --- |
| Network of Momentum | 1 | 1 | about 6 momentums |
| Ethereum | 2 | 1 | 1 epoch, about 6.4 minutes |
| BNB Smart Chain | 2 | 56 | fast finality, seconds |

The last column is what each chain's own consensus considers final. The thresholds the orchestrator actually waits for are bridge parameters: `confirmationsToFinality` from `getOrchestratorInfo` for wrap requests on NoM, and each EVM bridge contract's `confirmationsToFinality` block count for unwrap events, reported per network by the [health API](health-api.md). See [Bridge parameters](bridge-parameters.md).

---

*Adapted from the HyperCore Team's [NoM multi-chain infrastructure documentation](https://hypercore-team.github.io/) (MIT licence, copyright 2023 HyperCore Team; see [Attribution](attribution.md)), which describes [ZIP:sumamu-0001](https://forum.zenon.org/t/zip-sumamu-0001-final/1327). Source pages: [Architecture overview](https://hypercore-team.github.io/intro/architecture.html), [Participants](https://hypercore-team.github.io/intro/participants.html), [Decentralized bridge](https://hypercore-team.github.io/decentralized_bridge/intro.html).*
