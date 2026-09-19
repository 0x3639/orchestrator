---
sidebar_position: 2
title: How the bridge works
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

Each orchestrator node runs alongside a Pillar and holds one share of a threshold-signature (TSS) key. The TSS group can sign; it cannot move funds on its own, because the contracts only act on a valid group signature for a transfer that they can themselves verify.

There is no extra consensus round between orchestrators. Each node independently watches every network, waits for the transfer to reach that network's finality threshold, and then proposes it for signing in the next ceremony. A ceremony succeeds when a super-majority (more than two thirds) of the group proposes the identical set of transfers. That successful signature **is** the off-chain consensus on which transfers are valid.

Two consequences shape everything else in this guide:

- Every signer must see the same events. A signer that missed an event, or holds one the others do not, proposes a different set and stays out of the ceremony. [Signing stalls](operations/signing-stalls.md) covers how the orchestrator keeps its view converged with its peers.
- Every signer must judge finality from the chain, not from one provider. [EVM networks](networks.md) covers endpoint choice.

## Ceremony windows

Signing alternates between the two directions on a fixed schedule measured in momentums. The bridge's `windowSize` parameter (see [Bridge parameters](bridge-parameters.md)) divides momentum height into windows; even windows sign wraps, odd windows sign unwraps, and each signer runs at most one ceremony per window. If there is nothing to sign in the scheduled direction, the signer tries the other one.

## Networks and finality

Only EVM networks are bridged today: Ethereum-compatible execution, secp256k1 ECDSA signatures and the Ethereum JSON-RPC API. Each network has a network class and chain id, and a finality threshold in blocks below which the orchestrator will not sign:

| Network | Class | Chain id | Finality |
| --- | --- | --- | --- |
| Network of Momentum | 1 | 1 | 6 momentums |
| Ethereum | 2 | 1 | 1 epoch, about 6.4 minutes |
| BNB Smart Chain | 2 | 56 | fast finality, seconds |

The exact number of confirmations for each EVM network is a bridge parameter, `confirmationsToFinality`, reported per network by the [health API](health-api.md).

---

*Adapted from the HyperCore-Team documentation for the NoM multi-chain infrastructure, [hypercore-team.github.io](https://hypercore-team.github.io/), and ZIP:sumamu-0001. GPL v3.*
