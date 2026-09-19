---
sidebar_position: 3
title: Security model
---

# Security model

The bridge is built on one principle: **no effect without a cause**. Assets can be released on a destination network only because they were locked or burned on the source network, and every part of the design exists to make that verifiable and to stop the bridge when it is not.

## Request, then redeem

Every wrap and unwrap is two actions:

1. **Request** signals the user's intent and locks or burns the source-side assets.
2. **Redeem** delivers the assets on the destination once a valid orchestrator signature exists.

Registering a redeem on the destination does not deliver the assets immediately: there is a delay during which they remain unredeemable. During that delay every orchestrator checks that the registration has a matching request on the source network. The check runs in both directions, for redeems registered on an EVM contract (`RegisteredRedeem`) and for unwrap requests registered on NoM. If the source-side cause does not exist, the orchestrators halt the bridge.

## Time challenges

Sensitive administrative actions, such as changing the TSS public key, adding a network, or changing guardians, are split into two stages separated by a time challenge: the action is announced, and only after a minimum delay can it take effect. The delays are bridge constants, longer for administrator and guardian changes than for other operations. This gives operators time to react to a hostile or mistaken change.

## Distributed halting

The orchestrators halt the bridge on their own when any of them observes a redeem registration on a destination network with no corresponding request on the source network. The signer enters `EmergencyState`, the group signs halt messages for every network in a ceremony, and a selected signer submits each one. Halting needs the same threshold as any other signature.

On EVM networks the halt is an ordinary transaction sent from the submitting signer's **own EVM address**, which is derived from the producer key and shown as `evmAddress` in the [health API](health-api.md). Submission rotates between signers, so a signer whose address holds no gas cannot submit when it is selected and the halt waits for the next one. Keep the address funded on every bridged EVM network and include its balance in monitoring.

The administrator can halt any network unilaterally; ordinary signers halt only through the threshold-signed emergency ceremony. Lifting a halt is an administrator action and takes effect only after the bridge's minimum unhalt duration.

## Administrator and guardians

- **The administrator** manages the bridge: networks, tokens, parameters, halting and unhalting. It cannot move funds. Its actions are subject to time challenges.
- **Guardians** are elected community members who act only in an emergency. If the bridge enters the emergency state the administrator address becomes null, and the guardians vote a new one in; a candidate needs more than half of all guardians.

## What to monitor

Every operator is expected to run their own monitoring rather than rely on a shared one, because heterogeneous monitoring is more likely to catch fraud. Cover at least:

- **The orchestrator**: `stateName`, `unwrapsHash` and `latestUpdateHeight` from the [health API](health-api.md); ceremony and reconciliation log lines; peer count.
- **Your chain nodes**: sync state, reorgs, and endpoint agreement warnings from the orchestrator log.
- **The contracts**: on-chain halt flags, administrator actions and pending time challenges, which the bridge embedded contract exposes over the Zenon RPC.
- **The signer's EVM address**: gas balance for halts.

If your monitoring detects a security event, contact the other Pillar operators and the administrator so the bridge can be halted.

---

*Adapted from the HyperCore Team's [NoM multi-chain infrastructure documentation](https://hypercore-team.github.io/) (MIT licence, copyright 2023 HyperCore Team; see [Attribution](attribution.md)), which describes [ZIP:sumamu-0001](https://forum.zenon.org/t/zip-sumamu-0001-final/1327). Source pages: [Security](https://hypercore-team.github.io/security.html), [Participants](https://hypercore-team.github.io/intro/participants.html).*
