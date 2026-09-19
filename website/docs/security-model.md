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

After a redeem is registered there is a window during which the assets remain unredeemable. During that window every orchestrator checks that the matching source-side transaction exists. If it does not, the orchestrators halt the bridge.

## Time challenges

Sensitive administrative actions, such as changing the TSS public key, adding a network, or changing guardians, are split into two stages separated by a time challenge: the action is announced, and only after a minimum delay can it take effect. The delays are bridge constants, longer for administrator and guardian changes than for other operations. This gives operators time to react to a hostile or mistaken change.

## Distributed halting

The orchestrators halt the bridge on their own when any of them observes a redeem on a destination network with no corresponding request on the source network. The signer enters `EmergencyState`, the group signs a halt message in a ceremony, and one signer submits it on each network. Halting needs the same super-majority as any other signature.

On EVM networks the halt is an ordinary transaction sent from **this signer's EVM address**, which is derived from the producer key and shown as `evmAddress` in the [health API](health-api.md). That address must hold enough of the network's gas token to send a transaction, or the signer cannot participate in a halt. Check the balance as part of routine monitoring.

Only the administrator and the orchestrators can halt. Lifting a halt is an administrator action and takes effect only after the bridge's minimum unhalt duration.

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

*Adapted from the HyperCore-Team documentation for the NoM multi-chain infrastructure, [hypercore-team.github.io](https://hypercore-team.github.io/). GPL v3.*
