---
sidebar_position: 3
title: Security model
description: "The bridge's security model for operators: request and redeem, time challenges, distributed halting, administrator and guardians, and what to monitor."
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

Historical EVM scans apply the same check. For an apparent discrepancy, the signer checks the registration against every configured EVM endpoint and checks the wrap against each synced Zenon endpoint. An inconclusive lookup leaves the scan cursor in place for a retry. A confirmed discrepancy records the EVM log and Zenon lookup result in the local Zenon database and starts the halt procedure. After a restart the signer restores pending incidents before signing. The signer acknowledges the incident only after Zenon and every configured EVM contract report their explicit halt flags. An incident reported during runtime network startup stays pending until that chain also confirms its halt. The signer then resumes the scan and retains the evidence. A config saved during a redeem-only emergency records the halted state. Pending database evidence restores the emergency on restart, and an independently recorded emergency stays in effect.

A signer that replays history does not halt again for an unmatched registration that a completed halt already covered. It reads the contract state on every EVM endpoint, by block hash, at the block that is confirmation-deep below the lowest head any of them reports. If they all report the redeem closed (paid or revoked) and an administrator unhalt after the registration, the signer archives the evidence and continues the scan. A stored incident is released the same way, and a signer that restarts with one checks it in its first scan, before it acts on the restored emergency. A redeem that no endpoint shows settled in its latest state is not settled, and is reported without reading any older state. When it does look settled, an answer that is not unanimous, a closure or unhalt that is not final yet, or an endpoint that cannot serve state at that depth decides nothing: the scan stays on that range and retries, so use EVM endpoints that serve contract state at the confirmation depth. Every other unmatched registration holds the signer in `EmergencyState` until the halt flags are confirmed. That includes a stored incident whose settlement cannot be confirmed yet: it holds the signer like any other until the evidence is available or the operator acknowledges it.

An operator who has reviewed an incident with the administrator can release it on one signer by restarting that signer with `--redeem.acknowledge chainId:transactionHash:logIndex`. The flag can be repeated. The acknowledgement is stored with the evidence, so later starts do not need the flag. It does not depend on reading the contract state, changes nothing on chain, and does not stop a redeem that can still be paid.

A registration that is not final yet is checked when the subscription delivers it and whenever a range scan meets it. A check that cannot be confirmed on every endpoint is kept and repeated every few seconds until it can; nothing is decided on partial evidence, so an endpoint that stays unavailable keeps the check open. The signer logs a warning when it starts holding a registration and an error each minute it stays unchecked. Live subscriptions can raise a verified discrepancy during the challenge window. The signer writes redeem statuses only after the configured confirmation depth and agreement on the canonical block and event. It saves the ordinary scan height and the finalized redeem checkpoint together, so a restart revisits the unconfirmed blocks even when the ordinary scan reached the head. Redeem statuses and wrap rows are written with a sync, so they are on disk before that checkpoint can pass the logs they came from, and an explicit rewind keeps a finalized redeem checkpoint that is already below its target. On chains with provisional statuses written by the earlier PR build, the signer replays redeem events from contract deployment. Signatures and incident evidence are preserved.

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
