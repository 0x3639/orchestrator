---
sidebar_position: 5
title: Emergency reset
---

# Emergency reset

The old recovery procedure. Keep it for corruption of `events/` or `queues/`, or divergence that a [backfill](backfill.md) cannot reach. It is not the normal fix for a [signing stall](signing-stalls.md) any more.

:::danger
This rescans every EVM network from the bridge contract's deployment block. Set `RpcRequestsPerSecond` first, or the provider will throttle the scan and it can take hours. See [EVM networks](../networks.md).
:::

```bash
systemctl stop orchestrator
cp -a ~/.orchestrator/events ~/.orchestrator/events.bak-$(date +%F)
cp -a ~/.orchestrator/queues ~/.orchestrator/queues.bak-$(date +%F)
rm -rf ~/.orchestrator/events ~/.orchestrator/queues
systemctl start orchestrator
```

What happens next:

- Every historical unwrap is stored again as unsigned. Unlike the old code, the first ceremony reconciles them against Zenon and marks the ones Zenon already has, so history is **not** re-signed. Expect a few ceremony windows of `reconcile:` log lines while the backlog clears.
- The producer key, `config.json` and the `tss/` key shares are untouched. Never delete `tss/`; it holds this signer's share of the group key.
