---
title: Local sync checkpoints
description: "Save a known-good signer event store and pending queues, then replay only the history after that checkpoint."
---

# Local sync checkpoints

A checkpoint saves the orchestrator's Zenon and EVM event databases, their scan positions, and every pending EVM queue together. Restoring it lets this signer replay the tail from its saved positions. The standalone Python 3 helper works with the upstream Linux release `v0.0.9a-alphanet` and this checkout. It runs offline and requires no producer passphrase. [Upstream release](https://github.com/HyperCore-Team/orchestrator/releases/tag/v0.0.9a-alphanet)

## Choose a healthy point

Before taking a checkpoint, compare the signer's `getStatus` with a healthy peer at the same signing window. Record the time, build, chain ID, `latestUpdateHeight`, `unwrapsToSign` and `unwrapsHash`. Prefer a point when the signer is caught up and its pending work agrees with peers. Keep more than one named checkpoint so an earlier one is available if divergence is discovered later. A file checksum proves that a copy is intact. Agreement with chain history is a separate check. [Signing-stall diagnosis](signing-stalls.md)

## Create a checkpoint

Install the helper from this checkout on the signer host. These commands assume the unit and service account are both named `orchestrator`. Substitute the actual account on your host. The example checkpoint name identifies the last healthy Ethereum height.

```bash
sudo install -m 0755 tools/sync_checkpoint.py /usr/local/bin/orchestrator-checkpoint
sudo systemctl stop orchestrator
sudo -u orchestrator -H orchestrator-checkpoint create eth-21000000
sudo systemctl start orchestrator
sudo -u orchestrator -H orchestrator-checkpoint verify eth-21000000
```

The default destination is `~/.orchestrator/sync-backups/NAME` in the service user's home. Names are unique. The manifest covers the file list, empty directories and SHA-256 hashes. Creation requires the signer to be stopped. Verification reads the saved copy and can run while the signer is online. Only `events/` and `queues/` enter the checkpoint. Back up the producer key and TSS share separately under your existing key-management procedure.

The helper accepts `--data-dir PATH` for the directory that actually contains `events/` and `queues/`. Upstream stores them under the service user's home even when `config.json` sets a different `DataPath` for the signer lock. The helper honors that lock. If the configuration uses relative paths for TSS files or a passphrase file, invoke the helper from the service's working directory. Keep the two sync trees as regular directories on the same filesystem as their parent. Allow space for one full copy per checkpoint and one additional working copy during restore.

## Restore this signer

Save the incident logs and status first. Choose a checkpoint **from before the first missed or disputed event**. A newer one can preserve the same problem. Recover one signer at a time using its own checkpoint from the same bridge deployment.

:::caution Restore only checkpoints you created on your own signer
Never import or restore a checkpoint produced by another operator or an untrusted host. The SHA-256 manifest verifies file integrity against accidental corruption, **not** origin or authenticity; a tampered checkpoint can alter local event history or scan cursors.
:::

```bash
sudo systemctl stop orchestrator
sudo -u orchestrator -H orchestrator-checkpoint restore eth-21000000
# Start only after restore reports success.
sudo systemctl start orchestrator
```

After a restore the signer replays the bridge account from the checkpoint's position. If the checkpointed database holds the record of the account height it vouches from, the signer checks the source event of every unwrap request it replays at or above that height. Start it against EVM endpoints that serve receipts and logs back to the checkpoint. A database without the record, such as one that only ever ran an earlier build or stopped during its first sync, is replayed as a first read. [Signer states](states.md#the-bridge-account-listener)

Restore verifies and stages both trees before replacing either. It keeps the previous active data under `~/.orchestrator/pre-restore-*/previous/` and prints the exact path. Keep the producer key, configuration, TSS key shares and the development branch's `unwrap-pool-votes` database in their usual locations outside the sync directories. The signer needs its current votes even when replaying older sync data. If restore is interrupted, keep the service stopped and repeat the **same** command. During the switch a persistent marker at the signer's instance lock prevents it from starting against a mixture of old and new stores. Leave the lock and recovery directory in place for the helper.

Watch the first scan start at the saved height. After catch-up, compare pending counts and hashes with peers again in the same ceremony window. Recovery still requires the RPC provider to serve the history after the checkpoint. A checkpoint taken after divergence, a lagging endpoint, or a recurring upstream processing error can leave the signer out of agreement again.
