---
sidebar_position: 1
title: Overview
description: "What the Zenon bridge orchestrator is, the two jobs a signer does, how a node is wired, and where to start."
---

# Overview

The orchestrator is the signer node of the Zenon bridge. Each participating Pillar operator runs one. Together the orchestrators hold a threshold-signature (TSS) key, and the bridge contracts on Zenon and on each EVM network release assets only against that group's signature. [How the bridge works](architecture.md) explains the design; this guide covers running a node.

An orchestrator does two jobs:

- **Signs wraps**, Zenon to an EVM chain. It watches Zenon for wrap requests, signs them with the group, and one member submits the signature.
- **Signs unwraps**, an EVM chain back to Zenon. It watches the bridge contract on each EVM network for `Unwrapped` events, waits for finality, signs them with the group, and one member submits the request to Zenon.

It also watches for transfers that should not exist and, with the other signers, halts the bridge when it finds one. See [Security model](security-model.md).

## How a signer is wired

```
                 ┌───────────────┐        ┌──────────────────┐
  Zenon node ◄──►│               │◄──────►│ EVM endpoint(s)  │
  (ws://:35998)  │  orchestrator │        │ per network      │
                 │               │        └──────────────────┘
                 │  ~/.orchestrator/
                 │    config.json   producer   events/   queues/   tss/
                 └──────┬────────┘
                        │ :55055 libp2p (TSS peers)
                        │ :55000 health RPC (loopback)
```

- **`config.json`** holds every setting. The orchestrator rewrites it on each start with mode `0600`.
- **`producer`** is the encrypted Zenon producer key file. Its passphrase unlocks the key that identifies this signer and derives its EVM address.
- **`events/`** is a LevelDB store per network with the wrap and unwrap records this signer knows about, and the sync cursor.
- **`queues/`** is a persistent queue of live unwrap events awaiting finality.
- **`tss/`** holds this signer's share of the group key. Back it up; see [Install](install.md).

## About this guide

This guide documents the orchestrator build maintained by 0x3639 and describes the software as it behaves in that build. Version history, and what to do when moving a running signer between versions, is kept on one page, [Upgrade notes](upgrade-notes.md). Adapted material is credited on the page that uses it and on the [Attribution](attribution.md) page.

## Read next

1. [Install](install.md), then [Configuration](configuration.md).
2. [Producer key and passphrase](secrets.md) before the first start.
3. [EVM networks](networks.md) to choose endpoints and size provider limits before the first sync.
