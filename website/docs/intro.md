---
sidebar_position: 1
title: Overview
---

# Overview

The orchestrator is the signer node of the Zenon bridge. A set of pillar operators each run one, and together they form a threshold-signature (TSS) group that signs two kinds of transfer:

- **Wraps**, Zenon to an EVM chain. The orchestrator watches Zenon for wrap requests, signs them as a group, and one member submits the signature.
- **Unwraps**, an EVM chain back to Zenon. The orchestrator watches the bridge contract on each EVM network for `Unwrapped` events, waits for finality, signs them as a group, and one member submits the request to Zenon.

This site documents the **0x3639 fork** of the orchestrator, which carries operational fixes on top of the upstream HyperCore-Team code:

| Area | What changed | Where to read |
| --- | --- | --- |
| Health RPC | Binds to loopback by default, enforces request shape, rate-limits per client | [Health API](health-api.md) |
| Secrets | Producer passphrase can live outside `config.json`; files are owner-only | [Producer key and passphrase](secrets.md) |
| Logging | Startup config is allowlisted; RPC URLs and credentials are redacted at the log sink | [Upgrade notes](upgrade-notes.md) |
| Sync | Per-endpoint request cap, retry-in-place, progress logging | [First sync](operations/first-sync.md) |
| Signing | Signers keep their unwrap event sets converged; bounded backfill replaces the hard reset | [Signing stalls](operations/signing-stalls.md) |

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
- **`tss/`** holds the TSS key shares from the last key generation.

## Read next

1. [Install](install.md), then [Configuration](configuration.md).
2. [Producer key and passphrase](secrets.md) before the first start.
3. [EVM networks](networks.md) to size your provider settings before the first sync.
