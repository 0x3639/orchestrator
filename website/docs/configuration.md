---
sidebar_position: 3
title: Configuration
description: "Reference for every field in the orchestrator's config.json: data path, networks, TSS, health RPC, producer key, with defaults and caveats."
---

# Configuration

Everything lives in `~/.orchestrator/config.json`. On first start the orchestrator writes a default file; on every start it re-reads the file, fills in defaults for omitted fields, and rewrites it with mode `0600` inside a `0700` data directory.

## Complete example

```json
{
    "DataPath": "/home/orchestrator/.orchestrator",
    "GlobalState": 0,
    "Networks": {
        "Zenon": {
            "Urls": ["ws://127.0.0.1:35998"]
        },
        "BNB Chain": {
            "Urls": ["wss://bnb-a.example/v3/KEY", "wss://bnb-b.example/KEY"],
            "FilterQuerySize": 100,
            "RpcRequestsPerSecond": 2,
            "RpcBurst": 3
        },
        "Ethereum": {
            "Urls": ["wss://eth-a.example/v3/KEY", "wss://eth-b.example/KEY"],
            "FilterQuerySize": 100,
            "RpcRequestsPerSecond": 2,
            "RpcBurst": 3
        }
    },
    "TssConfig": {
        "Port": 55055,
        "Bootstrap": "",
        "BaseDir": "/home/orchestrator/.orchestrator/tss"
    },
    "HealthConfig": {
        "Address": "127.0.0.1",
        "Port": 55000,
        "CachedResponseDelay": 25,
        "ResponsesPerSecond": 2,
        "Burst": 2,
        "PerClientResponsesPerSecond": 2,
        "PerClientBurst": 2
    },
    "ProducerKeyFileName": "producer",
    "ProducerKeyFilePassphraseFile": "",
    "ProducerIndex": 0
}
```

## Top level

| Field | Default | Meaning |
| --- | --- | --- |
| `DataPath` | `~/.orchestrator` | **Leave at the default.** The orchestrator always reads `~/.orchestrator/config.json` of the running user at startup; there is no flag to point it elsewhere, despite what one log message suggests. A different `DataPath` inside that file only changes where the producer key is read from, where the instance lock file is placed and where the file is rewritten to, while `events/`, `queues/` and `logs/` stay under `~/.orchestrator` and `TssConfig.BaseDir` is independent. The directory is created `0700`, an existing wider mode is tightened, and it must be owned by the running user. |
| `GlobalState` | `0` | Persisted node state. Managed by the orchestrator; do not edit. See [States](operations/states.md). |
| `EvmAddress` | derived | The signer's EVM address, derived from the producer key. Written for information. |
| `ProducerKeyFileName` | `producer` | Name of the encrypted key file inside `DataPath`. |
| `ProducerKeyFilePassphrase` | empty | Passphrase stored in the file. See [Producer key and passphrase](secrets.md) for the safer sources. |
| `ProducerKeyFilePassphraseFile` | empty | Path to an owner-only file holding the passphrase. |
| `ProducerIndex` | `0` | Derivation index of the producer key inside the key file. |

## Networks

`Networks` is a map from network name to settings. `Zenon` is required. At startup the orchestrator reads the networks registered on the Zenon bridge contract and requires an entry here **for each of them**, by name; startup fails if one is missing. Entries for names the bridge does not register are ignored.

The file is loaded on top of the built-in defaults, which contain `Zenon` (`ws://127.0.0.1:35998`) plus `BNB Chain` and `Ethereum` entries pointing at `ws://127.0.0.1:8545` (sized for dRPC's free tier: `FilterQuerySize: 100`, `RpcRequestsPerSecond: 2`, `RpcBurst: 3`). Leaving one of those networks out of your file therefore does not remove it; the default entry remains and is rewritten into the file.

| Field | Default | Meaning |
| --- | --- | --- |
| `Urls` | required | Endpoints, tried in order at startup. For EVM networks, list two or more independent providers; see [EVM networks](networks.md). |
| `FilterQuerySize` | `100` (built-in `BNB Chain`, `Ethereum`); `0` for custom networks | Blocks per `eth_getLogs` query during sync. `0` falls back to `2000` at runtime. |
| `RpcRequestsPerSecond` | `2` (built-in `BNB Chain`, `Ethereum`); `0` for custom networks | Request cap per endpoint. `0` disables rate limiting (for example against a local full node). |
| `RpcBurst` | `3` (built-in `BNB Chain`, `Ethereum`); `0` for custom networks | Requests allowed at once before the cap applies. |

:::caution Explicit `0` runs uncapped
Omitted fields in a built-in network entry inherit their defaults, while an explicit `"RpcRequestsPerSecond": 0` in `config.json` leaves that network uncapped. If an earlier build wrote `"RpcRequestsPerSecond": 0` into your file, remove those fields or set explicit limits before restarting.
:::

## TssConfig

| Field | Default | Meaning |
| --- | --- | --- |
| `Port` | `55055` | libp2p port for TSS peers. Must be reachable by the other signers; firewall inbound access to known Pillar IPs until `go-tss`/`go-libp2p` is upgraded. |
| `Bootstrap` | empty | Multiaddr of a peer to bootstrap from. |
| `BaseDir` | `~/.orchestrator/tss` | Where key shares are stored. Independent of `DataPath`. |
| `PublicKey`, `DecompressedPublicKey`, `LocalPubKeys`, `PubKeyWhitelist` | managed | Written by the orchestrator after key generation. Do not edit. |
| `BaseConfig` | managed | Ceremony timeouts. Overridden at runtime by bridge metadata. |

## HealthConfig

| Field | Default | Meaning |
| --- | --- | --- |
| `Address` | `127.0.0.1` | Interface the health RPC listens on. Loopback keeps the unauthenticated endpoint off the network. Set `0.0.0.0` only behind a proxy. |
| `Port` | `55000` | Health RPC port. |
| `CachedResponseDelay` | `25` | Seconds a `getStatus` result is cached. |
| `ResponsesPerSecond`, `Burst` | `2`, `2` | Aggregate rate cap across all clients. Both must be positive; `0` rejects every request. |
| `PerClientResponsesPerSecond`, `PerClientBurst` | `2`, `2` | Cap per client IP. `PerClientResponsesPerSecond: 0` disables per-client limiting; a positive rate with `PerClientBurst: 0` rejects every request. |

See [Health API](health-api.md) for what the endpoint serves.
