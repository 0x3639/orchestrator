---
sidebar_position: 3
title: Configuration
---

# Configuration

Everything lives in `~/.orchestrator/config.json`. On first start the orchestrator writes a default file; on every start it re-reads the file, fills in defaults, and rewrites it with mode `0600` inside a `0700` data directory. Fields you leave out take their defaults, with one exception noted under [EVM networks](#networks).

## Complete example

```json
{
    "DataPath": "/home/orchestrator/.orchestrator",
    "GlobalState": 0,
    "Networks": {
        "Zenon": {
            "Urls": ["ws://127.0.0.1:35998"]
        },
        "Ethereum": {
            "Urls": ["wss://eth-a.example/v3/KEY", "wss://eth-b.example/KEY"],
            "FilterQuerySize": 2000,
            "RpcRequestsPerSecond": 3,
            "RpcBurst": 5
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
| `DataPath` | `~/.orchestrator` | Directory for `config.json` and the producer key file. Created `0700`; an existing wider mode is tightened. Must be owned by the user running the orchestrator. **Only those two files follow this setting**: `events/`, `queues/` and `logs/` are always created under `~/.orchestrator` of the running user, and `TssConfig.BaseDir` defaults to `~/.orchestrator/tss` independently. Leave `DataPath` at its default unless you also move `BaseDir` and accept the split. |
| `GlobalState` | `0` | Persisted node state. Managed by the orchestrator; do not edit. See [States](operations/states.md). |
| `EvmAddress` | derived | The signer's EVM address, derived from the producer key. Written for information. |
| `ProducerKeyFileName` | `producer` | Name of the encrypted key file inside `DataPath`. |
| `ProducerKeyFilePassphrase` | empty | Passphrase stored in the file. See [Producer key and passphrase](secrets.md) for the safer sources. |
| `ProducerKeyFilePassphraseFile` | empty | Path to an owner-only file holding the passphrase. |
| `ProducerIndex` | `0` | Derivation index of the producer key inside the key file. |

## Networks

`Networks` is a map from network name to settings. `Zenon` is required. At startup the orchestrator reads the networks registered on the Zenon bridge contract and requires an entry here **for each of them**, by name; startup fails if one is missing. Entries for names the bridge does not register are ignored.

The file is loaded on top of the built-in defaults, which already contain `BSC`, `Ethereum` and `Supernova` entries pointing at localhost. Leaving a network out of your file therefore does not remove it; the default entry remains and is rewritten into the file. The example above shows only the entries you should edit.

| Field | Default | Meaning |
| --- | --- | --- |
| `Urls` | required | Endpoints, tried in order at startup. For EVM networks, list two or more independent providers; see [EVM networks](networks.md). |
| `FilterQuerySize` | `2000` | Blocks per `eth_getLogs` query during sync. `0` falls back to `2000`. |
| `RpcRequestsPerSecond` | `0` (uncapped) | Request cap per endpoint. New configs default to `3`. |
| `RpcBurst` | `0` | Requests allowed at once before the cap applies. New configs default to `5`. |

:::caution Existing configs are not capped automatically
Network entries are replaced wholesale when the file is read, so an entry without `RpcRequestsPerSecond` is uncapped even though new files default to `3`. Add the field to each EVM network to opt in.
:::

## TssConfig

| Field | Default | Meaning |
| --- | --- | --- |
| `Port` | `55055` | libp2p port for TSS peers. Must be reachable by the other signers. |
| `Bootstrap` | empty | Multiaddr of a peer to bootstrap from. |
| `BaseDir` | `DataPath/tss` | Where key shares are stored. |
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
