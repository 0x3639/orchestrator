---
sidebar_position: 6
title: Health API
description: "The orchestrator's health RPC: request format, status codes, getStatus, getBuildInfo and getIdentity, rate limits, and how to expose it safely."
---

# Health API

A small JSON-RPC-style endpoint for monitoring. It is **unauthenticated**, which is why it listens on loopback by default. Anything that can reach the port can read the signer's identity, peer list and pending work.

## Request format

- Method `POST`, header `Content-Type: application/json`.
- Body is one JSON object, at most 4 KiB: `{"method": "<name>", "params": []}`. No method takes parameters.

```bash
curl -s -X POST http://127.0.0.1:55000 \
  -H 'Content-Type: application/json' \
  -d '{"method":"getStatus","params":[]}'
```

Responses are `{"result": ..., "error": ""}` or `{"result": null, "error": "<message>"}`.

| Status | Meaning |
| --- | --- |
| `405` | Not a `POST` |
| `415` | Content type is not `application/json` |
| `413` | Body over 4 KiB |
| `400` | Malformed JSON, more than one JSON value, or a non-empty `params` array |
| `404` | Unknown method |
| `429` | Rate limited, see below |
| `500` | The method itself failed; `error` says why |

Requests are validated before they consume any rate budget, so malformed traffic cannot starve real probes.

## Methods

### `getStatus`

Cached for `CachedResponseDelay` seconds.

```json
{
  "state": 0,
  "stateName": "LiveState",
  "frontierMomentum": 8523311,
  "networks": {
    "Ethereum": {
      "chainId": 1,
      "networkClass": 2,
      "contractAddress": "0x…",
      "contractDeploymentHeight": 17000000,
      "estimatedBlockTime": 12,
      "confirmationsToFinality": 64,
      "latestUpdateHeight": 21003400,
      "wrapsToSign": 0,
      "wrapsHash": "00",
      "unwrapsToSign": 2,
      "unwrapsHash": "9f3c…"
    }
  },
  "peersLen": 6,
  "peers": ["12D3Koo…"]
}
```

`unwrapsHash` is a digest of the identities (transaction hash and log index) of every unwrap event this signer still has to sign, in order; `wrapsHash` covers the first 100 pending wraps. **Compare them across signers.** Differing hashes mean the signers' backlogs differ. That can block a ceremony when the difference falls inside the pool taken from the front of the backlog; stale entries that Zenon already knows are cleared by reconciliation before signing and never block one. Matching hashes rule that out but do not guarantee a ceremony: the signers must also agree on the group membership in `LocalPubKeys` and be able to reach each other. See [Signing stalls](operations/signing-stalls.md).

`latestUpdateHeight` is the EVM sync cursor. It should track the chain head within a few minutes; a stalled value means sync is failing, see [First sync](operations/first-sync.md).

### `getBuildInfo`

```json
{"version": "v0.0.9a", "gitCommit": "6ca150e…", "goVersion": "go1.22.1"}
```

### `getIdentity`

```json
{
  "producer": "z1q…",
  "pillarName": "…",
  "tssPeerPubKey": "…",
  "tssPeerId": "12D3Koo…",
  "evmAddress": "0x…"
}
```

## Rate limits

Two token buckets, both configured under `HealthConfig`:

- **Per client**, keyed by the connecting IP: `PerClientResponsesPerSecond` and `PerClientBurst`. Forwarding headers are ignored, so every caller behind one proxy shares a bucket.
- **Aggregate** across all clients: `ResponsesPerSecond` and `Burst`.

A request is charged only after it passes validation and names a known method.

## Exposing it remotely

Keep `Address` at `127.0.0.1` and put a reverse proxy in front for remote monitoring. The proxy provides authentication, TLS and connection limits, none of which the orchestrator has. Setting `Address` to `0.0.0.0` makes the endpoint reachable by anyone who can route to the machine.

The listener has header, read, write and idle timeouts, so slow clients cannot pin connections.
