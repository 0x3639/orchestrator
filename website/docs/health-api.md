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

## Legacy remote monitoring

For existing remote scrapers, keep `Address` at `127.0.0.1` and put an authenticated reverse proxy in front. New deployments can use the local socket described below. The proxy provides authentication, TLS and connection limits, none of which the orchestrator has. Setting `Address` to `0.0.0.0` makes the endpoint reachable by anyone who can route to the machine.

The listener has header, read, write and idle timeouts, so slow clients cannot pin connections.

## Local v1 snapshot

Set `HealthConfig.SnapshotSocket` to a clean absolute Unix socket path, for example
`/run/orchestrator-health/health.sock`. An empty setting disables collection and
the local listener. The orchestrator serves `getHealthSnapshot` on this socket
during startup, before chain discovery or the wait for Zenon sync. A socket setup
failure is logged and the bridge continues starting.

Create the parent directory on each boot. It must be owned by the orchestrator
account, with no access for other users and no group write permission. On Linux,
a systemd tmpfiles entry like the following assigns a dedicated reporter group
and makes new sockets inherit that group. Create the group first and add the
reporter service account to it. The reporter needs no access to the signer’s
data directory or keys.

```text
d /run/orchestrator-health 2750 orchestrator orchestrator-health -
```

The socket has mode `0660`. Local readers share the aggregate request limit. The per-IP limit applies to TCP clients. Query it locally with:

```bash
curl --unix-socket /run/orchestrator-health/health.sock \
  -H 'Content-Type: application/json' \
  -d '{"method":"getHealthSnapshot","params":[]}' http://localhost/
```

The response uses the existing `result` and `error` envelope. The
[snapshot schema](https://github.com/0x3639/orchestrator/blob/dev/health/schemas/v1/snapshot.schema.json)
describes the object in `result`. The TCP listener continues to serve the three
legacy methods. Keep the Unix socket private when retiring public health RPC.

- **Startup and sync.** Read `phase`, `bridgeState`, and build version and commit.
  `phase` stays `syncingBridge` until the bridge account has been read to its
  tip and every EVM network has finished its startup scan, including while a
  [startup is held](operations/states.md#a-held-startup). Until it is `running`
  the signer signs no transfer; with `bridgeState` `EmergencyState` it is
  signing the halt.
  Zenon reports provider sync state, the observed frontier momentum, and the
  processed bridge-account receive height. These heights have different units.
  Each EVM chain reports provider sync state, observed head and the inclusive
  scan cursor after a successful write. For an overall sync verdict the collector
  needs independent chain references. A legacy Zenon cursor, a gap in the
  current run, or a restart beyond the last verified checkpoint produces partial
  receive coverage. An orderly shutdown saves a complete receive checkpoint
  independently of the bridge cursor. Matching checkpoints retain coverage
  across restarts.
- **Held listener.** `zenon.receiveHold` is `null` while the bridge-account
  listener advances or idles. When it cannot get past one account height it
  carries that `height`, the time the hold began (`since`) and the number of
  `attempts`. The listener reads nothing after a held height, so alert on any
  hold that lasts more than a few minutes. After ten minutes at one height a
  signer that is signing transfers stops itself; see [Signer states](operations/states.md). A
  receive that holds startup is reported in the same field. The field is
  absent from snapshots of earlier builds and present, as `null` or as an
  object, in every snapshot of this one. A collector that validates against a copy of the
  earlier schema rejects the extra property, so update the collector's schema
  before upgrading the first signer.
- **Pending work.** Each direction reports finality waits, unsigned observations,
  the actual selection for a signing window, signed records awaiting submission,
  and the last successfully persisted signing window. Each measurement carries
  its own `asOf` and `quality`. The first page of 100 wraps can yield partial
  counts, including zero. Pending age uses a bounded sample of locally observed
  unsigned IDs and is partial. IDs stay in memory. Before a worker has read a
  source, its observation is unknown.
- **Cost.** The socket copies cached state under a read lock and releases the
  lock before encoding the response. Reads cause no chain calls, database scans
  or signing. Queue counts reuse work already read by the signing and submission
  paths. With the feature enabled, a background worker checks each provider’s
  sync status at most once a minute, with a two-second deadline. Zenon uses a
  separate connection for this check. EVM sampling yields when the existing RPC
  rate budget is busy and also samples the local confirmation queue depth.

## Outbound heartbeat contract

A separate reporter on the operator host will read the socket and send the
`result` object to a collector on a monitoring host. The planned collector will accept the
[heartbeat schema](https://github.com/0x3639/orchestrator/blob/dev/health/schemas/v1/heartbeat.schema.json)
at `POST /v1/heartbeats`. The two schemas use stable URNs. Load both into the
validator and enable format checks. The fixtures under `health/schemas/v1/testdata`
can be shared with reporter and collector implementations. This release provides
the local API and wire contracts. Sending and receiving heartbeats are separate
service deliverables.

**Enrollment.** For each reporter, generate a private key and a certificate
signing request under its own OS account. Send the request to the monitoring
administrator through an established operator contact. The administrator
verifies the request fingerprint with that contact, assigns a node and stream,
and signs a short-lived client certificate with the monitoring certificate
authority (CA) and the TLS client-authentication usage. Return the certificate,
collector URL, stream ID, and any required CA trust bundle over an authenticated
channel. Keep the private key readable only by the reporter. For renewal, issue
a replacement on the same stream. Disable the enrollment or revoke the leaf
certificate immediately if the key is lost.

**Ingestion.** Require mutual TLS and verify server identity at the reporter.
At the collector, check the authenticated certificate’s active enrollment and
its assigned stream on every request. A stream ID or a copied certificate alone
cannot authenticate a sender. Where a reverse proxy terminates TLS, restrict
the backend to that trusted proxy and strip any client-supplied certificate
identity headers. Apply connection and handshake limits at the edge, then
a 64 KiB body limit, per-stream and global rates, schema validation, timestamp
and duplicate-key checks, and durable replay protection. An identical retry
cannot extend liveness. A site-wide outage is a collector alert. Bridge
signing has no dependency on this service.

## Moving operators off public RPC

1. Release the orchestrator with the socket, then deploy the collector and the
   separate reporter. Pin both services' contract tests to a tagged orchestrator
   schema version. Enroll one pilot operator and enable the socket on its host.
2. Run heartbeats alongside the existing scrape. Check the node and stream
   assignment, version, sync heights and queue counts. Stop the reporter to
   confirm the collector marks the stream stale. Restart the reporter with the
   orchestrator stopped to check that it reports a failed local read. Schedule
   the orchestrator stop during a maintenance window or use a test node.
3. After both paths agree on the pilot, enroll the remaining operators in
   batches. Switch each operator's dashboard and alerts to its heartbeat stream
   after successful delivery and freshness checks.
4. For each migrated host, bind the legacy health listener to loopback, remove
   its public proxy route and firewall rule, and retire that host's scrape job.
   Verify the old address is unreachable externally and new heartbeats continue.
