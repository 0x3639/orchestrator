---
sidebar_position: 9
title: Upgrade notes
---

# Upgrade notes

The rest of this guide describes the orchestrator as it behaves in the 0x3639 build. This page is the changelog: what differs from the upstream HyperCore-Team release and what to do about it when upgrading a running signer.

## dev, unreleased: changes since upstream v0.0.9a

### Breaking for existing deployments

- **Health RPC listens on loopback.** The endpoint bound to every interface; it binds to `127.0.0.1` unless `HealthConfig.Address` is set. A remote monitoring probe that worked before gets connection refused until you either put a proxy in front or set `Address` explicitly. The endpoint also enforces `POST`, `application/json`, a 4 KiB body and per-client rate caps. See [Health API](health-api.md).
- **Ownership and permission checks.** `config.json` is written `0600`, the data directory is tightened to `0700`, and the data directory, config file, producer key file and passphrase file must be owned by the running user. Deployments that run as root over a directory owned by another user, or mount secrets with a foreign owner, fail at startup with a message naming the path and both ids.
- **Empty passphrases are rejected** from every source.
- **Two EVM endpoints per network are expected.** Discarding a queued event, storing a historical log and pruning during backfill require agreement from every configured endpoint. With one endpoint that is one provider's word, and startup warns. See [EVM networks](networks.md).

### New configuration

- `ORCHESTRATOR_PRODUCER_PASSPHRASE` and `ProducerKeyFilePassphraseFile` as passphrase sources; `ProducerKeyFilePassphrase` in `config.json` still works, with a warning. See [Producer key and passphrase](secrets.md).
- `HealthConfig.Address`, `PerClientResponsesPerSecond`, `PerClientBurst`.
- `RpcRequestsPerSecond` and `RpcBurst` per EVM network. **Existing configs are uncapped until you add the fields**; new configs default to 3 per second, burst 5.
- `--evm.backfill-blocks N` command-line flag. See [Backfill](operations/backfill.md).

### Behaviour changes

- Startup no longer logs the whole configuration; an allowlisted summary is logged instead, and configured RPC URLs with their credentials are scrubbed from every log line.
- Sync retries a failed block range in place with backoff instead of abandoning the pass, freezes the range across retries, and logs progress.
- Queued unwrap events are never dropped on transport errors; a discard needs every endpoint to agree the block was reorged out, three times over.
- Duplicate event deliveries cannot reset a signed or sent record; the sync cursor only moves forward.
- The signing pool is reconciled against Zenon before each ceremony, so a signer that fell out of sync usually recovers by itself within a few ceremony windows, and a rebuilt event store does not re-sign history.
- Historical unwrap logs are validated against the endpoint-agreed canonical block before being stored.
- Bridge-contract reads share the per-endpoint request cap and carry a deadline.
- The `SHA256CHECKSUMS.txt` release asset has one digest per line; releases up to v0.0.9a wrote both on one line.

### Release builds

Only `master` publishes binaries to the GitHub release. `dev` builds upload as workflow artifacts and never overwrite released assets.

### Upgrade checklist

1. Add `RpcRequestsPerSecond` and `RpcBurst` to each EVM network, and a second independent endpoint.
2. Decide how the passphrase is supplied and, if moving it out of `config.json`, set the environment variable or passphrase file before restarting.
3. If anything probes the health API remotely, set `HealthConfig.Address` or add a proxy.
4. Check that the data directory and its files are owned by the user the service runs as.
5. Restart. If the signer had been stalling on unwraps, watch for `reconcile:` lines; if `unwrapsHash` still differs from peers after a few windows, run a [backfill](operations/backfill.md).
