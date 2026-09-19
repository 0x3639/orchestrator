---
sidebar_position: 7
title: Upgrade notes
---

# Upgrade notes

Behaviour that changed between upstream `v0.0.9a` and this fork's `dev` branch. Read before upgrading a running signer.

## Health RPC listens on loopback

The health endpoint bound to every interface. It now binds to `127.0.0.1` unless `HealthConfig.Address` is set. A remote monitoring probe that worked before will get connection refused until you either put a proxy in front or set `Address` explicitly. It also enforces `POST`, `application/json`, a 4 KiB body and a per-client rate cap. See [Health API](health-api.md).

## Passphrase handling

`ProducerKeyFilePassphrase` in `config.json` still works and is preserved as written, with a warning. Prefer `ORCHESTRATOR_PRODUCER_PASSPHRASE` or a passphrase file. Empty passphrases are no longer accepted from any source. See [Producer key and passphrase](secrets.md).

## File permissions and ownership checks

`config.json` is written `0600`, the data directory is tightened to `0700`, and the data directory, config file, producer key file and passphrase file must be owned by the running user. Deployments that run as root over a directory owned by another user, or mount secrets with a foreign owner, fail at startup with a clear message.

## Logging

The two whole-configuration log lines at startup are gone. Configured RPC URLs and their credentials are scrubbed from every log line. If you relied on grepping the full config from logs, use `config.json` instead.

## Sync and signing

- Requests per EVM endpoint can be capped with `RpcRequestsPerSecond` and `RpcBurst`. **Existing configs are uncapped until you add the fields.**
- Sync retries failed ranges in place and logs progress.
- Queued unwrap events are never dropped on transport errors; discards need every endpoint to agree the block was reorged out.
- The signing pool is reconciled against Zenon before each ceremony. A signer that fell out of sync usually recovers by itself within a few ceremony windows.
- `--evm.backfill-blocks N` replaces the events/queues wipe as the repair procedure. See [Backfill](operations/backfill.md).

## Configure two EVM endpoints

Several safety decisions, discarding a queued event, storing a historical log, pruning during backfill, now require agreement from every configured endpoint. With one endpoint that is one provider's word. Add a second, independently operated, full-history endpoint to each EVM network. See [EVM networks](networks.md).

## Release builds

Only `master` publishes binaries to the GitHub release. `dev` builds upload as workflow artifacts and never overwrite released assets.
