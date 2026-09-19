---
sidebar_position: 6
title: Troubleshooting
description: "Orchestrator log lines, what each one means and the action to take, plus how to compare signers and collect logs."
---

# Troubleshooting

Log lines you will meet, what they mean, and what to do.

| Log line | Meaning | Action |
| --- | --- | --- |
| `producer key passphrase not provided` | No passphrase source found | Set `ORCHESTRATOR_PRODUCER_PASSPHRASE` or `ProducerKeyFilePassphraseFile`. See [secrets](../secrets.md). |
| `… is owned by uid X but the orchestrator runs as uid Y` | Ownership check failed on the data dir, config, key or passphrase file | Fix ownership, or run as the owning user. |
| `passphrase file … must not be readable by group or others` | Passphrase file mode too wide | `chmod 0600` it. |
| `data directory … had mode 0755; restricted it to 0700` | The data directory was wider than owner-only and has been tightened | None; informational. |
| `network Ethereum has a single EVM endpoint` | Canonical-block agreement relies on one provider | Add a second independent endpoint. |
| `EVM RPC requests are not rate limited` | `RpcRequestsPerSecond` missing from this network | Add it; see [EVM networks](../networks.md). |
| `Sync … failed at block N (attempt k/12), retrying in …` | Provider rejected or timed out a query | Normal under throttling. Persistent: lower the rate or `FilterQuerySize`. |
| `… giving up until the next refresh` | 12 consecutive failures on one range | Sync resumes in 3 minutes from the same cursor. Check provider health. |
| `head N is below the range end M` | A lagging backend behind the endpoint | Retries automatically; persistent means the provider is inconsistent. |
| `canonical block N inconclusive, k of m endpoints did not answer` | An agreement endpoint is down | Fix that endpoint; nothing is decided until all answer. |
| `configured endpoints disagree on canonical block N` | Providers on different forks | Wait; if it persists one provider is broken. |
| `Event … not confirmable yet (…): attempt k` | Queued event waiting on a transient error | Normal; escalates to an error line every 10 attempts. |
| `Event … looks reorged out … agreement 1/3` | First of three checks before a discard | Normal for real reorgs; look at the reason if it repeats for many events. |
| `Skipping historical unwrap … every endpoint agrees the canonical block is …` | An orphan log from a forked backend | None; the canonical copy arrives separately. |
| `reconcile: N unsigned unwrap events already exist on Zenon` | Stale entries cleared before a ceremony | None; this is the self-heal. |
| `unwrap reconciliation incomplete for this ceremony` | Lookup budget spent | The signer sits out this window; continues next window. |
| `reconcile: cannot query Zenon` | Local Zenon RPC failed | Signer sits out the ceremony. Check the Zenon node. |
| `log subscription … lasted only …; resubscribing in …` | Provider accepts then drops subscriptions | Backoff is automatic; check the WebSocket endpoint. |
| `Too many requests, retry later` (health API `429`) | Monitoring probe exceeded the health rate cap | Raise `HealthConfig` limits or probe less often. |

## Comparing signers

The fastest way to tell "one bad signer" from "the bridge is stuck" is `unwrapsHash` from [`getStatus`](../health-api.md) on every signer. Matching hashes point at the ceremony or peer connectivity; a differing hash points at that signer's event store. See [Signing stalls](signing-stalls.md).

## Getting the logs

The orchestrator logs to stdout, so under systemd:

```bash
journalctl -u orchestrator -f
journalctl -u orchestrator --since "1 hour ago" | grep -E "Sync|reconcile|Backfill|Event "
```

Administrator-relevant events (halts, key rotations, metadata changes) are also written to `~/.orchestrator/logs/administrator.log`.
