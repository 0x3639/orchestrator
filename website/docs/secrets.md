---
sidebar_position: 4
title: Producer key and passphrase
---

# Producer key and passphrase

The producer key file identifies this signer on Zenon, and its entropy derives the signer's EVM private key. Anyone who obtains the encrypted file **and** its passphrase controls both identities. The orchestrator therefore keeps the two apart where it can, and locks down what it writes.

## Where the passphrase comes from

At startup the orchestrator resolves the passphrase from the first source that provides one:

1. **Environment variable** `ORCHESTRATOR_PRODUCER_PASSPHRASE`. Recommended for systemd deployments. It is never written to disk by the orchestrator.
2. **Passphrase file** named by `ProducerKeyFilePassphraseFile` in `config.json`. The file must be a regular file, owned by the orchestrator user, mode `0600` or stricter, and at most 4 KiB. Keep it outside `DataPath` so backups of the data directory do not carry it.
3. **`ProducerKeyFilePassphrase`** in `config.json`. Works, but keeps the passphrase next to the key it unlocks; a warning is logged at every start suggesting one of the sources above.
4. **Interactive prompt**, when running in a terminal and none of the above is set.

A value from the environment, file or prompt is held in memory only. It is never written back into `config.json`, and the value you stored there, if any, is preserved exactly as you wrote it.

:::note
Empty passphrases cannot be expressed through any source. A key file created without a passphrase must be re-created with one.
:::

## File permissions and ownership

| Path | Mode | Check |
| --- | --- | --- |
| `DataPath` | `0700`, tightened if wider | Must be owned by the running user |
| `config.json` | `0600`, rewritten atomically | Owned by the running user; symlinks refused; 1 MiB cap |
| Producer key file | as created | Owned by the running user |
| Passphrase file | `0600` or stricter | Owned by the running user; symlinks refused |

Ownership is checked by exact user id. Running as root against a directory owned by another user, or mounting a secret volume owned by a different id, fails at startup with a message naming the path and both ids. Use the environment variable in those layouts.

## The signer's EVM address

The producer key also derives this signer's EVM private key. Its address is written to `config.json` as `EvmAddress` and reported by the [health API](health-api.md). When this signer is selected to submit a bridge halt on an EVM network, the transaction is sent from this address, so it **must hold gas** on every bridged EVM network. A signer whose balance is empty cannot submit when selected, which delays the halt until another signer is chosen. Fund it after the first start and include its balance in monitoring; see [Security model](security-model.md).

## Back up the TSS share

`~/.orchestrator/tss` holds this signer's share of the group key, produced during key generation. Back it up to a second secure location as soon as a key generation completes. A signer that loses its share cannot sign until the next key generation ceremony, and a group that loses too many shares cannot sign at all. Back up the producer key file with the same care. Never back up the passphrase alongside either.

## What appears in logs

Nothing secret. The startup configuration line is an allowlist of non-sensitive fields, and every logger scrubs the configured RPC URLs, their credentials, path and query components from any message or error before it is written. Rotate a provider key if it was ever pasted into a log by hand.
