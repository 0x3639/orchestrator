---
sidebar_position: 2
title: Install
description: "Install the orchestrator from a release binary or source, run it as a systemd service, open its ports, fund the signer address and back up the TSS share."
---

# Install

The orchestrator runs on Linux only. The data directory helper refuses macOS and Windows.

## Release binaries

Every push to `master` publishes `orchestrator-linux-amd64.zip` and `orchestrator-linux-arm64.zip` with a `SHA256CHECKSUMS.txt` to the GitHub release for the current version. Pushes to `dev` produce the same binaries as a workflow artifact for testing, never as a release.

```bash
sha256sum orchestrator-linux-amd64.zip
cat SHA256CHECKSUMS.txt        # compare the digest for your file by eye
unzip orchestrator-linux-amd64.zip
chmod +x orchestrator-linux-amd64
sudo install -m 0755 orchestrator-linux-amd64 /usr/local/bin/orchestrator
```

The checksum file lists one digest per line, so `sha256sum -c SHA256CHECKSUMS.txt` also works.

## Build from source

Requires Go 1.22 or newer.

```bash
git clone https://github.com/0x3639/orchestrator.git
cd orchestrator
make build
# binary at ./build/orchestrator
```

`make build` regenerates `metadata/git_commit.go` so the health API reports the exact commit. The linker flag `--allow-multiple-definition` in the Makefile is required by a dependency.

:::note
`go build ./...` fails on `examples/tss/main.go`. That example is stale and is not part of the binary. Build `main.go` or use `make build`.
:::

## Run as a service

The orchestrator reads `config.json` from `~/.orchestrator` of the user it runs as. Run it as a dedicated user with no other duties, since that user owns the producer key.

```ini
# /etc/systemd/system/orchestrator.service
[Unit]
Description=Zenon bridge orchestrator
After=network-online.target

[Service]
User=orchestrator
ExecStart=/usr/local/bin/orchestrator
Restart=always
RestartSec=10
# Supply the producer passphrase without writing it to config.json.
# See "Producer key and passphrase" for the alternatives.
Environment=ORCHESTRATOR_PRODUCER_PASSPHRASE=change-me

[Install]
WantedBy=multi-user.target
```

The orchestrator exits with a stop signal on several unrecoverable conditions and expects to be restarted, so `Restart=always` is required.

## Ports

| Port | Protocol | Purpose |
| --- | --- | --- |
| `55055` | TCP, libp2p | TSS peer traffic. Must be reachable by the other signers; open it in the firewall. |
| `55000` | TCP, HTTP | [Health API](health-api.md). Loopback only by default. |

Check that nothing else holds the peer port before the first start:

```bash
ss -nlp | grep 55055
```

## After the first start

1. Fund the signer's EVM address, shown by `getIdentity` in the [health API](health-api.md), with gas on every bridged EVM network. It pays for halt transactions.
2. Once the first key generation has completed, back up `~/.orchestrator/tss`. See [Producer key and passphrase](secrets.md).

## Command-line flags

| Flag | Purpose |
| --- | --- |
| `--evm.backfill-blocks N` | One-shot repair: rewind every EVM sync cursor by `N` blocks at startup. See [Backfill](operations/backfill.md). |
| `--pprof` | Enable the Go profiling server. |
| `--pprof.addr`, `--pprof.port` | Where the profiling server listens. Defaults to `127.0.0.1:6060`. |
| `version` | Print the version and exit. |
