# Initial working version of the orchestrator for the Zenon TSS Bridge

Use `--ldflags '-extldflags "-Wl,--allow-multiple-definition"` as build arguments

## Checking the bridge laws

The 27 laws cover wrap finality, EVM event confirmation, deletion of stale
unwrap records, signing-pool selection and agreement, and inclusive EVM scan
limits. Bend checks the laws against the decision model. The Go tests compare
those decisions with the orchestrator using local RPC fixtures and event stores.
[Model](verification/bridge.bend) · [Laws](LAWS.bend) · [Proofs](PROOF.bend)

On Linux with Bend 2.0.40 on the path, run both checks from the repository root.

```sh
bend PROOF.bend --verdict
go test -race -count=1 -tags=bend -ldflags '-extldflags=-Wl,--allow-multiple-definition' ./...
```

A model proof can pass while a Go comparison fails. Keep the failing input and
the expected result so a later fix can be checked against the same case.
The split-tip regression, for example, feeds two signers identical local events
but different stable bridge views, then checks which pools they mark ready.
[Observed dev failures and proposed follow-up](https://github.com/0x3639/orchestrator/issues/12)

Tests specific to the earlier parallel-worker, ledger-reader, and certificate
prototype are preserved separately. The ordinary Go suite on `dev` uses serial
reconciliation. [Prototype tests and replay instructions](verification/testdata/prototype/README.md)
