# Initial working version of the orchestrator for the Zenon TSS Bridge

Use `--ldflags '-extldflags "-Wl,--allow-multiple-definition"` as build arguments

## Checking the bridge laws

The 34 laws cover wrap finality, EVM event confirmation, deletion of stale
unwrap records, terminal status updates, signing-pool selection and agreement,
inclusive EVM scan limits, and checkpoint advancement. Bend checks the laws against the decision model. The Go tests compare
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
but different stable bridge views, then compares each decision to the Bend quorum model. Other comparisons check
the stored status after a listener update and the cursor after an invalid log response.
[Observed dev failures and proposed follow-up](https://github.com/0x3639/orchestrator/issues/12)

Tests specific to the earlier parallel-worker, ledger-reader, and certificate
prototype are preserved separately. The ordinary Go suite on `dev` uses serial
reconciliation. [Prototype tests and replay instructions](verification/testdata/prototype/README.md)

## Upgrading unwrap signers

Each signer requests a threshold certificate for the signing key, window size,
ceremony number and exact ordered unwrap messages before requesting bridge
signatures. Resizing the window creates a distinct round even when the number
repeats. Before requesting bridge signatures the signer rechecks the window size,
its locally observed chain height, and the signing key.

Both unwrap rounds use go-tss's leaderless protocol. With three signers, two
matching signers can finish both rounds when the third chooses a different pool.
The signing threshold still comes from the original key shares. On a split,
allow time for peer discovery in both rounds. Run the three-signer regression
on Linux with the command below.

```sh
go test -race -count=1 -ldflags '-extldflags=-Wl,--allow-multiple-definition' -run '^TestUnwrapTwoOfThreeRealCoordinator$' ./tss
```

Upgrade the signing group together. An older signer can still request bridge
signatures directly. Back up and restore the current `unwrap-pool-votes`
directory alongside the TSS key shares. It records this signer's choice before
contacting peers, including rounds that time out. A retry in the same ceremony
can vote only for that same pool.
