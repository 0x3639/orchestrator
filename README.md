# Initial working version of the orchestrator for the Zenon TSS Bridge

Use `--ldflags '-extldflags "-Wl,--allow-multiple-definition"` as build arguments

## Checking the bridge laws

[LAWS.bend](LAWS.bend) specifies wrap finality, queued EVM event confirmation,
backfill deletion guards, selection of the unwrap signing pool, and block counts
for inclusive EVM log queries.
[PROOF.bend](PROOF.bend) proves those properties of the
[decision model](verification/bridge.bend). The Go comparison tests exercise
the production decisions with simulated RPC replies and an in-memory event store.

Run both checks with Bend 2.0.40 on the path. The Go command below uses the
project's Linux linker flags. The sync tests record the actual RPC queries and
check the saved cursor after a provider returns a log outside the requested range.

```sh
bend PROOF.bend
go test -race -count=1 -tags=bend -ldflags '-extldflags=-Wl,--allow-multiple-definition' ./node ./network ./db/unwrap ./rpc
```

Unwrap reconciliation overlaps up to four read-only Zenon lookups. Pool
selection and status writes follow storage order. A failed lookup or status
write aborts signing, and an exhausted lookup budget defers an incomplete pool
to the next ceremony.

Before requesting unwrap signatures, signers use the existing threshold key
to certify a SHA-256 digest of the key, ceremony number, and ordered bridge
messages under the `zenon/orchestrator/unwrap-pool/v1` domain. A split with
no signing quorum skips the unwrap ceremony. Certification adds one threshold
signing round per ready, nonempty pool. Each signer records its vote before
contacting peers and can retry only that pool for the same key and ceremony.
Keep the `unwrap-pool-votes` directory alongside the TSS key shares across
restarts and restores. Coordinate the signer upgrade, since older versions
can request bridge signatures without this agreement round.
