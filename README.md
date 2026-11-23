# Initial working version of the orchestrator for the Zenon TSS Bridge

Use `--ldflags '-extldflags "-Wl,--allow-multiple-definition"` as build arguments

## Checking the bridge laws

The 33 laws cover wrap finality, EVM source-event confirmation, deletion of stale
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
signatures. Each signer sorts pending unwraps by chain ID, transaction hash and
log index before reconciliation or the batch limit can exclude a transfer.
At scheduling time and again after agreement, the signer reads the
current window size from the Zenon endpoint between two frontier reads. Both
reads must identify the same momentum by height and hash. If the tip changes,
the signer retries once. A failed settings read or a second tip change skips
the attempt. The chain queries for one signing window share a ten-second
deadline. A live resize triggers a background settings refresh so the bridge
listener can process a subsequent halt while the settings endpoint is silent.
The signer rejects a tip below either its subscription height or the highest
height it has accepted for signing since startup. A window-size change at the
same height still takes effect, even if it lowers the ceremony number.
The next scheduling poll reads the settings again, so missing a
resize notification does not require a restart. Immediately before requesting
bridge signatures, the signer also rechecks live mode and the signing key.

The bridge stores the current window size in momentums. Its administrator can
set it to 100, then 50, then 100. After the third update, the contract returns
the original settings when the other fields are unchanged. Vote identity is an
orchestrator rule. It identifies a period by its size and the block height
divided by that size.
At heights 1710 and 1760, size 100 puts both attempts in window 17. Restoring
size 100 preserves the signer's original vote for that interval. The same batch
can retry, and a different batch can be proposed in a later eligible window.
[Bridge settings](https://github.com/zenon-network/go-zenon/blob/f18efb89169be331b51ad88e29476c950b700fce/vm/embedded/definition/bridge.go#L980-L1008)
and [administrator update](https://github.com/zenon-network/go-zenon/blob/f18efb89169be331b51ad88e29476c950b700fce/vm/embedded/implementation/bridge.go#L1373-L1405).

Both unwrap rounds use go-tss's leaderless protocol. With three signers, two
matching signers can finish both rounds when the third chooses a different pool.
The signing threshold still comes from the original key shares. On a split,
allow time for peer discovery in both rounds. If agreement takes the rest of
a window, the pending batch can be retried in the next eligible window.
Even-numbered windows give wraps priority and can try unwraps when there are
no wraps to sign. The recovery regression keeps the third signer offline across
both windows and verifies the signatures returned by the remaining two.
Run the real-coordinator regressions on Linux with the command below.

```sh
go test -race -count=1 -ldflags '-extldflags=-Wl,--allow-multiple-definition' -run '^Test(UnwrapTwoOfThree|OfflineSignerRecoversNextWindow)RealCoordinator$' ./tss
```

Upgrade the signing group together. An older signer can still request bridge
signatures directly. Back up and restore the current `unwrap-pool-votes`
directory alongside the TSS key shares. It records this signer's choice before
contacting peers, including rounds that time out. A retry in the same ceremony
can vote only for that same pool.
