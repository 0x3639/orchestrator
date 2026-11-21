# Initial working version of the orchestrator for the Zenon TSS Bridge

Use `--ldflags '-extldflags "-Wl,--allow-multiple-definition"` as build arguments

## Upgrading unwrap signers

Each signer requests a threshold certificate for the signing key, ceremony
number, and exact ordered unwrap messages before it requests bridge signatures.
A split without a quorum skips the unwrap signing round. A ready pool requires
one additional threshold round, and agreement that finishes after the ceremony
expires is discarded.

Upgrade the signing group together. An older signer can still request bridge
signatures directly. Back up and restore the current `unwrap-pool-votes`
directory alongside the TSS key shares. It records this signer's choice before
contacting peers, including rounds that time out. A retry in the same ceremony
can vote only for that same pool.
