# Replaying the prototype tests

These original Go tests exercise the four-worker reconciler, its cancellable RPC
reader, and the durable certificate and vote store from commit
`2e4b7cafe11e033e04d2a9e294a67eb540e31a09`. They include all 24 four-reply
orders, each with a successful pass and four possible lookup failures.
The active Bend suite exercises the corresponding observable behavior through
the current node and network manager. Go excludes this directory from `./...`
because the tests here require the prototype APIs.

To run these tests in a temporary copy of that commit, run the following
commands on Linux from the root of the Bend branch.

```sh
prototype_dir=$(mktemp -d)
git archive 2e4b7cafe11e033e04d2a9e294a67eb540e31a09 | tar -x -C "$prototype_dir"
cp verification/testdata/prototype/node/*_test.go "$prototype_dir/node/"
cp verification/testdata/prototype/rpc/*_test.go "$prototype_dir/rpc/"
cp verification/testdata/prototype/tss/*_test.go "$prototype_dir/tss/"
(cd "$prototype_dir" && go test -race -count=1 -ldflags '-extldflags=-Wl,--allow-multiple-definition' ./node ./rpc ./tss)
```
