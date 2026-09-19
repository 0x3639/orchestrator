package rpc

import (
	"context"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
)

// fakeEth serves eth_getBlockByNumber with a post-Pectra style block: it
// carries a requestsHash field the pinned go-ethereum does not know, and a
// hash that therefore cannot be reproduced by types.Header.Hash().
type fakeEth struct {
	number uint64
	hash   ecommon.Hash
}

func (f *fakeEth) GetBlockByNumber(ctx context.Context, number string, full bool) (map[string]interface{}, error) {
	n, err := hexutil.DecodeUint64(number)
	if err != nil || n != f.number {
		return nil, nil
	}
	return map[string]interface{}{
		"number":           hexutil.EncodeUint64(f.number),
		"hash":             f.hash,
		"parentHash":       ecommon.HexToHash("0x01"),
		"stateRoot":        ecommon.HexToHash("0x02"),
		"requestsHash":     ecommon.HexToHash("0xe3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
		"gasLimit":         "0x1c9c380",
		"gasUsed":          "0x0",
		"timestamp":        "0x0",
		"difficulty":       "0x0",
		"extraData":        "0x",
		"logsBloom":        etypes.Bloom{},
		"miner":            ecommon.Address{},
		"nonce":            etypes.BlockNonce{},
		"mixHash":          ecommon.Hash{},
		"receiptsRoot":     ecommon.Hash{},
		"sha3Uncles":       ecommon.Hash{},
		"transactionsRoot": ecommon.Hash{},
		"baseFeePerGas":    "0x0",
	}, nil
}

func newFakeClient(t *testing.T, svc *fakeEth) *ethclient.Client {
	t.Helper()
	server := gethrpc.NewServer()
	if err := server.RegisterName("eth", svc); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Stop)
	return ethclient.NewClient(gethrpc.DialInProc(server))
}

func TestBlockHashByNumberUsesReportedHash(t *testing.T) {
	// The real hash of Ethereum block 24188569, which the old
	// Header.Hash()-based check computed as a different value.
	want := ecommon.HexToHash("0xde953b0d94572c8ad978ff1acf71673b660aa33fdf50904a36380762a35f09b4")
	client := newFakeClient(t, &fakeEth{number: 24188569, hash: want})

	got, err := blockHashByNumber(context.Background(), client, 24188569)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("hash must be taken from the response: got %s want %s", got.Hex(), want.Hex())
	}

	// A locally recomputed hash would not match: prove the header route is
	// wrong for this block shape, which is why it must not be used.
	header, err := client.HeaderByNumber(context.Background(), nil)
	if err == nil && header != nil && header.Hash() == want {
		t.Fatal("test setup: recomputed header hash unexpectedly matched")
	}

	// A missing block is an error, never an empty hash.
	if _, err := blockHashByNumber(context.Background(), client, 1); err == nil {
		t.Fatal("missing block must error")
	}
}
