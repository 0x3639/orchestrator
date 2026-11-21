package rpc

import (
	"errors"
	"testing"

	sdkapi "github.com/MoonBaZZe/znn-sdk-go/api"
	sdkclient "github.com/MoonBaZZe/znn-sdk-go/rpc_client"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/api"
	"github.com/zenon-network/go-zenon/rpc/server"
)

type frontierLedger struct {
	block *api.AccountBlockMarshal
}

func (l frontierLedger) GetFrontierAccountBlock(address types.Address) (*api.AccountBlockMarshal, error) {
	if address != types.BridgeContract {
		return nil, errors.New("expected the bridge account")
	}
	return l.block, nil
}

func TestBridgeFrontierHashFromLedger(t *testing.T) {
	hash := types.Hash{42}
	for _, tc := range []struct {
		name    string
		address types.Address
		hash    types.Hash
		height  uint64
	}{
		{"bridge", types.BridgeContract, hash, 9},
		{"another account", types.TokenContract, hash, 9},
		{"missing hash", types.BridgeContract, types.ZeroHash, 9},
		{"missing height", types.BridgeContract, hash, 0},
		{"null", types.ZeroAddress, types.ZeroHash, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var block *api.AccountBlockMarshal
			if tc.name != "null" {
				block = new(api.AccountBlockMarshal)
				block.Address, block.Hash, block.Height = tc.address, tc.hash, tc.height
			}
			srv := server.NewServer()
			defer srv.Stop()
			if err := srv.RegisterName("ledger", frontierLedger{block}); err != nil {
				t.Fatal(err)
			}
			client := server.DialInProc(srv)
			defer client.Close()
			r := ZnnRpc{rpcClient: &sdkclient.RpcClient{LedgerApi: sdkapi.NewLedgerApi(client)}}
			got, err := r.GetBridgeFrontierHash()
			if tc.name == "bridge" {
				if got != hash || err != nil {
					t.Fatalf("frontier=%v err=%v, want %v", got, err, hash)
				}
			} else if err == nil || !got.IsZero() {
				t.Fatalf("invalid block yielded frontier=%v err=%v", got, err)
			}
		})
	}
}
