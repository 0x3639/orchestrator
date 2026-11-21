//go:build bend

package network

import (
	"math/big"
	"net/http/httptest"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/api/embedded"
	"github.com/zenon-network/go-zenon/rpc/server"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"orchestrator/common/config"
	"orchestrator/rpc"
)

type wrapReply struct {
	list *embedded.WrapTokenRequestList
}

func (f wrapReply) GetAllUnsignedWrapTokenRequests(uint32, uint32) *embedded.WrapTokenRequestList {
	return f.list
}

func readWrapPool(t *testing.T, list *embedded.WrapTokenRequestList) []*definition.WrapTokenRequest {
	t.Helper()
	srv := server.NewServer()
	defer srv.Stop()
	if err := srv.RegisterName("embedded.bridge", wrapReply{list}); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(srv)
	defer httpServer.Close()
	rpcs, err := rpc.NewRpcManager(config.BaseNetworkConfig{Urls: []string{httpServer.URL}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	m := NetworksManager{znnNetwork: &znnNetwork{rpcManager: rpcs}}
	pool, err := m.GetUnsignedWrapRequests()
	if err != nil {
		t.Fatal(err)
	}
	return pool
}
func wrapRequest(id byte, remaining uint64) *embedded.WrapTokenRequest {
	return &embedded.WrapTokenRequest{WrapTokenRequest: &definition.WrapTokenRequest{Id: types.Hash{id}, Amount: big.NewInt(1), Fee: big.NewInt(0)}, ConfirmationsToFinality: remaining}
}
func TestFinalizedUnsignedWraps(t *testing.T) {
	list := &embedded.WrapTokenRequestList{List: []*embedded.WrapTokenRequest{wrapRequest(1, 0), wrapRequest(2, 1), wrapRequest(3, 0)}}
	pool := readWrapPool(t, list)
	if len(pool) != 2 || pool[0].Id != list.List[0].Id || pool[1].Id != list.List[2].Id {
		t.Fatalf("incorrect final requests or order: %v", pool)
	}
	for _, empty := range []*embedded.WrapTokenRequestList{nil, {}} {
		if len(readWrapPool(t, empty)) != 0 {
			t.Fatal("expected empty pool")
		}
	}
}
