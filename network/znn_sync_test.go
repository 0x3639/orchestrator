package network

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/chain/nom"
	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/api"
	"github.com/zenon-network/go-zenon/rpc/server"
	"go.uber.org/zap"

	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/db/wrap"
	"orchestrator/rpc"
)

type startupLedger struct {
	page    func(uint64) *api.AccountBlockListMarshal
	lookup  func(types.Hash) (*api.AccountBlockMarshal, error)
	starts  []uint64
	lookups []types.Hash
}

func (l *startupLedger) GetAccountBlocksByHeight(_ types.Address, height, _ uint64) *api.AccountBlockListMarshal {
	l.starts = append(l.starts, height)
	return l.page(height)
}

func (l *startupLedger) GetAccountBlockByHash(hash types.Hash) (*api.AccountBlockMarshal, error) {
	l.lookups = append(l.lookups, hash)
	return l.lookup(hash)
}

type startupTransport struct{ handler http.Handler }

func (rt startupTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	defer req.Body.Close()
	w := httptest.NewRecorder()
	rt.handler.ServeHTTP(w, req)
	return w.Result(), nil
}

func startupNetwork(t *testing.T, ledger *startupLedger) *znnNetwork {
	t.Helper()
	srv := server.NewServer()
	if err := srv.RegisterName("ledger", ledger); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)
	previous := http.DefaultTransport
	http.DefaultTransport = startupTransport{srv}
	t.Cleanup(func() { http.DefaultTransport = previous })
	stop := make(chan os.Signal, 1)
	rpcs, err := rpc.NewRpcManager(config.BaseNetworkConfig{Urls: []string{"http://startup.invalid"}}, stop)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rpcs.Znn().Stop() })
	state := common.NewGlobalState(new(uint8))
	if err := state.SetFrontierMomentum(100); err != nil {
		t.Fatal(err)
	}
	return &znnNetwork{rpcManager: rpcs, dbManager: unwrapTestStores{znn: wrap.NewZnnStorage(zdb.NewMemDB(), stop)}, state: state, logger: zap.NewNop().Sugar()}
}

func startupReceive(height byte) *api.AccountBlockMarshal {
	return &api.AccountBlockMarshal{
		AccountBlockMarshal: nom.AccountBlockMarshal{BlockType: nom.BlockTypeContractReceive, Height: uint64(height), Address: types.BridgeContract,
			Hash: types.Hash{height}, FromBlockHash: types.Hash{height, 1}, Amount: "0"},
		ConfirmationDetail: &api.AccountBlockConfirmationDetail{MomentumHeight: 100},
	}
}

func startupSend(receive *api.AccountBlockMarshal) *api.AccountBlockMarshal {
	return &api.AccountBlockMarshal{AccountBlockMarshal: nom.AccountBlockMarshal{
		BlockType: nom.BlockTypeUserSend, Hash: receive.FromBlockHash, ToAddress: types.BridgeContract, Amount: "0", Data: []byte{0, 0, 0, 0},
	}}
}

func startupPage(blocks ...*api.AccountBlockMarshal) *api.AccountBlockListMarshal {
	return &api.AccountBlockListMarshal{List: append([]*api.AccountBlockMarshal{}, blocks...)}
}

func checkStartupCursor(t *testing.T, n *znnNetwork, want uint64) {
	t.Helper()
	height, err := n.eventsStore().GetLastUpdateHeight()
	if err != nil || height != want {
		t.Fatalf("saved height %d, err=%v, want %d", height, err, want)
	}
}

func TestStartupSyncRetriesReceiveBeforeReadingSend(t *testing.T) {
	receive := startupReceive(2)
	pending := *receive
	pending.ConfirmationDetail = nil
	attempts := 0
	l := &startupLedger{
		page: func(start uint64) *api.AccountBlockListMarshal {
			if start == receive.Height {
				return startupPage(&pending)
			}
			return startupPage()
		},
		lookup: func(hash types.Hash) (*api.AccountBlockMarshal, error) {
			if hash == receive.FromBlockHash {
				return startupSend(receive), nil
			}
			attempts++
			switch attempts {
			case 1:
				return nil, errors.New("temporary ledger failure")
			case 2:
				return nil, nil
			case 3:
				return &pending, nil
			default:
				return receive, nil
			}
		},
	}
	n := startupNetwork(t, l)
	if err := n.Sync(); err != nil {
		t.Fatal(err)
	}
	want := []types.Hash{receive.Hash, receive.Hash, receive.Hash, receive.Hash, receive.FromBlockHash}
	if !reflect.DeepEqual(l.lookups, want) {
		t.Fatalf("lookups %v, want %v", l.lookups, want)
	}
	checkStartupCursor(t, n, 2)
}

func TestStartupSyncSendReadPreservesCheckpoint(t *testing.T) {
	first, second := startupReceive(2), startupReceive(3)
	for _, tc := range []struct {
		name string
		send *api.AccountBlockMarshal
		err  error
		want string
	}{
		{"RPC error", nil, errors.New("ledger temporarily unavailable"), "ledger temporarily unavailable"},
		{"missing send", nil, nil, "missing or mismatched send"},
		{"different send", startupSend(first), nil, "missing or mismatched send"},
		{"incomplete send data", &api.AccountBlockMarshal{AccountBlockMarshal: nom.AccountBlockMarshal{Hash: second.FromBlockHash, Amount: "0"}}, nil, "method selector"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recovered := false
			l := &startupLedger{
				page: func(start uint64) *api.AccountBlockListMarshal {
					switch start {
					case 2:
						return startupPage(first, second)
					case 3:
						return startupPage(second)
					default:
						return startupPage()
					}
				},
				lookup: func(hash types.Hash) (*api.AccountBlockMarshal, error) {
					switch hash {
					case first.Hash:
						return first, nil
					case first.FromBlockHash:
						return startupSend(first), nil
					case second.Hash:
						return second, nil
					case second.FromBlockHash:
						if recovered {
							return startupSend(second), nil
						}
						return tc.send, tc.err
					default:
						return nil, errors.New("unexpected block hash")
					}
				},
			}
			n := startupNetwork(t, l)
			if err := n.Sync(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
			checkStartupCursor(t, n, 2)
			recovered = true
			if err := n.Sync(); err != nil {
				t.Fatal(err)
			}
			checkStartupCursor(t, n, 3)
			if want := []uint64{2, 3, 4}; !reflect.DeepEqual(l.starts, want) {
				t.Fatalf("requested pages %v, want %v", l.starts, want)
			}
		})
	}
}

func TestStartupSyncRejectsIncompletePages(t *testing.T) {
	for _, tc := range []struct {
		name string
		page *api.AccountBlockListMarshal
	}{
		{"null page", nil},
		{"missing list", &api.AccountBlockListMarshal{}},
		{"null block", startupPage(nil)},
		{"skipped height", startupPage(startupReceive(3))},
		{"empty page below frontier", &api.AccountBlockListMarshal{List: []*api.AccountBlockMarshal{}, Count: 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := startupNetwork(t, &startupLedger{page: func(uint64) *api.AccountBlockListMarshal { return tc.page }})
			if err := n.Sync(); err == nil {
				t.Fatal("expected incomplete page to fail")
			}
			checkStartupCursor(t, n, 1)
		})
	}
}
