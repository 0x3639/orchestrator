package network

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"
	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/protocol"
	"github.com/zenon-network/go-zenon/rpc/api"
	"github.com/zenon-network/go-zenon/rpc/server"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"go.uber.org/zap"

	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/db/wrap"
	"orchestrator/rpc"
)

type startupLedger struct {
	stallPage  chan struct{}
	page       func(uint64) *api.AccountBlockListMarshal
	lookup     func(types.Hash) (*api.AccountBlockMarshal, error)
	starts     []uint64
	lookups    []types.Hash
	syncStatus func() *protocol.SyncInfo
	tip        func() *api.Momentum
	bridgeInfo func() (*definition.BridgeInfoVariable, error)
}

func (l *startupLedger) GetAccountBlocksByHeight(ctx context.Context, _ types.Address, height, _ uint64) *api.AccountBlockListMarshal {
	l.starts = append(l.starts, height)
	if l.stallPage != nil {
		close(l.stallPage)
		<-ctx.Done()
		return nil
	}
	return l.page(height)
}

func (l *startupLedger) GetAccountBlockByHash(hash types.Hash) (*api.AccountBlockMarshal, error) {
	l.lookups = append(l.lookups, hash)
	return l.lookup(hash)
}

func (l *startupLedger) GetBridgeInfo() (*definition.BridgeInfoVariable, error) {
	return l.bridgeInfo()
}
func (l *startupLedger) SyncInfo() *protocol.SyncInfo       { return l.syncStatus() }
func (l *startupLedger) GetFrontierMomentum() *api.Momentum { return l.tip() }

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
	for _, name := range []string{"ledger", "stats", "embedded.bridge"} {
		if err := srv.RegisterName(name, ledger); err != nil {
			t.Fatal(err)
		}
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
	listenerCtx, cancelListener := context.WithCancel(context.Background())
	t.Cleanup(cancelListener)
	return &znnNetwork{listenerCtx: listenerCtx, cancelListener: cancelListener, rpcManager: rpcs, dbManager: unwrapTestStores{znn: wrap.NewZnnStorage(zdb.NewMemDB(), stop)}, state: state, logger: zap.NewNop().Sugar()}
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
	attempts, sendAttempts := 0, 0
	l := &startupLedger{
		page: func(start uint64) *api.AccountBlockListMarshal {
			if start == receive.Height {
				return startupPage(&pending)
			}
			return startupPage()
		},
		lookup: func(hash types.Hash) (*api.AccountBlockMarshal, error) {
			if hash == receive.FromBlockHash {
				sendAttempts++
				if sendAttempts == 1 {
					return nil, errors.New("temporary paired-send failure")
				}
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
	want := []types.Hash{receive.Hash, receive.Hash, receive.Hash, receive.Hash, receive.FromBlockHash, receive.FromBlockHash}
	if !reflect.DeepEqual(l.lookups, want) {
		t.Fatalf("lookups %v, want %v", l.lookups, want)
	}
	checkStartupCursor(t, n, 2)
}

func TestStartupSyncSendReadPreservesCheckpoint(t *testing.T) {
	first, second := startupReceive(2), startupReceive(3)
	wrongType, wrongDestination, short := startupSend(second), startupSend(second), startupSend(second)
	wrongType.BlockType = nom.BlockTypeUserReceive
	wrongDestination.ToAddress = types.ZeroAddress
	short.Data = nil
	for _, tc := range []struct {
		name string
		send *api.AccountBlockMarshal
		err  error
		want string
	}{
		{"RPC error", nil, errors.New("ledger temporarily unavailable"), "ledger temporarily unavailable"},
		{"missing send", nil, nil, "block is missing"},
		{"different send", startupSend(first), nil, "block hash differs"},
		{"paired block is a receive", wrongType, nil, "invalid paired send"},
		{"send to another address", wrongDestination, nil, "invalid paired send"},
		{"incomplete send data", short, nil, "method selector"},
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

// A recognized, adjacent non-receive must not mask a malformed entry.
// A valid later receive would persist a checkpoint over the bad height.
func TestStartupSyncRejectsUnknownTypeBeforeLaterReceive(t *testing.T) {
	for _, kind := range []uint64{0, 99, nom.BlockTypeUserReceive, nom.BlockTypeGenesisReceive} {
		first, bad, later := startupReceive(2), startupReceive(3), startupReceive(4)
		first.BlockType = nom.BlockTypeContractSend
		bad.BlockType = kind
		l := &startupLedger{page: func(uint64) *api.AccountBlockListMarshal { return startupPage(first, bad, later) }}
		n := startupNetwork(t, l)
		if err := n.Sync(); err == nil || !strings.Contains(err.Error(), "height 3") {
			t.Fatalf("type %d: got %v", kind, err)
		}
		checkStartupCursor(t, n, 1)
		if len(l.lookups) != 0 {
			t.Fatalf("type %d: unexpected lookups %v", kind, l.lookups)
		}
	}
}

func TestStartupSyncReceiveBudgetAndShutdown(t *testing.T) {
	for _, cancel := range []bool{true, false} {
		t.Run(map[bool]string{true: "shutdown", false: "exhaustion"}[cancel], func(t *testing.T) {
			receive := startupReceive(2)
			started := make(chan struct{}, 1)
			l := &startupLedger{
				page: func(uint64) *api.AccountBlockListMarshal { return startupPage(receive) },
				lookup: func(types.Hash) (*api.AccountBlockMarshal, error) {
					select {
					case started <- struct{}{}:
					default:
					}
					return nil, errors.New("receive temporarily unavailable")
				},
			}
			n := startupNetwork(t, l)
			result := make(chan error, 1)
			go func() { result <- n.Sync() }()
			<-started
			timeout := 20 * time.Second
			if cancel {
				if err := n.Stop(); err != nil {
					t.Fatal(err)
				}
				timeout = time.Second
			}
			select {
			case err := <-result:
				if err == nil || !strings.Contains(err.Error(), receive.Hash.String()) {
					t.Fatalf("error omitted receive identity: %v", err)
				}
				if cancel && !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				if !cancel && (len(l.lookups) != 4 || !strings.Contains(err.Error(), "receive temporarily unavailable")) {
					t.Fatalf("expected four reads and last RPC failure, got %d, %v", len(l.lookups), err)
				}
			case <-time.After(timeout):
				t.Fatal("startup did not return within the retry budget")
			}
			checkStartupCursor(t, n, 1)
		})
	}
}

func TestStartupSyncObservesFinalityAfterSyncStatus(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		cached, tip, afterSync uint64
		live, fail             bool
	}{
		{"subscription behind", 99, 101, 99, true, false},
		{"authoritative frontier behind", 99, 99, 99, false, true},
		{"one below finality", 100, 104, 104, true, false},
		{"exactly at finality", 100, 105, 105, false, false},
		{"subscription advances during sync read", 100, 104, 105, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			receive := startupReceive(2)
			send := startupSend(receive)
			var err error
			send.Data, err = definition.ABIBridge.PackMethod(definition.SetBridgeMetadataMethodName, "{}")
			if err != nil {
				t.Fatal(err)
			}
			l := &startupLedger{
				page: func(h uint64) *api.AccountBlockListMarshal {
					if h == receive.Height {
						return startupPage(receive)
					}
					return startupPage()
				},
				lookup: func(hash types.Hash) (*api.AccountBlockMarshal, error) {
					if hash == receive.Hash {
						return receive, nil
					}
					return send, nil
				},
			}
			n := startupNetwork(t, l)
			n.SetConfirmationsToFinality(5)
			if err := n.state.SetFrontierMomentum(tc.cached); err != nil {
				t.Fatal(err)
			}
			n.administrator = send.Address.String()
			dispatched := false
			n.setBridgeMetadata = func(*common.BridgeMetadata) { dispatched = true }
			var reads []string
			l.syncStatus = func() *protocol.SyncInfo {
				reads = append(reads, "sync")
				if err := n.state.SetFrontierMomentum(tc.afterSync); err != nil {
					t.Error(err)
				}
				return &protocol.SyncInfo{State: protocol.SyncDone}
			}
			l.tip = func() *api.Momentum {
				reads = append(reads, "frontier")
				return &api.Momentum{Momentum: &nom.Momentum{Height: tc.tip, Hash: types.Hash{1}}}
			}
			if err := n.Sync(); (err != nil) != tc.fail {
				t.Fatalf("sync error %v", err)
			}
			if !reflect.DeepEqual(reads, []string{"sync", "frontier"}) || dispatched != tc.live {
				t.Fatalf("read order %v, dispatched live=%v, want %v", reads, dispatched, tc.live)
			}
			if tc.fail {
				checkStartupCursor(t, n, 1)
			} else {
				checkStartupCursor(t, n, 2)
			}
		})
	}
}

func TestStartupStopInterruptsLedgerRead(t *testing.T) {
	l := &startupLedger{stallPage: make(chan struct{})}
	n := startupNetwork(t, l)
	result := make(chan error, 1)
	go func() { result <- n.Sync() }()
	select {
	case <-l.stallPage:
	case <-time.After(time.Second):
		t.Fatal("did not reach ledger read")
	}
	// Node.Stop calls this before acquiring the startup lock.
	n.CancelAccountBlockReads()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled ledger read returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("read outlived shutdown")
	}
	checkStartupCursor(t, n, 1)
}
