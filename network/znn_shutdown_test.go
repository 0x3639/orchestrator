package network

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"
	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/protocol"
	"github.com/zenon-network/go-zenon/rpc/api"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"orchestrator/common/config"
	"orchestrator/common/storage"
	"orchestrator/db/wrap"
)

type runtimeStores struct {
	unwrapTestStores
	open bool
}

func (s *runtimeStores) AddEvmEventStore(uint32, string, uint64) (func(), error) {
	s.open = true
	return func() { s.open = false }, nil
}

func TestShutdownDuringSetNetwork(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	for _, pendingRead := range []bool{false, true} {
		t.Run(fmt.Sprintf("pendingRPC=%t", pendingRead), func(t *testing.T) {
			info := &definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 111, Name: filepath.Base(filepath.Dir(t.TempDir())), ContractAddress: testContract.Hex()}
			receive := startupReceive(2)
			send := startupSend(receive)
			var err error
			send.Data, err = definition.ABIBridge.PackMethod(definition.SetNetworkMethodName, info.NetworkClass, info.Id, info.Name, info.ContractAddress, "")
			if err != nil {
				t.Fatal(err)
			}
			checking := make(chan struct{})
			var entered sync.Once
			stop := make(chan os.Signal, 1)
			stop <- syscall.SIGINT
			n := startupNetwork(t, &startupLedger{})
			store := &runtimeStores{unwrapTestStores: unwrapTestStores{znn: wrap.NewZnnStorage(zdb.NewMemDB(), stop)}}
			n.dbManager = store
			n.stopChan = stop
			n.SetConfirmationsToFinality(5)
			n.administrator = send.Address.String()
			n.networkManager = &NetworksManager{znnNetwork: n, logger: n.logger}
			n.networksInfo = map[string]config.BaseNetworkConfig{info.Name: {Urls: []string{"http://provisional.invalid"}}}
			n.listenerCtx, n.cancelListener = context.WithCancel(context.Background())
			defer n.cancelListener()
			previous := http.DefaultTransport
			http.DefaultTransport = startupTransport{http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				var call struct {
					ID     json.RawMessage
					Method string
				}
				if err := json.NewDecoder(req.Body).Decode(&call); err != nil {
					t.Error(err)
					return
				}
				var result any
				switch call.Method {
				case "ledger.getAccountBlocksByHeight":
					result = startupPage(receive)
				case "ledger.getAccountBlockByHash":
					result = send
				case "stats.syncInfo":
					result = &protocol.SyncInfo{State: protocol.SyncDone}
				case "ledger.getFrontierMomentum":
					result = &api.Momentum{Momentum: &nom.Momentum{Height: 100, Hash: types.Hash{1}}}
				case "embedded.bridge.getNetworkInfo":
					result = info
				case "eth_chainId":
					result = fmt.Sprintf("0x%x", info.Id)
				case "eth_syncing":
					entered.Do(func() { close(checking) })
					if pendingRead {
						<-req.Context().Done()
						return
					}
					result = map[string]string{"startingBlock": "0x0", "currentBlock": "0x1", "highestBlock": "0x40"}
				default:
					t.Errorf("unexpected method %s", call.Method)
				}
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": call.ID, "result": result}); err != nil {
					t.Error(err)
				}
			})}
			defer func() { http.DefaultTransport = previous }()
			n.listenerWorker.Add(1)
			go func() {
				defer n.listenerWorker.Done()
				n.followBridgeAccount(n.listenerCtx, nil, nil, n.refreshSettings)
			}()
			select {
			case <-checking:
			case <-time.After(3 * time.Second):
				t.Fatal("SetNetwork never reached the EVM sync check")
			}
			stopped := make(chan error, 1)
			go func() { stopped <- n.Stop() }()
			select {
			case err := <-stopped:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("shutdown blocked on provisional EVM network")
			}
			checkStartupCursor(t, n, 1)
			if store.open || n.networkManager.NetworksLength() != 0 || n.rpcManager.HasEvmNetwork(info.Id) {
				t.Fatal("failed runtime addition left an open store or registered network")
			}
			q, err := storage.CreateOrOpenQueue(info.NetworkClass, info.Name)
			if err != nil {
				t.Fatalf("queue lock after shutdown: %v", err)
			}
			if err = q.Close(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = storage.DeleteQueue(info.Name) })
		})
	}
}

func TestListenerStopRequestsCoalesce(t *testing.T) {
	n := startupNetwork(t, &startupLedger{})
	n.stopChan = make(chan os.Signal, 1)
	n.stopChan <- syscall.SIGINT
	n.listenerCtx = context.Background()
	done := make(chan bool, 1)
	go func() { done <- n.IsHalted() }() // A missing bridge RPC requests shutdown.
	select {
	case halted := <-done:
		if !halted || len(n.stopChan) != 1 {
			t.Fatal("lost fail-closed result or pending stop")
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown signal blocked the bridge interpreter")
	}
}
