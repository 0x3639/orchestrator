package network

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/protocol"
	"github.com/zenon-network/go-zenon/rpc/api"
	"github.com/zenon-network/go-zenon/rpc/api/subscribe"
	"github.com/zenon-network/go-zenon/rpc/server"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"go.uber.org/zap"

	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/db"
	"orchestrator/rpc"
)

// Settings calls on this WebSocket stay unanswered until the test releases
// them. All other methods, including the halt confirmation, remain available.
type stalledSettingsEndpoint struct {
	entered         chan struct{}
	release         chan struct{}
	blocks          map[types.Hash]*api.AccountBlockMarshal
	receipts        []subscribe.AccountBlock
	endSubscription chan func() error
}

func (p *stalledSettingsEndpoint) GetOrchestratorInfo() *definition.OrchestratorInfo {
	select {
	case p.entered <- struct{}{}:
	default:
	}
	<-p.release
	return &definition.OrchestratorInfo{WindowSize: 10, KeyGenThreshold: 1, ConfirmationsToFinality: 1, EstimatedMomentumTime: 1}
}
func (*stalledSettingsEndpoint) SyncInfo() *protocol.SyncInfo {
	return &protocol.SyncInfo{State: protocol.SyncDone}
}
func (*stalledSettingsEndpoint) GetBridgeInfo() *definition.BridgeInfoVariable {
	return &definition.BridgeInfoVariable{Halted: true}
}
func (*stalledSettingsEndpoint) GetFrontierMomentum() *api.Momentum {
	return &api.Momentum{Momentum: &nom.Momentum{Height: 170, Hash: types.Hash{1}}}
}
func (p *stalledSettingsEndpoint) GetAccountBlockByHash(hash types.Hash) *api.AccountBlockMarshal {
	return p.blocks[hash]
}
func (p *stalledSettingsEndpoint) GetAccountBlocksByHeight(_ types.Address, height, count uint64) *api.AccountBlockListMarshal {
	page := &api.AccountBlockListMarshal{List: []*api.AccountBlockMarshal{}}
	for _, receipt := range p.receipts {
		if receipt.Height >= height && uint64(len(page.List)) < count {
			page.List = append(page.List, p.blocks[receipt.Hash])
		}
	}
	return page
}
func (p *stalledSettingsEndpoint) AccountBlocksByAddress(ctx context.Context, address types.Address) (*server.Subscription, error) {
	notifier, ok := server.NotifierFromContext(ctx)
	if !ok || address != types.BridgeContract {
		return nil, errors.New("expected bridge subscription")
	}
	sub := notifier.CreateSubscription()
	// A malformed notification terminates the SDK subscription with a
	// decode error, including on versions that ignore a clean disconnect.
	p.endSubscription <- func() error { return notifier.Notify(sub.ID, "end of test") }
	return sub, notifier.Notify(sub.ID, p.receipts)
}

type checkpointStore struct {
	db.ZnnStorage
	saved chan uint64
}

func (s checkpointStore) GetLastUpdateHeight() (uint64, error) { return 0, nil }
func (s checkpointStore) SetLastUpdateHeight(h uint64) error   { s.saved <- h; return nil }

type settingsTestStores struct {
	networkEventStores
	store checkpointStore
}

func (s settingsTestStores) ZnnStorage() db.ZnnStorage { return s.store }

func TestStalledSettingsCannotDelayHalt(t *testing.T) {
	p := &stalledSettingsEndpoint{endSubscription: make(chan func() error, 1), entered: make(chan struct{}, 1), release: make(chan struct{}), blocks: make(map[types.Hash]*api.AccountBlockMarshal)}
	resize, err := definition.ABIBridge.PackMethod(definition.SetOrchestratorInfoMethodName, uint64(10), uint32(1), uint32(1), uint32(1))
	if err != nil {
		t.Fatal(err)
	}
	halt, err := definition.ABIBridge.PackMethod(definition.HaltMethodName, "")
	if err != nil {
		t.Fatal(err)
	}
	for i, data := range [][]byte{resize, halt} {
		recv, send := types.Hash{byte(2*i + 1)}, types.Hash{byte(2*i + 2)}
		p.receipts = append(p.receipts, subscribe.AccountBlock{Address: types.BridgeContract, Height: uint64(i + 1), BlockType: nom.BlockTypeContractReceive, Hash: recv, FromHash: send})
		p.blocks[recv] = &api.AccountBlockMarshal{AccountBlockMarshal: nom.AccountBlockMarshal{Height: uint64(i + 1), BlockType: nom.BlockTypeContractReceive, Hash: recv, Address: types.BridgeContract, FromBlockHash: send, Amount: "0"}, ConfirmationDetail: &api.AccountBlockConfirmationDetail{MomentumHeight: 170}}
		p.blocks[send] = &api.AccountBlockMarshal{AccountBlockMarshal: nom.AccountBlockMarshal{BlockType: nom.BlockTypeUserSend, ToAddress: types.BridgeContract, Hash: send, Address: types.ZeroAddress, Data: data, Amount: "0"}}
	}
	srv := server.NewServer()
	for _, name := range []string{"ledger", "stats", "embedded.bridge"} {
		if err := srv.RegisterName(name, p); err != nil {
			t.Fatal(err)
		}
	}
	endpoint := httptest.NewServer(srv.WebsocketHandler([]string{"*"}))
	stop := make(chan os.Signal, 1)
	clients, err := rpc.NewRpcManager(config.BaseNetworkConfig{Urls: []string{"ws" + strings.TrimPrefix(endpoint.URL, "http")}}, stop)
	if err != nil {
		t.Fatal(err)
	}
	state := common.NewGlobalState(new(uint8))
	if err := state.SetFrontierMomentum(170); err != nil {
		t.Fatal(err)
	}
	saved := make(chan uint64, 2)
	n := &znnNetwork{listenerCtx: context.Background(), rpcManager: clients, state: state, administrator: types.ZeroAddress.String(), dbManager: settingsTestStores{store: checkpointStore{saved: saved}}, stopChan: stop, logger: zap.NewNop().Sugar()}
	n.SetConfirmationsToFinality(1)
	release := sync.OnceFunc(func() { close(p.release) })
	var wg sync.WaitGroup
	defer func() {
		release()
		select {
		case end := <-p.endSubscription:
			if err := end(); err != nil {
				t.Error(err)
			}
		default:
		}
		wg.Wait()
		srv.Stop()
		endpoint.Close()
	}()
	result := make(chan error, 1)
	wg.Add(1)
	go func() { defer wg.Done(); _, err := n.CurrentCeremony(); result <- err }()
	select {
	case <-p.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("signer did not reach settings RPC")
	}
	wg.Add(1)
	go func() { defer wg.Done(); n.ListenForEmbeddedBridgeAccountBlocks() }()
	// The listener can advance both receipts while the signing read is stalled.
	deadline := time.After(14 * time.Second)
	for want := uint64(1); want <= 2; want++ {
		select {
		case h := <-saved:
			if h != want {
				t.Fatalf("checkpoint %d, want %d", h, want)
			}
		case <-deadline:
			t.Fatalf("listener stuck before checkpoint %d while the settings RPC was unanswered; halt was delayed", want)
		}
	}
	if got, err := state.GetState(); err != nil || got != common.HaltedState {
		t.Fatalf("after halt: state=%d err=%v", got, err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("signer returned %v, want deadline exceeded", err)
		}
	case <-time.After(rpc.ZnnSettingsTimeout + time.Second):
		t.Fatal("signer still waiting for settings after the halt")
	}
	release()
	if w, err := n.CurrentCeremony(); err != nil || w != (common.CeremonyWindow{Number: 17, Size: 10}) {
		t.Fatalf("settings read did not recover: window=%v err=%v", w, err)
	}
}
