package network

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/gorilla/websocket"
	"github.com/zenon-network/go-zenon/chain/nom"
	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/protocol"
	"github.com/zenon-network/go-zenon/rpc/api"
	"github.com/zenon-network/go-zenon/rpc/api/embedded"
	zserver "github.com/zenon-network/go-zenon/rpc/server"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"go.uber.org/zap"

	"orchestrator/common"
	"orchestrator/common/bridge"
	"orchestrator/common/config"
	"orchestrator/common/events"
	"orchestrator/common/storage"
	"orchestrator/db"
	"orchestrator/db/unwrap"
	"orchestrator/db/wrap"
	"orchestrator/health/snapshot"
	"orchestrator/rpc"
)

// heldZenon is the Zenon node of a signer that is started through
// NetworksManager.Start. It is served over a websocket, so the subscriptions a
// start opens are real ones. Its receives confirm at momentum 100.
type heldZenon struct {
	mu       sync.Mutex
	frontier uint64
	// The bridge account from height 2, in order, and the sends its receives
	// are paired with.
	account []*api.AccountBlockMarshal
	sends   map[types.Hash]*api.AccountBlockMarshal
	// Heights of receives whose paired send the node returns with another
	// destination, which the signer refuses at once.
	unreadable map[uint64]bool
	// How many of the account's blocks the node lists, or -1 for all.
	listed    int
	pageFails bool
	syncing   bool
	request   *definition.UnwrapTokenRequest
	metadata  string
	// The bridge allows key generation.
	allowKeyGen bool
	// The node returns no bridge info.
	noBridgeInfo bool
	// The bridge's halt flag, and an unhalt whose wait has not passed.
	halted, unhaltWait bool
	// A read of the bridge info fails.
	bridgeInfoFails bool
	// A read of the account waits here once it has said so on stalled.
	stall, stalled chan struct{}
	// The requests the bridge holds by log index, when a test sets them
	// apart, and the networks it lists by chain id.
	registered map[uint32]*definition.UnwrapTokenRequest
	networks   map[uint32]*definition.NetworkInfo
	// Heights the node lists without their confirmation, which a lookup by
	// hash returns.
	listedUnconfirmed map[uint64]bool
	// Blocks published to the node.
	published int
	// Runs, once, when the node is asked how much proof of work a block
	// needs, which a transaction's preparation asks last.
	onPrepare func()
}

func (z *heldZenon) GetFrontierAccountBlock(types.Address) (*nom.AccountBlock, error) {
	return nil, nil
}

func (z *heldZenon) GetRequiredPoWForAccountBlock(embedded.GetRequiredParam) (*embedded.GetRequiredResult, error) {
	z.mu.Lock()
	hook := z.onPrepare
	z.onPrepare = nil
	z.mu.Unlock()
	if hook != nil {
		hook()
	}
	return &embedded.GetRequiredResult{AvailablePlasma: 21000, BasePlasma: 21000}, nil
}

func (z *heldZenon) PublishRawTransaction(*nom.AccountBlock) error {
	z.mu.Lock()
	defer z.mu.Unlock()
	z.published++
	return nil
}

func (z *heldZenon) publishedBlocks() int {
	z.mu.Lock()
	defer z.mu.Unlock()
	return z.published
}

func (z *heldZenon) GetAccountBlocksByHeight(_ types.Address, height, _ uint64) (*api.AccountBlockListMarshal, error) {
	z.mu.Lock()
	stall, stalled := z.stall, z.stalled
	z.mu.Unlock()
	if stall != nil {
		select {
		case stalled <- struct{}{}:
		default:
		}
		<-stall
	}
	z.mu.Lock()
	defer z.mu.Unlock()
	if z.pageFails {
		return nil, errors.New("upstream timeout")
	}
	account := z.account
	if z.listed >= 0 {
		account = account[:z.listed]
	}
	page := startupPage()
	for _, block := range account {
		if block.Height < height {
			continue
		}
		if z.listedUnconfirmed[block.Height] {
			unconfirmed := *block
			unconfirmed.ConfirmationDetail = nil
			block = &unconfirmed
		}
		page.List = append(page.List, block)
	}
	page.Count = 1 + len(account)
	return page, nil
}

func (z *heldZenon) GetAccountBlockByHash(hash types.Hash) (*api.AccountBlockMarshal, error) {
	z.mu.Lock()
	defer z.mu.Unlock()
	for _, block := range z.account {
		if block.Hash == hash {
			return block, nil
		}
		if block.FromBlockHash == hash && z.sends[hash] != nil {
			send := *z.sends[hash]
			if z.unreadable[block.Height] {
				send.ToAddress = types.ZeroAddress
			}
			return &send, nil
		}
	}
	return nil, errors.New("unknown block")
}

func (z *heldZenon) GetFrontierMomentum() *api.Momentum {
	z.mu.Lock()
	defer z.mu.Unlock()
	return startupMomentum(z.frontier)
}

func (z *heldZenon) SyncInfo() *protocol.SyncInfo {
	z.mu.Lock()
	defer z.mu.Unlock()
	if z.syncing {
		return &protocol.SyncInfo{State: protocol.Syncing}
	}
	return &protocol.SyncInfo{State: protocol.SyncDone}
}

func (z *heldZenon) GetUnwrapTokenRequestByHashAndLog(_ types.Hash, index uint32) (*embedded.UnwrapTokenRequest, error) {
	z.mu.Lock()
	defer z.mu.Unlock()
	if z.registered == nil {
		return withRedeemDelay(z.request, 0), nil
	}
	if request := z.registered[index]; request != nil {
		return withRedeemDelay(request, 0), nil
	}
	return nil, constants.ErrDataNonExistent
}

func (z *heldZenon) GetNetworkInfo(_, id uint32) (*definition.NetworkInfo, error) {
	z.mu.Lock()
	defer z.mu.Unlock()
	if network := z.networks[id]; network != nil {
		return network, nil
	}
	return nil, constants.ErrUnknownNetwork
}

func (z *heldZenon) GetBridgeInfo() (*definition.BridgeInfoVariable, error) {
	z.mu.Lock()
	defer z.mu.Unlock()
	if z.bridgeInfoFails {
		return nil, errors.New("upstream timeout")
	}
	if z.noBridgeInfo {
		return nil, nil
	}
	info := &definition.BridgeInfoVariable{Metadata: z.metadata, AllowKeyGen: z.allowKeyGen, Halted: z.halted}
	if z.unhaltWait {
		info.UnhaltedAt, info.UnhaltDurationInMomentums = z.frontier, 1000
	}
	return info, nil
}

func (z *heldZenon) Momentums(ctx context.Context) (*zserver.Subscription, error) {
	return quietSubscription(ctx)
}

func (z *heldZenon) AccountBlocksByAddress(ctx context.Context, _ types.Address) (*zserver.Subscription, error) {
	return quietSubscription(ctx)
}

// quietSubscription is a subscription the node never publishes on.
func quietSubscription(ctx context.Context) (*zserver.Subscription, error) {
	notifier, supported := zserver.NotifierFromContext(ctx)
	if !supported {
		return nil, zserver.ErrNotificationsUnsupported
	}
	return notifier.CreateSubscription(), nil
}

func (z *heldZenon) change(change func(*heldZenon)) {
	z.mu.Lock()
	defer z.mu.Unlock()
	change(z)
}

// append adds a receive at the next account height with the given outcome,
// paired with a send that carries data.
func (z *heldZenon) append(outcome, data []byte, sender types.Address) uint64 {
	z.mu.Lock()
	defer z.mu.Unlock()
	receive := startupReceive(byte(2 + len(z.account)))
	receive.Data = outcome
	send := startupSend(receive)
	send.Address = sender
	if data != nil {
		send.Data = data
	}
	z.account = append(z.account, receive)
	z.sends[send.Hash] = send
	return receive.Height
}

// heldEvm is the EVM endpoint of that signer. It has no receipt for any
// transaction. Its chain is 200 blocks long, final after 5, and block 100
// holds the unwrap event when hasEvent is set. A query for a range of blocks,
// which only the scan makes, waits until the test opens the endpoint.
type heldEvm struct {
	t        *testing.T
	bridge   *replayBridge
	url      string
	hasEvent atomic.Bool
	// The recipient the event names, when it is not the bridge's own.
	eventTo atomic.Pointer[string]
	syncing atomic.Bool
	halted  atomic.Bool // the bridge contract on this chain is halted
	// The halt flag is clear, but isHalted still reports true: an unhalt
	// whose delay has not passed yet.
	unhaltDelay atomic.Bool
	// The halt flag cannot be read.
	haltedFails atomic.Bool
	// Runs, once, when the signer first asks for the halt flag.
	onHaltedRead atomic.Pointer[func()]
	// Runs, once, when the halt senders next read the halt flag itself.
	onFlagRead atomic.Pointer[func()]
	// Every transaction sent to this chain is rejected.
	sendFails atomic.Bool
	rejected  atomic.Int32 // transactions this chain rejected
	// The TSS address the contract holds, when one is set.
	tss atomic.Pointer[ecommon.Address]
	// Runs, once, when the TSS address is read.
	onTssRead atomic.Pointer[func()]
	sent      atomic.Int32 // transactions sent to this chain
	gate      chan struct{}
	opening   sync.Once
	calls     atomic.Int32
	receipts  atomic.Int32
	scans     atomic.Int32
	starts    atomic.Int32 // times a network asked whether the node is synced
}

func newHeldEvm(t *testing.T, b *replayBridge) *heldEvm {
	t.Helper()
	e := &heldEvm{t: t, bridge: b, gate: make(chan struct{})}
	e.url = replayServerWithErrors(t, e.answer)
	// Registered after the server, so it runs before the server waits for a
	// handler that is still at the gate.
	t.Cleanup(e.open)
	return e
}

func (e *heldEvm) open() { e.opening.Do(func() { close(e.gate) }) }

// event is the unwrap event in block 100.
func (e *heldEvm) event() etypes.Log {
	to := e.bridge.recipient.String()
	if named := e.eventTo.Load(); named != nil {
		to = *named
	}
	return e.bridge.sourceLog(e.t, to)
}

// haltedSelector is the call the signer makes to ask a bridge contract
// whether it is halted.
func haltedSelector(t *testing.T) []byte { return contractSelector(t, "isHalted") }

// contractSelector is the selector of a bridge contract method.
func contractSelector(t *testing.T, method string) []byte {
	contract, err := bridge.BridgeMetaData.GetAbi()
	if err != nil {
		t.Fatal(err)
	}
	return contract.Methods[method].ID
}

// isHaltRead reports whether selector reads whether the contract is halted:
// isHalted, or the halt flag itself, which the halt senders read.
func isHaltRead(t *testing.T, selector []byte) bool {
	contract, err := bridge.BridgeMetaData.GetAbi()
	if err != nil {
		t.Fatal(err)
	}
	return string(selector) == string(haltedSelector(t)) || string(selector) == string(contract.Methods["halted"].ID)
}

func (e *heldEvm) answer(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
	e.calls.Add(1)
	switch method {
	case "eth_syncing":
		e.starts.Add(1)
		if e.syncing.Load() {
			return map[string]string{"startingBlock": "0x0", "currentBlock": "0x1", "highestBlock": "0x40"}, nil
		}
		return false, nil
	case "eth_call":
		var call struct {
			Data  hexutil.Bytes `json:"data"`
			Input hexutil.Bytes `json:"input"`
		}
		if len(params) < 1 || json.Unmarshal(params[0], &call) != nil {
			e.t.Errorf("unexpected call %s", params)
		}
		if selector := append(call.Data, call.Input...); len(selector) >= 4 && isHaltRead(e.t, selector[:4]) {
			// The hook is for the startup check, which reads isHalted.
			if string(selector[:4]) == string(haltedSelector(e.t)) {
				if hook := e.onHaltedRead.Swap(nil); hook != nil {
					(*hook)()
				}
			} else if hook := e.onFlagRead.Swap(nil); hook != nil {
				(*hook)()
			}
			if e.haltedFails.Load() {
				return nil, &replayRPCError{-32000, "upstream timeout"}
			}
			if e.halted.Load() || e.unhaltDelay.Load() && string(selector[:4]) == string(haltedSelector(e.t)) {
				return "0x" + strings.Repeat("0", 63) + "1", nil
			}
			return "0x" + strings.Repeat("0", 64), nil
		}
		if selector := append(call.Data, call.Input...); len(selector) >= 4 && string(selector[:4]) == string(contractSelector(e.t, "tss")) {
			if hook := e.onTssRead.Swap(nil); hook != nil {
				(*hook)()
			}
			if tss := e.tss.Load(); tss != nil {
				return ecommon.BytesToHash(tss.Bytes()).Hex(), nil
			}
		}
		// Finality, deployment height and block time: 5 each.
		return "0x" + strings.Repeat("0", 63) + "5", nil
	case "eth_getBalance":
		return "0xde0b6b3a7640000", nil
	case "eth_getTransactionCount":
		return "0x0", nil
	case "eth_gasPrice":
		return "0x1", nil
	case "eth_estimateGas":
		return "0x5208", nil
	case "eth_sendRawTransaction":
		if e.sendFails.Load() {
			e.rejected.Add(1)
			return nil, &replayRPCError{-32000, "nonce too low"}
		}
		e.sent.Add(1)
		return ecommon.Hash{1}.Hex(), nil
	case "eth_blockNumber":
		return "0xc8", nil
	case "eth_getBlockByNumber":
		var number hexutil.Uint64
		if len(params) < 1 || json.Unmarshal(params[0], &number) != nil {
			e.t.Errorf("unexpected block request %s", params)
		}
		hash := ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 1000))
		if number == 100 {
			hash = e.bridge.blockHash
		}
		return map[string]interface{}{"number": number, "hash": hash}, nil
	case "eth_getTransactionReceipt":
		e.receipts.Add(1)
		return nil, nil
	case "eth_getLogs":
		var filter struct {
			From  *hexutil.Uint64 `json:"fromBlock"`
			To    *hexutil.Uint64 `json:"toBlock"`
			Block *ecommon.Hash   `json:"blockHash"`
		}
		if len(params) < 1 || json.Unmarshal(params[0], &filter) != nil {
			e.t.Errorf("unexpected log query %s", params)
			return []etypes.Log{}, nil
		}
		if filter.Block != nil {
			if e.hasEvent.Load() && *filter.Block == e.bridge.blockHash {
				return []etypes.Log{e.event()}, nil
			}
			return []etypes.Log{}, nil
		}
		if filter.From == nil || filter.To == nil {
			e.t.Errorf("unexpected log query %s", params)
			return []etypes.Log{}, nil
		}
		if *filter.From != *filter.To {
			<-e.gate
			e.scans.Add(1)
		}
		if e.hasEvent.Load() && *filter.From <= 100 && *filter.To >= 100 {
			return []etypes.Log{e.event()}, nil
		}
		return []etypes.Log{}, nil
	}
	e.t.Errorf("unexpected EVM method %s", method)
	return nil, nil
}

// pendingSubscriptionServer serves answer over a WebSocket, as an EVM node
// does, but never answers a subscription, whose setup stays pending. It
// returns the URL and counts the subscriptions asked for.
func pendingSubscriptionServer(t *testing.T, answer func(string, []json.RawMessage) (interface{}, *replayRPCError)) (string, *atomic.Int32) {
	return subscriptionServer(t, answer, func(int32) (interface{}, *replayRPCError, bool) { return nil, nil, false })
}

// subscriptionServer is pendingSubscriptionServer with the n-th subscription
// (from 1) answered as subscribe says: with a result or an error, or not at
// all when ok is false.
func subscriptionServer(t *testing.T, answer func(string, []json.RawMessage) (interface{}, *replayRPCError),
	subscribe func(n int32) (result interface{}, rpcErr *replayRPCError, ok bool)) (string, *atomic.Int32) {
	t.Helper()
	subscribes := new(atomic.Int32)
	var upgrader websocket.Upgrader
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			var call struct {
				ID     json.RawMessage   `json:"id"`
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			if err := conn.ReadJSON(&call); err != nil {
				return
			}
			reply := map[string]interface{}{"jsonrpc": "2.0", "id": call.ID}
			result, rpcErr := interface{}(nil), (*replayRPCError)(nil)
			switch call.Method {
			case "eth_subscribe":
				var ok bool
				if result, rpcErr, ok = subscribe(subscribes.Add(1)); !ok {
					continue
				}
			case "eth_unsubscribe":
				result = true
			default:
				result, rpcErr = answer(call.Method, call.Params)
			}
			if rpcErr != nil {
				reply["error"] = rpcErr
			} else {
				reply["result"] = result
			}
			if err := conn.WriteJSON(reply); err != nil {
				return
			}
		}
	}))
	t.Cleanup(func() { server.CloseClientConnections(); server.Close() })
	return "ws" + strings.TrimPrefix(server.URL, "http"), subscribes
}

var heldAdministrator = types.Address{7}

// heldStart is a signer with one EVM network (chain 1) whose networks are
// started the way the node starts them. The EVM cursor is at block 90, behind
// the block that holds the unwrap event.
type heldStart struct {
	t        *testing.T
	bridge   *replayBridge
	zenon    *heldZenon
	evm      *heldEvm
	m        *NetworksManager
	n        *znnNetwork
	net      *evmNetwork // chain 1
	zenonURL string
	stores   replayStores
	stop     chan os.Signal
	elapsed  atomic.Int64 // added to the signer's clock
	// How often the signer applied bridge metadata, and the pool size in
	// what it applied last.
	applied     atomic.Int32
	appliedPool atomic.Int64
	stopping    sync.Once
}

func newHeldStart(t *testing.T) *heldStart {
	t.Helper()
	t.Setenv("HOME", t.TempDir()) // the EVM network's queue is on disk
	h := &heldStart{t: t, bridge: newReplayBridge(t), stop: make(chan os.Signal, 1)}
	h.zenon = &heldZenon{frontier: 5000, sends: map[types.Hash]*api.AccountBlockMarshal{}, unreadable: map[uint64]bool{},
		listed: -1, request: h.bridge.request, metadata: "{}"}
	srv := zserver.NewServer()
	for _, name := range []string{"ledger", "stats", "embedded.bridge", "embedded.plasma"} {
		if err := srv.RegisterName(name, h.zenon); err != nil {
			t.Fatal(err)
		}
	}
	node := httptest.NewServer(srv.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() { srv.Stop(); node.Close() })
	h.evm = newHeldEvm(t, h.bridge)

	h.zenonURL = "ws" + strings.TrimPrefix(node.URL, "http")
	rpcs, err := rpc.NewRpcManager(config.BaseNetworkConfig{Urls: []string{h.zenonURL}}, h.stop)
	if err != nil {
		t.Fatal(err)
	}
	h.stores = replayStores{znn: wrap.NewZnnStorage(zdb.NewMemDB(), h.stop), evm: map[uint32]db.EvmStorage{}, evmMu: new(sync.RWMutex)}
	state := common.NewGlobalState(new(uint8))
	state.Health = snapshot.New("test", "")
	if err := state.SetFrontierMomentum(h.zenon.frontier); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.n = &znnNetwork{listenerCtx: ctx, cancelListener: cancel, dbManager: h.stores, rpcManager: rpcs, state: state,
		stopChan: h.stop, logger: zap.NewNop().Sugar(), retryPause: 30 * time.Millisecond,
		setBridgeMetadata: func(metadata *common.BridgeMetadata) {
			h.appliedPool.Store(int64(metadata.SignCeremonyPoolSize))
			h.applied.Add(1)
		}}
	h.n.SetConfirmationsToFinality(5)
	h.n.administrator = heldAdministrator.String()
	start := time.Now()
	h.n.now = func() time.Time { return start.Add(time.Duration(h.elapsed.Load())) }
	h.m = &NetworksManager{znnNetwork: h.n, stopChan: h.stop, logger: zap.NewNop().Sugar()}
	h.n.networkManager = h.m
	t.Cleanup(h.stopNode)
	h.net = h.addEvmNetwork(1, h.evm.url)
	return h
}

func (h *heldStart) watch() uint64 {
	h.t.Helper()
	from, err := h.stores.znn.BridgeWatchFrom()
	if err != nil {
		h.t.Fatal(err)
	}
	return from
}

// addEvmNetwork gives the signer a client, a store and a network for chain
// id, as Init does for a network the bridge lists.
func (h *heldStart) addEvmNetwork(id uint32, url string) *evmNetwork {
	h.t.Helper()
	name := fmt.Sprintf("held-start-%d", id)
	if err := h.n.rpcManager.AddEvmClient(config.BaseNetworkConfig{Urls: []string{url}}, id, name, h.bridge.contract); err != nil {
		h.t.Fatal(err)
	}
	h.stores.evmMu.Lock()
	h.stores.evm[id] = unwrap.NewEvmStorage(zdb.NewMemDB(), h.stop, 90)
	h.stores.evmMu.Unlock()
	params, err := config.NewEvmParams(&definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: id, Name: name, ContractAddress: h.bridge.contract.Hex()})
	if err != nil {
		h.t.Fatal(err)
	}
	queue, err := storage.CreateOrOpenQueue(definition.EvmClass, name)
	if err != nil {
		h.t.Fatal(err)
	}
	network := &evmNetwork{EvmParams: params, dbManager: h.stores, rpcManager: h.n.rpcManager, unconfirmedQueue: queue,
		client: h.n.rpcManager.Evm(id), state: h.n.state, stopChan: h.stop, logger: zap.NewNop().Sugar(), done: make(chan struct{})}
	h.m.AddEvmNetwork(network)
	return network
}

func (h *heldStart) stopNode() {
	h.stopping.Do(func() {
		h.evm.open()
		h.m.Stop()
		h.n.state.Health.Close()
	})
}

// start runs NetworksManager.Start, which Node.Start calls and whose error
// ends the process.
func (h *heldStart) start() error {
	h.t.Helper()
	result := make(chan error, 1)
	go func() { result <- h.m.Start() }()
	select {
	case err := <-result:
		if err == nil && !h.m.CompletesStartupLater() {
			// The node completes a startup that waited for the scans.
			err = h.m.CompleteStartup()
		}
		if err == nil {
			// The node goes on to apply what it reads from the bridge.
			h.n.state.MarkNodeReady()
		}
		return err
	case <-time.After(10 * time.Second):
		h.t.Fatal("the networks did not finish starting")
		return nil
	}
}

func (h *heldStart) setState(state uint8) {
	h.t.Helper()
	if err := h.n.state.SetState(state); err != nil {
		h.t.Fatal(err)
	}
}

// unwrapReceive adds a receive the bridge accepted an UnwrapToken send with.
func (h *heldStart) unwrapReceive() uint64 {
	h.t.Helper()
	data := h.bridge.send(h.t, h.bridge.request.ChainId, h.bridge.request.Amount, h.bridge.request.Signature).Data
	return h.zenon.append(acceptedReceive, data, types.Address{})
}

// plainReceive adds a receive of a send that calls no bridge method.
func (h *heldStart) plainReceive() uint64 {
	return h.zenon.append(nil, nil, types.Address{})
}

// metadataReceive adds a receive of a SetBridgeMetadata send from the
// bridge's administrator. The send sets a pool size of 7; the bridge's
// metadata as the node serves it has none.
func (h *heldStart) metadataReceive() uint64 {
	h.t.Helper()
	data, err := definition.ABIBridge.PackMethod(definition.SetBridgeMetadataMethodName, `{"SignCeremonyPoolSize":7}`)
	if err != nil {
		h.t.Fatal(err)
	}
	return h.zenon.append(acceptedReceive, data, heldAdministrator)
}

func (h *heldStart) setUnreadable(height uint64, unreadable bool) {
	h.zenon.change(func(z *heldZenon) { z.unreadable[height] = unreadable })
}

// watchedFrom is a database that followed the bridge account to its tip
// before, so that it checks what it reads from height on.
func (h *heldStart) watchedFrom(height uint64) {
	h.t.Helper()
	if err := h.stores.znn.SaveBridgeWatchFrom(height); err != nil {
		h.t.Fatal(err)
	}
}

func (h *heldStart) cursor() uint64 {
	h.t.Helper()
	height, err := h.stores.znn.GetLastUpdateHeight()
	if err != nil {
		h.t.Fatal(err)
	}
	return height
}

func (h *heldStart) evmCursor() uint64 {
	h.t.Helper()
	height, err := h.stores.evm[1].GetLastUpdateHeight()
	if err != nil {
		h.t.Fatal(err)
	}
	return height
}

func (h *heldStart) hold() *snapshot.ReceiveHold {
	return h.n.state.Health.Read().Zenon.ReceiveHold
}

func (h *heldStart) complete() bool { return h.n.state.StartupComplete() }

func (h *heldStart) stopRequested() bool {
	select {
	case <-h.stop:
		return true
	default:
		return false
	}
}

func (h *heldStart) scansFinished() bool {
	_, running := h.n.scans.finished()
	return !running
}

// retries waits for the held start to fail again count more times.
func (h *heldStart) retries(count uint64) {
	h.t.Helper()
	waitFor(h.t, "the hold to be reported", func() bool { return h.hold() != nil })
	seen := h.hold().Attempts
	waitFor(h.t, "more attempts", func() bool { hold := h.hold(); return hold != nil && hold.Attempts >= seen+count })
}

// expect checks the signer's state, the bridge account checkpoint and
// whether startup is complete.
func (h *heldStart) expect(step string, state uint8, cursor uint64, complete bool) {
	h.t.Helper()
	if got := replayState(h.t, h.n); got != state {
		h.t.Fatalf("%s: state %s, want %s", step, common.StateToText(got), common.StateToText(state))
	}
	if got := h.cursor(); got != cursor {
		h.t.Fatalf("%s: bridge account checkpoint %d, want %d", step, got, cursor)
	}
	if got := h.complete(); got != complete {
		h.t.Fatalf("%s: startup complete %v, want %v", step, got, complete)
	}
}

// A request this node must check, read after finality, whose transaction the
// endpoint has no receipt for and whose block the node's EVM cursor has not
// reached, cannot be judged yet. The EVM scan is what records the block, so a
// start held on that request lets the scan run. The receive stays where it is
// and nothing is signed until the scan has recorded the event and the request
// has been compared with it.
func TestHeldStartupLetsTheEvmScanRecordTheSource(t *testing.T) {
	h := newHeldStart(t)
	h.evm.hasEvent.Store(true)
	h.unwrapReceive()
	h.watchedFrom(2)

	if err := h.start(); err != nil {
		t.Fatalf("a start held for the record of a source event ended the process, before the EVM scan could run: %v", err)
	}
	waitFor(t, "the hold to be reported", func() bool { return h.hold() != nil })
	if hold := h.hold(); hold.Height != 2 || hold.Since.IsZero() {
		t.Fatalf("reported hold %+v, want height 2 with a start time", hold)
	}
	if !h.net.aheadOfBridgeAccount() {
		t.Fatal("the network does not know that its scan runs ahead of the read of the bridge account")
	}
	if !h.m.CompletesStartupLater() {
		t.Fatal("the manager does not say that it completes this startup itself")
	}
	h.retries(3)
	h.expect("before the scan", common.LiveState, 1, false)
	if row, err := h.stores.evm[1].GetUnwrapRequestByHashAndLog(h.bridge.txHash, 0); err != nil || row != nil {
		t.Fatalf("a row was stored for a request that is not checked yet: %v, err %v", row, err)
	}

	h.evm.open()
	waitFor(t, "startup to complete", h.complete)
	h.expect("after the scan", common.LiveState, 2, true)
	if h.net.aheadOfBridgeAccount() {
		t.Fatal("the network still takes its scan to be ahead of the bridge account")
	}
	row, err := h.stores.evm[1].GetUnwrapRequestByHashAndLog(h.bridge.txHash, 0)
	if err != nil || row == nil {
		t.Fatalf("stored row %v, err %v", row, err)
	}
	if row.RedeemStatus != common.PendingRedeemStatus || row.BlockHash != h.bridge.blockHash || row.BlockNumber != 100 {
		t.Fatalf("stored row has status %d, block %d %s; want the registered request with the block the scan recorded", row.RedeemStatus, row.BlockNumber, row.BlockHash)
	}
	if h.hold() != nil {
		t.Fatalf("the hold is still reported after startup completed: %+v", h.hold())
	}
	if h.evmCursor() != 200 {
		t.Fatalf("EVM cursor %d, want 200", h.evmCursor())
	}
	if h.stopRequested() {
		t.Fatal("a held start that completed asked the node to stop")
	}
}

// A request whose recorded block no longer answers waits for the record as
// well. A backfill the operator asked for rewinds the EVM scan; it runs while
// the start is held and writes the record again.
func TestHeldStartupLetsARequestedBackfillRun(t *testing.T) {
	h := newHeldStart(t)
	h.evm.hasEvent.Store(true)
	h.unwrapReceive()
	h.watchedFrom(2)
	// This signer recorded the event in a block that is no longer the
	// canonical one, and its EVM cursor is past the event.
	if err := h.stores.evm[1].AddUnwrapRequest(events.UnwrapRequestEvm{
		NetworkClass: definition.EvmClass, ChainId: 1, BlockNumber: 100, BlockHash: ecommon.HexToHash("0xdead"),
		TransactionHash: h.bridge.txHash, LogIndex: 0, From: ecommon.HexToAddress("0x03"),
		To: h.bridge.recipient.String(), Token: h.bridge.token, Amount: big.NewInt(100), RedeemStatus: common.UnredeemedStatus,
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.stores.evm[1].SetLastUpdateHeight(200); err != nil {
		t.Fatal(err)
	}
	h.m.SetEvmBackfillBlocks(150)

	if err := h.start(); err != nil {
		t.Fatalf("a start held for the record of a source event ended the process, before the backfill could run: %v", err)
	}
	h.retries(2)
	h.expect("before the backfill", common.LiveState, 1, false)
	h.evm.open()
	waitFor(t, "startup to complete", h.complete)
	h.expect("after the backfill", common.LiveState, 2, true)
	row, err := h.stores.evm[1].GetUnwrapRequestByHashAndLog(h.bridge.txHash, 0)
	if err != nil || row == nil || row.BlockHash != h.bridge.blockHash || row.RedeemStatus != common.PendingRedeemStatus {
		t.Fatalf("stored row %+v, err %v; want the registered request in the canonical block", row, err)
	}
}

// When the scan finishes without the event, the request stays held: its
// checkpoint does not move and startup does not complete. The time the scans
// took does not count; ten minutes after they finished a signer that is
// signing transfers stops.
func TestHeldStartupWithoutTheSourceStopsTenMinutesAfterTheScans(t *testing.T) {
	h := newHeldStart(t)
	h.unwrapReceive()
	h.watchedFrom(2)
	if err := h.start(); err != nil {
		t.Fatal(err)
	}
	h.retries(1)
	h.elapsed.Store(int64(3 * maxBridgeAccountHold))
	h.retries(3)
	if h.stopRequested() {
		t.Fatal("the node was stopped while the EVM scan could still record the event")
	}

	h.evm.open()
	waitFor(t, "the EVM scan to finish", h.scansFinished)
	h.elapsed.Store(int64(4*maxBridgeAccountHold - time.Second))
	h.retries(3)
	if h.stopRequested() {
		t.Fatal("the node was stopped before the limit had passed since the scan")
	}
	h.expect("held after the scan", common.LiveState, 1, false)

	h.elapsed.Store(int64(4 * maxBridgeAccountHold))
	select {
	case <-h.stop:
	case <-time.After(10 * time.Second):
		t.Fatal("the node was not stopped after the limit")
	}
	h.expect("stopped", common.LiveState, 1, false)
	if hold := h.hold(); hold == nil || hold.Height != 2 {
		t.Fatalf("the snapshot no longer shows the hold that stopped the node: %+v", hold)
	}
	if h.evmCursor() != 200 {
		t.Fatalf("EVM cursor %d, want 200: the scan did not run to the head", h.evmCursor())
	}
}

// Startup reads a request without its source, records the emergency and
// checkpoints that receive. The next receive cannot be read. The start still
// returns, so that the node goes on to the loop that signs the halt; the
// blocked receive keeps its place, startup is not complete, and the hold does
// not stop a signer in an emergency.
func TestStartupBlockedAfterARecordedEmergencyStillReturns(t *testing.T) {
	h := newHeldStart(t)
	h.zenon.change(func(z *heldZenon) { z.frontier = 104 }) // the receives are not final yet
	if err := h.n.state.SetFrontierMomentum(104); err != nil {
		t.Fatal(err)
	}
	h.unwrapReceive()
	blocked := h.plainReceive()
	h.setUnreadable(blocked, true)

	if err := h.start(); err != nil {
		t.Fatalf("the start of a signer with a recorded emergency ended the process: %v", err)
	}
	h.expect("start returned", common.EmergencyState, 2, false)
	h.retries(1)
	if hold := h.hold(); hold.Height != blocked {
		t.Fatalf("reported hold %+v, want height %d", hold, blocked)
	}

	h.evm.open()
	waitFor(t, "the EVM scan to finish", h.scansFinished)
	h.elapsed.Store(int64(5 * maxBridgeAccountHold))
	h.retries(3)
	if h.stopRequested() {
		t.Fatal("a held start stopped a signer in an emergency")
	}
	h.expect("held", common.EmergencyState, 2, false)

	h.setUnreadable(blocked, false)
	waitFor(t, "startup to complete", h.complete)
	h.expect("receive readable", common.EmergencyState, blocked, true)
}

// A signer in EmergencyState or HaltedState signs no transfer, and has the
// halt to sign or to keep. A receive that cannot be read does not end its
// start, whatever the reason.
func TestBlockedStartupKeepsASignerThatIsNotSigningTransfers(t *testing.T) {
	for _, state := range []uint8{common.EmergencyState, common.HaltedState} {
		t.Run(common.StateToText(state), func(t *testing.T) {
			h := newHeldStart(t)
			h.setState(state)
			h.setUnreadable(h.plainReceive(), true)
			if err := h.start(); err != nil {
				t.Fatalf("the start ended the process: %v", err)
			}
			h.retries(2)
			h.expect("held", state, 1, false)
		})
	}
}

// For a signer that is signing transfers, a receive that fails for a reason
// the EVM scan cannot remove ends the start, as before. No EVM network is
// started and nothing is left running.
func TestBlockedStartupEndsASignerThatIsSigningTransfers(t *testing.T) {
	h := newHeldStart(t)
	h.setUnreadable(h.plainReceive(), true)
	err := h.start()
	if err == nil || !strings.Contains(err.Error(), "invalid paired send") {
		t.Fatalf("start returned %v, want the failure of the receive", err)
	}
	h.expect("start failed", common.LiveState, 1, false)
	time.Sleep(200 * time.Millisecond)
	if calls := h.evm.calls.Load(); calls != 0 {
		t.Fatalf("%d calls to the EVM endpoint after a start that failed", calls)
	}
	if h.hold() != nil {
		t.Fatalf("a start that failed reports a hold: %+v", h.hold())
	}
}

// A start that nothing holds waits for the EVM scans, as before, and the node
// completes startup when it returns.
func TestStartWaitsForTheEvmScansAndCompletesStartup(t *testing.T) {
	h := newHeldStart(t)
	h.plainReceive()
	result := make(chan error, 1)
	go func() { result <- h.m.Start() }()
	waitFor(t, "the EVM scan to begin", func() bool { return h.evm.calls.Load() > 3 })
	select {
	case err := <-result:
		t.Fatalf("the start returned before the EVM scan finished: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	if h.complete() {
		t.Fatal("startup is complete before the EVM scan finished")
	}
	h.evm.open()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the start did not return")
	}
	// The node completes startup once it has looked at the halt flags.
	h.expect("started", common.LiveState, 2, false)
	if err := h.m.CompleteStartup(); err != nil {
		t.Fatal(err)
	}
	h.expect("completed", common.LiveState, 2, true)
	if h.m.CompletesStartupLater() {
		t.Fatal("the manager says it completes later a startup that was complete when it returned")
	}
	if h.evmCursor() != 200 || h.hold() != nil {
		t.Fatalf("EVM cursor %d, hold %+v", h.evmCursor(), h.hold())
	}
}

// Signing the halt needs the network clients, not the EVM scans. The start of
// a signer in EmergencyState or HaltedState returns without waiting for them,
// and startup is complete only once they have finished.
func TestSignerNotSigningTransfersDoesNotWaitForTheEvmScans(t *testing.T) {
	for _, state := range []uint8{common.EmergencyState, common.HaltedState} {
		t.Run(common.StateToText(state), func(t *testing.T) {
			h := newHeldStart(t)
			h.setState(state)
			h.plainReceive()
			if err := h.start(); err != nil {
				t.Fatal(err)
			}
			h.expect("start returned", state, 2, false)
			if h.hold() != nil {
				t.Fatalf("a bridge account that was read reports a hold: %+v", h.hold())
			}
			h.evm.open()
			waitFor(t, "startup to complete", h.complete)
			if h.evmCursor() != 200 {
				t.Fatalf("EVM cursor %d, want 200", h.evmCursor())
			}
		})
	}
}

// An EVM network that fails to start in the background ends the process when
// the signer is signing transfers, as it does when the start waits for it.
func TestEvmNetworkThatFailsBesideAHeldStartStopsASignerThatIsSigningTransfers(t *testing.T) {
	h := newHeldStart(t)
	h.unwrapReceive()
	h.watchedFrom(2)
	// The store's cursor is ahead of the chain, which the scan refuses.
	if err := h.stores.evm[1].SetLastUpdateHeight(900); err != nil {
		t.Fatal(err)
	}
	h.evm.open()
	if err := h.start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.stop:
	case <-time.After(10 * time.Second):
		t.Fatal("the node was not stopped")
	}
	h.expect("stopped", common.LiveState, 1, false)
	if h.scansFinished() {
		t.Fatal("a scan that failed counts as finished")
	}
}

// A signer in EmergencyState or HaltedState is kept when an EVM network fails
// to start in the background, and the network is started again.
func TestEvmNetworkThatFailsBesideASignerInAnEmergencyIsStartedAgain(t *testing.T) {
	h := newHeldStart(t)
	h.m.evmStartRetry = 50 * time.Millisecond
	h.evm.open()
	h.setState(common.EmergencyState)
	h.plainReceive()
	if err := h.stores.evm[1].SetLastUpdateHeight(900); err != nil {
		t.Fatal(err)
	}
	if err := h.start(); err != nil {
		t.Fatal(err)
	}
	// Two looks at the node are two starts, so the first one has failed.
	waitFor(t, "the network to be started again", func() bool { return h.evm.starts.Load() >= 2 })
	if h.stopRequested() {
		t.Fatal("an EVM network that did not start stopped a signer in an emergency")
	}
	if h.scansFinished() || h.complete() {
		t.Fatal("startup counts as complete with an EVM network that did not start")
	}
	if err := h.stores.evm[1].SetLastUpdateHeight(90); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "startup to complete", h.complete)
	if h.evmCursor() != 200 {
		t.Fatalf("EVM cursor %d, want 200", h.evmCursor())
	}
}

// A stop during the first read of the bridge account ends the start, whatever
// state the signer is in.
func TestStopDuringTheFirstReadEndsTheStart(t *testing.T) {
	h := newHeldStart(t)
	h.setState(common.EmergencyState)
	stall, stalled := make(chan struct{}), make(chan struct{}, 1)
	h.zenon.change(func(z *heldZenon) { z.stall, z.stalled = stall, stalled })
	t.Cleanup(func() { close(stall) })
	result := make(chan error, 1)
	go func() { result <- h.m.Start() }()
	select {
	case <-stalled:
	case <-time.After(5 * time.Second):
		t.Fatal("the start did not read the bridge account")
	}
	h.n.CancelAccountBlockReads()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the start returned %v, want the cancellation", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the start did not return")
	}
	time.Sleep(100 * time.Millisecond)
	if calls := h.evm.calls.Load(); calls != 0 {
		t.Fatalf("%d calls to the EVM endpoint after a start that was stopped", calls)
	}
}

// Stop ends a held start: the retries of the bridge account and an EVM
// network that is still waiting for its node.
func TestStopEndsAHeldStartup(t *testing.T) {
	h := newHeldStart(t)
	h.evm.syncing.Store(true) // the network waits ten seconds between looks
	h.setState(common.EmergencyState)
	h.setUnreadable(h.plainReceive(), true)
	if err := h.start(); err != nil {
		t.Fatal(err)
	}
	h.retries(1)
	waitFor(t, "the EVM network to ask its node", func() bool { return h.evm.calls.Load() > 0 })
	stopped := make(chan struct{})
	go func() { h.stopNode(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop waited for a held start")
	}
	attempts := h.hold().Attempts
	time.Sleep(150 * time.Millisecond)
	if h.hold().Attempts != attempts {
		t.Fatal("the held start kept retrying after Stop")
	}
	h.expect("stopped", common.EmergencyState, 1, false)
}

// A start reads a receive that is already final as history: what the receive
// changed is taken from what the node read from the bridge when it started. A
// retry of a held start can read a change made after that. It then reads no
// further receive, which the values the node holds would be used to judge, and
// does not complete. A signer that is signing transfers stops, and its next
// start reads the bridge as it is then.
func TestHeldStartupStopsReadingAtASettingsChange(t *testing.T) {
	h := newHeldStart(t)
	// Held for the record of a source event, which the scan supplies once
	// the test opens the endpoint.
	h.evm.hasEvent.Store(true)
	blocked := h.unwrapReceive()
	h.watchedFrom(blocked)
	if err := h.start(); err != nil {
		t.Fatal(err)
	}
	h.retries(2)
	changed := h.metadataReceive()
	h.plainReceive() // behind the change
	h.retries(2)
	h.evm.open()
	select {
	case <-h.stop:
	case <-time.After(20 * time.Second):
		t.Fatal("the node was not stopped")
	}
	h.expect("stopped", common.LiveState, changed, false)
	if h.applied.Load() != 0 {
		t.Fatal("a settings change read as history was applied from the send")
	}
	time.Sleep(150 * time.Millisecond)
	if got := h.cursor(); got != changed {
		t.Fatalf("the start read on to height %d after the settings change at %d", got, changed)
	}
}

// A signer in EmergencyState is not stopped for it: a restart could cost it
// the key it has loaded to sign the halt. It keeps running, reads no further
// and does not complete. While it does, it fixes no affiliate settings for a
// new event, since it knows the ones it holds may be out of date. Once it has
// signed the halt for the networks it knows and left EmergencyState it stops,
// and its next start reads the bridge as it is then.
func TestSettingsChangeStopsAHeldSignerInAnEmergencyOnceItHasHalted(t *testing.T) {
	h := newHeldStart(t)
	h.evm.open()
	h.setState(common.EmergencyState)
	h.n.state.SetTokensMap(1, types.ZnnTokenStandard.String(), h.bridge.token.Hex())
	h.n.state.SetIsAffiliateProgram(common.AffiliateProgram{Networks: map[uint32]common.AffiliateNetwork{1: {StartingHeight: 1, WZNN: true}}})
	blocked := h.plainReceive()
	h.setUnreadable(blocked, true)
	if err := h.start(); err != nil {
		t.Fatal(err)
	}
	h.retries(2)
	changed := h.metadataReceive()
	h.plainReceive() // behind the change
	h.setUnreadable(blocked, false)
	waitFor(t, "the hold behind the settings change", func() bool { hold := h.hold(); return hold != nil && hold.Height == changed+1 })
	time.Sleep(200 * time.Millisecond)
	if h.stopRequested() {
		t.Fatal("a settings change stopped a signer in an emergency")
	}
	h.expect("emergency", common.EmergencyState, changed, false)
	// Nothing is read at that height while the signer waits, so no attempts
	// are counted there.
	if hold := h.hold(); hold.Height != changed+1 || hold.Attempts != 1 {
		t.Fatalf("reported hold %+v, want one report at height %d", hold, changed+1)
	}
	// A new event: the bridge holds no request for it.
	h.zenon.change(func(z *heldZenon) { z.registered = map[uint32]*definition.UnwrapTokenRequest{} })
	log := h.bridge.sourceLog(t, testRecipient+common.AffiliateProgramAddressSeparator+testRecipient)
	if transfers, err := h.net.unwrapRequestsFromLog(log, nil); err == nil {
		t.Fatalf("the network fixed affiliate settings for an event while the node's settings may be out of date: %d transfers", len(transfers))
	}
	if _, pinned, err := h.stores.evm[1].UnwrapAffiliatePolicy(affiliateEvidenceKey(log)); err != nil || pinned {
		t.Fatalf("affiliate settings fixed %v, err %v", pinned, err)
	}

	h.setState(common.HaltedState)
	select {
	case <-h.stop:
	case <-time.After(10 * time.Second):
		t.Fatal("the node was not stopped once the signer had left the emergency")
	}
	h.expect("stopped", common.HaltedState, changed, false)
}

// A start that reads a settings change as history and then fails on a later
// receive ends, as every failed start did: the next start reads the bridge
// again. Going on in this process would judge what follows with the values
// from before the change. Only a signer in EmergencyState is kept.
func TestStartThatReadASettingsChangeBeforeItFailedEnds(t *testing.T) {
	t.Run("signing transfers, request waits for its source record", func(t *testing.T) {
		h := newHeldStart(t)
		h.evm.hasEvent.Store(true)
		changed := h.metadataReceive()
		blocked := h.unwrapReceive()
		h.watchedFrom(blocked)
		err := h.start()
		if err == nil || !strings.Contains(err.Error(), "cannot be judged") {
			t.Fatalf("the start returned %v, want the failure of the request that waits for its source record", err)
		}
		h.expect("start failed", common.LiveState, changed, false)
		time.Sleep(200 * time.Millisecond)
		if starts := h.evm.starts.Load(); starts != 0 {
			t.Fatalf("the EVM network was started %d times after a start that failed", starts)
		}
	})
	t.Run("halted", func(t *testing.T) {
		h := newHeldStart(t)
		h.setState(common.HaltedState)
		changed := h.metadataReceive()
		h.setUnreadable(h.plainReceive(), true)
		err := h.start()
		if err == nil || !strings.Contains(err.Error(), "invalid paired send") {
			t.Fatalf("the start returned %v, want the failure of the receive", err)
		}
		h.expect("start failed", common.HaltedState, changed, false)
	})
}

// A signer in EmergencyState is kept in that case as in any other, and reads
// no further.
func TestSignerInAnEmergencyThatReadASettingsChangeBeforeItFailedReadsNoFurther(t *testing.T) {
	h := newHeldStart(t)
	h.evm.open()
	h.setState(common.EmergencyState)
	changed := h.metadataReceive()
	blocked := h.plainReceive()
	h.setUnreadable(blocked, true)
	if err := h.start(); err != nil {
		t.Fatalf("the start ended the process: %v", err)
	}
	waitFor(t, "the hold to be reported", func() bool { return h.hold() != nil })
	h.setUnreadable(blocked, false)
	time.Sleep(300 * time.Millisecond)
	h.expect("held", common.EmergencyState, changed, false)
	if hold := h.hold(); hold == nil || hold.Height != blocked {
		t.Fatalf("reported hold %+v, want height %d", hold, blocked)
	}
	if h.stopRequested() {
		t.Fatal("a settings change stopped a signer in an emergency")
	}
}

// A page can list a receive before it is confirmed. The start then looks it
// up by hash and waits for the confirmation. From that lookup on it knows the
// block is confirmed, whatever becomes of the lookups that follow, and an
// endpoint that later ends below the block does not complete the start.
func TestHeldStartupRemembersAReceiveConfirmedWhenItWasLookedUp(t *testing.T) {
	h := newHeldStart(t)
	h.evm.open()
	h.setState(common.EmergencyState)
	blocked := h.plainReceive()
	h.zenon.change(func(z *heldZenon) { z.listedUnconfirmed = map[uint64]bool{blocked: true} })
	h.setUnreadable(blocked, true)
	if err := h.start(); err != nil {
		t.Fatal(err)
	}
	h.retries(2)
	h.zenon.change(func(z *heldZenon) { z.listed = 0 })
	h.retries(3)
	h.expect("the endpoint lists nothing", common.EmergencyState, 1, false)
	h.zenon.change(func(z *heldZenon) { z.listed = -1 })
	h.setUnreadable(blocked, false)
	waitFor(t, "startup to complete", h.complete)
	h.expect("complete", common.EmergencyState, blocked, true)
}

// Once the listener follows the bridge account, a network the bridge added
// that cannot be started stops the signer in any state, as before: a halted
// signer would otherwise go back to signing without it. While the start is
// still reading the account, a signer in EmergencyState or HaltedState is kept
// and the receive is held.
func TestNetworkAddThatFailsStopsASignerOnceTheListenerFollowsTheAccount(t *testing.T) {
	for _, tc := range []struct {
		name    string
		state   uint8
		startup bool // the start is still reading the account
		stops   bool
	}{
		{"halted, listener", common.HaltedState, false, true},
		{"emergency, listener", common.EmergencyState, false, true},
		{"halted, start still reading", common.HaltedState, true, false},
		{"live, start still reading", common.LiveState, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newReplayBridge(t)
			added := &definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 7, Name: "not-configured", ContractAddress: b.contract.Hex()}
			url := replayServer(t, func(method string) interface{} {
				if method == "embedded.bridge.getNetworkInfo" {
					return added
				}
				t.Errorf("unexpected method %s", method)
				return nil
			})
			n, _ := replayNetwork(t, b, url, nil)
			n.stopChan = make(chan os.Signal, 1)
			if err := n.state.SetState(tc.state); err != nil {
				t.Fatal(err)
			}
			n.startupRead = make(chan struct{})
			if !tc.startup {
				close(n.startupRead)
			}
			data, err := definition.ABIBridge.PackMethod(definition.SetNetworkMethodName, added.NetworkClass, added.Id, added.Name, added.ContractAddress, "")
			if err != nil {
				t.Fatal(err)
			}
			send := &api.AccountBlock{AccountBlock: nom.AccountBlock{Hash: types.Hash{9}, Address: heldAdministrator, Data: data}}
			n.administrator = heldAdministrator.String()
			if err := n.InterpretSendBlockData(send, true, 42); err == nil {
				t.Fatal("a network without an endpoint was added")
			}
			stopped := false
			select {
			case <-n.stopChan:
				stopped = true
			default:
			}
			if stopped != tc.stops {
				t.Fatalf("stop requested %v, want %v", stopped, tc.stops)
			}
		})
	}
}

// The node looks at the bridge's halt flags when it starts, for the state the
// signer is in then. The read of the bridge account takes a signer out of key
// generation the bridge has ended; in a held start that happens later. The
// flags are looked at again before startup completes.
func TestHeldStartupLooksAtTheHaltFlagsBeforeItCompletes(t *testing.T) {
	h := newHeldStart(t)
	h.evm.hasEvent.Store(true)
	h.evm.halted.Store(true)
	h.setState(common.KeyGenState) // the bridge no longer allows key generation
	blocked := h.unwrapReceive()
	h.watchedFrom(blocked)
	if err := h.start(); err != nil {
		t.Fatal(err)
	}
	h.retries(2)
	h.expect("held", common.KeyGenState, 1, false)
	h.evm.open()
	waitFor(t, "startup to complete", h.complete)
	h.expect("complete", common.HaltedState, blocked, true)
}

// The halt flags are looked at for the state the signer is in once the node
// has applied what it reads from the bridge when it starts, which can change
// that state. A startup that completes in the background waits for that.
func TestBackgroundStartupWaitsForTheNodeBeforeItLooksAtTheHaltFlags(t *testing.T) {
	h := newHeldStart(t)
	h.evm.open()
	h.evm.halted.Store(true)
	h.setState(common.HaltedState)
	h.plainReceive()
	if err := h.m.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the EVM scan to finish", h.scansFinished)
	time.Sleep(200 * time.Millisecond)
	if h.complete() {
		t.Fatal("startup completed before the node had applied what it reads from the bridge")
	}
	h.setState(common.LiveState) // as applying the bridge's metadata can
	h.n.state.MarkNodeReady()
	waitFor(t, "startup to complete", h.complete)
	h.expect("complete", common.HaltedState, 2, true)
}

// A request that disagrees with the affiliate settings stops the scan at its
// event. The scan still records where the event is, so the read of the bridge
// account, held for exactly that, compares the request with the event and
// judges it.
func TestHeldStartupJudgesARequestTheScanCannotInterpret(t *testing.T) {
	h := newHeldStart(t)
	// The settings in force split this transfer with an affiliate.
	h.n.state.SetTokensMap(1, types.ZnnTokenStandard.String(), h.bridge.token.Hex())
	h.n.state.SetIsAffiliateProgram(common.AffiliateProgram{Networks: map[uint32]common.AffiliateNetwork{1: {StartingHeight: 1, WZNN: true}}})
	to := testRecipient + common.AffiliateProgramAddressSeparator + testRecipient
	h.evm.eventTo.Store(&to)
	h.evm.hasEvent.Store(true)
	// The bridge holds a request for another amount than the event's.
	h.bridge.request.Amount = big.NewInt(999)
	h.zenon.change(func(z *heldZenon) { z.registered = map[uint32]*definition.UnwrapTokenRequest{0: h.bridge.request} })
	blocked := h.unwrapReceive()
	h.watchedFrom(blocked)
	if err := h.start(); err != nil {
		t.Fatal(err)
	}
	h.retries(2)
	h.expect("held", common.LiveState, 1, false)
	h.evm.open()
	waitFor(t, "the request to be judged", func() bool { return replayState(t, h.n) == common.EmergencyState })
	waitFor(t, "the receive to be checkpointed", func() bool { return h.cursor() == blocked })
	if _, pinned, err := h.stores.evm[1].UnwrapAffiliatePolicy(affiliateEvidenceKey(h.evm.event())); err != nil || pinned {
		t.Fatalf("affiliate settings were fixed for an event whose request disagrees with them: %v, err %v", pinned, err)
	}
	// What the scan left is where the event is, not a record of the request:
	// that is for the receive to write, and this one was refused.
	if number, hash, found, err := h.stores.evm[1].UnwrapSourceLocation(h.bridge.txHash, 0); err != nil || !found || number != 100 || hash != h.bridge.blockHash {
		t.Fatalf("recorded location: block %d %s, found %v, err %v", number, hash, found, err)
	}
	if row, err := h.stores.evm[1].GetUnwrapRequestByHashAndLog(h.bridge.txHash, 0); err != nil || row != nil {
		t.Fatalf("a record was stored for a request that has no matching source event: %+v, err %v", row, err)
	}
}

// A stored request can carry a block that has since been replaced on the
// source chain. Where the scan has recorded the event's block on its own
// since, the request is compared with the event there.
func TestRecordedLocationAnswersWhenTheStoredBlockWasReplaced(t *testing.T) {
	for _, tc := range []struct {
		name    string
		located bool
		held    bool
	}{
		{"location recorded", true, false},
		{"no location recorded", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := newLateNode(t, nil)
			// The record names a block that is no longer the canonical one.
			if err := l.stores.evm[1].AddUnwrapRequest(events.UnwrapRequestEvm{
				NetworkClass: definition.EvmClass, ChainId: 1, BlockNumber: 100, BlockHash: ecommon.HexToHash("0xdead"),
				TransactionHash: l.bridge.txHash, LogIndex: l.bridge.request.LogIndex, From: ecommon.HexToAddress("0x03"),
				To: l.bridge.recipient.String(), Token: l.bridge.token, Amount: big.NewInt(100), RedeemStatus: common.UnredeemedStatus,
			}); err != nil {
				t.Fatal(err)
			}
			if tc.located {
				if err := l.stores.evm[1].SaveUnwrapSourceLocation(l.bridge.txHash, l.bridge.request.LogIndex, 100, l.bridge.blockHash); err != nil {
					t.Fatal(err)
				}
			}
			l.source.Store(l.droppedFromIndex())
			l.at(5000, acceptedReceive)
			if err := l.listen(); (err != nil) != tc.held {
				t.Fatalf("error %v, want held %v", err, tc.held)
			}
			wantCursor := uint64(2)
			if tc.held {
				wantCursor = 1
			}
			l.expect("listener", common.LiveState, wantCursor)
		})
	}
}

// A page can list a confirmed block behind one that has no confirmation yet.
// The start notes it all the same; a block without a hash ends what it reads
// of the page.
func TestListedBlocksAreNotedBehindAnUnconfirmedOne(t *testing.T) {
	block := func(height byte, confirmed, hashed bool) *api.AccountBlock {
		listed := startupReceive(height).FromApiMarshalJson()
		if !confirmed {
			listed.ConfirmationDetail = nil
		}
		if !hashed {
			listed.Hash = types.ZeroHash
		}
		return listed
	}
	n := &znnNetwork{}
	n.noteListed([]*api.AccountBlock{block(2, false, true), block(3, true, true), block(4, true, false), block(5, true, true)}, 2)
	if n.listedThrough != 3 || n.pageListed != 3 {
		t.Fatalf("noted through %d for the start and %d for the pass, want 3", n.listedThrough, n.pageListed)
	}
	n.noteListed([]*api.AccountBlock{block(2, true, true)}, 2)
	if n.listedThrough != 3 {
		t.Fatalf("a shorter page lowered what the start was listed to %d", n.listedThrough)
	}
}

// A settings send the bridge rejected changed nothing. A held start that read
// one completes.
func TestHeldStartupCompletesAfterASettingsSendTheBridgeRejected(t *testing.T) {
	h := newHeldStart(t)
	h.evm.open()
	h.setState(common.EmergencyState)
	blocked := h.plainReceive()
	h.setUnreadable(blocked, true)
	if err := h.start(); err != nil {
		t.Fatal(err)
	}
	h.retries(2)
	data, err := definition.ABIBridge.PackMethod(definition.SetBridgeMetadataMethodName, "{}")
	if err != nil {
		t.Fatal(err)
	}
	rejected := h.zenon.append(rejectedReceive, data, types.Address{9})
	h.setUnreadable(blocked, false)
	waitFor(t, "startup to complete", h.complete)
	h.expect("complete", common.EmergencyState, rejected, true)
	if h.stopRequested() {
		t.Fatal("a send the bridge rejected stopped the node")
	}
}

// What the account already held when the hold began stays what it is to any
// start. On a database's first read, a request that is past its redeem delay
// needs no EVM lookup, held start or not.
func TestHeldStartupKeepsEarlierRequestsAsFirstReadHistory(t *testing.T) {
	h := newHeldStart(t)
	h.evm.open()
	h.setState(common.EmergencyState)
	h.bridge.request.Redeemed = 1 // long past its redeem delay
	blocked := h.plainReceive()
	h.setUnreadable(blocked, true)
	last := h.unwrapReceive()
	if err := h.start(); err != nil {
		t.Fatal(err)
	}
	h.retries(3)
	h.setUnreadable(blocked, false)
	waitFor(t, "startup to complete", h.complete)
	h.expect("complete", common.EmergencyState, last, true)
	if lookups := h.evm.receipts.Load(); lookups != 0 {
		t.Fatalf("%d source lookups for an old request on a first read", lookups)
	}
	if row, err := h.stores.evm[1].GetUnwrapRequestByHashAndLog(h.bridge.txHash, 0); err != nil || row == nil {
		t.Fatalf("the old request was not stored: %v, err %v", row, err)
	}
	if got := h.watch(); got != last+1 {
		t.Fatalf("the database vouches for the account from %d, want %d", got, last+1)
	}
}

// The hold of a start is the hold of one block. A pass that fails before it
// reaches that block does not move the hold or restart its clock. An endpoint
// that then ends below the block has not let the start past it, and nothing is
// recorded for the tip it reports.
func TestHeldStartupKeepsItsHoldUntilTheBlockIsRead(t *testing.T) {
	h := newHeldStart(t)
	h.evm.open()
	h.setState(common.EmergencyState)
	// A block that is not a receive writes no checkpoint, so every pass
	// starts below the held receive.
	h.zenon.change(func(z *heldZenon) {
		send := startupReceive(2)
		send.BlockType = nom.BlockTypeContractSend
		z.account = append(z.account, send)
	})
	blocked := h.plainReceive()
	h.setUnreadable(blocked, true)
	if err := h.start(); err != nil {
		t.Fatal(err)
	}
	h.retries(2)
	first := h.hold()
	if first.Height != blocked {
		t.Fatalf("reported hold %+v, want height %d", first, blocked)
	}
	same := func(step string) {
		t.Helper()
		h.retries(3)
		if hold := h.hold(); hold.Height != blocked || !hold.Since.Equal(first.Since) {
			t.Fatalf("%s: the hold moved or restarted: %+v, first %+v", step, hold, first)
		}
		h.expect(step, common.EmergencyState, 1, false)
		if got := h.watch(); got != 0 {
			t.Fatalf("%s: the database vouches for the account from %d before the start has reached the tip", step, got)
		}
	}
	h.elapsed.Store(int64(time.Minute))
	h.zenon.change(func(z *heldZenon) { z.pageFails = true })
	same("page read fails")
	h.zenon.change(func(z *heldZenon) { z.pageFails, z.listed = false, 0 })
	same("the endpoint lists nothing")
	h.zenon.change(func(z *heldZenon) { z.listed = -1 })
	h.setUnreadable(blocked, false)
	waitFor(t, "startup to complete", h.complete)
	h.expect("complete", common.EmergencyState, blocked, true)
	if h.hold() != nil {
		t.Fatalf("the hold is still reported: %+v", h.hold())
	}
	if got := h.watch(); got != blocked+1 {
		t.Fatalf("the database vouches for the account from %d, want %d", got, blocked+1)
	}
}

// A start remembers the confirmed blocks it was listed, not only the one it
// is held at. Once the held block is read, an endpoint that ends there, below
// blocks it listed before, has not shown the start the tip.
func TestHeldStartupDoesNotCompleteBelowBlocksItWasListed(t *testing.T) {
	h := newHeldStart(t)
	h.evm.open()
	h.setState(common.EmergencyState)
	blocked := h.plainReceive()
	h.plainReceive()
	last := h.plainReceive()
	h.setUnreadable(blocked, true)
	if err := h.start(); err != nil {
		t.Fatal(err)
	}
	h.retries(2)
	// The endpoint now ends at the held block, which it serves.
	h.zenon.change(func(z *heldZenon) { z.listed = 1 })
	h.setUnreadable(blocked, false)
	waitFor(t, "the held block to be read", func() bool { return h.cursor() == blocked })
	waitFor(t, "the hold at the next block", func() bool { hold := h.hold(); return hold != nil && hold.Height == blocked+1 })
	h.retries(3)
	h.expect("the endpoint ends below listed blocks", common.EmergencyState, blocked, false)
	if got := h.watch(); got != 0 {
		t.Fatalf("the database vouches for the account from %d on the word of an endpoint that ends below blocks it listed", got)
	}
	h.zenon.change(func(z *heldZenon) { z.listed = -1 })
	waitFor(t, "startup to complete", h.complete)
	h.expect("complete", common.EmergencyState, last, true)
}

// A page is known to list a block before anything on it is looked up. A start
// that fails on such a lookup is held at that block all the same, and an
// endpoint that then ends below the block does not complete it.
func TestHeldStartupKnowsABlockItFailedBeforeReading(t *testing.T) {
	h := newHeldStart(t)
	h.evm.open()
	h.setState(common.EmergencyState)
	// A receive that is not final yet, on a node that says it is syncing:
	// the start asks for the sync status before it reads the page's entries.
	h.zenon.change(func(z *heldZenon) { z.frontier, z.syncing = 104, true })
	if err := h.n.state.SetFrontierMomentum(104); err != nil {
		t.Fatal(err)
	}
	blocked := h.plainReceive()
	if err := h.start(); err != nil {
		t.Fatal(err)
	}
	h.retries(2)
	if hold := h.hold(); hold.Height != blocked {
		t.Fatalf("reported hold %+v, want height %d", hold, blocked)
	}
	h.zenon.change(func(z *heldZenon) { z.syncing, z.listed = false, 0 })
	h.retries(4)
	h.expect("the endpoint lists nothing", common.EmergencyState, 1, false)
	if got := h.watch(); got != 0 {
		t.Fatalf("the database vouches for the account from %d on the word of an endpoint that ends below a block it listed", got)
	}
	h.zenon.change(func(z *heldZenon) { z.listed = -1 })
	waitFor(t, "startup to complete", h.complete)
	h.expect("complete", common.EmergencyState, blocked, true)
}

// A network the bridge added that cannot be started holds the receive that
// added it, here one a held start reads before it is final. That does not
// stop a signer in an emergency.
func TestNetworkAddedDuringAHeldStartThatFailsDoesNotStopASignerInAnEmergency(t *testing.T) {
	h := newHeldStart(t)
	h.evm.open()
	h.setState(common.EmergencyState)
	h.zenon.change(func(z *heldZenon) { z.frontier = 104 }) // the receives are not final, so the start applies them
	if err := h.n.state.SetFrontierMomentum(104); err != nil {
		t.Fatal(err)
	}
	blocked := h.plainReceive()
	h.setUnreadable(blocked, true)
	if err := h.start(); err != nil {
		t.Fatal(err)
	}
	h.retries(1)
	// The bridge lists a network this signer has no endpoint configured for.
	added := &definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 7, Name: "not-configured", ContractAddress: h.bridge.contract.Hex()}
	h.zenon.change(func(z *heldZenon) { z.networks = map[uint32]*definition.NetworkInfo{7: added} })
	data, err := definition.ABIBridge.PackMethod(definition.SetNetworkMethodName, added.NetworkClass, added.Id, added.Name, added.ContractAddress, "")
	if err != nil {
		t.Fatal(err)
	}
	adding := h.zenon.append(acceptedReceive, data, heldAdministrator)
	h.setUnreadable(blocked, false)
	waitFor(t, "the hold at the receive that adds the network", func() bool { hold := h.hold(); return hold != nil && hold.Height == adding })
	h.retries(3)
	if h.stopRequested() {
		t.Fatal("a network that could not be started stopped a signer in an emergency")
	}
	h.expect("held", common.EmergencyState, blocked, false)
}

// runtimeStores also opens a store for a network the listener adds.
type runtimeHeldStores struct {
	replayStores
	stop chan os.Signal
}

func (s runtimeHeldStores) AddEvmEventStore(id uint32, _ string, height uint64) (func(), error) {
	s.evm[id] = unwrap.NewEvmStorage(zdb.NewMemDB(), s.stop, height)
	return func() { delete(s.evm, id) }, nil
}

// The listener starts a network the bridge adds while the account is being
// read, and adds it to the list. The start that follows brings up the networks
// that were there before, and does not start that one a second time.
func TestNetworkAddedDuringTheFirstReadIsStartedOnce(t *testing.T) {
	h := newHeldStart(t)
	h.evm.open()
	second := newHeldEvm(t, h.bridge)
	second.open()
	h.zenon.change(func(z *heldZenon) { z.frontier = 104 }) // the receive is not final, so the start applies it
	if err := h.n.state.SetFrontierMomentum(104); err != nil {
		t.Fatal(err)
	}
	added := &definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 2, Name: "held-start-2", ContractAddress: h.bridge.contract.Hex()}
	h.zenon.change(func(z *heldZenon) { z.networks = map[uint32]*definition.NetworkInfo{2: added} })
	h.n.dbManager = runtimeHeldStores{h.stores, h.stop}
	h.n.networksInfo = map[string]config.BaseNetworkConfig{added.Name: {Urls: []string{second.url}}}
	data, err := definition.ABIBridge.PackMethod(definition.SetNetworkMethodName, added.NetworkClass, added.Id, added.Name, added.ContractAddress, "")
	if err != nil {
		t.Fatal(err)
	}
	adding := h.zenon.append(acceptedReceive, data, heldAdministrator)

	if err := h.start(); err != nil {
		t.Fatal(err)
	}
	h.expect("started", common.LiveState, adding, true)
	if h.m.NetworksLength() != 2 {
		t.Fatalf("%d networks, want the one the bridge listed at first and the one it added", h.m.NetworksLength())
	}
	if starts := second.starts.Load(); starts != 1 {
		t.Fatalf("the network the bridge added was started %d times", starts)
	}
	if starts := h.evm.starts.Load(); starts != 1 {
		t.Fatalf("the network that was there before was started %d times", starts)
	}
}

// A scan that runs ahead of the read of the bridge account can meet the event
// of a request the bridge registered under other settings than those in force
// now. Had the account been read first, the stored request would have shown
// that. So before the scan fixes how the settings apply to an event, it asks
// the bridge for the request.
func TestScanAheadOfTheBridgeAccountAsksTheBridgeForTheRegisteredRequest(t *testing.T) {
	for _, tc := range []struct {
		name       string
		ahead      bool
		registered bool
		local      bool // the store holds this node's own split reading of the event
		conflict   bool
		// The bridge's metadata as it holds it now, if not the node's.
		metadata string
	}{
		{"ahead, request registered whole", true, true, false, true, ""},
		{"ahead, the bridge answers with another request", true, false, false, true, ""},
		{"ahead, request registered whole, own reading stored", true, true, true, true, ""},
		{"ahead, no request registered", true, false, false, false, ""},
		{"ahead, no request registered, own reading stored", true, false, true, false, ""},
		{"account read, request registered whole", false, true, false, false, ""},
		// The bridge no longer splits the transfer, in a receive the read of
		// the account has not reached. Nothing says so but the bridge.
		{"ahead, no request registered, the bridge's settings changed", true, false, false, true,
			`{"affiliateProgram":{"networks":{"1":{"startingHeight":1,"wZNN":false}}}}`},
		{"ahead, no request registered, the bridge's settings agree", true, false, false, false,
			`{"affiliateProgram":{"networks":{"1":{"startingHeight":1,"wZNN":true}}}}`},
		{"account read, the bridge's settings changed", false, false, false, false,
			`{"affiliateProgram":{"networks":{"1":{"startingHeight":1,"wZNN":false}}}}`},
		// The bridge's settings no longer list the chain, so they leave it as
		// updates the read has not reached set it.
		{"ahead, no request registered, the bridge's settings no longer list the chain", true, false, false, true,
			`{"affiliateProgram":{"networks":{"2":{"startingHeight":1,"wZNN":true}}}}`},
		{"ahead, no request registered, the bridge's settings are the ones applied", true, false, false, false,
			`{"affiliateProgram":{"networks":{"1":{"startingHeight":1,"wZNN":true}}}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHeldStart(t)
			// The settings in force split this transfer with an affiliate.
			h.n.state.SetTokensMap(1, types.ZnnTokenStandard.String(), h.bridge.token.Hex())
			h.n.state.SetIsAffiliateProgram(common.AffiliateProgram{Networks: map[uint32]common.AffiliateNetwork{1: {StartingHeight: 1, WZNN: true}}})
			log := h.bridge.sourceLog(t, testRecipient+common.AffiliateProgramAddressSeparator+testRecipient)
			// The bridge holds the transfer whole, for the amount of the event.
			registered := map[uint32]*definition.UnwrapTokenRequest{}
			if tc.registered {
				registered[0] = h.bridge.request
			} else if tc.conflict && tc.metadata == "" {
				// Asked for the affiliate's request, the node returns the
				// one under another log index. That establishes nothing.
				registered[common.AffiliateLogIndexAddition] = h.bridge.request
			}
			h.zenon.change(func(z *heldZenon) {
				z.registered = registered
				// Unless the case says otherwise, the bridge holds the
				// settings the node applied.
				z.metadata = `{"affiliateProgram":{"networks":{"1":{"startingHeight":1,"wZNN":true}}}}`
				if tc.metadata != "" {
					z.metadata = tc.metadata
				}
			})
			if tc.local {
				if err := h.stores.evm[1].AddUnwrapRequest(events.UnwrapRequestEvm{
					NetworkClass: definition.EvmClass, ChainId: 1, BlockNumber: 100, BlockHash: h.bridge.blockHash,
					TransactionHash: h.bridge.txHash, LogIndex: 0, From: ecommon.HexToAddress("0x03"),
					To: h.bridge.recipient.String(), Token: h.bridge.token, Amount: big.NewInt(101), RedeemStatus: common.UnredeemedStatus,
				}); err != nil {
					t.Fatal(err)
				}
			}
			read := make(chan struct{})
			if !tc.ahead {
				close(read)
			}
			h.net.bridgeRead = read

			transfers, err := h.net.unwrapRequestsFromLog(log, nil)
			_, pinned, storeErr := h.stores.evm[1].UnwrapAffiliatePolicy(affiliateEvidenceKey(log))
			if storeErr != nil {
				t.Fatal(storeErr)
			}
			if tc.conflict {
				if err == nil || pinned {
					t.Fatalf("the scan fixed settings that disagree with the registered request: %d transfers, err %v, settings fixed %v", len(transfers), err, pinned)
				}
				return
			}
			if err != nil || len(transfers) != 2 || !pinned {
				t.Fatalf("%d transfers, err %v, settings fixed %v; want the transfer split in two under the settings in force", len(transfers), err, pinned)
			}
		})
	}
}

// The bridge listener adds and removes networks while other goroutines read
// the list, among them a start that brings the networks up in the background.
func TestNetworkListIsReadWhileItChanges(t *testing.T) {
	m := &NetworksManager{logger: zap.NewNop().Sugar()}
	network := func(id uint32) *evmNetwork {
		params, err := config.NewEvmParams(&definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: id})
		if err != nil {
			t.Fatal(err)
		}
		return &evmNetwork{EvmParams: params, done: make(chan struct{})}
	}
	m.AddEvmNetwork(network(1))
	var readers sync.WaitGroup
	done := make(chan struct{})
	for range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				for _, n := range m.Networks() {
					_ = n.ChainId()
				}
				_ = m.NetworksLength()
				_ = m.GetSigners()
			}
		}()
	}
	for i := 0; i < 5000; i++ {
		m.AddEvmNetwork(network(2))
		m.RemoveEvmNetwork(2)
	}
	close(done)
	readers.Wait()
	if m.NetworksLength() != 1 || m.Networks()[0].ChainId() != 1 {
		t.Fatalf("networks after the changes: %d", m.NetworksLength())
	}
}

// A network the bridge removed before a background start reached it is not
// started. Here the bridge removes it during the first read of the bridge
// account, after the start has taken the list of networks.
func TestBackgroundStartSkipsANetworkTheBridgeRemoved(t *testing.T) {
	h := newHeldStart(t)
	h.setState(common.EmergencyState)
	h.plainReceive()
	h.evm.open()
	second := newHeldEvm(t, h.bridge)
	second.open()
	t.Cleanup(h.addEvmNetwork(2, second.url).Stop)
	stall, stalled := make(chan struct{}), make(chan struct{}, 1)
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(stall) }) })
	h.zenon.change(func(z *heldZenon) { z.stall, z.stalled = stall, stalled })
	result := make(chan error, 1)
	go func() { result <- h.start() }()
	select {
	case <-stalled:
	case <-time.After(10 * time.Second):
		t.Fatal("the first read did not begin")
	}
	h.m.RemoveEvmNetwork(2)
	release.Do(func() { close(stall) })
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	waitFor(t, "startup to complete", h.complete)
	if calls := second.calls.Load(); calls != 0 {
		t.Fatalf("%d calls to the endpoint of a network the bridge removed", calls)
	}
}

// The EVM scan can run ahead of the read of the bridge account, and records
// the redeem of a wrap request it finds on the destination chain. The receive
// that made the request, read later, does not put the request back to
// unredeemed.
func TestWrapReceiveReadAfterItsRedeemKeepsTheRedeemStatus(t *testing.T) {
	for _, tc := range []struct {
		name   string
		stored *uint32
		want   uint32
	}{
		{"no row yet", nil, common.UnredeemedStatus},
		{"redeem registered", &[]uint32{common.PendingRedeemStatus}[0], common.PendingRedeemStatus},
		{"redeemed", &[]uint32{common.RedeemedStatus}[0], common.RedeemedStatus},
		{"revoked", &[]uint32{common.RevokedStatus}[0], common.RevokedStatus},
		{"unredeemed", &[]uint32{common.UnredeemedStatus}[0], common.UnredeemedStatus},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newReplayBridge(t)
			id := types.Hash{9}
			request := &definition.WrapTokenRequest{NetworkClass: definition.EvmClass, ChainId: 1, Id: id, ToAddress: "0x04",
				TokenStandard: types.ZnnTokenStandard, TokenAddress: strings.ToLower(b.token.Hex()), Amount: big.NewInt(100), Fee: big.NewInt(1), Signature: "signature"}
			url := replayServer(t, func(method string) interface{} {
				if method == "embedded.bridge.getWrapTokenRequestById" {
					return request
				}
				t.Errorf("unexpected method %s", method)
				return nil
			})
			n, stores := replayNetwork(t, b, url, nil)
			if tc.stored != nil {
				row := common.ZnnWrapToOrchestratorWrap(request)
				row.RedeemStatus = *tc.stored
				if err := stores.znn.AddWrapRequest(row); err != nil {
					t.Fatal(err)
				}
			}
			data, err := definition.ABIBridge.PackMethod(definition.WrapTokenMethodName, request.NetworkClass, request.ChainId, request.ToAddress)
			if err != nil {
				t.Fatal(err)
			}
			send := &api.AccountBlock{AccountBlock: nom.AccountBlock{Hash: id, Data: data}}
			if err := n.InterpretSendBlockData(send, false, 42); err != nil {
				t.Fatal(err)
			}
			row, err := stores.znn.GetWrapRequestById(id)
			if err != nil || row == nil {
				t.Fatalf("stored row %v, err %v", row, err)
			}
			if row.RedeemStatus != tc.want || row.Signature != request.Signature {
				t.Fatalf("stored status %d and signature %q, want status %d and the bridge's signature", row.RedeemStatus, row.Signature, tc.want)
			}
		})
	}
}

// The bridge can remove a network while a background start is in the middle
// of that network's scan. The network keeps its client, so the scan ends on a
// stopped client instead of finding none, and the start goes on without it.
func TestBackgroundStartSurvivesTheRemovalOfTheNetworkItIsScanning(t *testing.T) {
	h := newHeldStart(t)
	h.setState(common.EmergencyState)
	h.plainReceive()
	if err := h.start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the scan to begin", func() bool { return h.evm.calls.Load() > 3 })
	// As the listener does for a network the bridge removed.
	h.m.RemoveEvmNetwork(1)
	h.n.rpcManager.RemoveEvmClient(1)
	h.evm.open()
	waitFor(t, "startup to complete", h.complete)
	if h.m.NetworksLength() != 0 {
		t.Fatalf("%d networks after the removal", h.m.NetworksLength())
	}
	if h.stopRequested() {
		t.Fatal("the removal of a network during its scan stopped the node")
	}
}
