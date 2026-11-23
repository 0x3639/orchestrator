package network

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/zenon-network/go-zenon/chain/nom"
	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/protocol"
	"github.com/zenon-network/go-zenon/rpc/api"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"go.uber.org/zap"

	"orchestrator/common"
	"orchestrator/common/bridge"
	"orchestrator/common/config"
	"orchestrator/common/events"
	"orchestrator/db"
	"orchestrator/db/unwrap"
	"orchestrator/db/wrap"
	"orchestrator/rpc"
)

type replayStores struct {
	znn db.ZnnStorage
	evm map[uint32]db.EvmStorage
}

func (s replayStores) ZnnStorage() db.ZnnStorage          { return s.znn }
func (s replayStores) EvmStorage(id uint32) db.EvmStorage { return s.evm[id] }
func (s replayStores) HasEvmStorage(id uint32) bool       { _, ok := s.evm[id]; return ok }
func (s replayStores) AddEvmEventStore(uint32, string, uint64) (func(), error) {
	panic("unexpected runtime network")
}

// replayBridge is one registered unwrap on chain 1 and the EVM log behind it.
type replayBridge struct {
	contract  ecommon.Address
	token     ecommon.Address
	txHash    ecommon.Hash
	blockHash ecommon.Hash
	recipient types.Address
	request   *definition.UnwrapTokenRequest
}

func newReplayBridge(t *testing.T) *replayBridge {
	t.Helper()
	b := &replayBridge{
		contract: ecommon.HexToAddress("0x01"), token: ecommon.HexToAddress("0x02"),
		txHash: ecommon.HexToHash("0xaa"), blockHash: ecommon.HexToHash("0xab"),
	}
	var err error
	if b.recipient, err = types.ParseAddress(testRecipient); err != nil {
		t.Fatal(err)
	}
	b.request = &definition.UnwrapTokenRequest{
		NetworkClass: definition.EvmClass, ChainId: 1,
		TransactionHash: types.Hash(b.txHash), LogIndex: 0,
		ToAddress: b.recipient, TokenAddress: strings.ToLower(b.token.Hex()),
		TokenStandard: types.ZnnTokenStandard, Amount: big.NewInt(100),
		Signature: "test-signature",
	}
	return b
}

// sourceLog is the Unwrapped event as the EVM contract emitted it, with the
// recipient spelled the way the user typed it.
func (b *replayBridge) sourceLog(t *testing.T, to string) etypes.Log {
	t.Helper()
	contractABI, err := bridge.BridgeMetaData.GetAbi()
	if err != nil {
		t.Fatal(err)
	}
	data, err := contractABI.Events["Unwrapped"].Inputs.NonIndexed().Pack(to, big.NewInt(100))
	if err != nil {
		t.Fatal(err)
	}
	return etypes.Log{
		Address: b.contract, TxHash: b.txHash, BlockHash: b.blockHash, BlockNumber: 100, Index: 0, Data: data,
		Topics: []ecommon.Hash{common.UnwrapSigHash, ecommon.HexToHash("0x03"), ecommon.BytesToHash(b.token.Bytes())},
	}
}

func (b *replayBridge) receipt() *etypes.Receipt {
	return &etypes.Receipt{Status: etypes.ReceiptStatusSuccessful, TxHash: b.txHash, BlockHash: b.blockHash, BlockNumber: big.NewInt(100), Logs: []*etypes.Log{}}
}

func (b *replayBridge) send(t *testing.T, chainID uint32, amount *big.Int, signature string) *api.AccountBlock {
	t.Helper()
	method, err := definition.ABIBridge.PackMethod(definition.UnwrapTokenMethodName,
		b.request.NetworkClass, chainID, b.request.TransactionHash, b.request.LogIndex,
		b.request.ToAddress, b.request.TokenAddress, amount, signature)
	if err != nil {
		t.Fatal(err)
	}
	return &api.AccountBlock{AccountBlock: nom.AccountBlock{Hash: types.Hash{9}, Data: method}}
}

func replayServer(t *testing.T, answer func(method string) interface{}) string {
	t.Helper()
	return replayServerWithParams(t, func(method string, _ []json.RawMessage) interface{} { return answer(method) })
}

func replayServerWithParams(t *testing.T, answer func(method string, params []json.RawMessage) interface{}) string {
	t.Helper()
	return replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		return answer(method, params), nil
	})
}

type replayRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func replayServerWithErrors(t *testing.T, answer func(method string, params []json.RawMessage) (interface{}, *replayRPCError)) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var call struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&call); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		reply := map[string]interface{}{"jsonrpc": "2.0", "id": call.ID}
		if result, rpcErr := answer(call.Method, call.Params); rpcErr != nil {
			reply["error"] = rpcErr
		} else {
			reply["result"] = result
		}
		_ = json.NewEncoder(w).Encode(reply)
	}))
	t.Cleanup(server.Close)
	return server.URL
}

// replayNetwork wires a listener to a Zenon endpoint and one EVM endpoint per
// chain ID.
func replayNetwork(t *testing.T, b *replayBridge, znnURL string, evmURLs map[uint32]string) (*znnNetwork, replayStores) {
	t.Helper()
	stop := make(chan os.Signal, 8)
	rpcs, err := rpc.NewRpcManager(config.BaseNetworkConfig{Urls: []string{znnURL}}, stop)
	if err != nil {
		t.Fatal(err)
	}
	stores := replayStores{znn: wrap.NewZnnStorage(zdb.NewMemDB(), stop), evm: map[uint32]db.EvmStorage{}}
	for id, url := range evmURLs {
		if err := rpcs.AddEvmClient(config.BaseNetworkConfig{Urls: []string{url}}, id, "replay-test", b.contract); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(rpcs.Evm(id).Stop)
		stores.evm[id] = unwrap.NewEvmStorage(zdb.NewMemDB(), stop, 1)
	}
	stateValue := common.LiveState
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &znnNetwork{listenerCtx: ctx, cancelListener: cancel, dbManager: stores, rpcManager: rpcs,
		state: common.NewGlobalState(&stateValue), stopChan: stop, logger: zap.NewNop().Sugar()}, stores
}

func replayState(t *testing.T, n *znnNetwork) uint8 {
	t.Helper()
	state, err := n.state.GetState()
	if err != nil {
		t.Fatal(err)
	}
	return state
}

// Anyone can resend UnwrapToken for a registered transaction and log index
// and put any chain ID in it. The check must use the registered chain.
func TestResentUnwrapNamingAnotherChainIsCheckedOnTheRegisteredChain(t *testing.T) {
	b := newReplayBridge(t)
	b.request.Redeemed = 1
	log := b.sourceLog(t, b.recipient.String())
	var chain1Receipts atomic.Int32
	chain1 := replayServer(t, func(method string) interface{} {
		switch method {
		case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
			return b.request
		case "eth_getTransactionReceipt":
			chain1Receipts.Add(1)
			return b.receipt()
		case "eth_getLogs":
			return []etypes.Log{log}
		}
		t.Errorf("unexpected method %s", method)
		return nil
	})
	var chain2Calls atomic.Int32
	chain2 := replayServer(t, func(string) interface{} { chain2Calls.Add(1); return nil })
	n, stores := replayNetwork(t, b, chain1, map[uint32]string{1: chain1, 2: chain2})

	if err := n.InterpretSendBlockData(b.send(t, 2, big.NewInt(1), "garbage"), true, 42); err != nil {
		t.Fatal(err)
	}
	if row, err := stores.evm[1].GetUnwrapRequestByHashAndLog(b.txHash, 0); err != nil || row == nil {
		t.Fatalf("no row in the store of chain 1, where the request is registered: %v, err %v", row, err)
	}
	if row, err := stores.evm[2].GetUnwrapRequestByHashAndLog(b.txHash, 0); err != nil || row != nil {
		t.Fatalf("a row was written to the store of chain 2: %v, err %v", row, err)
	}
	if got := replayState(t, n); got != common.LiveState {
		t.Fatalf("resent unwrap naming chain 2 changed the state to %s", common.StateToText(got))
	}
	if calls := chain2Calls.Load(); calls != 0 {
		t.Fatalf("the check queried chain 2 %d times for a request registered on chain 1", calls)
	}
	if chain1Receipts.Load() == 0 {
		t.Fatal("the source event was never looked up on chain 1, where the request is registered")
	}
}

// Bridge networks are keyed by class and chain ID. A request registered on
// another class that shares a chain ID with a configured EVM network says
// nothing about that EVM network.
func TestUnwrapRegisteredOnAnotherNetworkClassIsNotCheckedOnEvm(t *testing.T) {
	b := newReplayBridge(t)
	b.request.NetworkClass = definition.EvmClass + 1
	var evmCalls atomic.Int32
	url := replayServer(t, func(method string) interface{} {
		if method == "embedded.bridge.getUnwrapTokenRequestByHashAndLog" {
			return b.request
		}
		evmCalls.Add(1)
		return nil
	})
	n, stores := replayNetwork(t, b, url, map[uint32]string{1: url})
	if err := n.InterpretSendBlockData(b.send(t, 1, b.request.Amount, b.request.Signature), true, 42); err != nil {
		t.Fatal(err)
	}
	if got := replayState(t, n); got != common.LiveState {
		t.Fatalf("state %s", common.StateToText(got))
	}
	if evmCalls.Load() != 0 {
		t.Fatalf("%d EVM calls for a request on another network class", evmCalls.Load())
	}
	if row, err := stores.evm[1].GetUnwrapRequestByHashAndLog(b.txHash, 0); err != nil || row != nil {
		t.Fatalf("stored a row on the EVM network: %v, err %v", row, err)
	}
}

// Redeem and revoke look the request up on Zenon and then update the EVM store
// by chain ID. A request on another network class must not touch the row an
// EVM network holds under the same transaction hash and log index, and a
// request on that EVM network must still be recorded.
func TestRedeemAndRevokeRecordOnlyRequestsOnTheEvmNetwork(t *testing.T) {
	for _, tc := range []struct {
		name     string
		method   string
		class    uint32
		redeemed uint8
		revoked  uint8
		want     uint32
	}{
		{"redeem registered, EVM", definition.RedeemUnwrapMethodName, definition.EvmClass, 0, 0, common.PendingRedeemStatus},
		{"redeem completed, EVM", definition.RedeemUnwrapMethodName, definition.EvmClass, 1, 0, common.RedeemedStatus},
		{"redeem attempted on a revoked request, EVM", definition.RedeemUnwrapMethodName, definition.EvmClass, 0, 1, common.RevokedStatus},
		{"revoke, EVM", definition.RevokeUnwrapRequestMethodName, definition.EvmClass, 0, 1, common.RevokedStatus},
		{"redeem, another class", definition.RedeemUnwrapMethodName, definition.EvmClass + 1, 1, 0, common.UnredeemedStatus},
		{"revoke, another class", definition.RevokeUnwrapRequestMethodName, definition.EvmClass + 1, 0, 1, common.UnredeemedStatus},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newReplayBridge(t)
			b.request.NetworkClass = tc.class
			b.request.Redeemed, b.request.Revoked = tc.redeemed, tc.revoked
			url := replayServer(t, func(rpcMethod string) interface{} {
				if rpcMethod == "embedded.bridge.getUnwrapTokenRequestByHashAndLog" {
					return b.request
				}
				t.Errorf("unexpected method %s", rpcMethod)
				return nil
			})
			n, stores := replayNetwork(t, b, url, map[uint32]string{1: url})
			// The EVM network's own unsigned row under the same key.
			local := events.UnwrapRequestEvm{
				NetworkClass: definition.EvmClass, ChainId: 1, BlockNumber: 100, BlockHash: b.blockHash,
				TransactionHash: b.txHash, LogIndex: 0, From: ecommon.HexToAddress("0x03"), To: b.recipient.String(), Token: b.token,
				Amount: big.NewInt(100), RedeemStatus: common.UnredeemedStatus,
			}
			if err := stores.evm[1].AddUnwrapRequest(local); err != nil {
				t.Fatal(err)
			}
			data, err := definition.ABIBridge.PackMethod(tc.method, b.request.TransactionHash, b.request.LogIndex)
			if err != nil {
				t.Fatal(err)
			}
			send := &api.AccountBlock{AccountBlock: nom.AccountBlock{Hash: types.Hash{9}, Address: b.recipient, Data: data}}
			n.administrator = send.Address.String()
			if err := n.InterpretSendBlockData(send, true, 42); err != nil {
				t.Fatal(err)
			}
			row, err := stores.evm[1].GetUnwrapRequestByHashAndLog(b.txHash, 0)
			if err != nil || row == nil {
				t.Fatalf("stored row: %v, err %v", row, err)
			}
			if row.RedeemStatus != tc.want {
				t.Fatalf("EVM row status %d, want %d", row.RedeemStatus, tc.want)
			}
		})
	}
}

// The receive registered the request, so an endpoint that reports none is
// behind the ledger the receive was read from. While the receive is live the
// listener waits for it; dropping it would drop the source check with it.
func TestAcceptedLiveUnwrapWaitsForTheRegisteredRequest(t *testing.T) {
	for _, tc := range []struct {
		name string
		live bool
		// answer is the Zenon endpoint's reply to the request lookup.
		answer func() (interface{}, *replayRPCError)
		held   bool
	}{
		{"live, no data", true, func() (interface{}, *replayRPCError) { return nil, &replayRPCError{-32000, "data non existent"} }, true},
		{"live, null result", true, func() (interface{}, *replayRPCError) { return nil, nil }, true},
		{"live, network removed", true, func() (interface{}, *replayRPCError) { return nil, &replayRPCError{-32000, "unknown network"} }, false},
		{"history, no data", false, func() (interface{}, *replayRPCError) { return nil, &replayRPCError{-32000, "data non existent"} }, false},
		{"history, null result", false, func() (interface{}, *replayRPCError) { return nil, nil }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newReplayBridge(t)
			frontier := uint64(104)
			if !tc.live {
				frontier = 1000
			}
			ledger := newReplayLedger(t, b, frontier, receiveStatus(vmResultSuccess))
			url := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
				if result, ok := ledger.answer(t, method, params); ok {
					return result, nil
				}
				if method == "embedded.bridge.getUnwrapTokenRequestByHashAndLog" {
					return tc.answer()
				}
				t.Errorf("unexpected method %s", method)
				return nil, nil
			})
			n, stores := replayNetwork(t, b, url, map[uint32]string{1: url})
			n.SetConfirmationsToFinality(5)
			if err := n.state.SetFrontierMomentum(frontier); err != nil {
				t.Fatal(err)
			}
			height, _, err := n.processBridgeAccountPage(n.listenerCtx, 1, n.refreshSettings)
			wantHeight := uint64(2)
			if tc.held {
				wantHeight = 1
			}
			if (err != nil) != tc.held || height != wantHeight {
				t.Fatalf("height %d, err %v; want height %d, held %v", height, err, wantHeight, tc.held)
			}
			if got := replayState(t, n); got != common.LiveState {
				t.Fatalf("state %s", common.StateToText(got))
			}
			cursor, err := stores.znn.GetLastUpdateHeight()
			if err != nil || cursor != wantHeight {
				t.Fatalf("checkpoint %d, err %v, want %d", cursor, err, wantHeight)
			}
		})
	}
}

// go-zenon's VM result codes for an embedded receive (vm/vm.go).
const (
	vmResultInvalid uint64 = iota
	vmResultSuccess
	vmResultFail
)

func receiveStatus(status uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, status)
}

// replayLedger serves consecutive bridge receives from account height 2, each
// paired with its own copy of the UnwrapToken send, the way a Zenon node
// returns them.
type replayLedger struct {
	blocks   map[types.Hash]*api.AccountBlockMarshal
	receives []*api.AccountBlockMarshal
	frontier uint64
}

func newReplayLedger(t *testing.T, b *replayBridge, frontier uint64, statuses ...[]byte) *replayLedger {
	t.Helper()
	l := &replayLedger{blocks: map[types.Hash]*api.AccountBlockMarshal{}, frontier: frontier}
	for i, status := range statuses {
		receive := startupReceive(byte(2 + i))
		receive.Data = status
		send := startupSend(receive)
		send.Data = b.send(t, b.request.ChainId, b.request.Amount, b.request.Signature).Data
		l.receives = append(l.receives, receive)
		l.blocks[receive.Hash], l.blocks[send.Hash] = receive, send
	}
	return l
}

func (l *replayLedger) answer(t *testing.T, method string, params []json.RawMessage) (interface{}, bool) {
	switch method {
	case "ledger.getAccountBlocksByHeight":
		var height uint64
		if len(params) < 2 || json.Unmarshal(params[1], &height) != nil {
			t.Errorf("unexpected page request %s", params)
		}
		var page []*api.AccountBlockMarshal
		for _, receive := range l.receives {
			if receive.Height >= height {
				page = append(page, receive)
			}
		}
		return startupPage(page...), true
	case "ledger.getAccountBlockByHash":
		var hash types.Hash
		if len(params) < 1 || json.Unmarshal(params[0], &hash) != nil {
			t.Errorf("unexpected block request %s", params)
		}
		return l.blocks[hash], true
	case "stats.syncInfo":
		return &protocol.SyncInfo{State: protocol.SyncDone}, true
	case "ledger.getFrontierMomentum":
		return &api.Momentum{Momentum: &nom.Momentum{Height: l.frontier, Hash: types.Hash{1}}}, true
	}
	return nil, false
}

// The bridge rejects a duplicate UnwrapToken and still produces a receive
// block. That receive registered nothing, so a missing source receipt is not
// evidence of a forged request. An accepted receive with the same missing
// receipt is exactly what a forged registration looks like. Without a
// readable status the listener can tell neither apart and must wait.
func TestLiveListenerActsOnTheBridgeReceiveStatus(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status []byte
		want   uint8
		held   bool
	}{
		{"bridge rejected the send", receiveStatus(vmResultFail), common.LiveState, false},
		{"bridge accepted the send", receiveStatus(vmResultSuccess), common.EmergencyState, false},
		{"invalid status", receiveStatus(vmResultInvalid), common.LiveState, true},
		{"unknown status", receiveStatus(3), common.LiveState, true},
		{"no status", nil, common.LiveState, true},
		{"short status", []byte{2}, common.LiveState, true},
		{"oversized status", append(receiveStatus(vmResultFail), 0), common.LiveState, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newReplayBridge(t)
			b.request.Redeemed = 1
			ledger := newReplayLedger(t, b, 104, tc.status)
			var lookups atomic.Int32
			url := replayServerWithParams(t, func(method string, params []json.RawMessage) interface{} {
				if result, ok := ledger.answer(t, method, params); ok {
					return result
				}
				lookups.Add(1)
				switch method {
				case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
					return b.request
				case "eth_getTransactionReceipt":
					return nil // the endpoint no longer serves this old receipt
				}
				t.Errorf("unexpected method %s", method)
				return nil
			})
			n, stores := replayNetwork(t, b, url, map[uint32]string{1: url})
			n.SetConfirmationsToFinality(5)
			if err := n.state.SetFrontierMomentum(104); err != nil {
				t.Fatal(err)
			}
			// A redeemed row with its EVM block and sender, as this signer
			// stored it long before the replay.
			sender := ecommon.HexToAddress("0x03")
			stored := common.ZnnUnwrapToOrchestatorUnwrap(b.request)
			stored.BlockNumber, stored.BlockHash, stored.From = 100, b.blockHash, sender
			if err := stores.evm[1].AddUnwrapRequest(stored); err != nil {
				t.Fatal(err)
			}

			height, _, err := n.processBridgeAccountPage(n.listenerCtx, 1, n.refreshSettings)
			wantHeight := uint64(2)
			if tc.held {
				wantHeight = 1
			}
			if (err != nil) != tc.held || height != wantHeight {
				t.Fatalf("height %d, err %v; want height %d, held %v", height, err, wantHeight, tc.held)
			}
			if got := replayState(t, n); got != tc.want {
				t.Fatalf("state %s, want %s", common.StateToText(got), common.StateToText(tc.want))
			}
			if accepted := tc.want == common.EmergencyState; !accepted && lookups.Load() != 0 {
				t.Fatalf("%d request or source lookups for a receive that was not accepted", lookups.Load())
			}
			cursor, err := stores.znn.GetLastUpdateHeight()
			if err != nil || cursor != wantHeight {
				t.Fatalf("checkpoint %d, err %v, want %d", cursor, err, wantHeight)
			}
			row, err := stores.evm[1].GetUnwrapRequestByHashAndLog(b.txHash, 0)
			if err != nil || row == nil {
				t.Fatalf("stored row: %v, err %v", row, err)
			}
			if row.RedeemStatus != common.RedeemedStatus || row.BlockHash != b.blockHash || row.From != sender {
				t.Fatalf("the receive rewrote the stored row: status %d, block %s, sender %s", row.RedeemStatus, row.BlockHash, row.From)
			}
		})
	}
}

// Each receive carries its own status. Rejected duplicates around an accepted
// registration that has no source event must not hide it.
func TestRejectedDuplicatesDoNotHideAForgedRegistration(t *testing.T) {
	rejected, accepted := receiveStatus(vmResultFail), receiveStatus(vmResultSuccess)
	for name, statuses := range map[string][][]byte{
		"duplicate, then registration":          {rejected, accepted},
		"registration, then duplicate":          {accepted, rejected},
		"duplicates before and after":           {rejected, accepted, rejected},
		"only duplicates (control, stays live)": {rejected, rejected},
	} {
		t.Run(name, func(t *testing.T) {
			b := newReplayBridge(t)
			ledger := newReplayLedger(t, b, 104, statuses...)
			url := replayServerWithParams(t, func(method string, params []json.RawMessage) interface{} {
				if result, ok := ledger.answer(t, method, params); ok {
					return result
				}
				switch method {
				case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
					return b.request
				case "eth_getTransactionReceipt":
					return nil // no such transaction on the source chain
				}
				t.Errorf("unexpected method %s", method)
				return nil
			})
			n, _ := replayNetwork(t, b, url, map[uint32]string{1: url})
			n.SetConfirmationsToFinality(5)
			if err := n.state.SetFrontierMomentum(104); err != nil {
				t.Fatal(err)
			}
			height, _, err := n.processBridgeAccountPage(n.listenerCtx, 1, n.refreshSettings)
			if want := uint64(1 + len(statuses)); err != nil || height != want {
				t.Fatalf("height %d, err %v, want %d", height, err, want)
			}
			want := common.EmergencyState
			if strings.HasPrefix(name, "only duplicates") {
				want = common.LiveState
			}
			if got := replayState(t, n); got != want {
				t.Fatalf("state %s, want %s", common.StateToText(got), common.StateToText(want))
			}
		})
	}
}

// The listener retries a held receive while the frontier moves on. A receive
// that was live when it was first held still gets its source check when it
// resolves after finality. A receive first seen after finality does not gain
// one by being held.
func TestHeldLiveReceiveKeepsItsSourceCheck(t *testing.T) {
	var unreadable []byte
	accepted, rejected := receiveStatus(vmResultSuccess), receiveStatus(vmResultFail)
	// The receive confirms at momentum 100 and finality is 5 momentums.
	const live, final = 104, 110
	for _, tc := range []struct {
		name              string
		firstFrontier     uint64
		firstStatus       []byte
		firstReceiptFails bool
		resolvedStatus    []byte
		want              uint8
		heldAsLiveAtFirst bool
		replaced          bool // another receive takes the held one's height
	}{
		{"status unreadable while live, then accepted", live, unreadable, false, accepted, common.EmergencyState, true, false},
		{"source lookup failed while live, then no source", live, accepted, true, accepted, common.EmergencyState, true, false},
		{"status unreadable while live, then rejected", live, unreadable, false, rejected, common.LiveState, true, false},
		{"status unreadable after finality, then accepted", final, unreadable, false, accepted, common.LiveState, false, false},
		{"held while live, then replaced by a receive first seen after finality", live, unreadable, false, accepted, common.LiveState, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newReplayBridge(t)
			var ledger atomic.Pointer[replayLedger]
			var receiptFails atomic.Bool
			ledger.Store(newReplayLedger(t, b, tc.firstFrontier, tc.firstStatus))
			receiptFails.Store(tc.firstReceiptFails)
			url := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
				if result, ok := ledger.Load().answer(t, method, params); ok {
					return result, nil
				}
				switch method {
				case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
					return b.request, nil
				case "eth_getTransactionReceipt":
					if receiptFails.Load() {
						return nil, &replayRPCError{Code: -32000, Message: "upstream timeout"}
					}
					return nil, nil // no such transaction on the source chain
				}
				t.Errorf("unexpected method %s", method)
				return nil, nil
			})
			n, _ := replayNetwork(t, b, url, map[uint32]string{1: url})
			n.SetConfirmationsToFinality(5)
			if err := n.state.SetFrontierMomentum(tc.firstFrontier); err != nil {
				t.Fatal(err)
			}

			height, _, err := n.processBridgeAccountPage(n.listenerCtx, 1, n.refreshSettings)
			if err == nil || height != 1 {
				t.Fatalf("first pass: height %d, err %v, want the receive held at 1", height, err)
			}
			if got := replayState(t, n); got != common.LiveState {
				t.Fatalf("first pass: state %s", common.StateToText(got))
			}
			if held := n.heldLiveReceive != types.ZeroHash; held != tc.heldAsLiveAtFirst {
				t.Fatalf("first pass: held as live %v, want %v", held, tc.heldAsLiveAtFirst)
			}

			// The endpoint recovers only after the receive is final.
			const later = 120
			resolved := newReplayLedger(t, b, later, tc.resolvedStatus)
			if tc.replaced {
				receive := resolved.receives[0]
				delete(resolved.blocks, receive.Hash)
				receive.Hash = types.Hash{9}
				resolved.blocks[receive.Hash] = receive
			}
			ledger.Store(resolved)
			receiptFails.Store(false)
			if err := n.state.SetFrontierMomentum(later); err != nil {
				t.Fatal(err)
			}
			height, _, err = n.processBridgeAccountPage(n.listenerCtx, 1, n.refreshSettings)
			if err != nil || height != 2 {
				t.Fatalf("second pass: height %d, err %v, want 2", height, err)
			}
			if got := replayState(t, n); got != tc.want {
				t.Fatalf("second pass: state %s, want %s", common.StateToText(got), common.StateToText(tc.want))
			}
			if n.heldLiveReceive != types.ZeroHash {
				t.Fatal("the listener still marks a receive it has finished")
			}
		})
	}
}

// Startup reads each receive by hash. An accepted registration is recorded
// against the local row. A rejected duplicate, whose source event does exist,
// must leave that row alone.
func TestStartupSyncRecordsAnAcceptedUnwrapAndSkipsARejectedOne(t *testing.T) {
	for _, tc := range []struct {
		name          string
		status        []byte
		wantStatus    uint32
		wantSignature string
		held          bool
	}{
		{"bridge accepted the send", receiveStatus(vmResultSuccess), common.PendingRedeemStatus, "test-signature", false},
		{"bridge rejected the send", receiveStatus(vmResultFail), common.UnredeemedStatus, "", false},
		{"no status", nil, common.UnredeemedStatus, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newReplayBridge(t)
			log := b.sourceLog(t, b.recipient.String())
			ledger := newReplayLedger(t, b, 1000, tc.status)
			url := replayServerWithParams(t, func(method string, params []json.RawMessage) interface{} {
				if result, ok := ledger.answer(t, method, params); ok {
					return result
				}
				switch method {
				case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
					return b.request
				case "eth_getTransactionReceipt":
					return b.receipt()
				case "eth_getLogs":
					return []etypes.Log{log}
				}
				t.Errorf("unexpected method %s", method)
				return nil
			})
			n, stores := replayNetwork(t, b, url, map[uint32]string{1: url})
			n.SetConfirmationsToFinality(5)
			if err := n.state.SetFrontierMomentum(1000); err != nil {
				t.Fatal(err)
			}
			// The unsigned row this signer ingested from the EVM log.
			sender := ecommon.HexToAddress("0x03")
			if err := stores.evm[1].AddUnwrapRequest(events.UnwrapRequestEvm{
				NetworkClass: definition.EvmClass, ChainId: 1, BlockNumber: 100, BlockHash: b.blockHash,
				TransactionHash: b.txHash, LogIndex: 0, From: sender, To: b.recipient.String(), Token: b.token,
				Amount: big.NewInt(100), RedeemStatus: common.UnredeemedStatus,
			}); err != nil {
				t.Fatal(err)
			}

			if err := n.Sync(); (err != nil) != tc.held {
				t.Fatalf("Sync error %v, want held %v", err, tc.held)
			}
			if got := replayState(t, n); got != common.LiveState {
				t.Fatalf("state %s after a historical receive", common.StateToText(got))
			}
			wantCursor := uint64(2)
			if tc.held {
				wantCursor = 1
			}
			cursor, err := stores.znn.GetLastUpdateHeight()
			if err != nil || cursor != wantCursor {
				t.Fatalf("cursor %d, err %v, want %d", cursor, err, wantCursor)
			}
			row, err := stores.evm[1].GetUnwrapRequestByHashAndLog(b.txHash, 0)
			if err != nil || row == nil {
				t.Fatalf("stored row: %v, err %v", row, err)
			}
			if row.RedeemStatus != tc.wantStatus || row.Signature != tc.wantSignature {
				t.Fatalf("row status %d signature %q, want status %d signature %q", row.RedeemStatus, row.Signature, tc.wantStatus, tc.wantSignature)
			}
		})
	}
}

// The EVM contract emits the recipient as typed, and bech32 accepts an
// all-uppercase spelling. Zenon registers the parsed address.
func TestUnwrapRecipientIsComparedByValue(t *testing.T) {
	const affiliate = "z1qpp8g8v4hrd5ernfrgsn86jmv09ra6zrvhvmr7"
	affiliateAddress, err := types.ParseAddress(affiliate)
	if err != nil {
		t.Fatal(err)
	}
	embeddedAffiliate := types.BridgeContract.String()
	upper := strings.ToUpper
	for _, tc := range []struct {
		name string
		// to is the recipient string in the EVM log, given the registered
		// beneficiary. The log always carries an amount of 100.
		to func(beneficiary string) string
		// program enables the affiliate split, under which signers register
		// 101 for the beneficiary and 2 for the affiliate at a derived index.
		program bool
		// rewardRow registers the affiliate's row instead of the beneficiary's.
		rewardRow bool
		// amount is the registered amount.
		amount int64
		want   uint8
	}{
		{"lowercase", func(a string) string { return a }, false, false, 100, common.LiveState},
		{"uppercase", upper, false, false, 100, common.LiveState},
		{"another address", func(string) string { return affiliate }, false, false, 100, common.EmergencyState},
		{"another address in uppercase", func(string) string { return upper(affiliate) }, false, false, 100, common.EmergencyState},
		{"affiliate split, uppercase beneficiary", func(a string) string { return upper(a) + "&" + affiliate }, true, false, 101, common.LiveState},
		{"affiliate split, uppercase affiliate", func(a string) string { return a + "&" + upper(affiliate) }, true, true, 2, common.LiveState},
		{"affiliate split, another beneficiary", func(string) string { return upper(affiliate) + "&" + affiliate }, true, false, 101, common.EmergencyState},
		{"embedded affiliate leaves the plain row", func(a string) string { return upper(a) + "&" + embeddedAffiliate }, true, false, 100, common.LiveState},
		{"embedded affiliate earns the beneficiary no bonus", func(a string) string { return a + "&" + embeddedAffiliate }, true, false, 101, common.EmergencyState},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newReplayBridge(t)
			log := b.sourceLog(t, tc.to(b.recipient.String()))
			b.request.Amount = big.NewInt(tc.amount)
			if tc.rewardRow {
				b.request.LogIndex = common.AffiliateLogIndexAddition
				b.request.ToAddress = affiliateAddress
			}
			url := replayServer(t, func(method string) interface{} {
				switch method {
				case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
					return b.request
				case "eth_getTransactionReceipt":
					return b.receipt()
				case "eth_getLogs":
					return []etypes.Log{log}
				}
				t.Errorf("unexpected method %s", method)
				return nil
			})
			n, _ := replayNetwork(t, b, url, map[uint32]string{1: url})
			if tc.program {
				n.state.SetTokensMap(1, types.ZnnTokenStandard.String(), b.token.Hex())
				n.state.SetIsAffiliateProgram(common.AffiliateProgram{Networks: map[uint32]common.AffiliateNetwork{
					1: {StartingHeight: 1, ZNN: true, WZNN: true},
				}})
			}
			if err := n.InterpretSendBlockData(b.send(t, 1, b.request.Amount, b.request.Signature), true, 42); err != nil {
				t.Fatal(err)
			}
			if got := replayState(t, n); got != tc.want {
				t.Fatalf("state %s, want %s", common.StateToText(got), common.StateToText(tc.want))
			}
		})
	}
}

// Signers create no request for an unwrap whose beneficiary is an embedded
// contract address, so a registration paying one was not signed from this
// source event, however the log spells it. The same holds for the affiliate
// reward derived from such an event.
func TestRegistrationPayingAnEmbeddedBeneficiaryIsNotASourceMatch(t *testing.T) {
	embedded := types.BridgeContract
	if !types.IsEmbeddedAddress(embedded) {
		t.Fatal("fixture address is not embedded")
	}
	for _, tc := range []struct {
		name string
		// to is the recipient string in the EVM log, which carries amount 100.
		to        string
		program   bool
		rewardRow bool
		amount    int64
	}{
		{"lowercase", embedded.String(), false, false, 100},
		{"uppercase", strings.ToUpper(embedded.String()), false, false, 100},
		{"with affiliate", embedded.String() + "&" + testRecipient, false, false, 100},
		{"with affiliate, program on", embedded.String() + "&" + testRecipient, true, false, 101},
		{"reward row, program on", embedded.String() + "&" + testRecipient, true, true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newReplayBridge(t)
			b.request.ToAddress = embedded
			b.request.Amount = big.NewInt(tc.amount)
			if tc.rewardRow {
				// The reward for an ordinary affiliate address, derived from
				// an event whose beneficiary signers refuse.
				b.request.LogIndex = common.AffiliateLogIndexAddition
				b.request.ToAddress = b.recipient
			}
			log := b.sourceLog(t, tc.to)
			url := replayServer(t, func(method string) interface{} {
				switch method {
				case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
					return b.request
				case "eth_getTransactionReceipt":
					return b.receipt()
				case "eth_getLogs":
					return []etypes.Log{log}
				}
				t.Errorf("unexpected method %s", method)
				return nil
			})
			n, _ := replayNetwork(t, b, url, map[uint32]string{1: url})
			if tc.program {
				n.state.SetTokensMap(1, types.ZnnTokenStandard.String(), b.token.Hex())
				n.state.SetIsAffiliateProgram(common.AffiliateProgram{Networks: map[uint32]common.AffiliateNetwork{
					1: {StartingHeight: 1, ZNN: true, WZNN: true},
				}})
			}
			if err := n.InterpretSendBlockData(b.send(t, 1, b.request.Amount, b.request.Signature), true, 42); err != nil {
				t.Fatal(err)
			}
			if got := replayState(t, n); got != common.EmergencyState {
				t.Fatalf("state %s, want %s", common.StateToText(got), common.StateToText(common.EmergencyState))
			}
		})
	}
}
