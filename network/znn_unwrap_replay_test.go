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
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": call.ID, "result": answer(call.Method, call.Params)})
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
	chain1 := replayServer(t, func(method string) interface{} {
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
	var chain2Calls atomic.Int32
	chain2 := replayServer(t, func(string) interface{} { chain2Calls.Add(1); return nil })
	n, _ := replayNetwork(t, b, chain1, map[uint32]string{1: chain1, 2: chain2})

	if err := n.InterpretSendBlockData(b.send(t, 2, big.NewInt(1), "garbage"), true, 42); err != nil {
		t.Fatal(err)
	}
	if got := replayState(t, n); got != common.LiveState {
		t.Fatalf("resent unwrap naming chain 2 changed the state to %s", common.StateToText(got))
	}
	if calls := chain2Calls.Load(); calls != 0 {
		t.Fatalf("the check queried chain 2 %d times for a request registered on chain 1", calls)
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

// replayLedger serves one bridge receive at account height 2 and its paired
// UnwrapToken send, the way a Zenon node returns them.
type replayLedger struct {
	receive, send *api.AccountBlockMarshal
	frontier      uint64
}

func newReplayLedger(t *testing.T, b *replayBridge, status []byte, frontier uint64) *replayLedger {
	t.Helper()
	receive := startupReceive(2)
	receive.Data = status
	send := startupSend(receive)
	send.Data = b.send(t, b.request.ChainId, b.request.Amount, b.request.Signature).Data
	return &replayLedger{receive: receive, send: send, frontier: frontier}
}

func (l *replayLedger) answer(t *testing.T, method string, params []json.RawMessage) (interface{}, bool) {
	switch method {
	case "ledger.getAccountBlocksByHeight":
		var height uint64
		if len(params) < 2 || json.Unmarshal(params[1], &height) != nil {
			t.Errorf("unexpected page request %s", params)
		}
		if height == l.receive.Height {
			return startupPage(l.receive), true
		}
		return startupPage(), true
	case "ledger.getAccountBlockByHash":
		var hash types.Hash
		if len(params) < 1 || json.Unmarshal(params[0], &hash) != nil {
			t.Errorf("unexpected block request %s", params)
		}
		if hash == l.receive.Hash {
			return l.receive, true
		}
		return l.send, true
	case "stats.syncInfo":
		return &protocol.SyncInfo{State: protocol.SyncDone}, true
	case "ledger.getFrontierMomentum":
		return &api.Momentum{Momentum: &nom.Momentum{Height: l.frontier, Hash: types.Hash{1}}}, true
	}
	return nil, false
}

// The bridge rejects a duplicate UnwrapToken and still produces a receive
// block. That receive registered nothing, so a missing source receipt is not
// evidence of a forged request. Every other status is checked: a successful
// receive with the same missing receipt is exactly what a forged registration
// looks like.
func TestLiveListenerChecksEveryUnwrapReceiveExceptARejectedOne(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status []byte
		want   uint8
	}{
		{"bridge rejected the send", receiveStatus(vmResultFail), common.LiveState},
		{"bridge accepted the send", receiveStatus(vmResultSuccess), common.EmergencyState},
		{"invalid status", receiveStatus(vmResultInvalid), common.EmergencyState},
		{"unknown status", receiveStatus(3), common.EmergencyState},
		{"no status", nil, common.EmergencyState},
		{"short status", []byte{2}, common.EmergencyState},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newReplayBridge(t)
			b.request.Redeemed = 1
			ledger := newReplayLedger(t, b, tc.status, 104)
			url := replayServerWithParams(t, func(method string, params []json.RawMessage) interface{} {
				if result, ok := ledger.answer(t, method, params); ok {
					return result
				}
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
			if err != nil || height != 2 {
				t.Fatalf("height %d, err %v", height, err)
			}
			if got := replayState(t, n); got != tc.want {
				t.Fatalf("state %s, want %s", common.StateToText(got), common.StateToText(tc.want))
			}
			cursor, err := stores.znn.GetLastUpdateHeight()
			if err != nil || cursor != 2 {
				t.Fatalf("cursor %d, err %v; the receive must still be checkpointed", cursor, err)
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

// Startup reads each receive by hash. An accepted registration is recorded
// against the local row. A rejected duplicate, whose source event does exist,
// must leave that row alone.
func TestStartupSyncRecordsAnAcceptedUnwrapAndSkipsARejectedOne(t *testing.T) {
	for _, tc := range []struct {
		name          string
		status        []byte
		wantStatus    uint32
		wantSignature string
	}{
		{"bridge accepted the send", receiveStatus(vmResultSuccess), common.PendingRedeemStatus, "test-signature"},
		{"bridge rejected the send", receiveStatus(vmResultFail), common.UnredeemedStatus, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newReplayBridge(t)
			log := b.sourceLog(t, b.recipient.String())
			ledger := newReplayLedger(t, b, tc.status, 1000)
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

			if err := n.Sync(); err != nil {
				t.Fatal(err)
			}
			if got := replayState(t, n); got != common.LiveState {
				t.Fatalf("state %s after a historical receive", common.StateToText(got))
			}
			cursor, err := stores.znn.GetLastUpdateHeight()
			if err != nil || cursor != 2 {
				t.Fatalf("cursor %d, err %v", cursor, err)
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
	upper := strings.ToUpper
	for _, tc := range []struct {
		name string
		// to is the recipient string in the EVM log, given the registered address.
		to func(registered string) string
		// program enables the affiliate split, which registers 101% for the
		// beneficiary and 2% for the affiliate at a derived log index.
		program   bool
		rewardRow bool
		want      uint8
	}{
		{"lowercase", func(a string) string { return a }, false, false, common.LiveState},
		{"uppercase", upper, false, false, common.LiveState},
		{"another address", func(string) string { return affiliate }, false, false, common.EmergencyState},
		{"another address in uppercase", func(string) string { return upper(affiliate) }, false, false, common.EmergencyState},
		{"affiliate split, uppercase beneficiary", func(a string) string { return upper(a) + "&" + affiliate }, true, false, common.LiveState},
		{"affiliate split, uppercase affiliate", func(a string) string { return a + "&" + upper(affiliate) }, true, true, common.LiveState},
		{"affiliate split, another beneficiary", func(string) string { return upper(affiliate) + "&" + affiliate }, true, false, common.EmergencyState},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newReplayBridge(t)
			log := b.sourceLog(t, tc.to(b.recipient.String()))
			if tc.program {
				b.request.Amount = big.NewInt(101)
			}
			if tc.rewardRow {
				b.request.LogIndex = common.AffiliateLogIndexAddition
				b.request.ToAddress = affiliateAddress
				b.request.Amount = big.NewInt(2)
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
