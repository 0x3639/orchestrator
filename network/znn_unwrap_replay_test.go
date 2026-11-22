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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var call struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&call); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": call.ID, "result": answer(call.Method)})
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

func receiveStatus(status uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, status)
}

// The bridge rejects a duplicate UnwrapToken and still produces a receive
// block. That receive registered nothing, so a missing source receipt is not
// evidence of a forged request. A successful receive with the same missing
// receipt still is.
func TestRejectedUnwrapReceiveIsIgnored(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status []byte
		want   uint8
	}{
		{"bridge rejected the send", receiveStatus(1), common.LiveState},
		{"bridge accepted the send", receiveStatus(0), common.EmergencyState},
		{"receive without a status", nil, common.EmergencyState},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newReplayBridge(t)
			b.request.Redeemed = 1
			url := replayServer(t, func(method string) interface{} {
				switch method {
				case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
					return b.request
				case "eth_getTransactionReceipt":
					return nil // the endpoint no longer serves this old receipt
				case "stats.syncInfo":
					return &protocol.SyncInfo{State: protocol.SyncDone}
				case "ledger.getFrontierMomentum":
					return &api.Momentum{Momentum: &nom.Momentum{Height: 104, Hash: types.Hash{1}}}
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

			receive := &api.AccountBlock{
				AccountBlock: nom.AccountBlock{BlockType: nom.BlockTypeContractReceive, Height: 2, Hash: types.Hash{2},
					Address: types.BridgeContract, Data: tc.status},
				ConfirmationDetail: &api.AccountBlockConfirmationDetail{MomentumHeight: 100},
			}
			send := b.send(t, 1, b.request.Amount, b.request.Signature)
			if err := n.interpretConfirmedReceive(n.listenerCtx, receive, send, listenerHistory, n.refreshSettings); err != nil {
				t.Fatal(err)
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

// The EVM contract emits the recipient as typed, and bech32 accepts an
// all-uppercase spelling. Zenon registers the parsed address.
func TestUnwrapRecipientIsComparedByValue(t *testing.T) {
	other, err := types.ParseAddress("z1qpp8g8v4hrd5ernfrgsn86jmv09ra6zrvhvmr7")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		to   func(types.Address) string
		want uint8
	}{
		{"lowercase", func(a types.Address) string { return a.String() }, common.LiveState},
		{"uppercase", func(a types.Address) string { return strings.ToUpper(a.String()) }, common.LiveState},
		{"another address", func(types.Address) string { return other.String() }, common.EmergencyState},
		{"another address in uppercase", func(types.Address) string { return strings.ToUpper(other.String()) }, common.EmergencyState},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newReplayBridge(t)
			log := b.sourceLog(t, tc.to(b.recipient))
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
			if err := n.InterpretSendBlockData(b.send(t, 1, b.request.Amount, b.request.Signature), true, 42); err != nil {
				t.Fatal(err)
			}
			if got := replayState(t, n); got != tc.want {
				t.Fatalf("state %s, want %s", common.StateToText(got), common.StateToText(tc.want))
			}
		})
	}
}
