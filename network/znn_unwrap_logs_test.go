package network

import (
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

type unwrapTestStores struct {
	znn db.ZnnStorage
	evm db.EvmStorage
}

func (s unwrapTestStores) ZnnStorage() db.ZnnStorage         { return s.znn }
func (s unwrapTestStores) EvmStorage(uint32) db.EvmStorage   { return s.evm }
func (s unwrapTestStores) HasEvmStorage(chainID uint32) bool { return chainID == 1 }
func (s unwrapTestStores) AddEvmEventStore(uint32, string, uint64) {
	panic("unexpected network creation in unwrap test")
}

// Exercise the actual Zenon interpreter, RPC log filtering, ABI decoding and
// memory stores without connecting to a chain or starting a signer.
func TestInterpretUnwrapSelectsLogByIdentity(t *testing.T) {
	cases := []struct {
		name       string
		index      uint
		affiliate  bool
		historical bool
		change     func(etypes.Log) []etypes.Log
		valid      bool
	}{
		{name: "first log", valid: true},
		{name: "unrelated logs precede bridge log", index: 9, valid: true},
		{name: "historical filtered block", index: 9, historical: true, valid: true},
		{name: "affiliate synthetic index", index: 9, affiliate: true, valid: true},
		{name: "multiple bridge logs", index: 1, valid: true, change: func(log etypes.Log) []etypes.Log {
			other := log
			other.Index = 3
			other.TxHash = ecommon.HexToHash("0xbb")
			other.Data = nil
			return []etypes.Log{log, other}
		}},
		{name: "wrong transaction", change: func(log etypes.Log) []etypes.Log {
			log.TxHash = ecommon.HexToHash("0xbb")
			return []etypes.Log{log}
		}},
		{name: "wrong block", change: func(log etypes.Log) []etypes.Log {
			log.BlockHash = ecommon.HexToHash("0xcc")
			return []etypes.Log{log}
		}},
		{name: "wrong index", change: func(log etypes.Log) []etypes.Log {
			log.Index = 2
			return []etypes.Log{log}
		}},
		{name: "wrong contract", change: func(log etypes.Log) []etypes.Log {
			log.Address = ecommon.HexToAddress("0xdd")
			return []etypes.Log{log}
		}},
		{name: "removed log", change: func(log etypes.Log) []etypes.Log {
			log.Removed = true
			return []etypes.Log{log}
		}},
		{name: "missing topics", change: func(log etypes.Log) []etypes.Log {
			log.Topics = []ecommon.Hash{}
			return []etypes.Log{log}
		}},
		{name: "wrong topic", change: func(log etypes.Log) []etypes.Log {
			log.Topics = []ecommon.Hash{ecommon.HexToHash("0xee")}
			return []etypes.Log{log}
		}},
		{name: "missing log", change: func(etypes.Log) []etypes.Log { return []etypes.Log{} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			contract := ecommon.HexToAddress("0x01")
			token := ecommon.HexToAddress("0x02")
			txHash := ecommon.HexToHash("0xaa")
			blockHash := ecommon.HexToHash("0xab")
			// This fixed, public test address is also used by the storage tests.
			recipient, err := types.ParseAddress("z1qqjnwjjpnue8xmmpanz6csze6tcmtzzdtfsww7")
			if err != nil {
				t.Fatal(err)
			}
			request := &definition.UnwrapTokenRequest{
				NetworkClass: definition.EvmClass, ChainId: 1,
				TransactionHash: types.Hash(txHash), LogIndex: uint32(tc.index),
				ToAddress: recipient, TokenAddress: strings.ToLower(token.Hex()),
				TokenStandard: types.ZnnTokenStandard, Amount: big.NewInt(100),
				Signature: "test-signature",
			}
			to := recipient.String()
			if tc.affiliate {
				request.LogIndex += common.AffiliateLogIndexAddition
				request.Amount = big.NewInt(2)
				to += common.AffiliateProgramAddressSeparator + recipient.String()
			}
			contractABI, err := bridge.BridgeMetaData.GetAbi()
			if err != nil {
				t.Fatal(err)
			}
			data, err := contractABI.Events["Unwrapped"].Inputs.NonIndexed().Pack(to, big.NewInt(100))
			if err != nil {
				t.Fatal(err)
			}
			log := etypes.Log{
				Address: contract, TxHash: txHash, BlockHash: blockHash,
				BlockNumber: 100, Index: tc.index, Data: data,
				Topics: []ecommon.Hash{common.UnwrapSigHash, ecommon.HexToHash("0x03"), ecommon.BytesToHash(token.Bytes())},
			}
			logs := []etypes.Log{log}
			if tc.change != nil {
				logs = tc.change(log)
			}
			var filteredCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var call struct {
					ID     json.RawMessage   `json:"id"`
					Method string            `json:"method"`
					Params []json.RawMessage `json:"params"`
				}
				if err := json.NewDecoder(r.Body).Decode(&call); err != nil {
					t.Errorf("decode RPC request: %v", err)
					http.Error(w, "invalid request", http.StatusBadRequest)
					return
				}
				var result interface{}
				switch call.Method {
				case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
					result = request
				case "eth_getTransactionReceipt":
					result = &etypes.Receipt{
						Status: etypes.ReceiptStatusSuccessful, TxHash: txHash,
						BlockHash: blockHash, BlockNumber: big.NewInt(100),
						Logs: []*etypes.Log{},
					}
				case "eth_getLogs":
					filteredCalls.Add(1)
					var filter struct {
						BlockHash ecommon.Hash      `json:"blockHash"`
						Addresses []ecommon.Address `json:"address"`
					}
					if len(call.Params) != 1 {
						t.Errorf("unexpected log filter parameters: %d", len(call.Params))
					} else if err := json.Unmarshal(call.Params[0], &filter); err != nil {
						t.Errorf("decode log filter: %v", err)
					} else if filter.BlockHash != blockHash || len(filter.Addresses) != 1 || filter.Addresses[0] != contract {
						t.Errorf("expected a bridge-address filter for the receipt block, got %+v", filter)
					}
					result = logs
				default:
					t.Errorf("unexpected RPC method: %s", call.Method)
				}
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": call.ID, "result": result}); err != nil {
					t.Errorf("encode RPC response: %v", err)
				}
			}))
			t.Cleanup(server.Close)
			stop := make(chan os.Signal, 8)
			rpcs, err := rpc.NewRpcManager(config.BaseNetworkConfig{Urls: []string{server.URL}}, stop)
			if err != nil {
				t.Fatal(err)
			}
			if err := rpcs.AddEvmClient(config.BaseNetworkConfig{Urls: []string{server.URL}}, 1, "unwrap-test", contract); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(rpcs.Evm(1).Stop)
			stores := unwrapTestStores{
				znn: wrap.NewZnnStorage(zdb.NewMemDB(), stop),
				evm: unwrap.NewEvmStorage(zdb.NewMemDB(), stop, 1),
			}
			stateValue := common.LiveState
			state := common.NewGlobalState(&stateValue)
			if tc.affiliate {
				state.SetTokensMap(1, types.ZnnTokenStandard.String(), strings.ToLower(token.Hex()))
				state.SetIsAffiliateProgram(common.AffiliateProgram{Networks: map[uint32]common.AffiliateNetwork{1: {StartingHeight: 1, ZNN: true}}})
			}
			network := &znnNetwork{dbManager: stores, rpcManager: rpcs, state: state, stopChan: stop, logger: zap.NewNop().Sugar()}
			method, err := definition.ABIBridge.PackMethod(definition.UnwrapTokenMethodName,
				request.NetworkClass, request.ChainId, request.TransactionHash, request.LogIndex,
				request.ToAddress, request.TokenAddress, request.Amount, request.Signature)
			if err != nil {
				t.Fatal(err)
			}
			if err := network.InterpretSendBlockData(&api.AccountBlock{AccountBlock: nom.AccountBlock{Data: method}}, !tc.historical, 42); err != nil {
				t.Fatal(err)
			}
			if filteredCalls.Load() != 1 {
				t.Fatalf("expected one filtered block query, got %d", filteredCalls.Load())
			}
			gotState, err := state.GetState()
			if err != nil {
				t.Fatal(err)
			}
			stored, err := stores.evm.GetUnwrapRequestByHashAndLog(txHash, request.LogIndex)
			if err != nil {
				t.Fatal(err)
			}
			if tc.valid {
				if gotState != common.LiveState {
					t.Fatalf("valid unwrap changed node state to %d", gotState)
				}
				if stored == nil || stored.RedeemStatus != common.PendingRedeemStatus || stored.Amount.Cmp(request.Amount) != 0 {
					t.Fatalf("valid unwrap was not persisted with its expected status and amount: %+v", stored)
				}
			} else {
				if gotState != common.EmergencyState {
					t.Fatalf("mismatched live unwrap left node in state %d", gotState)
				}
				if stored != nil {
					t.Fatal("mismatched live unwrap was persisted")
				}
			}
			height, err := stores.znn.GetLastUpdateHeight()
			if err != nil || height != 42 {
				t.Fatalf("unexpected receive cursor: %d, %v", height, err)
			}
		})
	}
}
