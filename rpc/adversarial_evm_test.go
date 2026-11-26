package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"go.uber.org/zap"

	"orchestrator/common"
	"orchestrator/common/config"
)

func FuzzAgreeCanonicalHash(f *testing.F) {
	f.Add([]byte{1, 1})
	f.Add([]byte{1, 2})
	f.Add([]byte{0, 1})
	f.Add([]byte{1, 0})
	f.Add([]byte{})
	f.Add([]byte{3, 3, 3, 3})

	f.Fuzz(func(t *testing.T, encoded []byte) {
		if len(encoded) > 16 {
			encoded = encoded[:16]
		}
		results := make([]headerResult, len(encoded))
		hasError := false
		var firstHash ecommon.Hash
		allSame := len(encoded) > 0
		for i, b := range encoded {
			if b%4 == 0 {
				hasError = true
				results[i] = headerResult{endpoint: i, err: errors.New("endpoint failure")}
			} else {
				h := ecommon.HexToHash("0x0" + string('0'+b%4))
				results[i] = headerResult{endpoint: i, hash: h}
				if i == 0 {
					firstHash = h
				} else if h != firstHash {
					allSame = false
				}
			}
		}

		got, err := agreeCanonicalHash(100, results)
		wantOK := len(encoded) > 0 && !hasError && allSame
		if wantOK {
			if err != nil || got != firstHash {
				t.Fatalf("expected unanimous %s, got %s err=%v", firstHash.Hex(), got.Hex(), err)
			}
		} else {
			if err == nil {
				t.Fatalf("accepted non-unanimous result %s for encoded=%v", got.Hex(), encoded)
			}
		}
	})
}

type byzantineEVMServer struct {
	blockHash ecommon.Hash
	receipt   *etypes.Receipt
	logs      []etypes.Log
	httpCode  int
	rawBody   string
}

func (b *byzantineEVMServer) start(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if b.httpCode != 0 {
			w.WriteHeader(b.httpCode)
			_, _ = w.Write([]byte(b.rawBody))
			return
		}
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")

		var res any
		switch req.Method {
		case "eth_getBlockByNumber":
			res = map[string]any{
				"number": "0x64",
				"hash":   b.blockHash,
			}
		case "eth_getTransactionReceipt":
			res = b.receipt
		case "eth_getLogs":
			if b.logs == nil {
				res = []etypes.Log{}
			} else {
				res = b.logs
			}
		case "eth_blockNumber":
			res = hexutil.EncodeUint64(200)
		case "eth_chainId":
			res = hexutil.EncodeUint64(1)
		default:
			res = nil
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result":  res,
		})
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func newTwoEndpointEvmRpc(t *testing.T, url0, url1 string, bridge ecommon.Address) *EvmRpc {
	t.Helper()
	c0, err := ethclient.Dial(url0)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &EvmRpc{
		rpcClient:     c0,
		Urls:          config.UrlsInfo{Urls: []string{url0, url1}, ReconnectTries: map[uint32]uint32{}},
		bridgeAddress: bridge,
		ctx:           ctx,
		cancel:        cancel,
		logger:        zap.NewNop().Sugar(),
		secondary:     make(map[string]*secondaryClient),
	}
	t.Cleanup(r.Stop)
	return r
}

func TestAdversarialByzantineEVMEndpoints(t *testing.T) {
	bridge := ecommon.HexToAddress("0x000000000000000000000000000000000000b1d9")
	blockHash := ecommon.HexToHash("0xaaa1")
	txHash := ecommon.HexToHash("0xbbb2")
	validLog := etypes.Log{
		Address:     bridge,
		Topics:      []ecommon.Hash{common.UnwrapSigHash, ecommon.HexToHash("0x1"), ecommon.HexToHash("0x2")},
		Data:        ecommon.LeftPadBytes([]byte{99}, 32),
		BlockNumber: 100,
		TxHash:      txHash,
		BlockHash:   blockHash,
		Index:       4,
	}
	validReceipt := &etypes.Receipt{
		Status:      etypes.ReceiptStatusSuccessful,
		TxHash:      txHash,
		BlockHash:   blockHash,
		BlockNumber: big.NewInt(100),
		Logs:        []*etypes.Log{&validLog},
	}

	// 1. Height + confirmations uint64 overflow MUST be rejected cleanly.
	t.Run("uint64_confirmation_overflow_rejected", func(t *testing.T) {
		u0 := (&byzantineEVMServer{blockHash: blockHash, receipt: validReceipt}).start(t)
		u1 := (&byzantineEVMServer{blockHash: blockHash, receipt: validReceipt}).start(t)
		r := newTwoEndpointEvmRpc(t, u0, u1, bridge)
		if _, err := r.AgreedUnwrapLog(math.MaxUint64-5, blockHash, txHash, 4, 10); err == nil {
			t.Fatal("accepted overflowing height + confirmations")
		}
	})

	// 2. Tampered log Data on secondary endpoint MUST be rejected.
	t.Run("byzantine_tampered_payload_rejected", func(t *testing.T) {
		tampered := validLog
		tampered.Data = ecommon.LeftPadBytes([]byte{255}, 32)
		tamperedReceipt := *validReceipt
		tamperedReceipt.Logs = []*etypes.Log{&tampered}

		u0 := (&byzantineEVMServer{blockHash: blockHash, receipt: validReceipt, logs: []etypes.Log{validLog}}).start(t)
		u1 := (&byzantineEVMServer{blockHash: blockHash, receipt: &tamperedReceipt, logs: []etypes.Log{tampered}}).start(t)
		r := newTwoEndpointEvmRpc(t, u0, u1, bridge)

		if _, err := r.AgreedUnwrapLog(100, blockHash, txHash, 4, 10); err == nil {
			t.Fatal("accepted unwrap log when secondary endpoint returned tampered amount/data")
		}
	})

	// 3. Reorged/Removed log or failed tx receipt (Status=0) MUST be rejected.
	t.Run("removed_log_and_failed_receipt_rejected", func(t *testing.T) {
		removedLog := validLog
		removedLog.Removed = true
		failedReceipt := *validReceipt
		failedReceipt.Status = etypes.ReceiptStatusFailed

		u0 := (&byzantineEVMServer{blockHash: blockHash, receipt: validReceipt, logs: []etypes.Log{validLog}}).start(t)
		u1 := (&byzantineEVMServer{blockHash: blockHash, receipt: &failedReceipt, logs: []etypes.Log{removedLog}}).start(t)
		r := newTwoEndpointEvmRpc(t, u0, u1, bridge)

		if _, err := r.AgreedUnwrapLog(100, blockHash, txHash, 4, 10); err == nil {
			t.Fatal("accepted unwrap log when secondary returned failed receipt + removed log")
		}
	})

	// 4. Forged log from wrong contract address MUST be rejected.
	t.Run("wrong_contract_address_rejected", func(t *testing.T) {
		wrongContract := validLog
		wrongContract.Address = ecommon.HexToAddress("0xdead")
		wrongReceipt := *validReceipt
		wrongReceipt.Logs = []*etypes.Log{&wrongContract}

		u0 := (&byzantineEVMServer{blockHash: blockHash, receipt: validReceipt, logs: []etypes.Log{validLog}}).start(t)
		u1 := (&byzantineEVMServer{blockHash: blockHash, receipt: &wrongReceipt, logs: []etypes.Log{wrongContract}}).start(t)
		r := newTwoEndpointEvmRpc(t, u0, u1, bridge)

		if _, err := r.AgreedUnwrapLog(100, blockHash, txHash, 4, 10); err == nil {
			t.Fatal("accepted log emitted by non-bridge contract")
		}
	})

	// 5. HTTP 429 / malformed JSON from one endpoint MUST reject without panic.
	t.Run("http_429_and_malformed_json_rejected", func(t *testing.T) {
		u0 := (&byzantineEVMServer{blockHash: blockHash, receipt: validReceipt, logs: []etypes.Log{validLog}}).start(t)
		u1 := (&byzantineEVMServer{httpCode: http.StatusTooManyRequests, rawBody: "<html>rate limited</html>"}).start(t)
		r := newTwoEndpointEvmRpc(t, u0, u1, bridge)

		if _, err := r.AgreedUnwrapLog(100, blockHash, txHash, 4, 10); err == nil {
			t.Fatal("accepted unwrap log when secondary returned HTTP 429")
		}
	})

	// 6. Missing or unexpected redeem topics MUST fail fast before any RPC call.
	t.Run("malformed_redeem_topics_rejected", func(t *testing.T) {
		u0 := (&byzantineEVMServer{blockHash: blockHash}).start(t)
		u1 := (&byzantineEVMServer{blockHash: blockHash}).start(t)
		r := newTwoEndpointEvmRpc(t, u0, u1, bridge)

		if _, err := r.AgreedRedeemLog(etypes.Log{}, 10); err == nil {
			t.Fatal("accepted redeem log with empty topics")
		}
		if _, err := r.AgreedRedeemLog(etypes.Log{Topics: []ecommon.Hash{ecommon.HexToHash("0x9999")}}, 10); err == nil {
			t.Fatal("accepted redeem log with unexpected topic")
		}
	})
}
