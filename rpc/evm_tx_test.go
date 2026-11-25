package rpc

import (
	"context"
	"encoding/json"
	"errors"
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
	"github.com/ethereum/go-ethereum/crypto"

	"orchestrator/common"
	"orchestrator/common/config"
)

type evmTxMockServer struct {
	t *testing.T

	mu              sync.Mutex
	chainID         uint64
	blockNumber     uint64
	baseFee         *big.Int // nil => non-1559 legacy chain
	tipCap          *big.Int
	gasPrice        *big.Int
	confirmedNonce  uint64
	pendingNonce    uint64
	balance         *big.Int
	estimateGas     uint64
	sendErr         string
	hangSend        chan struct{}
	sentTxs         []*etypes.Transaction
	knownTxs        map[ecommon.Hash]*etypes.Transaction
	txByHashErr     string
	sendCalls       atomic.Int32
}

func newEvmTxMockServer(t *testing.T) (*evmTxMockServer, string) {
	t.Helper()
	m := &evmTxMockServer{
		t:           t,
		chainID:     1,
		blockNumber: 100,
		gasPrice:    big.NewInt(10_000_000_000), // 10 gwei
		balance:     big.NewInt(0).Mul(big.NewInt(10), big.NewInt(1e18)),
		estimateGas: 100_000,
		knownTxs:    make(map[ecommon.Hash]*etypes.Transaction),
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			JSONRPC string            `json:"jsonrpc"`
			ID      json.RawMessage   `json:"id"`
			Method  string            `json:"method"`
			Params  []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		result, rpcErr := m.handle(r.Context(), req.Method, req.Params)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		if rpcErr != nil {
			resp["error"] = rpcErr
		} else {
			resp["result"] = result
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return m, srv.URL
}

func (m *evmTxMockServer) handle(ctx context.Context, method string, params []json.RawMessage) (interface{}, map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch method {
	case "eth_chainId":
		return hexutil.EncodeUint64(m.chainID), nil
	case "eth_blockNumber":
		return hexutil.EncodeUint64(m.blockNumber), nil
	case "eth_getBalance":
		return (*hexutil.Big)(new(big.Int).Set(m.balance)), nil
	case "eth_getTransactionCount":
		var tag string
		if len(params) >= 2 {
			_ = json.Unmarshal(params[1], &tag)
		}
		if tag == "pending" {
			return hexutil.EncodeUint64(m.pendingNonce), nil
		}
		return hexutil.EncodeUint64(m.confirmedNonce), nil
	case "eth_getBlockByNumber":
		res := map[string]interface{}{
			"number": hexutil.EncodeUint64(m.blockNumber),
			"hash":   ecommon.HexToHash("0x11"),
		}
		if m.baseFee != nil {
			res["baseFeePerGas"] = (*hexutil.Big)(new(big.Int).Set(m.baseFee))
		}
		return res, nil
	case "eth_maxPriorityFeePerGas":
		if m.tipCap == nil {
			return nil, map[string]interface{}{"code": -32601, "message": "method not found"}
		}
		return (*hexutil.Big)(new(big.Int).Set(m.tipCap)), nil
	case "eth_gasPrice":
		return (*hexutil.Big)(new(big.Int).Set(m.gasPrice)), nil
	case "eth_estimateGas":
		return hexutil.EncodeUint64(m.estimateGas), nil
	case "eth_getTransactionByHash":
		if m.txByHashErr != "" {
			return nil, map[string]interface{}{"code": -32000, "message": m.txByHashErr}
		}
		var hash ecommon.Hash
		if len(params) >= 1 {
			_ = json.Unmarshal(params[0], &hash)
		}
		tx, ok := m.knownTxs[hash]
		if !ok || tx == nil {
			return nil, nil
		}
		return map[string]interface{}{
			"hash":                 tx.Hash().Hex(),
			"nonce":                hexutil.EncodeUint64(tx.Nonce()),
			"from":                 ecommon.HexToAddress("0x1111111111111111111111111111111111111111").Hex(),
			"to":                   tx.To().Hex(),
			"value":                (*hexutil.Big)(tx.Value()),
			"gas":                  hexutil.EncodeUint64(tx.Gas()),
			"gasPrice":             (*hexutil.Big)(tx.GasPrice()),
			"maxFeePerGas":         (*hexutil.Big)(tx.GasFeeCap()),
			"maxPriorityFeePerGas": (*hexutil.Big)(tx.GasTipCap()),
			"input":                hexutil.Bytes(tx.Data()),
			"type":                 hexutil.EncodeUint64(uint64(tx.Type())),
			"chainId":              (*hexutil.Big)(tx.ChainId()),
			"v":                    "0x0",
			"r":                    "0x1",
			"s":                    "0x1",
		}, nil
	case "eth_sendRawTransaction":
		m.sendCalls.Add(1)
		if m.hangSend != nil {
			ch := m.hangSend
			m.mu.Unlock()
			select {
			case <-ch:
			case <-ctx.Done():
			}
			m.mu.Lock()
			return nil, map[string]interface{}{"code": -32000, "message": "request cancelled"}
		}
		if m.sendErr != "" {
			return nil, map[string]interface{}{"code": -32000, "message": m.sendErr}
		}
		var raw hexutil.Bytes
		if len(params) >= 1 && json.Unmarshal(params[0], &raw) == nil {
			var tx etypes.Transaction
			if err := tx.UnmarshalBinary(raw); err == nil {
				m.sentTxs = append(m.sentTxs, &tx)
				m.knownTxs[tx.Hash()] = &tx
				if tx.Nonce() >= m.pendingNonce {
					m.pendingNonce = tx.Nonce() + 1
				}
			}
		}
		return ecommon.HexToHash("0xaa").Hex(), nil
	default:
		return nil, map[string]interface{}{"code": -32601, "message": "unexpected method " + method}
	}
}

func TestSendTransactionRespectsContextCancellation(t *testing.T) {
	mock, url := newEvmTxMockServer(t)
	release := make(chan struct{})
	defer close(release)
	mock.hangSend = release

	ctx, cancel := context.WithCancel(context.Background())
	contract := ecommon.HexToAddress("0x1111111111111111111111111111111111111111")
	client, err := NewEvmRpcClientContext(ctx, config.BaseNetworkConfig{Urls: []string{url}}, "test-evm", contract)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	tx := etypes.NewTx(&etypes.LegacyTx{
		Nonce:    0,
		To:       &contract,
		Value:    big.NewInt(0),
		Gas:      21000,
		GasPrice: big.NewInt(1),
	})
	signed, err := etypes.SignTx(tx, etypes.LatestSignerForChainID(big.NewInt(1)), key)
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- client.SendTransaction(signed)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected SendTransaction to fail after context cancel")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("SendTransaction hung past context cancellation")
	}
}

func TestHaltAndSetTssTxEIP1559DynamicFeesAndReplacementBump(t *testing.T) {
	mock, url := newEvmTxMockServer(t)
	mock.baseFee = big.NewInt(20_000_000_000) // 20 gwei
	mock.tipCap = big.NewInt(2_000_000_000)   // 2 gwei
	mock.confirmedNonce = 5
	mock.pendingNonce = 5

	contract := ecommon.HexToAddress("0x2222222222222222222222222222222222222222")
	client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{url}}, "test-1559", contract)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	client.SetChainID(1)

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	signer := etypes.LatestSignerForChainID(big.NewInt(1))

	// Attempt 1: fresh EIP-1559 halt tx.
	tx1, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	if tx1.Type() != etypes.DynamicFeeTxType {
		t.Fatalf("tx1 type = %d, want DynamicFeeTxType (%d)", tx1.Type(), etypes.DynamicFeeTxType)
	}
	if tx1.Nonce() != 5 {
		t.Fatalf("tx1 nonce = %d, want 5", tx1.Nonce())
	}
	wantFeeCap1 := big.NewInt(42_000_000_000) // 2*20 + 2 gwei
	if tx1.GasTipCap().Cmp(big.NewInt(2_000_000_000)) != 0 || tx1.GasFeeCap().Cmp(wantFeeCap1) != 0 {
		t.Fatalf("tx1 fees tip=%s cap=%s, want tip=2gwei cap=42gwei", tx1.GasTipCap(), tx1.GasFeeCap())
	}
	signed1, err := etypes.SignTx(tx1, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SendTransaction(signed1); err != nil {
		t.Fatal(err)
	}

	// Now confirmed=5, pending=6. Retry halt: must reuse nonce 5 and bump >=20%.
	tx2, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	if tx2.Nonce() != 5 {
		t.Fatalf("tx2 replacement nonce = %d, want 5 (not pendingNonce 6)", tx2.Nonce())
	}
	wantTip2 := big.NewInt(2_400_000_000)  // +20%
	wantCap2 := big.NewInt(50_400_000_000) // +20%
	if tx2.GasTipCap().Cmp(wantTip2) < 0 || tx2.GasFeeCap().Cmp(wantCap2) < 0 {
		t.Fatalf("tx2 fees tip=%s cap=%s, want >= (%s, %s)", tx2.GasTipCap(), tx2.GasFeeCap(), wantTip2, wantCap2)
	}

	// Simulate "replacement transaction underpriced" on tx2: must still record watermark
	// so tx3 bumps another >=20% above tx2.
	mock.mu.Lock()
	mock.sendErr = "replacement transaction underpriced"
	mock.mu.Unlock()
	signed2, err := etypes.SignTx(tx2, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SendTransaction(signed2); err == nil {
		t.Fatal("expected underpriced error")
	}

	// Gas spike on attempt 3: baseFee jumps to 80 gwei.
	mock.mu.Lock()
	mock.sendErr = ""
	mock.baseFee = big.NewInt(80_000_000_000)
	mock.mu.Unlock()

	tx3, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	if tx3.Nonce() != 5 {
		t.Fatalf("tx3 nonce = %d, want 5", tx3.Nonce())
	}
	if tx3.GasTipCap().Cmp(bumpFee(tx2.GasTipCap())) < 0 {
		t.Fatalf("tx3 tip %s did not bump >=20%% over tx2 tip %s", tx3.GasTipCap(), tx2.GasTipCap())
	}
	minSpikeCap := new(big.Int).Add(new(big.Int).Mul(big.NewInt(80_000_000_000), big.NewInt(2)), tx3.GasTipCap())
	if tx3.GasFeeCap().Cmp(minSpikeCap) < 0 {
		t.Fatalf("tx3 feeCap %s below spike headroom %s", tx3.GasFeeCap(), minSpikeCap)
	}

	// A distinct action (setTss) while halt is pending at nonce 5 must use pendingNonce (6)
	// rather than colliding with halt's nonce.
	newTss := ecommon.HexToAddress("0x3333333333333333333333333333333333333333")
	setTssTx, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	if setTssTx.Type() != etypes.DynamicFeeTxType {
		t.Fatalf("setTssTx type = %d, want DynamicFeeTxType", setTssTx.Type())
	}
	if setTssTx.Nonce() != 6 {
		t.Fatalf("setTssTx nonce = %d, want pendingNonce 6", setTssTx.Nonce())
	}

	// Once nonce 5 confirms (confirmed=6, pending=6), next halt uses nonce 6 with fresh fees.
	mock.mu.Lock()
	mock.confirmedNonce = 6
	mock.pendingNonce = 6
	mock.baseFee = big.NewInt(10_000_000_000)
	mock.mu.Unlock()

	tx4, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	if tx4.Nonce() != 6 {
		t.Fatalf("tx4 nonce = %d, want 6 after confirmation", tx4.Nonce())
	}
	wantCap4 := big.NewInt(22_000_000_000) // 2*10 + 2 gwei (no stale bump)
	if tx4.GasFeeCap().Cmp(wantCap4) != 0 {
		t.Fatalf("tx4 feeCap = %s, want fresh %s", tx4.GasFeeCap(), wantCap4)
	}
}

func TestHaltTxLegacyFallbackBumpAndNonceTooLowReset(t *testing.T) {
	mock, url := newEvmTxMockServer(t)
	mock.baseFee = nil // legacy chain
	mock.confirmedNonce = 3
	mock.pendingNonce = 3
	mock.gasPrice = big.NewInt(10_000_000_000) // 10 gwei

	contract := ecommon.HexToAddress("0x4444444444444444444444444444444444444444")
	client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{url}}, "test-legacy", contract)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	signer := etypes.LatestSignerForChainID(big.NewInt(1))

	tx1, err := client.GetHaltTxEvm(sender, []byte{9}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	if tx1.Type() != etypes.LegacyTxType || tx1.Nonce() != 3 || tx1.GasPrice().Cmp(big.NewInt(10_000_000_000)) != 0 {
		t.Fatalf("unexpected legacy tx1: type=%d nonce=%d gasPrice=%s", tx1.Type(), tx1.Nonce(), tx1.GasPrice())
	}
	signed1, err := etypes.SignTx(tx1, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SendTransaction(signed1); err != nil {
		t.Fatal(err)
	}

	// Retry while unconfirmed: same nonce 3, gasPrice bumped +20% (12 gwei).
	tx2, err := client.GetHaltTxEvm(sender, []byte{9}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	if tx2.Type() != etypes.LegacyTxType || tx2.Nonce() != 3 {
		t.Fatalf("unexpected legacy tx2: type=%d nonce=%d", tx2.Type(), tx2.Nonce())
	}
	if tx2.GasPrice().Cmp(big.NewInt(12_000_000_000)) != 0 {
		t.Fatalf("tx2 gasPrice = %s, want 12 gwei", tx2.GasPrice())
	}

	// If tx2 fails with "nonce too low", tracked pending state resets so next build uses fresh nonce.
	mock.mu.Lock()
	mock.sendErr = "nonce too low"
	mock.mu.Unlock()
	signed2, err := etypes.SignTx(tx2, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SendTransaction(signed2); err == nil {
		t.Fatal("expected nonce too low error")
	}

	mock.mu.Lock()
	mock.sendErr = ""
	mock.confirmedNonce = 4
	mock.pendingNonce = 4
	mock.mu.Unlock()
	tx3, err := client.GetHaltTxEvm(sender, []byte{9}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	if tx3.Nonce() != 4 || tx3.GasPrice().Cmp(big.NewInt(10_000_000_000)) != 0 {
		t.Fatalf("tx3 after reset: nonce=%d gasPrice=%s, want nonce=4 gasPrice=10gwei", tx3.Nonce(), tx3.GasPrice())
	}
}

func TestLookupEvmConcurrentAddRemoveDoesNotStopOrPanic(t *testing.T) {
	_, url := newEvmTxMockServer(t)
	stop := make(chan os.Signal, 1)
	mgr := &Manager{
		evmClients: make(map[uint32]*EvmRpc),
		stopChan:   stop,
	}
	if client, ok := mgr.LookupEvm(99); ok || client != nil {
		t.Fatalf("expected missing LookupEvm to return (nil, false), got (%v, %v)", client, ok)
	}
	select {
	case sig := <-stop:
		t.Fatalf("LookupEvm requested stop %v for missing client", sig)
	default:
	}

	contract := ecommon.HexToAddress("0x5555555555555555555555555555555555555555")
	var wg sync.WaitGroup
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				if c, ok := mgr.LookupEvm(1); ok && c == nil {
					t.Error("LookupEvm returned ok=true with nil client")
				}
			}
		}
	}()

	for i := 0; i < 20; i++ {
		c, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{url}}, "race-test", contract)
		if err != nil {
			t.Fatal(err)
		}
		mgr.RegisterEvmClient(1, c)
		got, ok := mgr.LookupEvm(1)
		if !ok || got != c {
			t.Fatalf("LookupEvm did not return registered client")
		}
		// Remove without DeleteDirectories side effects on host temp dirs: stop directly via map swap.
		mgr.evmMu.Lock()
		delete(mgr.evmClients, 1)
		mgr.evmMu.Unlock()
		c.Stop()
	}
	close(done)
	wg.Wait()

	select {
	case sig := <-stop:
		t.Fatalf("unexpected stop signal %v", sig)
	default:
	}
}

type memPendingTxStore struct {
	mu      sync.Mutex
	rows    map[string]*common.PendingEvmTx
	batches [][]common.PendingEvmTxOp
}

func newMemPendingTxStore() *memPendingTxStore {
	return &memPendingTxStore{rows: make(map[string]*common.PendingEvmTx)}
}

func (s *memPendingTxStore) key(sender, target ecommon.Address, action string) string {
	return sender.Hex() + ":" + target.Hex() + ":" + action
}

func cloneCommonPending(tx *common.PendingEvmTx) *common.PendingEvmTx {
	if tx == nil {
		return nil
	}
	cp := *tx
	if tx.GasPrice != nil {
		cp.GasPrice = new(big.Int).Set(tx.GasPrice)
	}
	if tx.TipCap != nil {
		cp.TipCap = new(big.Int).Set(tx.TipCap)
	}
	if tx.FeeCap != nil {
		cp.FeeCap = new(big.Int).Set(tx.FeeCap)
	}
	return &cp
}

func (s *memPendingTxStore) LoadPendingEvmTx(sender, target ecommon.Address, action string) (*common.PendingEvmTx, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.rows[s.key(sender, target, action)]
	if !ok || row == nil {
		return nil, nil
	}
	return cloneCommonPending(row), nil
}

func (s *memPendingTxStore) SavePendingEvmTx(sender, target ecommon.Address, action string, tx *common.PendingEvmTx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[s.key(sender, target, action)] = cloneCommonPending(tx)
	return nil
}

func (s *memPendingTxStore) DeletePendingEvmTx(sender, target ecommon.Address, action string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.rows, s.key(sender, target, action))
	return nil
}

func (s *memPendingTxStore) ApplyPendingEvmTxBatch(ops []common.PendingEvmTxOp) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := make([]common.PendingEvmTxOp, len(ops))
	for i, op := range ops {
		copied[i] = common.PendingEvmTxOp{
			Sender:   op.Sender,
			Contract: op.Contract,
			Kind:     op.Kind,
			Record:   cloneCommonPending(op.Record),
		}
		if op.Record == nil {
			delete(s.rows, s.key(op.Sender, op.Contract, op.Kind))
		} else {
			s.rows[s.key(op.Sender, op.Contract, op.Kind)] = cloneCommonPending(op.Record)
		}
	}
	s.batches = append(s.batches, copied)
	return nil
}

func TestHaltAndSetTssRecoverStrandedNonceGap(t *testing.T) {
	contract := ecommon.HexToAddress("0x6666666666666666666666666666666666666666")
	newTss := ecommon.HexToAddress("0x7777777777777777777777777777777777777777")

	for _, action := range []string{"halt", "setTss"} {
		t.Run(action, func(t *testing.T) {
			mock, url := newEvmTxMockServer(t)
			mock.baseFee = big.NewInt(20_000_000_000)
			mock.tipCap = big.NewInt(2_000_000_000)
			// Predecessor tx occupies nonce 5, so our first tx lands on nonce 6.
			mock.confirmedNonce = 5
			mock.pendingNonce = 6

			store := newMemPendingTxStore()
			client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{url}}, "gap-"+action, contract)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Stop()
			client.SetChainID(1)
			client.SetPendingTxStore(store)

			key, err := crypto.GenerateKey()
			if err != nil {
				t.Fatal(err)
			}
			sender := crypto.PubkeyToAddress(key.PublicKey)

			// Simulate a previously submitted target action at nonce 6 (behind an external
			// predecessor at nonce 5) whose predecessor was subsequently evicted from the
			// mempool (confirmed=5, pending=5), leaving nonce 6 stranded behind a gap.
			if err := store.SavePendingEvmTx(sender, contract, action, &common.PendingEvmTx{
				Nonce:     6,
				TipCap:    big.NewInt(2_000_000_000),
				FeeCap:    big.NewInt(42_000_000_000),
				IsDynamic: true,
			}); err != nil {
				t.Fatal(err)
			}

			buildTx := func() (*etypes.Transaction, error) {
				if action == "halt" {
					return client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
				}
				return client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
			}
			mock.mu.Lock()
			mock.confirmedNonce = 5
			mock.pendingNonce = 5
			mock.mu.Unlock()

			tx2, err := buildTx()
			if err != nil {
				t.Fatal(err)
			}
			if tx2.Nonce() != 5 {
				t.Fatalf("after predecessor eviction, tx2 nonce = %d, want 5 (must not keep stranded nonce 6)", tx2.Nonce())
			}
		})
	}
}

func TestHaltAndSetTssRestartRecoveryWithAndWithoutStore(t *testing.T) {
	contract := ecommon.HexToAddress("0x8888888888888888888888888888888888888888")
	newTss := ecommon.HexToAddress("0x9999999999999999999999999999999999999999")

	for _, action := range []string{"halt", "setTss"} {
		for _, withStore := range []bool{true, false} {
			name := action + "/without_store"
			if withStore {
				name = action + "/with_store"
			}
			t.Run(name, func(t *testing.T) {
				mock, url := newEvmTxMockServer(t)
				mock.baseFee = big.NewInt(20_000_000_000)
				mock.tipCap = big.NewInt(2_000_000_000)
				mock.confirmedNonce = 5
				mock.pendingNonce = 5

				var store *memPendingTxStore
				if withStore {
					store = newMemPendingTxStore()
				}

				makeClient := func() *EvmRpc {
					c, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{url}}, "restart-"+action, contract)
					if err != nil {
						t.Fatal(err)
					}
					c.SetChainID(1)
					if store != nil {
						c.SetPendingTxStore(store)
					}
					return c
				}

				key, err := crypto.GenerateKey()
				if err != nil {
					t.Fatal(err)
				}
				sender := crypto.PubkeyToAddress(key.PublicKey)
				signer := etypes.LatestSignerForChainID(big.NewInt(1))

				buildTx := func(c *EvmRpc) (*etypes.Transaction, error) {
					if action == "halt" {
						return c.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
					}
					return c.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
				}

				// Process 1 sends tx at nonce 5, then crashes/restarts while tx is still pending.
				c1 := makeClient()
				tx1, err := buildTx(c1)
				if err != nil {
					t.Fatal(err)
				}
				signed1, err := etypes.SignTx(tx1, signer, key)
				if err != nil {
					t.Fatal(err)
				}
				if err := c1.SendTransaction(signed1); err != nil {
					t.Fatal(err)
				}
				c1.Stop()

				// Process 2 starts fresh while confirmed=5, pending=6.
				c2 := makeClient()
				defer c2.Stop()

				tx2, err := buildTx(c2)
				if err != nil {
					t.Fatal(err)
				}
				if tx2.Nonce() != 5 {
					t.Fatalf("after restart, tx2 nonce = %d, want 5 (must replace stuck nonce instead of queueing at 6)", tx2.Nonce())
				}
				if withStore {
					if tx2.GasTipCap().Cmp(bumpFee(tx1.GasTipCap())) < 0 || tx2.GasFeeCap().Cmp(bumpFee(tx1.GasFeeCap())) < 0 {
						t.Fatalf("with store after restart, tx2 did not immediately bump >=20%%: tx1=(%s,%s) tx2=(%s,%s)",
							tx1.GasTipCap(), tx1.GasFeeCap(), tx2.GasTipCap(), tx2.GasFeeCap())
					}
				} else {
					// Without persistent store, an underpriced rejection on first replacement
					// records the watermark so the next attempt bumps >=20% at nonce 5.
					mock.mu.Lock()
					mock.sendErr = "replacement transaction underpriced"
					mock.mu.Unlock()
					signed2, err := etypes.SignTx(tx2, signer, key)
					if err != nil {
						t.Fatal(err)
					}
					if err := c2.SendTransaction(signed2); err == nil {
						t.Fatal("expected replacement underpriced error")
					}
					mock.mu.Lock()
					mock.sendErr = ""
					mock.mu.Unlock()
					tx3, err := buildTx(c2)
					if err != nil {
						t.Fatal(err)
					}
					if tx3.Nonce() != 5 {
						t.Fatalf("tx3 nonce = %d, want 5", tx3.Nonce())
					}
					if tx3.GasTipCap().Cmp(bumpFee(tx2.GasTipCap())) < 0 || tx3.GasFeeCap().Cmp(bumpFee(tx2.GasFeeCap())) < 0 {
						t.Fatalf("without store after underpriced, tx3 did not bump >=20%%: tx2=(%s,%s) tx3=(%s,%s)",
							tx2.GasTipCap(), tx2.GasFeeCap(), tx3.GasTipCap(), tx3.GasFeeCap())
					}
				}
			})
		}
	}
}

func TestFirstActionAfterRestartRespectsOtherActionPersistedNonce(t *testing.T) {
	contract := ecommon.HexToAddress("0xabababababababababababababababababababab")
	newTss := ecommon.HexToAddress("0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd")

	for _, firstAction := range []string{"setTss", "halt"} {
		secondAction := "halt"
		if firstAction == "halt" {
			secondAction = "setTss"
		}
		t.Run(firstAction+"_then_restart_then_"+secondAction, func(t *testing.T) {
			mock, url := newEvmTxMockServer(t)
			mock.baseFee = big.NewInt(20_000_000_000)
			mock.tipCap = big.NewInt(2_000_000_000)
			mock.confirmedNonce = 5
			mock.pendingNonce = 5

			store := newMemPendingTxStore()
			makeClient := func() *EvmRpc {
				c, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{url}}, "order-"+firstAction, contract)
				if err != nil {
					t.Fatal(err)
				}
				c.SetChainID(1)
				c.SetPendingTxStore(store)
				return c
			}
			key, err := crypto.GenerateKey()
			if err != nil {
				t.Fatal(err)
			}
			sender := crypto.PubkeyToAddress(key.PublicKey)
			signer := etypes.LatestSignerForChainID(big.NewInt(1))

			build := func(c *EvmRpc, action string) (*etypes.Transaction, error) {
				if action == "halt" {
					return c.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
				}
				return c.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
			}

			// Process 1 submits firstAction at nonce 5, then restarts while confirmed=5, pending=6.
			c1 := makeClient()
			tx1, err := build(c1, firstAction)
			if err != nil {
				t.Fatal(err)
			}
			if tx1.Nonce() != 5 {
				t.Fatalf("firstAction nonce = %d, want 5", tx1.Nonce())
			}
			signed1, err := etypes.SignTx(tx1, signer, key)
			if err != nil {
				t.Fatal(err)
			}
			if err := c1.SendTransaction(signed1); err != nil {
				t.Fatal(err)
			}
			c1.Stop()

			// Process 2 starts fresh and builds secondAction FIRST.
			c2 := makeClient()
			defer c2.Stop()
			tx2, err := build(c2, secondAction)
			if err != nil {
				t.Fatal(err)
			}
			if firstAction == "halt" {
				// setTss MUST NEVER supersede an active emergency halt at nonce 5.
				if tx2.Nonce() != 6 {
					t.Fatalf("after restart, setTss stole halt's nonce: got %d, want 6", tx2.Nonce())
				}
			} else {
				// Emergency halt explicitly supersedes an unconfirmed setTss at
				// confirmedNonce 5 with >=20% bumped fees and transfers ownership
				// on send so emergency halts are never stranded behind stuck setTss.
				if tx2.Nonce() != 5 {
					t.Fatalf("after restart, emergency halt got nonce %d, want 5 (superseding setTss)", tx2.Nonce())
				}
				if tx2.GasTipCap().Cmp(bumpFee(tx1.GasTipCap())) < 0 || tx2.GasFeeCap().Cmp(bumpFee(tx1.GasFeeCap())) < 0 {
					t.Fatalf("superseding halt did not bump >=20%% over setTss: setTss=(%s,%s) halt=(%s,%s)",
						tx1.GasTipCap(), tx1.GasFeeCap(), tx2.GasTipCap(), tx2.GasFeeCap())
				}
				signed2, err := etypes.SignTx(tx2, signer, key)
				if err != nil {
					t.Fatal(err)
				}
				if err := c2.SendTransaction(signed2); err != nil {
					t.Fatal(err)
				}
				if oldRec, err := store.LoadPendingEvmTx(sender, contract, evmTxKindSetTss); err != nil || oldRec != nil {
					t.Fatalf("expected superseded setTss record to be cleared, got (%+v, %v)", oldRec, err)
				}
				if newRec, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err != nil || newRec == nil || newRec.Nonce != 5 {
					t.Fatalf("expected halt to own nonce 5 in store, got (%+v, %v)", newRec, err)
				}
			}
		})
	}
}

type failingPendingTxStore struct {
	*memPendingTxStore
	failSave        bool
	failLoadKind    string
	failBatchOnCall int // 1-based batch call to fail (0 = disabled)
	batchCalls      int
}

func (s *failingPendingTxStore) LoadPendingEvmTx(sender, target ecommon.Address, action string) (*common.PendingEvmTx, error) {
	if s.failLoadKind != "" && (s.failLoadKind == "*" || s.failLoadKind == action) {
		return nil, errors.New("corrupt pending tx record")
	}
	return s.memPendingTxStore.LoadPendingEvmTx(sender, target, action)
}

func (s *failingPendingTxStore) SavePendingEvmTx(sender, target ecommon.Address, action string, tx *common.PendingEvmTx) error {
	if s.failSave {
		return errors.New("disk full")
	}
	return s.memPendingTxStore.SavePendingEvmTx(sender, target, action, tx)
}

func (s *failingPendingTxStore) ApplyPendingEvmTxBatch(ops []common.PendingEvmTxOp) error {
	s.batchCalls++
	if s.failSave || (s.failBatchOnCall > 0 && s.batchCalls == s.failBatchOnCall) {
		return errors.New("disk full")
	}
	return s.memPendingTxStore.ApplyPendingEvmTxBatch(ops)
}

func TestSendTransactionPersistsBeforeBroadcastHandlesEOFAndStoreFailure(t *testing.T) {
	contract := ecommon.HexToAddress("0x1212121212121212121212121212121212121212")
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	signer := etypes.LatestSignerForChainID(big.NewInt(1))

	t.Run("persists before broadcast and retains watermark on EOF drop", func(t *testing.T) {
		store := newMemPendingTxStore()
		var persistedBeforeSend atomic.Bool
		var confirmedNonce, pendingNonce atomic.Uint64
		confirmedNonce.Store(5)
		pendingNonce.Store(5)
		var dropNextSend atomic.Bool
		dropNextSend.Store(true)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     json.RawMessage   `json:"id"`
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if req.Method == "eth_sendRawTransaction" {
				// Verify watermark was ALREADY persisted BEFORE the RPC arrived.
				if rec, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err == nil && rec != nil && rec.Nonce == 5 {
					persistedBeforeSend.Store(true)
				}
				// Simulate node accepting tx into mempool then connection dropping (EOF).
				pendingNonce.Store(6)
				if dropNextSend.Load() {
					if hj, ok := w.(http.Hijacker); ok {
						conn, _, err := hj.Hijack()
						if err == nil {
							_ = conn.Close()
							return
						}
					}
				}
			}
			var result interface{}
			switch req.Method {
			case "eth_chainId":
				result = hexutil.EncodeUint64(1)
			case "eth_blockNumber":
				result = hexutil.EncodeUint64(100)
			case "eth_getBalance":
				result = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
			case "eth_getTransactionCount":
				var tag string
				if len(req.Params) >= 2 {
					_ = json.Unmarshal(req.Params[1], &tag)
				}
				if tag == "pending" {
					result = hexutil.EncodeUint64(pendingNonce.Load())
				} else {
					result = hexutil.EncodeUint64(confirmedNonce.Load())
				}
			case "eth_getBlockByNumber":
				result = map[string]interface{}{
					"number":        hexutil.EncodeUint64(100),
					"hash":          ecommon.HexToHash("0x11"),
					"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
				}
			case "eth_maxPriorityFeePerGas":
				result = (*hexutil.Big)(big.NewInt(2_000_000_000))
			case "eth_estimateGas":
				result = hexutil.EncodeUint64(100_000)
			case "eth_sendRawTransaction":
				result = ecommon.HexToHash("0xbb").Hex()
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "result": result})
		}))
		t.Cleanup(srv.Close)

		c1, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "eof-test", contract)
		if err != nil {
			t.Fatal(err)
		}
		c1.SetChainID(1)
		c1.SetPendingTxStore(store)

		tx1, err := c1.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		signed1, err := etypes.SignTx(tx1, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		if err := c1.SendTransaction(signed1); err == nil {
			t.Fatal("expected EOF error from dropped connection")
		}
		c1.Stop()

		if !persistedBeforeSend.Load() {
			t.Fatal("pending tx watermark was not persisted before eth_sendRawTransaction reached the server")
		}

		// Simulate process restart after EOF: fresh client MUST recover nonce 5 and bump >=20%.
		dropNextSend.Store(false)
		c2, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "eof-test", contract)
		if err != nil {
			t.Fatal(err)
		}
		defer c2.Stop()
		c2.SetChainID(1)
		c2.SetPendingTxStore(store)

		tx2, err := c2.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		if tx2.Nonce() != 5 {
			t.Fatalf("after EOF+restart, tx2 nonce = %d, want 5", tx2.Nonce())
		}
		if tx2.GasTipCap().Cmp(bumpFee(tx1.GasTipCap())) < 0 || tx2.GasFeeCap().Cmp(bumpFee(tx1.GasFeeCap())) < 0 {
			t.Fatalf("after EOF+restart, tx2 did not bump >=20%%: tx1=(%s,%s) tx2=(%s,%s)",
				tx1.GasTipCap(), tx1.GasFeeCap(), tx2.GasTipCap(), tx2.GasFeeCap())
		}
	})

	t.Run("definitive pre-mempool rejection rolls back watermark and store failure aborts send", func(t *testing.T) {
		mock, url := newEvmTxMockServer(t)
		mock.baseFee = big.NewInt(20_000_000_000)
		mock.tipCap = big.NewInt(2_000_000_000)
		mock.confirmedNonce = 5
		mock.pendingNonce = 5
		mock.sendErr = "insufficient funds for gas * price + value"

		store := &failingPendingTxStore{memPendingTxStore: newMemPendingTxStore()}
		client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{url}}, "rollback-test", contract)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Stop()
		client.SetChainID(1)
		client.SetPendingTxStore(store)

		tx1, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		signed1, err := etypes.SignTx(tx1, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.SendTransaction(signed1); err == nil {
			t.Fatal("expected insufficient funds error")
		}
		if rec, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err != nil || rec != nil {
			t.Fatalf("expected definitive rejection to roll back watermark, got (%+v, %v)", rec, err)
		}

		// Store persistence failure before broadcast must return error without broadcasting.
		store.failSave = true
		beforeCalls := mock.sendCalls.Load()
		if err := client.SendTransaction(signed1); err == nil {
			t.Fatal("expected SendTransaction to fail when pending tx store write fails")
		}
		if mock.sendCalls.Load() != beforeCalls {
			t.Fatal("SendTransaction broadcast tx despite failing to persist watermark")
		}
	})
}

func TestSendTransactionRateLimiterWaitCountsTowardSendTimeout(t *testing.T) {
	mock, url := newEvmTxMockServer(t)
	contract := ecommon.HexToAddress("0x3434343434343434343434343434343434343434")

	// Deplete the single burst token with a very slow refill rate (1 req / 100s).
	client, err := NewEvmRpcClient(config.BaseNetworkConfig{
		Urls:                 []string{url},
		RpcRequestsPerSecond: 0.01,
		RpcBurst:             1,
	}, "rate-limit-send", contract)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	// Consume the 1 initial burst token.
	if _, err := client.BlockNumber(); err != nil {
		t.Fatal(err)
	}
	client.txMu.Lock()
	client.sendTimeout = 100 * time.Millisecond
	client.txMu.Unlock()

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	tx := etypes.NewTx(&etypes.LegacyTx{
		Nonce:    0,
		To:       &contract,
		Value:    big.NewInt(0),
		Gas:      21000,
		GasPrice: big.NewInt(1),
	})
	signed, err := etypes.SignTx(tx, etypes.LatestSignerForChainID(big.NewInt(1)), key)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	err = client.SendTransaction(signed)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected rate-limited SendTransaction to fail with deadline exceeded")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "exceed") {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("SendTransaction took %s waiting on rate limiter instead of honoring sendTimeout", elapsed)
	}
	if mock.sendCalls.Load() != 0 {
		t.Fatalf("expected 0 send calls while rate-limited, got %d", mock.sendCalls.Load())
	}
}

func TestAmbiguousBroadcastPersistedOwnerRespectedWithStaleOrFailingPendingNonce(t *testing.T) {
	contract := ecommon.HexToAddress("0x5656565656565656565656565656565656565656")
	newTss := ecommon.HexToAddress("0x7878787878787878787878787878787878787878")

	for _, firstAction := range []string{"setTss", "halt"} {
		secondAction := "halt"
		if firstAction == "halt" {
			secondAction = "setTss"
		}
		for _, failPendingRPC := range []bool{false, true} {
			mode := "stale_pending_eq_confirmed"
			if failPendingRPC {
				mode = "pending_rpc_unavailable"
			}
			t.Run(firstAction+"_ambiguous_then_"+secondAction+"_"+mode, func(t *testing.T) {
				store := newMemPendingTxStore()
				key, err := crypto.GenerateKey()
				if err != nil {
					t.Fatal(err)
				}
				sender := crypto.PubkeyToAddress(key.PublicKey)
				signer := etypes.LatestSignerForChainID(big.NewInt(1))

				var dropSend atomic.Bool
				dropSend.Store(true)
				var failPending atomic.Bool

				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var req struct {
						ID     json.RawMessage   `json:"id"`
						Method string            `json:"method"`
						Params []json.RawMessage `json:"params"`
					}
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
					if req.Method == "eth_sendRawTransaction" && dropSend.Load() {
						if hj, ok := w.(http.Hijacker); ok {
							if conn, _, err := hj.Hijack(); err == nil {
								_ = conn.Close()
								return
							}
						}
					}
					resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
					switch req.Method {
					case "eth_chainId":
						resp["result"] = hexutil.EncodeUint64(1)
					case "eth_blockNumber":
						resp["result"] = hexutil.EncodeUint64(100)
					case "eth_getBalance":
						resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
					case "eth_getTransactionCount":
						var tag string
						if len(req.Params) >= 2 {
							_ = json.Unmarshal(req.Params[1], &tag)
						}
						if tag == "pending" && failPending.Load() {
							resp["error"] = map[string]interface{}{"code": -32000, "message": "pending nonce unavailable"}
						} else {
							// Stale endpoint reports pending == confirmed == 5.
							resp["result"] = hexutil.EncodeUint64(5)
						}
					case "eth_getBlockByNumber":
						resp["result"] = map[string]interface{}{
							"number":        hexutil.EncodeUint64(100),
							"hash":          ecommon.HexToHash("0x11"),
							"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
						}
					case "eth_maxPriorityFeePerGas":
						resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
					case "eth_estimateGas":
						resp["result"] = hexutil.EncodeUint64(100_000)
					default:
						resp["result"] = ecommon.HexToHash("0xcc").Hex()
					}
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(resp)
				}))
				t.Cleanup(srv.Close)

				build := func(c *EvmRpc, action string) (*etypes.Transaction, error) {
					if action == "halt" {
						return c.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
					}
					return c.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
				}

				// Process 1: ambiguous EOF broadcast of firstAction at nonce 5.
				c1, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "ambig-1", contract)
				if err != nil {
					t.Fatal(err)
				}
				c1.SetChainID(1)
				c1.SetPendingTxStore(store)
				tx1, err := build(c1, firstAction)
				if err != nil {
					t.Fatal(err)
				}
				if tx1.Nonce() != 5 {
					t.Fatalf("firstAction nonce = %d, want 5", tx1.Nonce())
				}
				signed1, err := etypes.SignTx(tx1, signer, key)
				if err != nil {
					t.Fatal(err)
				}
				if err := c1.SendTransaction(signed1); err == nil {
					t.Fatal("expected ambiguous EOF error")
				}
				c1.Stop()

				// Process 2 (after restart): endpoint reports pending == confirmed == 5 or
				// fails the pending nonce RPC. Building secondAction first MUST respect
				// firstAction's persisted ownership of nonce 5 and allocate nonce 6.
				dropSend.Store(false)
				failPending.Store(failPendingRPC)
				c2, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "ambig-2", contract)
				if err != nil {
					t.Fatal(err)
				}
				defer c2.Stop()
				c2.SetChainID(1)
				c2.SetPendingTxStore(store)

				tx2, err := build(c2, secondAction)
				if err != nil {
					t.Fatal(err)
				}
				if firstAction == "halt" {
					// setTss must NEVER steal halt's persisted nonce 5 even when pending RPC is stale or unavailable.
					if tx2.Nonce() != 6 {
						t.Fatalf("setTss stole persisted nonce 5 owned by halt (%s): got nonce %d, want 6", mode, tx2.Nonce())
					}
				} else {
					// Emergency halt must NOT strand itself at nonce 6 behind an ambiguous/lost setTss at 5;
					// it explicitly supersedes nonce 5 with >=20% bumped fees and transfers ownership.
					if tx2.Nonce() != 5 {
						t.Fatalf("emergency halt after ambiguous setTss (%s) got nonce %d, want 5", mode, tx2.Nonce())
					}
					if tx2.GasTipCap().Cmp(bumpFee(tx1.GasTipCap())) < 0 || tx2.GasFeeCap().Cmp(bumpFee(tx1.GasFeeCap())) < 0 {
						t.Fatalf("superseding halt (%s) did not bump >=20%% over setTss: setTss=(%s,%s) halt=(%s,%s)",
							mode, tx1.GasTipCap(), tx1.GasFeeCap(), tx2.GasTipCap(), tx2.GasFeeCap())
					}
					signed2, err := etypes.SignTx(tx2, signer, key)
					if err != nil {
						t.Fatal(err)
					}
					if err := c2.SendTransaction(signed2); err != nil {
						t.Fatal(err)
					}
					if oldRec, err := store.LoadPendingEvmTx(sender, contract, evmTxKindSetTss); err != nil || oldRec != nil {
						t.Fatalf("superseded setTss record still in store: (%+v, %v)", oldRec, err)
					}
					if newRec, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err != nil || newRec == nil || newRec.Nonce != 5 {
						t.Fatalf("halt record missing after ownership transfer: (%+v, %v)", newRec, err)
					}
				}
			})
		}
	}
}

func TestHaltSupersedingSetTssTransferAndRollbackAreAtomic(t *testing.T) {
	contract := ecommon.HexToAddress("0x6161616161616161616161616161616161616161")
	newTss := ecommon.HexToAddress("0x6262626262626262626262626262626262626262")
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	signer := etypes.LatestSignerForChainID(big.NewInt(1))

	t.Run("definitive rejection rolls back superseded setTss atomically", func(t *testing.T) {
		mock, url := newEvmTxMockServer(t)
		mock.baseFee = big.NewInt(20_000_000_000)
		mock.tipCap = big.NewInt(2_000_000_000)
		mock.confirmedNonce = 5
		mock.pendingNonce = 5

		store := &failingPendingTxStore{memPendingTxStore: newMemPendingTxStore()}
		client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{url}}, "atomic-rollback", contract)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Stop()
		client.SetChainID(1)
		client.SetPendingTxStore(store)

		// 1. Send setTss at nonce 5.
		setTssTx, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		signedSetTss, err := etypes.SignTx(setTssTx, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.SendTransaction(signedSetTss); err != nil {
			t.Fatal(err)
		}

		// 2. Build emergency halt superseding setTss at nonce 5, then fail broadcast with definitive rejection.
		mock.mu.Lock()
		mock.sendErr = "insufficient funds for gas * price + value"
		mock.mu.Unlock()
		haltTx, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		if haltTx.Nonce() != 5 {
			t.Fatalf("superseding halt nonce = %d, want 5", haltTx.Nonce())
		}
		signedHalt, err := etypes.SignTx(haltTx, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.SendTransaction(signedHalt); err == nil {
			t.Fatal("expected insufficient funds rejection")
		}

		// Verify ownership transfer (batch 2) and rollback (batch 3) each committed
		// both keys together in one atomic batch.
		store.mu.Lock()
		batches := append([][]common.PendingEvmTxOp(nil), store.batches...)
		store.mu.Unlock()
		if len(batches) != 3 {
			t.Fatalf("batch count = %d, want 3 (setTss save, halt transfer, atomic rollback)", len(batches))
		}
		if len(batches[1]) != 2 || len(batches[2]) != 2 {
			t.Fatalf("transfer/rollback batch lengths = (%d, %d), want (2, 2)", len(batches[1]), len(batches[2]))
		}
		if haltRec, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err != nil || haltRec != nil {
			t.Fatalf("after rollback, halt = (%+v, %v), want nil", haltRec, err)
		}
		restored, err := store.LoadPendingEvmTx(sender, contract, evmTxKindSetTss)
		if err != nil || restored == nil || restored.Nonce != 5 || restored.TxHash != signedSetTss.Hash() {
			t.Fatalf("after rollback, setTss = (%+v, %v), want restored nonce 5 hash %s", restored, err, signedSetTss.Hash())
		}
	})

	t.Run("interrupted rollback batch never erases both records", func(t *testing.T) {
		mock, url := newEvmTxMockServer(t)
		mock.baseFee = big.NewInt(20_000_000_000)
		mock.tipCap = big.NewInt(2_000_000_000)
		mock.confirmedNonce = 5
		mock.pendingNonce = 5

		store := &failingPendingTxStore{
			memPendingTxStore: newMemPendingTxStore(),
			failBatchOnCall:   3, // fail the rollback batch
		}
		client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{url}}, "atomic-rollback-fail", contract)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Stop()
		client.SetChainID(1)
		client.SetPendingTxStore(store)

		setTssTx, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		signedSetTss, err := etypes.SignTx(setTssTx, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.SendTransaction(signedSetTss); err != nil {
			t.Fatal(err)
		}

		mock.mu.Lock()
		mock.sendErr = "insufficient funds for gas * price + value"
		mock.mu.Unlock()
		haltTx, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		signedHalt, err := etypes.SignTx(haltTx, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.SendTransaction(signedHalt); err == nil {
			t.Fatal("expected insufficient funds rejection")
		}

		// Because the atomic rollback batch failed, durable state MUST still hold
		// the committed halt ownership + bumped fee watermark (never both empty).
		haltRec, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt)
		if err != nil || haltRec == nil || haltRec.Nonce != 5 {
			t.Fatalf("after failed rollback batch, durable halt record = (%+v, %v), want nonce 5 preserved", haltRec, err)
		}
	})
}

func TestPendingTxStoreReadErrorFailsClosed(t *testing.T) {
	mock, url := newEvmTxMockServer(t)
	mock.baseFee = big.NewInt(20_000_000_000)
	mock.tipCap = big.NewInt(2_000_000_000)
	mock.confirmedNonce = 5
	mock.pendingNonce = 5

	contract := ecommon.HexToAddress("0x6363636363636363636363636363636363636363")
	newTss := ecommon.HexToAddress("0x6464646464646464646464646464646464646464")
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	signer := etypes.LatestSignerForChainID(big.NewInt(1))

	store := &failingPendingTxStore{
		memPendingTxStore: newMemPendingTxStore(),
		failLoadKind:      evmTxKindHalt,
	}
	client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{url}}, "read-err-closed", contract)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	client.SetChainID(1)
	client.SetPendingTxStore(store)

	// Unreadable halt record MUST fail GetSetTssEcdsaPubKeyEvmTx instead of
	// treating halt as absent and allocating confirmedNonce 5.
	if _, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract); err == nil {
		t.Fatal("expected GetSetTssEcdsaPubKeyEvmTx to fail when halt pending record is unreadable")
	}
	// Unreadable halt record MUST also fail SendTransaction for halt before broadcast.
	rawTx := etypes.NewTx(&etypes.LegacyTx{
		Nonce:    5,
		To:       &contract,
		Value:    big.NewInt(0),
		Gas:      100_000,
		GasPrice: big.NewInt(10_000_000_000),
		Data:     common.EvmContractAbi.Methods["halt"].ID,
	})
	signed, err := etypes.SignTx(rawTx, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SendTransaction(signed); err == nil {
		t.Fatal("expected SendTransaction to fail when halt pending record is unreadable")
	}
	if mock.sendCalls.Load() != 0 {
		t.Fatalf("SendTransaction broadcast despite unreadable pending tx record: calls=%d", mock.sendCalls.Load())
	}
}

func TestClearObsoleteHaltTxClearsDroppedAndPreservesLiveMempoolHalt(t *testing.T) {
	contract := ecommon.HexToAddress("0x6565656565656565656565656565656565656565")
	newTss := ecommon.HexToAddress("0x6666666666666666666666666666666666666666")
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	signer := etypes.LatestSignerForChainID(big.NewInt(1))

	t.Run("dropped halt is cleared after emergency ends so setTss reuses confirmed nonce", func(t *testing.T) {
		mock, url := newEvmTxMockServer(t)
		mock.baseFee = big.NewInt(20_000_000_000)
		mock.tipCap = big.NewInt(2_000_000_000)
		mock.confirmedNonce = 5
		mock.pendingNonce = 5

		store := newMemPendingTxStore()
		client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{url}}, "clear-dropped-halt", contract)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Stop()
		client.SetChainID(1)
		client.SetPendingTxStore(store)

		// Ambiguous halt submission that never entered the mempool (or was evicted).
		mock.mu.Lock()
		mock.sendErr = "connection reset by peer"
		mock.mu.Unlock()
		haltTx, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		signedHalt, err := etypes.SignTx(haltTx, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.SendTransaction(signedHalt); err == nil {
			t.Fatal("expected ambiguous transport error")
		}
		mock.mu.Lock()
		mock.sendErr = ""
		mock.mu.Unlock()

		// Before clearing, setTss respects the halt reservation and picks nonce 6.
		beforeSetTss, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		if beforeSetTss.Nonce() != 6 {
			t.Fatalf("before clear, setTss nonce = %d, want 6", beforeSetTss.Nonce())
		}

		// After emergency ends, ClearObsoleteHaltTx sees confirmed=5, pending=5,
		// and TransactionByHash == NotFound, so it clears the obsolete halt record.
		if err := client.ClearObsoleteHaltTx(sender); err != nil {
			t.Fatalf("ClearObsoleteHaltTx: %v", err)
		}
		afterSetTss, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		if afterSetTss.Nonce() != 5 {
			t.Fatalf("after ClearObsoleteHaltTx, setTss nonce = %d, want 5 (not stranded at 6)", afterSetTss.Nonce())
		}
	})

	t.Run("live mempool halt is preserved by ClearObsoleteHaltTx", func(t *testing.T) {
		mock, url := newEvmTxMockServer(t)
		mock.baseFee = big.NewInt(20_000_000_000)
		mock.tipCap = big.NewInt(2_000_000_000)
		mock.confirmedNonce = 5
		mock.pendingNonce = 5

		store := newMemPendingTxStore()
		client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{url}}, "keep-live-halt", contract)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Stop()
		client.SetChainID(1)
		client.SetPendingTxStore(store)

		haltTx, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		signedHalt, err := etypes.SignTx(haltTx, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.SendTransaction(signedHalt); err != nil {
			t.Fatal(err)
		}

		// Even if pendingNonce is reported stale as 5 while eth_getTransactionByHash
		// still knows signedHalt, ClearObsoleteHaltTx MUST keep halt ownership.
		mock.mu.Lock()
		mock.pendingNonce = 5
		mock.mu.Unlock()
		if err := client.ClearObsoleteHaltTx(sender); err != nil {
			t.Fatalf("ClearObsoleteHaltTx: %v", err)
		}
		setTssTx, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		if setTssTx.Nonce() != 6 {
			t.Fatalf("setTss after ClearObsoleteHaltTx on live halt got nonce %d, want 6", setTssTx.Nonce())
		}
	})
}

func TestAnySecondaryHasUnwrapLogChunkedRetainsPartialProgressOnTimeout(t *testing.T) {
	contract := ecommon.HexToAddress("0x6767676767676767676767676767676767676767")
	txHash := ecommon.HexToHash("0x9999")

	primarySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID json.RawMessage `json:"id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "result": hexutil.EncodeUint64(100)})
	}))
	t.Cleanup(primarySrv.Close)

	var logCalls atomic.Int32
	secondarySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_blockNumber":
			resp["result"] = hexutil.EncodeUint64(100)
		case "eth_getLogs":
			call := logCalls.Add(1)
			if call >= 3 {
				// Block the 3rd chunk until the per-attempt timeout expires.
				select {
				case <-r.Context().Done():
					return
				case <-time.After(2 * time.Second):
				}
			}
			resp["result"] = []etypes.Log{}
		default:
			resp["result"] = hexutil.EncodeUint64(1)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(secondarySrv.Close)

	client, err := NewEvmRpcClient(config.BaseNetworkConfig{
		Urls:            []string{primarySrv.URL, secondarySrv.URL},
		FilterQuerySize: 5,
	}, "partial-progress", contract)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	client.txMu.Lock()
	client.secondaryLogTimeout = 150 * time.Millisecond
	client.txMu.Unlock()

	found, lowest, complete, err := client.AnySecondaryHasUnwrapLogChunked(context.Background(), 1, 100, 100, txHash, 0, 10)
	if err != nil {
		t.Fatalf("expected partial progress retention instead of error, got err=%v", err)
	}
	if found || complete {
		t.Fatalf("got (found=%v, complete=%v), want (false, false)", found, complete)
	}
	// Chunks 1 ([96..100]) and 2 ([91..95]) completed before chunk 3 timed out.
	if lowest != 91 {
		t.Fatalf("lowestVerified = %d, want 91", lowest)
	}
}

func TestHaltFillsFirstAvailableNonceBelowStrandedSetTssGap(t *testing.T) {
	mock, url := newEvmTxMockServer(t)
	mock.baseFee = big.NewInt(20_000_000_000)
	mock.tipCap = big.NewInt(2_000_000_000)
	mock.confirmedNonce = 5
	mock.pendingNonce = 5

	contract := ecommon.HexToAddress("0x6868686868686868686868686868686868686868")
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)

	store := newMemPendingTxStore()
	// Stranded setTss persisted at nonce 6 while confirmed == pending == 5.
	if err := store.SavePendingEvmTx(sender, contract, evmTxKindSetTss, &common.PendingEvmTx{
		Nonce:     6,
		TxHash:    ecommon.HexToHash("0xdead"),
		TipCap:    big.NewInt(2_000_000_000),
		FeeCap:    big.NewInt(42_000_000_000),
		IsDynamic: true,
	}); err != nil {
		t.Fatal(err)
	}

	client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{url}}, "gap-halt", contract)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	client.SetChainID(1)
	client.SetPendingTxStore(store)

	haltTx, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	if haltTx.Nonce() != 5 {
		t.Fatalf("halt nonce = %d, want 5 (must fill first available nonce below stranded setTss at 6)", haltTx.Nonce())
	}
}

func TestLateRejectionFromOlderHaltAttemptDoesNotEraseNewerAcceptedHalt(t *testing.T) {
	contract := ecommon.HexToAddress("0x6969696969696969696969696969696969696969")
	newTss := ecommon.HexToAddress("0x6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a")
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	signer := etypes.LatestSignerForChainID(big.NewInt(1))

	store := newMemPendingTxStore()
	var halt1Hash, halt2Hash ecommon.Hash
	halt1InFlight := make(chan struct{})
	releaseHalt1 := make(chan struct{})
	var confirmedNonce, pendingNonce atomic.Uint64
	confirmedNonce.Store(5)
	pendingNonce.Store(5)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_blockNumber":
			resp["result"] = hexutil.EncodeUint64(100)
		case "eth_getBalance":
			resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
		case "eth_getTransactionCount":
			var tag string
			if len(req.Params) >= 2 {
				_ = json.Unmarshal(req.Params[1], &tag)
			}
			if tag == "pending" {
				resp["result"] = hexutil.EncodeUint64(pendingNonce.Load())
			} else {
				resp["result"] = hexutil.EncodeUint64(confirmedNonce.Load())
			}
		case "eth_getBlockByNumber":
			resp["result"] = map[string]interface{}{
				"number":        hexutil.EncodeUint64(100),
				"hash":          ecommon.HexToHash("0x11"),
				"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
			}
		case "eth_maxPriorityFeePerGas":
			resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
		case "eth_estimateGas":
			resp["result"] = hexutil.EncodeUint64(100_000)
		case "eth_sendRawTransaction":
			var raw hexutil.Bytes
			_ = json.Unmarshal(req.Params[0], &raw)
			var tx etypes.Transaction
			_ = tx.UnmarshalBinary(raw)
			if tx.Hash() == halt1Hash {
				close(halt1InFlight)
				<-releaseHalt1
				resp["error"] = map[string]interface{}{"code": -32000, "message": "insufficient funds for gas * price + value"}
			} else {
				pendingNonce.Store(tx.Nonce() + 1)
				resp["result"] = tx.Hash().Hex()
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)

	client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "concurrent-halt-rollback", contract)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	client.SetChainID(1)
	client.SetPendingTxStore(store)

	// 1. Submit initial setTss at nonce 5.
	setTssTx, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedSetTss, err := etypes.SignTx(setTssTx, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SendTransaction(signedSetTss); err != nil {
		t.Fatal(err)
	}

	// 2. Start halt1 (superseding setTss at nonce 5), pause it in-flight.
	halt1, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedHalt1, err := etypes.SignTx(halt1, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	halt1Hash = signedHalt1.Hash()
	halt1Done := make(chan error, 1)
	go func() { halt1Done <- client.SendTransaction(signedHalt1) }()
	<-halt1InFlight

	// 3. Build and send halt2 (bumped fees, same nonce 5) while halt1 is in-flight; halt2 succeeds.
	halt2, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedHalt2, err := etypes.SignTx(halt2, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	halt2Hash = signedHalt2.Hash()
	if err := client.SendTransaction(signedHalt2); err != nil {
		t.Fatalf("halt2 send failed: %v", err)
	}

	// 4. Release halt1 so it returns a late definitive "insufficient funds" rejection.
	close(releaseHalt1)
	if err := <-halt1Done; err == nil {
		t.Fatal("expected halt1 insufficient funds error")
	}

	// halt2 MUST remain recorded in durable storage and setTss MUST stay cleared!
	gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt)
	if err != nil || gotHalt == nil || gotHalt.TxHash != halt2Hash {
		t.Fatalf("after late halt1 rejection, halt = (%+v, %v), want halt2 hash %s", gotHalt, err, halt2Hash)
	}
	if gotSetTss, err := store.LoadPendingEvmTx(sender, contract, evmTxKindSetTss); err != nil || gotSetTss != nil {
		t.Fatalf("after late halt1 rejection, setTss = (%+v, %v), want nil", gotSetTss, err)
	}
}

func TestAnySecondaryHasUnwrapLogChunkedAttemptDeadlineCoversLimiterWait(t *testing.T) {
	contract := ecommon.HexToAddress("0x6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b")
	txHash := ecommon.HexToHash("0x7777")

	makeSrv := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
			switch req.Method {
			case "eth_blockNumber":
				resp["result"] = hexutil.EncodeUint64(100)
			case "eth_getLogs":
				resp["result"] = []etypes.Log{}
			default:
				resp["result"] = hexutil.EncodeUint64(1)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		}))
	}
	primarySrv := makeSrv()
	t.Cleanup(primarySrv.Close)
	secondarySrv := makeSrv()
	t.Cleanup(secondarySrv.Close)

	// 1 req/s with burst 2: BlockNumber + chunk 1 consume both burst tokens;
	// chunk 2 must wait ~1s for a permit, which exceeds the 80ms attempt budget.
	client, err := NewEvmRpcClient(config.BaseNetworkConfig{
		Urls:                 []string{primarySrv.URL, secondarySrv.URL},
		FilterQuerySize:      5,
		RpcRequestsPerSecond: 1,
		RpcBurst:             2,
	}, "limiter-budget", contract)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	client.txMu.Lock()
	client.secondaryLogTimeout = 80 * time.Millisecond
	client.txMu.Unlock()

	start := time.Now()
	found, lowest, complete, err := client.AnySecondaryHasUnwrapLogChunked(context.Background(), 1, 100, 100, txHash, 0, 10)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("expected partial progress retention on limiter budget expiry, got err=%v", err)
	}
	if found || complete {
		t.Fatalf("got (found=%v, complete=%v), want (false, false)", found, complete)
	}
	if lowest != 96 {
		t.Fatalf("lowestVerified = %d, want 96 (chunk 1 [96..100] preserved)", lowest)
	}
	if elapsed > 400*time.Millisecond {
		t.Fatalf("attempt took %s instead of honoring 80ms budget across limiter wait", elapsed)
	}
}

func TestOverlappingDefinitiveRejectionsRestoreOriginalSetTssNotRejectedHalt(t *testing.T) {
	contract := ecommon.HexToAddress("0x6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c")
	newTss := ecommon.HexToAddress("0x6d6d6d6d6d6d6d6d6d6d6d6d6d6d6d6d6d6d6d6d")
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	signer := etypes.LatestSignerForChainID(big.NewInt(1))

	store := newMemPendingTxStore()
	var haltAHash, haltBHash ecommon.Hash
	haltAEntered := make(chan struct{})
	haltBEntered := make(chan struct{})
	releaseHaltA := make(chan struct{})
	releaseHaltB := make(chan struct{})
	var confirmedNonce, pendingNonce atomic.Uint64
	confirmedNonce.Store(5)
	pendingNonce.Store(5)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_blockNumber":
			resp["result"] = hexutil.EncodeUint64(100)
		case "eth_getBalance":
			resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
		case "eth_getTransactionCount":
			var tag string
			if len(req.Params) >= 2 {
				_ = json.Unmarshal(req.Params[1], &tag)
			}
			if tag == "pending" {
				resp["result"] = hexutil.EncodeUint64(pendingNonce.Load())
			} else {
				resp["result"] = hexutil.EncodeUint64(confirmedNonce.Load())
			}
		case "eth_getBlockByNumber":
			resp["result"] = map[string]interface{}{
				"number":        hexutil.EncodeUint64(100),
				"hash":          ecommon.HexToHash("0x11"),
				"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
			}
		case "eth_maxPriorityFeePerGas":
			resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
		case "eth_estimateGas":
			resp["result"] = hexutil.EncodeUint64(100_000)
		case "eth_sendRawTransaction":
			var raw hexutil.Bytes
			_ = json.Unmarshal(req.Params[0], &raw)
			var tx etypes.Transaction
			_ = tx.UnmarshalBinary(raw)
			switch tx.Hash() {
			case haltAHash:
				close(haltAEntered)
				<-releaseHaltA
				resp["error"] = map[string]interface{}{"code": -32000, "message": "insufficient funds for gas * price + value"}
			case haltBHash:
				close(haltBEntered)
				<-releaseHaltB
				resp["error"] = map[string]interface{}{"code": -32000, "message": "insufficient funds for gas * price + value"}
			default:
				pendingNonce.Store(tx.Nonce() + 1)
				resp["result"] = tx.Hash().Hex()
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)

	client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "overlap-reject", contract)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	client.SetChainID(1)
	client.SetPendingTxStore(store)

	// 1. Accepted setTss at nonce 5.
	setTssTx, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedSetTss, err := etypes.SignTx(setTssTx, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SendTransaction(signedSetTss); err != nil {
		t.Fatal(err)
	}

	// 2. Halt A supersedes setTss at nonce 5 and pauses in-flight.
	haltA, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedHaltA, err := etypes.SignTx(haltA, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	haltAHash = signedHaltA.Hash()
	haltADone := make(chan error, 1)
	go func() { haltADone <- client.SendTransaction(signedHaltA) }()
	<-haltAEntered

	// 3. Halt B replaces Halt A at nonce 5 and pauses in-flight.
	haltB, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedHaltB, err := etypes.SignTx(haltB, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	haltBHash = signedHaltB.Hash()
	haltBDone := make(chan error, 1)
	go func() { haltBDone <- client.SendTransaction(signedHaltB) }()
	<-haltBEntered

	// 4. Reject Halt A FIRST, then reject Halt B SECOND.
	close(releaseHaltA)
	if err := <-haltADone; err == nil {
		t.Fatal("expected halt A rejection")
	}
	close(releaseHaltB)
	if err := <-haltBDone; err == nil {
		t.Fatal("expected halt B rejection")
	}

	// Durable store MUST NOT retain rejected Halt A, and MUST restore accepted setTss!
	if gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err != nil || gotHalt != nil {
		t.Fatalf("after overlapping definitive rejections, halt = (%+v, %v), want nil (must not restore rejected halt A)", gotHalt, err)
	}
	gotSetTss, err := store.LoadPendingEvmTx(sender, contract, evmTxKindSetTss)
	if err != nil || gotSetTss == nil || gotSetTss.Nonce != 5 || gotSetTss.TxHash != signedSetTss.Hash() {
		t.Fatalf("after overlapping definitive rejections, setTss = (%+v, %v), want restored original setTss %s", gotSetTss, err, signedSetTss.Hash())
	}
}

func TestAnySecondaryHasUnwrapLogChunkedReusesVerifiedHeadAtLowRateLimit(t *testing.T) {
	contract := ecommon.HexToAddress("0x6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e")
	txHash := ecommon.HexToHash("0x8888")

	var blockNumberCalls atomic.Int32
	var getLogsCalls atomic.Int32
	makeSrv := func(track bool) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
			switch req.Method {
			case "eth_blockNumber":
				if track {
					blockNumberCalls.Add(1)
				}
				resp["result"] = hexutil.EncodeUint64(100)
			case "eth_getLogs":
				if track {
					getLogsCalls.Add(1)
				}
				resp["result"] = []etypes.Log{}
			default:
				resp["result"] = hexutil.EncodeUint64(1)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		}))
	}
	primarySrv := makeSrv(false)
	t.Cleanup(primarySrv.Close)
	secondarySrv := makeSrv(true)
	t.Cleanup(secondarySrv.Close)

	// 1 req per 2s (0.5 req/s) with burst 1, and attempt budget 250ms (< 2s refill):
	// mirrors RpcRequestsPerSecond=0.05 (20s refill) with 15s attempt budget.
	client, err := NewEvmRpcClient(config.BaseNetworkConfig{
		Urls:                 []string{primarySrv.URL, secondarySrv.URL},
		FilterQuerySize:      5,
		RpcRequestsPerSecond: 0.5,
		RpcBurst:             1,
	}, "low-rate-corrob", contract)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	client.txMu.Lock()
	client.secondaryLogTimeout = 250 * time.Millisecond
	client.txMu.Unlock()

	// Attempt 1 spends the single burst permit on eth_blockNumber (recording head 100 >= 100)
	// and hits the limiter budget before eth_getLogs.
	if _, _, _, err := client.AnySecondaryHasUnwrapLogChunked(context.Background(), 1, 100, 100, txHash, 0, 10); err == nil {
		t.Fatal("expected attempt 1 to fail after spending burst 1 on head check")
	}
	if blockNumberCalls.Load() != 1 || getLogsCalls.Load() != 0 {
		t.Fatalf("after attempt 1: blockNumberCalls=%d getLogsCalls=%d, want (1, 0)", blockNumberCalls.Load(), getLogsCalls.Load())
	}

	// Wait for 1 permit to refill, then retry: MUST reuse verified head (no second
	// eth_blockNumber call) and advance chunk 1 ([96..100])!
	time.Sleep(2100 * time.Millisecond)
	found, lowest, complete, err := client.AnySecondaryHasUnwrapLogChunked(context.Background(), 1, 100, 100, txHash, 0, 10)
	if err != nil {
		t.Fatalf("attempt 2 failed instead of advancing log chunks using preserved head: %v", err)
	}
	if found || complete || lowest != 96 {
		t.Fatalf("attempt 2 = (found=%v, lowest=%d, complete=%v), want (false, 96, false)", found, lowest, complete)
	}
	if blockNumberCalls.Load() != 1 || getLogsCalls.Load() != 1 {
		t.Fatalf("after attempt 2: blockNumberCalls=%d getLogsCalls=%d, want (1, 1)", blockNumberCalls.Load(), getLogsCalls.Load())
	}
}

func TestCrossActionLineageNeverRestoresRejectedSetTssRetry(t *testing.T) {
	contract := ecommon.HexToAddress("0x6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f")
	newTss := ecommon.HexToAddress("0x7070707070707070707070707070707070707070")
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	signer := etypes.LatestSignerForChainID(big.NewInt(1))

	store := newMemPendingTxStore()
	var setTssRetryHash, haltHash ecommon.Hash
	setTssRetryEntered := make(chan struct{})
	haltEntered := make(chan struct{})
	releaseSetTssRetry := make(chan struct{})
	releaseHalt := make(chan struct{})
	var confirmedNonce, pendingNonce atomic.Uint64
	confirmedNonce.Store(5)
	pendingNonce.Store(5)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_blockNumber":
			resp["result"] = hexutil.EncodeUint64(100)
		case "eth_getBalance":
			resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
		case "eth_getTransactionCount":
			var tag string
			if len(req.Params) >= 2 {
				_ = json.Unmarshal(req.Params[1], &tag)
			}
			if tag == "pending" {
				resp["result"] = hexutil.EncodeUint64(pendingNonce.Load())
			} else {
				resp["result"] = hexutil.EncodeUint64(confirmedNonce.Load())
			}
		case "eth_getBlockByNumber":
			resp["result"] = map[string]interface{}{
				"number":        hexutil.EncodeUint64(100),
				"hash":          ecommon.HexToHash("0x11"),
				"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
			}
		case "eth_maxPriorityFeePerGas":
			resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
		case "eth_estimateGas":
			resp["result"] = hexutil.EncodeUint64(100_000)
		case "eth_sendRawTransaction":
			var raw hexutil.Bytes
			_ = json.Unmarshal(req.Params[0], &raw)
			var tx etypes.Transaction
			_ = tx.UnmarshalBinary(raw)
			switch tx.Hash() {
			case setTssRetryHash:
				close(setTssRetryEntered)
				<-releaseSetTssRetry
				resp["error"] = map[string]interface{}{"code": -32000, "message": "insufficient funds for gas * price + value"}
			case haltHash:
				close(haltEntered)
				<-releaseHalt
				resp["error"] = map[string]interface{}{"code": -32000, "message": "insufficient funds for gas * price + value"}
			default:
				pendingNonce.Store(tx.Nonce() + 1)
				resp["result"] = tx.Hash().Hex()
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)

	client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "cross-action-lineage", contract)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	client.SetChainID(1)
	client.SetPendingTxStore(store)

	// 1. Accepted setTss1 at nonce 5.
	setTss1, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedSetTss1, err := etypes.SignTx(setTss1, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SendTransaction(signedSetTss1); err != nil {
		t.Fatal(err)
	}

	// 2. Start setTss2 retry at nonce 5 and pause in-flight.
	setTss2, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedSetTss2, err := etypes.SignTx(setTss2, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	setTssRetryHash = signedSetTss2.Hash()
	setTss2Done := make(chan error, 1)
	go func() { setTss2Done <- client.SendTransaction(signedSetTss2) }()
	<-setTssRetryEntered

	// 3. Emergency halt supersedes setTss2 at nonce 5 and pauses in-flight.
	halt1, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedHalt1, err := etypes.SignTx(halt1, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	haltHash = signedHalt1.Hash()
	halt1Done := make(chan error, 1)
	go func() { halt1Done <- client.SendTransaction(signedHalt1) }()
	<-haltEntered

	// 4. Reject setTss2 FIRST while halt1 is pending: MUST NOT recreate a second owner!
	close(releaseSetTssRetry)
	if err := <-setTss2Done; err == nil {
		t.Fatal("expected setTss2 rejection")
	}
	if gotSetTss, err := store.LoadPendingEvmTx(sender, contract, evmTxKindSetTss); err != nil || gotSetTss != nil {
		t.Fatalf("while halt is pending, rejected setTss2 recreated second owner: (%+v, %v)", gotSetTss, err)
	}
	if gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err != nil || gotHalt == nil || gotHalt.TxHash != haltHash {
		t.Fatalf("while halt is pending, halt owner lost: (%+v, %v)", gotHalt, err)
	}

	// 5. Reject halt1 SECOND: MUST restore accepted setTss1 (NOT rejected setTss2)!
	close(releaseHalt)
	if err := <-halt1Done; err == nil {
		t.Fatal("expected halt1 rejection")
	}
	if gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err != nil || gotHalt != nil {
		t.Fatalf("after halt1 rejection, halt = (%+v, %v), want nil", gotHalt, err)
	}
	gotSetTss, err := store.LoadPendingEvmTx(sender, contract, evmTxKindSetTss)
	if err != nil || gotSetTss == nil || gotSetTss.TxHash != signedSetTss1.Hash() {
		t.Fatalf("after halt1 rejection, setTss = (%+v, %v), want accepted setTss1 %s (not rejected setTss2 %s)",
			gotSetTss, err, signedSetTss1.Hash(), setTssRetryHash)
	}
}

func TestLateRejectionOfOlderNonceDoesNotEraseAcceptedNewerNonce(t *testing.T) {
	contract := ecommon.HexToAddress("0x7171717171717171717171717171717171717171")
	newTss := ecommon.HexToAddress("0x7272727272727272727272727272727272727272")

	t.Run("same action newer nonce preserved when older nonce rejects late", func(t *testing.T) {
		key, err := crypto.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		sender := crypto.PubkeyToAddress(key.PublicKey)
		signer := etypes.LatestSignerForChainID(big.NewInt(1))
		store := newMemPendingTxStore()

		var halt5Hash ecommon.Hash
		halt5Entered := make(chan struct{})
		releaseHalt5 := make(chan struct{})
		var confirmedNonce, pendingNonce atomic.Uint64
		confirmedNonce.Store(5)
		pendingNonce.Store(5)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     json.RawMessage   `json:"id"`
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
			switch req.Method {
			case "eth_chainId":
				resp["result"] = hexutil.EncodeUint64(1)
			case "eth_blockNumber":
				resp["result"] = hexutil.EncodeUint64(100)
			case "eth_getBalance":
				resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
			case "eth_getTransactionCount":
				var tag string
				if len(req.Params) >= 2 {
					_ = json.Unmarshal(req.Params[1], &tag)
				}
				if tag == "pending" {
					resp["result"] = hexutil.EncodeUint64(pendingNonce.Load())
				} else {
					resp["result"] = hexutil.EncodeUint64(confirmedNonce.Load())
				}
			case "eth_getBlockByNumber":
				resp["result"] = map[string]interface{}{
					"number":        hexutil.EncodeUint64(100),
					"hash":          ecommon.HexToHash("0x11"),
					"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
				}
			case "eth_maxPriorityFeePerGas":
				resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
			case "eth_estimateGas":
				resp["result"] = hexutil.EncodeUint64(100_000)
			case "eth_sendRawTransaction":
				var raw hexutil.Bytes
				_ = json.Unmarshal(req.Params[0], &raw)
				var tx etypes.Transaction
				_ = tx.UnmarshalBinary(raw)
				if tx.Hash() == halt5Hash {
					close(halt5Entered)
					<-releaseHalt5
					resp["error"] = map[string]interface{}{"code": -32000, "message": "insufficient funds for gas * price + value"}
				} else {
					pendingNonce.Store(tx.Nonce() + 1)
					resp["result"] = tx.Hash().Hex()
				}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		}))
		t.Cleanup(srv.Close)

		client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "late-older-nonce-same", contract)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Stop()
		client.SetChainID(1)
		client.SetPendingTxStore(store)

		// Prebuild halt at nonce 5, then advance confirmed/pending nonce to 6 and prebuild halt at nonce 6.
		halt5, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		signedHalt5, err := etypes.SignTx(halt5, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		halt5Hash = signedHalt5.Hash()

		confirmedNonce.Store(6)
		pendingNonce.Store(6)
		halt6, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		if halt6.Nonce() != 6 {
			t.Fatalf("halt6 nonce = %d, want 6", halt6.Nonce())
		}
		signedHalt6, err := etypes.SignTx(halt6, signer, key)
		if err != nil {
			t.Fatal(err)
		}

		// Start nonce 5 (paused in-flight), then send and accept nonce 6.
		halt5Done := make(chan error, 1)
		go func() { halt5Done <- client.SendTransaction(signedHalt5) }()
		<-halt5Entered

		if err := client.SendTransaction(signedHalt6); err != nil {
			t.Fatalf("expected halt6 send to succeed, got %v", err)
		}

		// Now reject nonce 5 late: MUST NOT delete or overwrite durable nonce 6!
		close(releaseHalt5)
		if err := <-halt5Done; err == nil {
			t.Fatal("expected halt5 rejection")
		}

		gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt)
		if err != nil || gotHalt == nil || gotHalt.Nonce != 6 || gotHalt.TxHash != signedHalt6.Hash() {
			t.Fatalf("durable halt after late nonce-5 rejection = (%+v, %v), want nonce 6 hash %s", gotHalt, err, signedHalt6.Hash())
		}
	})

	t.Run("cross action newer nonce preserved when superseded nonce rejects late", func(t *testing.T) {
		key, err := crypto.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		sender := crypto.PubkeyToAddress(key.PublicKey)
		signer := etypes.LatestSignerForChainID(big.NewInt(1))
		store := newMemPendingTxStore()

		var halt5Hash ecommon.Hash
		halt5Entered := make(chan struct{})
		releaseHalt5 := make(chan struct{})
		var confirmedNonce, pendingNonce atomic.Uint64
		confirmedNonce.Store(5)
		pendingNonce.Store(5)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     json.RawMessage   `json:"id"`
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
			switch req.Method {
			case "eth_chainId":
				resp["result"] = hexutil.EncodeUint64(1)
			case "eth_blockNumber":
				resp["result"] = hexutil.EncodeUint64(100)
			case "eth_getBalance":
				resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
			case "eth_getTransactionCount":
				var tag string
				if len(req.Params) >= 2 {
					_ = json.Unmarshal(req.Params[1], &tag)
				}
				if tag == "pending" {
					resp["result"] = hexutil.EncodeUint64(pendingNonce.Load())
				} else {
					resp["result"] = hexutil.EncodeUint64(confirmedNonce.Load())
				}
			case "eth_getBlockByNumber":
				resp["result"] = map[string]interface{}{
					"number":        hexutil.EncodeUint64(100),
					"hash":          ecommon.HexToHash("0x11"),
					"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
				}
			case "eth_maxPriorityFeePerGas":
				resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
			case "eth_estimateGas":
				resp["result"] = hexutil.EncodeUint64(100_000)
			case "eth_sendRawTransaction":
				var raw hexutil.Bytes
				_ = json.Unmarshal(req.Params[0], &raw)
				var tx etypes.Transaction
				_ = tx.UnmarshalBinary(raw)
				if tx.Hash() == halt5Hash {
					close(halt5Entered)
					<-releaseHalt5
					resp["error"] = map[string]interface{}{"code": -32000, "message": "insufficient funds for gas * price + value"}
				} else {
					pendingNonce.Store(tx.Nonce() + 1)
					resp["result"] = tx.Hash().Hex()
				}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		}))
		t.Cleanup(srv.Close)

		client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "late-older-nonce-cross", contract)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Stop()
		client.SetChainID(1)
		client.SetPendingTxStore(store)

		// 1. Accepted setTss at nonce 5.
		setTss5, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		signedSetTss5, err := etypes.SignTx(setTss5, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.SendTransaction(signedSetTss5); err != nil {
			t.Fatal(err)
		}

		// 2. Halt supersedes setTss at nonce 5 and pauses in-flight.
		halt5, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		signedHalt5, err := etypes.SignTx(halt5, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		halt5Hash = signedHalt5.Hash()
		halt5Done := make(chan error, 1)
		go func() { halt5Done <- client.SendTransaction(signedHalt5) }()
		<-halt5Entered

		// 3. A newer setTss at nonce 6 is sent and accepted while halt5 is still in-flight.
		setTss6, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		if setTss6.Nonce() != 6 {
			t.Fatalf("setTss6 nonce = %d, want 6", setTss6.Nonce())
		}
		signedSetTss6, err := etypes.SignTx(setTss6, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.SendTransaction(signedSetTss6); err != nil {
			t.Fatal(err)
		}

		// 4. Reject halt5 late: MUST clear halt5 and preserve setTss at nonce 6
		// (never rolling setTss back to nonce 5!).
		close(releaseHalt5)
		if err := <-halt5Done; err == nil {
			t.Fatal("expected halt5 rejection")
		}
		if gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err != nil || gotHalt != nil {
			t.Fatalf("durable halt after halt5 rejection = (%+v, %v), want nil", gotHalt, err)
		}
		gotSetTss, err := store.LoadPendingEvmTx(sender, contract, evmTxKindSetTss)
		if err != nil || gotSetTss == nil || gotSetTss.Nonce != 6 || gotSetTss.TxHash != signedSetTss6.Hash() {
			t.Fatalf("durable setTss after halt5 rejection = (%+v, %v), want nonce 6 hash %s", gotSetTss, err, signedSetTss6.Hash())
		}
	})
}

func TestTwoDefinitiveRejectionsPreserveAcceptedLowerNonceReservation(t *testing.T) {
	contract := ecommon.HexToAddress("0x7373737373737373737373737373737373737373")
	newTss := ecommon.HexToAddress("0x7474747474747474747474747474747474747474")
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	signer := etypes.LatestSignerForChainID(big.NewInt(1))
	store := newMemPendingTxStore()

	var halt5Hash, setTss6Hash ecommon.Hash
	halt5Entered := make(chan struct{})
	setTss6Entered := make(chan struct{})
	releaseHalt5 := make(chan struct{})
	releaseSetTss6 := make(chan struct{})
	var confirmedNonce, pendingNonce atomic.Uint64
	confirmedNonce.Store(5)
	pendingNonce.Store(5)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_blockNumber":
			resp["result"] = hexutil.EncodeUint64(100)
		case "eth_getBalance":
			resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
		case "eth_getTransactionCount":
			var tag string
			if len(req.Params) >= 2 {
				_ = json.Unmarshal(req.Params[1], &tag)
			}
			if tag == "pending" {
				resp["result"] = hexutil.EncodeUint64(pendingNonce.Load())
			} else {
				resp["result"] = hexutil.EncodeUint64(confirmedNonce.Load())
			}
		case "eth_getBlockByNumber":
			resp["result"] = map[string]interface{}{
				"number":        hexutil.EncodeUint64(100),
				"hash":          ecommon.HexToHash("0x11"),
				"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
			}
		case "eth_maxPriorityFeePerGas":
			resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
		case "eth_estimateGas":
			resp["result"] = hexutil.EncodeUint64(100_000)
		case "eth_sendRawTransaction":
			var raw hexutil.Bytes
			_ = json.Unmarshal(req.Params[0], &raw)
			var tx etypes.Transaction
			_ = tx.UnmarshalBinary(raw)
			switch tx.Hash() {
			case halt5Hash:
				close(halt5Entered)
				<-releaseHalt5
				resp["error"] = map[string]interface{}{"code": -32000, "message": "insufficient funds for gas * price + value"}
			case setTss6Hash:
				close(setTss6Entered)
				<-releaseSetTss6
				resp["error"] = map[string]interface{}{"code": -32000, "message": "insufficient funds for gas * price + value"}
			default:
				pendingNonce.Store(tx.Nonce() + 1)
				resp["result"] = tx.Hash().Hex()
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)

	client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "two-rejections-preserve-5", contract)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	client.SetChainID(1)
	client.SetPendingTxStore(store)

	// 1. Accept setTss(5).
	setTss5, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedSetTss5, err := etypes.SignTx(setTss5, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SendTransaction(signedSetTss5); err != nil {
		t.Fatal(err)
	}

	// 2. Start halt(5) superseding setTss(5), paused in-flight.
	halt5, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedHalt5, err := etypes.SignTx(halt5, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	halt5Hash = signedHalt5.Hash()
	halt5Done := make(chan error, 1)
	go func() { halt5Done <- client.SendTransaction(signedHalt5) }()
	<-halt5Entered

	// 3. Start setTss(6), paused in-flight.
	setTss6, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	if setTss6.Nonce() != 6 {
		t.Fatalf("setTss6 nonce = %d, want 6", setTss6.Nonce())
	}
	signedSetTss6, err := etypes.SignTx(setTss6, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	setTss6Hash = signedSetTss6.Hash()
	setTss6Done := make(chan error, 1)
	go func() { setTss6Done <- client.SendTransaction(signedSetTss6) }()
	<-setTss6Entered

	// 4. Reject halt(5) FIRST (setTss(6) occupies setTss slot so setTss(5) waits).
	close(releaseHalt5)
	if err := <-halt5Done; err == nil {
		t.Fatal("expected halt5 rejection")
	}

	// 5. Reject setTss(6) SECOND: MUST reconcile surviving ownership across
	// nonces and restore accepted setTss(5) instead of deleting the slot!
	close(releaseSetTss6)
	if err := <-setTss6Done; err == nil {
		t.Fatal("expected setTss6 rejection")
	}

	if gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err != nil || gotHalt != nil {
		t.Fatalf("durable halt after both rejections = (%+v, %v), want nil", gotHalt, err)
	}
	gotSetTss, err := store.LoadPendingEvmTx(sender, contract, evmTxKindSetTss)
	if err != nil || gotSetTss == nil || gotSetTss.Nonce != 5 || gotSetTss.TxHash != signedSetTss5.Hash() {
		t.Fatalf("durable setTss after both rejections = (%+v, %v), want accepted setTss(5) %s", gotSetTss, err, signedSetTss5.Hash())
	}
}

func TestTwoOverlappingHaltRejectionsPreserveAcceptedRotationReservation(t *testing.T) {
	contract := ecommon.HexToAddress("0x7575757575757575757575757575757575757575")
	newTss := ecommon.HexToAddress("0x7676767676767676767676767676767676767676")
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	signer := etypes.LatestSignerForChainID(big.NewInt(1))
	store := newMemPendingTxStore()

	var halt5Hash, halt6Hash ecommon.Hash
	halt5Entered := make(chan struct{})
	halt6Entered := make(chan struct{})
	releaseHalt5 := make(chan struct{})
	releaseHalt6 := make(chan struct{})
	var confirmedNonce, pendingNonce atomic.Uint64
	confirmedNonce.Store(5)
	pendingNonce.Store(5)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_blockNumber":
			resp["result"] = hexutil.EncodeUint64(100)
		case "eth_getBalance":
			resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
		case "eth_getTransactionCount":
			var tag string
			if len(req.Params) >= 2 {
				_ = json.Unmarshal(req.Params[1], &tag)
			}
			if tag == "pending" {
				resp["result"] = hexutil.EncodeUint64(pendingNonce.Load())
			} else {
				resp["result"] = hexutil.EncodeUint64(confirmedNonce.Load())
			}
		case "eth_getBlockByNumber":
			resp["result"] = map[string]interface{}{
				"number":        hexutil.EncodeUint64(100),
				"hash":          ecommon.HexToHash("0x11"),
				"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
			}
		case "eth_maxPriorityFeePerGas":
			resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
		case "eth_estimateGas":
			resp["result"] = hexutil.EncodeUint64(100_000)
		case "eth_sendRawTransaction":
			var raw hexutil.Bytes
			_ = json.Unmarshal(req.Params[0], &raw)
			var tx etypes.Transaction
			_ = tx.UnmarshalBinary(raw)
			switch tx.Hash() {
			case halt5Hash:
				close(halt5Entered)
				<-releaseHalt5
				resp["error"] = map[string]interface{}{"code": -32000, "message": "insufficient funds for gas * price + value"}
			case halt6Hash:
				close(halt6Entered)
				<-releaseHalt6
				resp["error"] = map[string]interface{}{"code": -32000, "message": "insufficient funds for gas * price + value"}
			default:
				pendingNonce.Store(tx.Nonce() + 1)
				resp["result"] = tx.Hash().Hex()
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)

	client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "two-halt-rejections-preserve-settss5", contract)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	client.SetChainID(1)
	client.SetPendingTxStore(store)

	// 1. Accept setTss(5).
	setTss5, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedSetTss5, err := etypes.SignTx(setTss5, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SendTransaction(signedSetTss5); err != nil {
		t.Fatal(err)
	}

	// 2. Build halt(5) superseding setTss(5) AND prebuild halt(6) before sending either.
	halt5, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	if halt5.Nonce() != 5 {
		t.Fatalf("halt5 nonce = %d, want 5", halt5.Nonce())
	}
	signedHalt5, err := etypes.SignTx(halt5, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	halt5Hash = signedHalt5.Hash()

	halt6Raw := etypes.NewTx(&etypes.DynamicFeeTx{
		ChainID:   big.NewInt(1),
		Nonce:     6,
		GasTipCap: halt5.GasTipCap(),
		GasFeeCap: halt5.GasFeeCap(),
		Gas:       halt5.Gas(),
		To:        halt5.To(),
		Value:     big.NewInt(0),
		Data:      halt5.Data(),
	})
	signedHalt6, err := etypes.SignTx(halt6Raw, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	halt6Hash = signedHalt6.Hash()

	// 3. Start halt(5) then halt(6) in-flight concurrently.
	halt5Done := make(chan error, 1)
	go func() { halt5Done <- client.SendTransaction(signedHalt5) }()
	<-halt5Entered

	halt6Done := make(chan error, 1)
	go func() { halt6Done <- client.SendTransaction(signedHalt6) }()
	<-halt6Entered

	// 4. Reject halt(5) FIRST: MUST preserve newer halt(6) AND restore accepted setTss(5)!
	close(releaseHalt5)
	if err := <-halt5Done; err == nil {
		t.Fatal("expected halt5 rejection")
	}
	if gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err != nil || gotHalt == nil || gotHalt.Nonce != 6 || gotHalt.TxHash != halt6Hash {
		t.Fatalf("while halt6 is in-flight, durable halt = (%+v, %v), want halt(6) %s", gotHalt, err, halt6Hash)
	}
	if gotSetTss, err := store.LoadPendingEvmTx(sender, contract, evmTxKindSetTss); err != nil || gotSetTss == nil || gotSetTss.Nonce != 5 || gotSetTss.TxHash != signedSetTss5.Hash() {
		t.Fatalf("after halt5 rejection while halt6 is in-flight, durable setTss = (%+v, %v), want restored setTss(5) %s", gotSetTss, err, signedSetTss5.Hash())
	}

	// 5. Reject halt(6) SECOND: halt slot clears, and accepted setTss(5) remains durable!
	close(releaseHalt6)
	if err := <-halt6Done; err == nil {
		t.Fatal("expected halt6 rejection")
	}
	if gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err != nil || gotHalt != nil {
		t.Fatalf("after halt6 rejection, durable halt = (%+v, %v), want nil", gotHalt, err)
	}
	gotSetTss, err := store.LoadPendingEvmTx(sender, contract, evmTxKindSetTss)
	if err != nil || gotSetTss == nil || gotSetTss.Nonce != 5 || gotSetTss.TxHash != signedSetTss5.Hash() {
		t.Fatalf("after both halt rejections, durable setTss = (%+v, %v), want accepted setTss(5) %s", gotSetTss, err, signedSetTss5.Hash())
	}
}

func TestTransientRollbackFailureRecoversAcceptedRotationBeforeClearingLastHalt(t *testing.T) {
	contract := ecommon.HexToAddress("0x7777777777777777777777777777777777777777")
	newTss := ecommon.HexToAddress("0x7878787878787878787878787878787878787878")
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	signer := etypes.LatestSignerForChainID(big.NewInt(1))
	// Batch calls:
	//   1: persist setTss(5)
	//   2: persist halt(5) + delete setTss(5)
	//   3: persist halt(6)
	//   4: rollback halt(5) trying to restore setTss(5) -> FAIL TRANSIENTLY!
	//   5: rollback halt(6) -> MUST recover setTss(5) atomically while clearing halt(6)!
	store := &failingPendingTxStore{
		memPendingTxStore: newMemPendingTxStore(),
		failBatchOnCall:   4,
	}

	var halt5Hash, halt6Hash ecommon.Hash
	halt5Entered := make(chan struct{})
	halt6Entered := make(chan struct{})
	releaseHalt5 := make(chan struct{})
	releaseHalt6 := make(chan struct{})
	var confirmedNonce, pendingNonce atomic.Uint64
	confirmedNonce.Store(5)
	pendingNonce.Store(5)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_blockNumber":
			resp["result"] = hexutil.EncodeUint64(100)
		case "eth_getBalance":
			resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
		case "eth_getTransactionCount":
			var tag string
			if len(req.Params) >= 2 {
				_ = json.Unmarshal(req.Params[1], &tag)
			}
			if tag == "pending" {
				resp["result"] = hexutil.EncodeUint64(pendingNonce.Load())
			} else {
				resp["result"] = hexutil.EncodeUint64(confirmedNonce.Load())
			}
		case "eth_getBlockByNumber":
			resp["result"] = map[string]interface{}{
				"number":        hexutil.EncodeUint64(100),
				"hash":          ecommon.HexToHash("0x11"),
				"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
			}
		case "eth_maxPriorityFeePerGas":
			resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
		case "eth_estimateGas":
			resp["result"] = hexutil.EncodeUint64(100_000)
		case "eth_sendRawTransaction":
			var raw hexutil.Bytes
			_ = json.Unmarshal(req.Params[0], &raw)
			var tx etypes.Transaction
			_ = tx.UnmarshalBinary(raw)
			switch tx.Hash() {
			case halt5Hash:
				close(halt5Entered)
				<-releaseHalt5
				resp["error"] = map[string]interface{}{"code": -32000, "message": "insufficient funds for gas * price + value"}
			case halt6Hash:
				close(halt6Entered)
				<-releaseHalt6
				resp["error"] = map[string]interface{}{"code": -32000, "message": "insufficient funds for gas * price + value"}
			default:
				pendingNonce.Store(tx.Nonce() + 1)
				resp["result"] = tx.Hash().Hex()
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)

	client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "transient-rollback-recover-settss5", contract)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	client.SetChainID(1)
	client.SetPendingTxStore(store)

	// 1. Accept setTss(5) (batch call 1).
	setTss5, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedSetTss5, err := etypes.SignTx(setTss5, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SendTransaction(signedSetTss5); err != nil {
		t.Fatal(err)
	}

	// 2. Build halt(5) and halt(6).
	halt5, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedHalt5, err := etypes.SignTx(halt5, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	halt5Hash = signedHalt5.Hash()

	halt6Raw := etypes.NewTx(&etypes.DynamicFeeTx{
		ChainID:   big.NewInt(1),
		Nonce:     6,
		GasTipCap: halt5.GasTipCap(),
		GasFeeCap: halt5.GasFeeCap(),
		Gas:       halt5.Gas(),
		To:        halt5.To(),
		Value:     big.NewInt(0),
		Data:      halt5.Data(),
	})
	signedHalt6, err := etypes.SignTx(halt6Raw, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	halt6Hash = signedHalt6.Hash()

	// 3. Start halt(5) (batch call 2) and halt(6) (batch call 3) in-flight.
	halt5Done := make(chan error, 1)
	go func() { halt5Done <- client.SendTransaction(signedHalt5) }()
	<-halt5Entered

	halt6Done := make(chan error, 1)
	go func() { halt6Done <- client.SendTransaction(signedHalt6) }()
	<-halt6Entered

	// 4. Reject halt(5) FIRST -> batch call 4 transiently FAILS trying to restore setTss(5)!
	close(releaseHalt5)
	if err := <-halt5Done; err == nil {
		t.Fatal("expected halt5 rejection")
	}
	if store.batchCalls != 4 {
		t.Fatalf("expected transient failure on batch call 4, saw %d batch calls", store.batchCalls)
	}

	// 5. Storage recovers; reject halt(6) SECOND -> batch call 5 MUST recover
	// deferred setTss(5) atomically while clearing halt(6)!
	close(releaseHalt6)
	if err := <-halt6Done; err == nil {
		t.Fatal("expected halt6 rejection")
	}

	if gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err != nil || gotHalt != nil {
		t.Fatalf("after halt6 rejection, durable halt = (%+v, %v), want nil", gotHalt, err)
	}
	gotSetTss, err := store.LoadPendingEvmTx(sender, contract, evmTxKindSetTss)
	if err != nil || gotSetTss == nil || gotSetTss.Nonce != 5 || gotSetTss.TxHash != signedSetTss5.Hash() {
		t.Fatalf("after transient rollback failure + halt6 rejection, durable setTss = (%+v, %v), want recovered setTss(5) %s", gotSetTss, err, signedSetTss5.Hash())
	}
}

func TestClearObsoleteHaltDoesNotResurrectEarlierDroppedHalt(t *testing.T) {
	contract := ecommon.HexToAddress("0x7979797979797979797979797979797979797979")
	newTss := ecommon.HexToAddress("0x7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a")
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	signer := etypes.LatestSignerForChainID(big.NewInt(1))
	store := newMemPendingTxStore()

	var confirmedNonce, pendingNonce atomic.Uint64
	confirmedNonce.Store(5)
	pendingNonce.Store(5)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_blockNumber":
			resp["result"] = hexutil.EncodeUint64(100)
		case "eth_getBalance":
			resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
		case "eth_getTransactionCount":
			var tag string
			if len(req.Params) >= 2 {
				_ = json.Unmarshal(req.Params[1], &tag)
			}
			if tag == "pending" {
				resp["result"] = hexutil.EncodeUint64(pendingNonce.Load())
			} else {
				resp["result"] = hexutil.EncodeUint64(confirmedNonce.Load())
			}
		case "eth_getBlockByNumber":
			resp["result"] = map[string]interface{}{
				"number":        hexutil.EncodeUint64(100),
				"hash":          ecommon.HexToHash("0x11"),
				"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
			}
		case "eth_maxPriorityFeePerGas":
			resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
		case "eth_estimateGas":
			resp["result"] = hexutil.EncodeUint64(100_000)
		case "eth_getTransactionByHash":
			resp["result"] = nil
		case "eth_sendRawTransaction":
			var raw hexutil.Bytes
			_ = json.Unmarshal(req.Params[0], &raw)
			var tx etypes.Transaction
			_ = tx.UnmarshalBinary(raw)
			resp["result"] = tx.Hash().Hex()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)

	client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "clear-obsolete-no-resurrect", contract)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	client.SetChainID(1)
	client.SetPendingTxStore(store)

	// 1. Send halt(5) and halt(6); then both drop from mempool (confirmed == pending == 5).
	halt5, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedHalt5, err := etypes.SignTx(halt5, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SendTransaction(signedHalt5); err != nil {
		t.Fatal(err)
	}

	halt6Raw := etypes.NewTx(&etypes.DynamicFeeTx{
		ChainID:   big.NewInt(1),
		Nonce:     6,
		GasTipCap: halt5.GasTipCap(),
		GasFeeCap: halt5.GasFeeCap(),
		Gas:       halt5.Gas(),
		To:        halt5.To(),
		Value:     big.NewInt(0),
		Data:      halt5.Data(),
	})
	signedHalt6, err := etypes.SignTx(halt6Raw, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SendTransaction(signedHalt6); err != nil {
		t.Fatal(err)
	}

	// 2. ClearObsoleteHaltTx clears the dropped halt reservation.
	if err := client.ClearObsoleteHaltTx(sender); err != nil {
		t.Fatal(err)
	}
	if gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err != nil || gotHalt != nil {
		t.Fatalf("after ClearObsoleteHaltTx, durable halt = (%+v, %v), want nil", gotHalt, err)
	}

	// 3. Building the next rotation MUST NOT resurrect dropped halt(5) and MUST select nonce 5!
	rotTx, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	if rotTx.Nonce() != 5 {
		t.Fatalf("rotation nonce after ClearObsoleteHaltTx = %d, want 5 (dropped halt(5) must not be resurrected)", rotTx.Nonce())
	}
	if gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err != nil || gotHalt != nil {
		t.Fatalf("durable halt after rotation build = (%+v, %v), want nil", gotHalt, err)
	}
}

func TestRetryingFailedRollbackDeletesRejectedHaltAndRestoresSetTssAtomically(t *testing.T) {
	contract := ecommon.HexToAddress("0x7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b")
	newTss := ecommon.HexToAddress("0x7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c")
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	signer := etypes.LatestSignerForChainID(big.NewInt(1))
	// Batch calls:
	//   1: persist setTss(5)
	//   2: persist halt(5) + delete setTss(5)
	//   3: rollback halt(5) [{halt: nil}, {setTss: 5}] -> FAIL TRANSIENTLY!
	//   4: retry failed rollback on next build -> MUST delete halt(5) AND restore setTss(5) in ONE batch!
	store := &failingPendingTxStore{
		memPendingTxStore: newMemPendingTxStore(),
		failBatchOnCall:   3,
	}

	var halt5Hash ecommon.Hash
	var confirmedNonce, pendingNonce atomic.Uint64
	confirmedNonce.Store(5)
	pendingNonce.Store(5)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_blockNumber":
			resp["result"] = hexutil.EncodeUint64(100)
		case "eth_getBalance":
			resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
		case "eth_getTransactionCount":
			var tag string
			if len(req.Params) >= 2 {
				_ = json.Unmarshal(req.Params[1], &tag)
			}
			if tag == "pending" {
				resp["result"] = hexutil.EncodeUint64(pendingNonce.Load())
			} else {
				resp["result"] = hexutil.EncodeUint64(confirmedNonce.Load())
			}
		case "eth_getBlockByNumber":
			resp["result"] = map[string]interface{}{
				"number":        hexutil.EncodeUint64(100),
				"hash":          ecommon.HexToHash("0x11"),
				"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
			}
		case "eth_maxPriorityFeePerGas":
			resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
		case "eth_estimateGas":
			resp["result"] = hexutil.EncodeUint64(100_000)
		case "eth_sendRawTransaction":
			var raw hexutil.Bytes
			_ = json.Unmarshal(req.Params[0], &raw)
			var tx etypes.Transaction
			_ = tx.UnmarshalBinary(raw)
			if tx.Hash() == halt5Hash {
				resp["error"] = map[string]interface{}{"code": -32000, "message": "insufficient funds for gas * price + value"}
			} else {
				pendingNonce.Store(tx.Nonce() + 1)
				resp["result"] = tx.Hash().Hex()
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)

	client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "atomic-retry-failed-rollback", contract)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	client.SetChainID(1)
	client.SetPendingTxStore(store)

	// 1. Accept setTss(5) (batch call 1).
	setTss5, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedSetTss5, err := etypes.SignTx(setTss5, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SendTransaction(signedSetTss5); err != nil {
		t.Fatal(err)
	}

	// 2. Halt(5) supersedes setTss(5) (batch call 2) and is rejected; rollback batch call 3 fails transiently.
	halt5, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedHalt5, err := etypes.SignTx(halt5, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	halt5Hash = signedHalt5.Hash()
	if err := client.SendTransaction(signedHalt5); err == nil {
		t.Fatal("expected halt5 rejection")
	}
	if store.batchCalls != 3 {
		t.Fatalf("expected transient failure on batch call 3, got %d", store.batchCalls)
	}

	// 3. Storage recovers; next rotation build MUST retry delete(halt5)+restore(setTss5)
	// in ONE atomic batch (never leaving two owners at nonce 5) and reuse nonce 5!
	retryRot, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	if retryRot.Nonce() != 5 {
		t.Fatalf("retry rotation nonce = %d, want 5", retryRot.Nonce())
	}
	if gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err != nil || gotHalt != nil {
		t.Fatalf("after rollback retry, durable halt = (%+v, %v), want nil (must not persist two owners for nonce 5)", gotHalt, err)
	}
	gotSetTss, err := store.LoadPendingEvmTx(sender, contract, evmTxKindSetTss)
	if err != nil || gotSetTss == nil || gotSetTss.Nonce != 5 || gotSetTss.TxHash != signedSetTss5.Hash() {
		t.Fatalf("after rollback retry, durable setTss = (%+v, %v), want restored setTss(5) %s", gotSetTss, err, signedSetTss5.Hash())
	}
}

func TestLateSetTssSuccessPreservesFailedHaltRollbackRetryMarker(t *testing.T) {
	contract := ecommon.HexToAddress("0x7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d")
	newTss := ecommon.HexToAddress("0x7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e")
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	signer := etypes.LatestSignerForChainID(big.NewInt(1))

	store := &failingPendingTxStore{memPendingTxStore: newMemPendingTxStore()}
	var setTss5Hash, halt5Hash ecommon.Hash
	setTss5Entered := make(chan struct{})
	releaseSetTss5 := make(chan struct{})
	var confirmedNonce, pendingNonce atomic.Uint64
	confirmedNonce.Store(5)
	pendingNonce.Store(5)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_blockNumber":
			resp["result"] = hexutil.EncodeUint64(100)
		case "eth_getBalance":
			resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
		case "eth_getTransactionCount":
			var tag string
			if len(req.Params) >= 2 {
				_ = json.Unmarshal(req.Params[1], &tag)
			}
			if tag == "pending" {
				resp["result"] = hexutil.EncodeUint64(pendingNonce.Load())
			} else {
				resp["result"] = hexutil.EncodeUint64(confirmedNonce.Load())
			}
		case "eth_getBlockByNumber":
			resp["result"] = map[string]interface{}{
				"number":        hexutil.EncodeUint64(100),
				"hash":          ecommon.HexToHash("0x11"),
				"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
			}
		case "eth_maxPriorityFeePerGas":
			resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
		case "eth_estimateGas":
			resp["result"] = hexutil.EncodeUint64(100_000)
		case "eth_sendRawTransaction":
			var raw hexutil.Bytes
			_ = json.Unmarshal(req.Params[0], &raw)
			var tx etypes.Transaction
			_ = tx.UnmarshalBinary(raw)
			switch tx.Hash() {
			case setTss5Hash:
				close(setTss5Entered)
				<-releaseSetTss5
				pendingNonce.Store(6)
				resp["result"] = tx.Hash().Hex()
			case halt5Hash:
				resp["error"] = map[string]interface{}{"code": -32000, "message": "insufficient funds for gas * price + value"}
			default:
				resp["result"] = tx.Hash().Hex()
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)

	client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "late-settss-preserves-rollback", contract)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	client.SetChainID(1)
	client.SetPendingTxStore(store)

	// 1. Start setTss(5) paused in-flight.
	setTss5, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedSetTss5, err := etypes.SignTx(setTss5, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	setTss5Hash = signedSetTss5.Hash()
	setTss5Done := make(chan error, 1)
	go func() { setTss5Done <- client.SendTransaction(signedSetTss5) }()
	<-setTss5Entered

	// 2. Halt(5) supersedes setTss(5) (batch call 2) and is rejected; fail its rollback
	// AND fail the immediate retry during setTss(5)'s delayed success so the retry
	// marker MUST survive until storage recovers!
	halt5, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedHalt5, err := etypes.SignTx(halt5, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	halt5Hash = signedHalt5.Hash()
	store.failBatchOnCall = 3 // fail halt(5)'s rollback batch
	if err := client.SendTransaction(signedHalt5); err == nil {
		t.Fatal("expected halt5 rejection")
	}

	// Keep failing storage during setTss(5)'s delayed success so only the
	// marker preservation keeps the retry alive for the next rotation build.
	store.failSave = true
	close(releaseSetTss5)
	if err := <-setTss5Done; err != nil {
		t.Fatalf("expected setTss5 delayed success, got %v", err)
	}

	// 3. Storage recovers; next rotation build MUST retry the preserved rollback
	// marker, delete rejected halt(5), restore setTss(5), and reuse nonce 5!
	store.failSave = false
	retryRot, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	if retryRot.Nonce() != 5 {
		t.Fatalf("next rotation nonce = %d, want 5 (failed halt rollback marker was discarded!)", retryRot.Nonce())
	}
	if gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err != nil || gotHalt != nil {
		t.Fatalf("durable halt after rollback retry = (%+v, %v), want nil", gotHalt, err)
	}
	gotSetTss, err := store.LoadPendingEvmTx(sender, contract, evmTxKindSetTss)
	if err != nil || gotSetTss == nil || gotSetTss.Nonce != 5 || gotSetTss.TxHash != signedSetTss5.Hash() {
		t.Fatalf("durable setTss after rollback retry = (%+v, %v), want setTss(5) %s", gotSetTss, err, signedSetTss5.Hash())
	}
}

func TestClearObsoleteHaltConcurrentReplacementPreservesRollbackHistory(t *testing.T) {
	contract := ecommon.HexToAddress("0x7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f")
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	signer := etypes.LatestSignerForChainID(big.NewInt(1))
	store := newMemPendingTxStore()

	var haltNewHash ecommon.Hash
	lookupEntered := make(chan struct{})
	releaseLookup := make(chan struct{})
	haltNewEntered := make(chan struct{})
	releaseHaltNew := make(chan struct{})
	var confirmedNonce, pendingNonce atomic.Uint64
	confirmedNonce.Store(5)
	pendingNonce.Store(5)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_blockNumber":
			resp["result"] = hexutil.EncodeUint64(100)
		case "eth_getBalance":
			resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
		case "eth_getTransactionCount":
			var tag string
			if len(req.Params) >= 2 {
				_ = json.Unmarshal(req.Params[1], &tag)
			}
			if tag == "pending" {
				resp["result"] = hexutil.EncodeUint64(pendingNonce.Load())
			} else {
				resp["result"] = hexutil.EncodeUint64(confirmedNonce.Load())
			}
		case "eth_getBlockByNumber":
			resp["result"] = map[string]interface{}{
				"number":        hexutil.EncodeUint64(100),
				"hash":          ecommon.HexToHash("0x11"),
				"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
			}
		case "eth_maxPriorityFeePerGas":
			resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
		case "eth_estimateGas":
			resp["result"] = hexutil.EncodeUint64(100_000)
		case "eth_getTransactionByHash":
			close(lookupEntered)
			<-releaseLookup
			resp["result"] = nil
		case "eth_sendRawTransaction":
			var raw hexutil.Bytes
			_ = json.Unmarshal(req.Params[0], &raw)
			var tx etypes.Transaction
			_ = tx.UnmarshalBinary(raw)
			if tx.Hash() == haltNewHash {
				close(haltNewEntered)
				<-releaseHaltNew
				resp["error"] = map[string]interface{}{"code": -32000, "message": "insufficient funds for gas * price + value"}
			} else {
				resp["result"] = tx.Hash().Hex()
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)

	client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "clear-obsolete-concurrent-replace", contract)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	client.SetChainID(1)
	client.SetPendingTxStore(store)

	// 1. Send halt_old(5).
	haltOld, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedHaltOld, err := etypes.SignTx(haltOld, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SendTransaction(signedHaltOld); err != nil {
		t.Fatal(err)
	}

	// 2. Start ClearObsoleteHaltTx and pause during eth_getTransactionByHash(halt_old).
	clearDone := make(chan error, 1)
	go func() { clearDone <- client.ClearObsoleteHaltTx(sender) }()
	<-lookupEntered

	// 3. While ClearObsoleteHaltTx is paused, build & start replacement halt_new(5).
	haltNew, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedHaltNew, err := etypes.SignTx(haltNew, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	haltNewHash = signedHaltNew.Hash()
	haltNewDone := make(chan error, 1)
	go func() { haltNewDone <- client.SendTransaction(signedHaltNew) }()
	<-haltNewEntered

	// 4. Release ClearObsoleteHaltTx: MUST preserve both halt_new's record AND its rollback stack!
	close(releaseLookup)
	if err := <-clearDone; err != nil {
		t.Fatalf("ClearObsoleteHaltTx error: %v", err)
	}

	// 5. Reject halt_new(5) definitively: MUST roll back halt_new(5) to halt_old(5)
	// rather than leaving rejected halt_new(5) as the durable owner!
	close(releaseHaltNew)
	if err := <-haltNewDone; err == nil {
		t.Fatal("expected halt_new rejection")
	}
	gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt)
	if err != nil || gotHalt == nil || gotHalt.TxHash != signedHaltOld.Hash() {
		t.Fatalf("after concurrent ClearObsoleteHaltTx + halt_new rejection, durable halt = (%+v, %v), want rolled back to halt_old %s (not rejected halt_new %s)",
			gotHalt, err, signedHaltOld.Hash(), haltNewHash)
	}
}

func TestClearObsoleteHaltResolvesFailedRollbackWithoutErasingLiveRotation(t *testing.T) {
	contract := ecommon.HexToAddress("0x8080808080808080808080808080808080808080")
	newTss := ecommon.HexToAddress("0x8181818181818181818181818181818181818181")
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	signer := etypes.LatestSignerForChainID(big.NewInt(1))
	store := &failingPendingTxStore{memPendingTxStore: newMemPendingTxStore()}

	var setTss5Hash, halt5Hash ecommon.Hash
	setTss5Entered := make(chan struct{})
	releaseSetTss5 := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSetTss5) }) }
	var confirmedNonce, pendingNonce atomic.Uint64
	confirmedNonce.Store(5)
	pendingNonce.Store(5)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_blockNumber":
			resp["result"] = hexutil.EncodeUint64(100)
		case "eth_getBalance":
			resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
		case "eth_getTransactionCount":
			var tag string
			if len(req.Params) >= 2 {
				_ = json.Unmarshal(req.Params[1], &tag)
			}
			if tag == "pending" {
				resp["result"] = hexutil.EncodeUint64(pendingNonce.Load())
			} else {
				resp["result"] = hexutil.EncodeUint64(confirmedNonce.Load())
			}
		case "eth_getBlockByNumber":
			resp["result"] = map[string]interface{}{
				"number":        hexutil.EncodeUint64(100),
				"hash":          ecommon.HexToHash("0x11"),
				"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
			}
		case "eth_maxPriorityFeePerGas":
			resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
		case "eth_estimateGas":
			resp["result"] = hexutil.EncodeUint64(100_000)
		case "eth_getTransactionByHash":
			resp["result"] = nil
		case "eth_sendRawTransaction":
			var raw hexutil.Bytes
			_ = json.Unmarshal(req.Params[0], &raw)
			var tx etypes.Transaction
			_ = tx.UnmarshalBinary(raw)
			switch tx.Hash() {
			case setTss5Hash:
				close(setTss5Entered)
				select {
				case <-releaseSetTss5:
				case <-r.Context().Done():
					return
				}
				pendingNonce.Store(6)
				resp["result"] = tx.Hash().Hex()
			case halt5Hash:
				resp["error"] = map[string]interface{}{"code": -32000, "message": "insufficient funds for gas * price + value"}
			default:
				resp["result"] = tx.Hash().Hex()
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(func() {
		release()
		srv.Close()
	})

	client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "clear-obsolete-resolves-failed-rollback", contract)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	client.SetChainID(1)
	client.SetPendingTxStore(store)

	// 1. Start setTss(5) paused in-flight (batch call 1).
	setTss5, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedSetTss5, err := etypes.SignTx(setTss5, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	setTss5Hash = signedSetTss5.Hash()
	setTss5Done := make(chan error, 1)
	go func() { setTss5Done <- client.SendTransaction(signedSetTss5) }()
	select {
	case <-setTss5Entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for setTss5 send to reach server")
	}

	// 2. Halt(5) supersedes setTss(5) (batch call 2) and is rejected; fail its rollback (batch call 3).
	halt5, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedHalt5, err := etypes.SignTx(halt5, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	halt5Hash = signedHalt5.Hash()
	store.failBatchOnCall = 3
	if err := client.SendTransaction(signedHalt5); err == nil {
		t.Fatal("expected halt5 rejection")
	}

	// 3. Storage recovers; ClearObsoleteHaltTx runs BEFORE setTss(5) finishes.
	// It MUST resolve the failed rollback atomically (restoring setTss(5) and
	// deleting halt(5)) instead of wiping the nonce stack!
	if err := client.ClearObsoleteHaltTx(sender); err != nil {
		t.Fatalf("ClearObsoleteHaltTx error: %v", err)
	}

	// 4. setTss(5) succeeds; durable rotation watermark MUST still be present!
	release()
	if err := <-setTss5Done; err != nil {
		t.Fatalf("expected setTss5 success, got %v", err)
	}
	if gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err != nil || gotHalt != nil {
		t.Fatalf("durable halt = (%+v, %v), want nil", gotHalt, err)
	}
	gotSetTss, err := store.LoadPendingEvmTx(sender, contract, evmTxKindSetTss)
	if err != nil || gotSetTss == nil || gotSetTss.Nonce != 5 || gotSetTss.TxHash != signedSetTss5.Hash() {
		t.Fatalf("after ClearObsoleteHaltTx + setTss5 success, durable setTss = (%+v, %v), want setTss(5) %s", gotSetTss, err, signedSetTss5.Hash())
	}
}

func TestClearObsoleteHaltRestoresLiveRotationUnderAmbiguousHalt(t *testing.T) {
	contract := ecommon.HexToAddress("0x8282828282828282828282828282828282828282")
	newTss := ecommon.HexToAddress("0x8383838383838383838383838383838383838383")
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	signer := etypes.LatestSignerForChainID(big.NewInt(1))
	store := newMemPendingTxStore()

	var setTss5Hash, halt5Hash ecommon.Hash
	setTss5Entered := make(chan struct{})
	releaseSetTss5 := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSetTss5) }) }
	var confirmedNonce, pendingNonce atomic.Uint64
	confirmedNonce.Store(5)
	pendingNonce.Store(5)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_blockNumber":
			resp["result"] = hexutil.EncodeUint64(100)
		case "eth_getBalance":
			resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
		case "eth_getTransactionCount":
			var tag string
			if len(req.Params) >= 2 {
				_ = json.Unmarshal(req.Params[1], &tag)
			}
			if tag == "pending" {
				resp["result"] = hexutil.EncodeUint64(pendingNonce.Load())
			} else {
				resp["result"] = hexutil.EncodeUint64(confirmedNonce.Load())
			}
		case "eth_getBlockByNumber":
			resp["result"] = map[string]interface{}{
				"number":        hexutil.EncodeUint64(100),
				"hash":          ecommon.HexToHash("0x11"),
				"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
			}
		case "eth_maxPriorityFeePerGas":
			resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
		case "eth_estimateGas":
			resp["result"] = hexutil.EncodeUint64(100_000)
		case "eth_getTransactionByHash":
			resp["result"] = nil
		case "eth_sendRawTransaction":
			var raw hexutil.Bytes
			_ = json.Unmarshal(req.Params[0], &raw)
			var tx etypes.Transaction
			_ = tx.UnmarshalBinary(raw)
			switch tx.Hash() {
			case setTss5Hash:
				close(setTss5Entered)
				select {
				case <-releaseSetTss5:
				case <-r.Context().Done():
					return
				}
				pendingNonce.Store(6)
				resp["result"] = tx.Hash().Hex()
			case halt5Hash:
				// Ambiguous failure (no failedRollback marker is created).
				resp["error"] = map[string]interface{}{"code": -32000, "message": "replacement transaction underpriced"}
			default:
				resp["result"] = tx.Hash().Hex()
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(func() {
		release()
		srv.Close()
	})

	client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "clear-obsolete-ambiguous-halt", contract)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	client.SetChainID(1)
	client.SetPendingTxStore(store)

	// 1. Start setTss(5) paused in-flight.
	setTss5, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedSetTss5, err := etypes.SignTx(setTss5, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	setTss5Hash = signedSetTss5.Hash()
	setTss5Done := make(chan error, 1)
	go func() { setTss5Done <- client.SendTransaction(signedSetTss5) }()
	select {
	case <-setTss5Entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for setTss5 send to reach server")
	}

	// 2. Superseding halt(5) returns ambiguous "replacement transaction underpriced".
	halt5, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signedHalt5, err := etypes.SignTx(halt5, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	halt5Hash = signedHalt5.Hash()
	if err := client.SendTransaction(signedHalt5); err == nil {
		t.Fatal("expected replacement transaction underpriced error")
	}

	// 3. ClearObsoleteHaltTx sees no pending halt and clears halt(5):
	// MUST atomically restore surviving setTss(5) into durable store even
	// without any failedRollbacks marker!
	if err := client.ClearObsoleteHaltTx(sender); err != nil {
		t.Fatalf("ClearObsoleteHaltTx error: %v", err)
	}

	// 4. setTss(5) succeeds; durable rotation record MUST be present!
	release()
	if err := <-setTss5Done; err != nil {
		t.Fatalf("expected setTss5 success, got %v", err)
	}
	if gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err != nil || gotHalt != nil {
		t.Fatalf("durable halt = (%+v, %v), want nil", gotHalt, err)
	}
	gotSetTss, err := store.LoadPendingEvmTx(sender, contract, evmTxKindSetTss)
	if err != nil || gotSetTss == nil || gotSetTss.Nonce != 5 || gotSetTss.TxHash != signedSetTss5.Hash() {
		t.Fatalf("after ClearObsoleteHaltTx under ambiguous halt + setTss5 success, durable setTss = (%+v, %v), want setTss(5) %s", gotSetTss, err, signedSetTss5.Hash())
	}
}

func TestClearObsoleteHaltAtomicAcrossNoncesPrunesConsumedAndPropagatesErrors(t *testing.T) {
	contract := ecommon.HexToAddress("0x8484848484848484848484848484848484848484")
	newTss := ecommon.HexToAddress("0x8585858585858585858585858585858585858585")

	t.Run("earlier nonce rotation restore and halt6 delete commit in one atomic batch with error propagation", func(t *testing.T) {
		key, err := crypto.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		sender := crypto.PubkeyToAddress(key.PublicKey)
		signer := etypes.LatestSignerForChainID(big.NewInt(1))
		store := &failingPendingTxStore{memPendingTxStore: newMemPendingTxStore()}

		var setTss5Hash, halt5Hash, halt6Hash ecommon.Hash
		setTss5Entered := make(chan struct{})
		releaseSetTss5 := make(chan struct{})
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(releaseSetTss5) }) }
		var confirmedNonce, pendingNonce atomic.Uint64
		confirmedNonce.Store(5)
		pendingNonce.Store(5)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     json.RawMessage   `json:"id"`
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
			switch req.Method {
			case "eth_chainId":
				resp["result"] = hexutil.EncodeUint64(1)
			case "eth_blockNumber":
				resp["result"] = hexutil.EncodeUint64(100)
			case "eth_getBalance":
				resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
			case "eth_getTransactionCount":
				var tag string
				if len(req.Params) >= 2 {
					_ = json.Unmarshal(req.Params[1], &tag)
				}
				if tag == "pending" {
					resp["result"] = hexutil.EncodeUint64(pendingNonce.Load())
				} else {
					resp["result"] = hexutil.EncodeUint64(confirmedNonce.Load())
				}
			case "eth_getBlockByNumber":
				resp["result"] = map[string]interface{}{
					"number":        hexutil.EncodeUint64(100),
					"hash":          ecommon.HexToHash("0x11"),
					"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
				}
			case "eth_maxPriorityFeePerGas":
				resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
			case "eth_estimateGas":
				resp["result"] = hexutil.EncodeUint64(100_000)
			case "eth_getTransactionByHash":
				resp["result"] = nil
			case "eth_sendRawTransaction":
				var raw hexutil.Bytes
				_ = json.Unmarshal(req.Params[0], &raw)
				var tx etypes.Transaction
				_ = tx.UnmarshalBinary(raw)
				switch tx.Hash() {
				case setTss5Hash:
					close(setTss5Entered)
					select {
					case <-releaseSetTss5:
					case <-r.Context().Done():
						return
					}
					pendingNonce.Store(6)
					resp["result"] = tx.Hash().Hex()
				case halt5Hash, halt6Hash:
					resp["error"] = map[string]interface{}{"code": -32000, "message": "replacement transaction underpriced"}
				default:
					resp["result"] = tx.Hash().Hex()
				}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		}))
		t.Cleanup(func() {
			release()
			srv.Close()
		})

		client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "atomic-clear-earlier-nonce", contract)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Stop()
		client.SetChainID(1)
		client.SetPendingTxStore(store)

		// 1. Start setTss(5) in flight.
		setTss5, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		signedSetTss5, err := etypes.SignTx(setTss5, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		setTss5Hash = signedSetTss5.Hash()
		setTss5Done := make(chan error, 1)
		go func() { setTss5Done <- client.SendTransaction(signedSetTss5) }()
		select {
		case <-setTss5Entered:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for setTss5 send")
		}

		// 2. Ambiguous halt(5) supersedes setTss(5), then ambiguous halt(6) advances halt slot to 6.
		halt5, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		signedHalt5, err := etypes.SignTx(halt5, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		halt5Hash = signedHalt5.Hash()
		if err := client.SendTransaction(signedHalt5); err == nil {
			t.Fatal("expected ambiguous error for halt5")
		}
		halt6Raw := etypes.NewTx(&etypes.DynamicFeeTx{
			ChainID:   big.NewInt(1),
			Nonce:     6,
			GasTipCap: halt5.GasTipCap(),
			GasFeeCap: halt5.GasFeeCap(),
			Gas:       halt5.Gas(),
			To:        halt5.To(),
			Value:     big.NewInt(0),
			Data:      halt5.Data(),
		})
		signedHalt6, err := etypes.SignTx(halt6Raw, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		halt6Hash = signedHalt6.Hash()
		if err := client.SendTransaction(signedHalt6); err == nil {
			t.Fatal("expected ambiguous error for halt6")
		}

		// 3. First ClearObsoleteHaltTx fails the single atomic batch: MUST return error
		// AND keep halt(6) in durable store (not delete halt(6) before restoring setTss(5))!
		store.failSave = true
		if err := client.ClearObsoleteHaltTx(sender); err == nil {
			t.Fatal("expected ClearObsoleteHaltTx to propagate storage batch failure")
		}
		if gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err != nil || gotHalt == nil || gotHalt.Nonce != 6 {
			t.Fatalf("after failed cleanup batch, halt = (%+v, %v), want halt(6) preserved for retry", gotHalt, err)
		}

		// 4. Storage recovers; retry ClearObsoleteHaltTx atomically deletes halt(6) AND restores setTss(5)!
		store.failSave = false
		if err := client.ClearObsoleteHaltTx(sender); err != nil {
			t.Fatalf("retry ClearObsoleteHaltTx failed: %v", err)
		}
		release()
		if err := <-setTss5Done; err != nil {
			t.Fatalf("setTss5 failed: %v", err)
		}
		if gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err != nil || gotHalt != nil {
			t.Fatalf("durable halt after retry cleanup = (%+v, %v), want nil", gotHalt, err)
		}
		gotSetTss, err := store.LoadPendingEvmTx(sender, contract, evmTxKindSetTss)
		if err != nil || gotSetTss == nil || gotSetTss.Nonce != 5 || gotSetTss.TxHash != signedSetTss5.Hash() {
			t.Fatalf("durable setTss after retry cleanup = (%+v, %v), want setTss(5) %s", gotSetTss, err, signedSetTss5.Hash())
		}
	})

	t.Run("cleanup does not restore rotation at already consumed nonce below confirmedNonce", func(t *testing.T) {
		key, err := crypto.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		sender := crypto.PubkeyToAddress(key.PublicKey)
		signer := etypes.LatestSignerForChainID(big.NewInt(1))
		store := newMemPendingTxStore()

		var confirmedNonce, pendingNonce atomic.Uint64
		confirmedNonce.Store(5)
		pendingNonce.Store(5)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     json.RawMessage   `json:"id"`
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
			switch req.Method {
			case "eth_chainId":
				resp["result"] = hexutil.EncodeUint64(1)
			case "eth_blockNumber":
				resp["result"] = hexutil.EncodeUint64(100)
			case "eth_getBalance":
				resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
			case "eth_getTransactionCount":
				var tag string
				if len(req.Params) >= 2 {
					_ = json.Unmarshal(req.Params[1], &tag)
				}
				if tag == "pending" {
					resp["result"] = hexutil.EncodeUint64(pendingNonce.Load())
				} else {
					resp["result"] = hexutil.EncodeUint64(confirmedNonce.Load())
				}
			case "eth_getBlockByNumber":
				resp["result"] = map[string]interface{}{
					"number":        hexutil.EncodeUint64(100),
					"hash":          ecommon.HexToHash("0x11"),
					"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
				}
			case "eth_maxPriorityFeePerGas":
				resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
			case "eth_estimateGas":
				resp["result"] = hexutil.EncodeUint64(100_000)
			case "eth_getTransactionByHash":
				resp["result"] = nil
			case "eth_sendRawTransaction":
				var raw hexutil.Bytes
				_ = json.Unmarshal(req.Params[0], &raw)
				var tx etypes.Transaction
				_ = tx.UnmarshalBinary(raw)
				resp["result"] = tx.Hash().Hex()
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		}))
		t.Cleanup(srv.Close)

		client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "clear-prunes-consumed", contract)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Stop()
		client.SetChainID(1)
		client.SetPendingTxStore(store)

		// Send setTss(5), supersede with halt(5), send halt(6).
		setTss5, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		signedSetTss5, err := etypes.SignTx(setTss5, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.SendTransaction(signedSetTss5); err != nil {
			t.Fatal(err)
		}
		halt5, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		signedHalt5, err := etypes.SignTx(halt5, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.SendTransaction(signedHalt5); err != nil {
			t.Fatal(err)
		}
		halt6Raw := etypes.NewTx(&etypes.DynamicFeeTx{
			ChainID:   big.NewInt(1),
			Nonce:     6,
			GasTipCap: halt5.GasTipCap(),
			GasFeeCap: halt5.GasFeeCap(),
			Gas:       halt5.Gas(),
			To:        halt5.To(),
			Value:     big.NewInt(0),
			Data:      halt5.Data(),
		})
		signedHalt6, err := etypes.SignTx(halt6Raw, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.SendTransaction(signedHalt6); err != nil {
			t.Fatal(err)
		}

		// Nonce 5 is now confirmed (confirmed == pending == 6); clearing halt(6) MUST NOT restore consumed setTss(5)!
		confirmedNonce.Store(6)
		pendingNonce.Store(6)
		if err := client.ClearObsoleteHaltTx(sender); err != nil {
			t.Fatalf("ClearObsoleteHaltTx error: %v", err)
		}
		if gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err != nil || gotHalt != nil {
			t.Fatalf("durable halt = (%+v, %v), want nil", gotHalt, err)
		}
		if gotSetTss, err := store.LoadPendingEvmTx(sender, contract, evmTxKindSetTss); err != nil || gotSetTss != nil {
			t.Fatalf("durable setTss = (%+v, %v), want nil (must not restore consumed nonce 5)", gotSetTss, err)
		}
	})

	t.Run("cleanup defers while matching halt broadcast is in flight", func(t *testing.T) {
		key, err := crypto.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		sender := crypto.PubkeyToAddress(key.PublicKey)
		signer := etypes.LatestSignerForChainID(big.NewInt(1))
		store := newMemPendingTxStore()

		haltEntered := make(chan struct{})
		releaseHalt := make(chan struct{})
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(releaseHalt) }) }
		var confirmedNonce, pendingNonce atomic.Uint64
		confirmedNonce.Store(5)
		pendingNonce.Store(5)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     json.RawMessage   `json:"id"`
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
			switch req.Method {
			case "eth_chainId":
				resp["result"] = hexutil.EncodeUint64(1)
			case "eth_blockNumber":
				resp["result"] = hexutil.EncodeUint64(100)
			case "eth_getBalance":
				resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
			case "eth_getTransactionCount":
				var tag string
				if len(req.Params) >= 2 {
					_ = json.Unmarshal(req.Params[1], &tag)
				}
				if tag == "pending" {
					resp["result"] = hexutil.EncodeUint64(pendingNonce.Load())
				} else {
					resp["result"] = hexutil.EncodeUint64(confirmedNonce.Load())
				}
			case "eth_getBlockByNumber":
				resp["result"] = map[string]interface{}{
					"number":        hexutil.EncodeUint64(100),
					"hash":          ecommon.HexToHash("0x11"),
					"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
				}
			case "eth_maxPriorityFeePerGas":
				resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
			case "eth_estimateGas":
				resp["result"] = hexutil.EncodeUint64(100_000)
			case "eth_getTransactionByHash":
				resp["result"] = nil
			case "eth_sendRawTransaction":
				var raw hexutil.Bytes
				_ = json.Unmarshal(req.Params[0], &raw)
				var tx etypes.Transaction
				_ = tx.UnmarshalBinary(raw)
				close(haltEntered)
				select {
				case <-releaseHalt:
				case <-r.Context().Done():
					return
				}
				pendingNonce.Store(6)
				resp["result"] = tx.Hash().Hex()
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		}))
		t.Cleanup(func() {
			release()
			srv.Close()
		})

		client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "clear-defers-inflight-halt", contract)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Stop()
		client.SetChainID(1)
		client.SetPendingTxStore(store)

		halt5, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		signedHalt5, err := etypes.SignTx(halt5, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		haltDone := make(chan error, 1)
		go func() { haltDone <- client.SendTransaction(signedHalt5) }()
		select {
		case <-haltEntered:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for halt5 broadcast to reach server")
		}

		// Cleanup while broadcast is in flight MUST defer rather than erasing durable halt(5).
		if err := client.ClearObsoleteHaltTx(sender); err != nil {
			t.Fatalf("ClearObsoleteHaltTx error: %v", err)
		}
		release()
		if err := <-haltDone; err != nil {
			t.Fatalf("halt5 broadcast failed: %v", err)
		}
		gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt)
		if err != nil || gotHalt == nil || gotHalt.Nonce != 5 || gotHalt.TxHash != signedHalt5.Hash() {
			t.Fatalf("after cleanup during in-flight broadcast, durable halt = (%+v, %v), want halt(5) %s", gotHalt, err, signedHalt5.Hash())
		}
		rotTx, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		if rotTx.Nonce() != 6 {
			t.Fatalf("subsequent rotation nonce = %d, want 6 (must respect halt(5))", rotTx.Nonce())
		}
	})

	t.Run("failed rollback does not restore consumed rotation after confirmedNonce advances", func(t *testing.T) {
		key, err := crypto.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		sender := crypto.PubkeyToAddress(key.PublicKey)
		signer := etypes.LatestSignerForChainID(big.NewInt(1))
		store := &failingPendingTxStore{memPendingTxStore: newMemPendingTxStore()}

		var halt5Hash ecommon.Hash
		var confirmedNonce, pendingNonce atomic.Uint64
		confirmedNonce.Store(5)
		pendingNonce.Store(5)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     json.RawMessage   `json:"id"`
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
			switch req.Method {
			case "eth_chainId":
				resp["result"] = hexutil.EncodeUint64(1)
			case "eth_blockNumber":
				resp["result"] = hexutil.EncodeUint64(100)
			case "eth_getBalance":
				resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
			case "eth_getTransactionCount":
				var tag string
				if len(req.Params) >= 2 {
					_ = json.Unmarshal(req.Params[1], &tag)
				}
				if tag == "pending" {
					resp["result"] = hexutil.EncodeUint64(pendingNonce.Load())
				} else {
					resp["result"] = hexutil.EncodeUint64(confirmedNonce.Load())
				}
			case "eth_getBlockByNumber":
				resp["result"] = map[string]interface{}{
					"number":        hexutil.EncodeUint64(100),
					"hash":          ecommon.HexToHash("0x11"),
					"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
				}
			case "eth_maxPriorityFeePerGas":
				resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
			case "eth_estimateGas":
				resp["result"] = hexutil.EncodeUint64(100_000)
			case "eth_getTransactionByHash":
				resp["result"] = nil
			case "eth_sendRawTransaction":
				var raw hexutil.Bytes
				_ = json.Unmarshal(req.Params[0], &raw)
				var tx etypes.Transaction
				_ = tx.UnmarshalBinary(raw)
				if tx.Hash() == halt5Hash {
					resp["error"] = map[string]interface{}{"code": -32000, "message": "insufficient funds for gas * price + value"}
				} else {
					pendingNonce.Store(6)
					resp["result"] = tx.Hash().Hex()
				}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		}))
		t.Cleanup(srv.Close)

		client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "failed-rollback-prunes-consumed", contract)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Stop()
		client.SetChainID(1)
		client.SetPendingTxStore(store)

		// 1. Accept setTss(5) (batch call 1).
		setTss5, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		signedSetTss5, err := etypes.SignTx(setTss5, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.SendTransaction(signedSetTss5); err != nil {
			t.Fatal(err)
		}

		// 2. Halt(5) supersedes setTss(5) (batch call 2) and is rejected; rollback (batch call 3) fails transiently.
		halt5, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		signedHalt5, err := etypes.SignTx(halt5, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		halt5Hash = signedHalt5.Hash()
		store.failBatchOnCall = 3
		if err := client.SendTransaction(signedHalt5); err == nil {
			t.Fatal("expected halt5 rejection")
		}

		// 3. Confirmed nonce advances to 6 (nonce 5 is now consumed!).
		// ClearObsoleteHaltTx MUST prune nonce 5 BEFORE retrying failed rollbacks
		// so consumed setTss(5) is NOT restored and halt(5) is cleared!
		confirmedNonce.Store(6)
		pendingNonce.Store(6)
		if err := client.ClearObsoleteHaltTx(sender); err != nil {
			t.Fatalf("ClearObsoleteHaltTx error: %v", err)
		}
		if gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt); err != nil || gotHalt != nil {
			t.Fatalf("durable halt = (%+v, %v), want nil", gotHalt, err)
		}
		if gotSetTss, err := store.LoadPendingEvmTx(sender, contract, evmTxKindSetTss); err != nil || gotSetTss != nil {
			t.Fatalf("durable setTss = (%+v, %v), want nil (must not restore consumed nonce 5)", gotSetTss, err)
		}
	})

	t.Run("cleanup defers while superseded halt A remains in flight under timed-out replacement B", func(t *testing.T) {
		key, err := crypto.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		sender := crypto.PubkeyToAddress(key.PublicKey)
		signer := etypes.LatestSignerForChainID(big.NewInt(1))
		store := newMemPendingTxStore()

		var haltAHash, haltBHash ecommon.Hash
		haltAEntered := make(chan struct{})
		releaseHaltA := make(chan struct{})
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(releaseHaltA) }) }
		var confirmedNonce, pendingNonce atomic.Uint64
		confirmedNonce.Store(5)
		pendingNonce.Store(5)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     json.RawMessage   `json:"id"`
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
			switch req.Method {
			case "eth_chainId":
				resp["result"] = hexutil.EncodeUint64(1)
			case "eth_blockNumber":
				resp["result"] = hexutil.EncodeUint64(100)
			case "eth_getBalance":
				resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
			case "eth_getTransactionCount":
				var tag string
				if len(req.Params) >= 2 {
					_ = json.Unmarshal(req.Params[1], &tag)
				}
				if tag == "pending" {
					resp["result"] = hexutil.EncodeUint64(pendingNonce.Load())
				} else {
					resp["result"] = hexutil.EncodeUint64(confirmedNonce.Load())
				}
			case "eth_getBlockByNumber":
				resp["result"] = map[string]interface{}{
					"number":        hexutil.EncodeUint64(100),
					"hash":          ecommon.HexToHash("0x11"),
					"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
				}
			case "eth_maxPriorityFeePerGas":
				resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
			case "eth_estimateGas":
				resp["result"] = hexutil.EncodeUint64(100_000)
			case "eth_getTransactionByHash":
				resp["result"] = nil
			case "eth_sendRawTransaction":
				var raw hexutil.Bytes
				_ = json.Unmarshal(req.Params[0], &raw)
				var tx etypes.Transaction
				_ = tx.UnmarshalBinary(raw)
				switch tx.Hash() {
				case haltAHash:
					close(haltAEntered)
					select {
					case <-releaseHaltA:
					case <-r.Context().Done():
						return
					}
					pendingNonce.Store(6)
					resp["result"] = tx.Hash().Hex()
				case haltBHash:
					// Ambiguous timeout/failure for replacement B.
					resp["error"] = map[string]interface{}{"code": -32000, "message": "replacement transaction underpriced"}
				default:
					resp["result"] = tx.Hash().Hex()
				}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		}))
		t.Cleanup(func() {
			release()
			srv.Close()
		})

		client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "clear-defers-superseded-inflight", contract)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Stop()
		client.SetChainID(1)
		client.SetPendingTxStore(store)

		// 1. Start halt A at nonce 5 paused in flight.
		haltA, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		signedHaltA, err := etypes.SignTx(haltA, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		haltAHash = signedHaltA.Hash()
		haltADone := make(chan error, 1)
		go func() { haltADone <- client.SendTransaction(signedHaltA) }()
		select {
		case <-haltAEntered:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for halt A to reach server")
		}

		// 2. Replacement halt B at nonce 5 returns an ambiguous error while halt A is still in flight.
		haltB, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		signedHaltB, err := etypes.SignTx(haltB, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		haltBHash = signedHaltB.Hash()
		if err := client.SendTransaction(signedHaltB); err == nil {
			t.Fatal("expected ambiguous error for halt B")
		}

		// 3. Cleanup MUST defer while superseded halt A is still broadcasting!
		if err := client.ClearObsoleteHaltTx(sender); err != nil {
			t.Fatalf("ClearObsoleteHaltTx error: %v", err)
		}
		release()
		if err := <-haltADone; err != nil {
			t.Fatalf("halt A broadcast failed: %v", err)
		}
		gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt)
		if err != nil || gotHalt == nil || gotHalt.Nonce != 5 {
			t.Fatalf("after cleanup while superseded halt A was in flight, durable halt = (%+v, %v), want nonce 5 preserved", gotHalt, err)
		}
	})

	t.Run("same-hash rebroadcast during cleanup invalidates stale lookup snapshot", func(t *testing.T) {
		key, err := crypto.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		sender := crypto.PubkeyToAddress(key.PublicKey)
		signer := etypes.LatestSignerForChainID(big.NewInt(1))
		store := newMemPendingTxStore()

		lookupEntered := make(chan struct{})
		releaseLookup := make(chan struct{})
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(releaseLookup) }) }
		var sendCalls atomic.Int32
		var confirmedNonce, pendingNonce atomic.Uint64
		confirmedNonce.Store(5)
		pendingNonce.Store(5)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     json.RawMessage   `json:"id"`
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
			switch req.Method {
			case "eth_chainId":
				resp["result"] = hexutil.EncodeUint64(1)
			case "eth_blockNumber":
				resp["result"] = hexutil.EncodeUint64(100)
			case "eth_getBalance":
				resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
			case "eth_getTransactionCount":
				var tag string
				if len(req.Params) >= 2 {
					_ = json.Unmarshal(req.Params[1], &tag)
				}
				if tag == "pending" {
					resp["result"] = hexutil.EncodeUint64(pendingNonce.Load())
				} else {
					resp["result"] = hexutil.EncodeUint64(confirmedNonce.Load())
				}
			case "eth_getBlockByNumber":
				resp["result"] = map[string]interface{}{
					"number":        hexutil.EncodeUint64(100),
					"hash":          ecommon.HexToHash("0x11"),
					"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
				}
			case "eth_maxPriorityFeePerGas":
				resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
			case "eth_estimateGas":
				resp["result"] = hexutil.EncodeUint64(100_000)
			case "eth_getTransactionByHash":
				close(lookupEntered)
				select {
				case <-releaseLookup:
				case <-r.Context().Done():
					return
				}
				// Negative snapshot taken before same-hash rebroadcast.
				resp["result"] = nil
			case "eth_sendRawTransaction":
				var raw hexutil.Bytes
				_ = json.Unmarshal(req.Params[0], &raw)
				var tx etypes.Transaction
				_ = tx.UnmarshalBinary(raw)
				if sendCalls.Add(1) == 1 {
					// Initial broadcast returns an ambiguous error so halt stays tracked.
					resp["error"] = map[string]interface{}{"code": -32000, "message": "replacement transaction underpriced"}
				} else {
					// Identical same-hash rebroadcast succeeds!
					pendingNonce.Store(6)
					resp["result"] = tx.Hash().Hex()
				}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		}))
		t.Cleanup(func() {
			release()
			srv.Close()
		})

		client, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}}, "clear-invalidated-by-same-hash-rebroadcast", contract)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Stop()
		client.SetChainID(1)
		client.SetPendingTxStore(store)

		// 1. Initial broadcast fails ambiguously so signedHalt5 is persisted.
		halt5, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
		if err != nil {
			t.Fatal(err)
		}
		signedHalt5, err := etypes.SignTx(halt5, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.SendTransaction(signedHalt5); err == nil {
			t.Fatal("expected initial ambiguous error")
		}

		// 2. Start ClearObsoleteHaltTx and pause right inside eth_getTransactionByHash.
		clearDone := make(chan error, 1)
		go func() { clearDone <- client.ClearObsoleteHaltTx(sender) }()
		select {
		case <-lookupEntered:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for cleanup hash lookup")
		}

		// 3. Rebroadcast the IDENTICAL signedHalt5 and let it succeed completely
		// (so inFlight is already false again before cleanup resumes!).
		if err := client.SendTransaction(signedHalt5); err != nil {
			t.Fatalf("same-hash rebroadcast failed: %v", err)
		}

		// 4. Resume cleanup with its stale negative lookup: MUST see sendGen changed
		// and preserve the accepted durable halt(5) reservation!
		release()
		if err := <-clearDone; err != nil {
			t.Fatalf("ClearObsoleteHaltTx error: %v", err)
		}
		gotHalt, err := store.LoadPendingEvmTx(sender, contract, evmTxKindHalt)
		if err != nil || gotHalt == nil || gotHalt.Nonce != 5 || gotHalt.TxHash != signedHalt5.Hash() {
			t.Fatalf("stale cleanup erased accepted same-hash rebroadcast: durable halt = (%+v, %v), want halt(5) %s", gotHalt, err, signedHalt5.Hash())
		}
	})
}
