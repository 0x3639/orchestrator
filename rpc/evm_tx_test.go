package rpc

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
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
	mu   sync.Mutex
	rows map[string]*common.PendingEvmTx
}

func newMemPendingTxStore() *memPendingTxStore {
	return &memPendingTxStore{rows: make(map[string]*common.PendingEvmTx)}
}

func (s *memPendingTxStore) key(sender, target ecommon.Address, action string) string {
	return sender.Hex() + ":" + target.Hex() + ":" + action
}

func (s *memPendingTxStore) LoadPendingEvmTx(sender, target ecommon.Address, action string) (*common.PendingEvmTx, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.rows[s.key(sender, target, action)]
	if !ok || row == nil {
		return nil, nil
	}
	cp := *row
	if row.GasPrice != nil {
		cp.GasPrice = new(big.Int).Set(row.GasPrice)
	}
	if row.TipCap != nil {
		cp.TipCap = new(big.Int).Set(row.TipCap)
	}
	if row.FeeCap != nil {
		cp.FeeCap = new(big.Int).Set(row.FeeCap)
	}
	return &cp, nil
}

func (s *memPendingTxStore) SavePendingEvmTx(sender, target ecommon.Address, action string, tx *common.PendingEvmTx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	s.rows[s.key(sender, target, action)] = &cp
	return nil
}

func (s *memPendingTxStore) DeletePendingEvmTx(sender, target ecommon.Address, action string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.rows, s.key(sender, target, action))
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
			signer := etypes.LatestSignerForChainID(big.NewInt(1))

			// Pre-seed a distinct in-process pending action at nonce 5 so build uses pendingNonce 6.
			buildTx := func() (*etypes.Transaction, error) {
				if action == "halt" {
					return client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
				}
				return client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
			}
			// Occupy nonce 5 with the opposite action first.
			if action == "halt" {
				mock.pendingNonce = 5
				other, err := client.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
				if err != nil {
					t.Fatal(err)
				}
				signedOther, err := etypes.SignTx(other, signer, key)
				if err != nil {
					t.Fatal(err)
				}
				if err := client.SendTransaction(signedOther); err != nil {
					t.Fatal(err)
				}
			} else {
				mock.pendingNonce = 5
				other, err := client.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
				if err != nil {
					t.Fatal(err)
				}
				signedOther, err := etypes.SignTx(other, signer, key)
				if err != nil {
					t.Fatal(err)
				}
				if err := client.SendTransaction(signedOther); err != nil {
					t.Fatal(err)
				}
			}

			// Now pendingNonce == 6; build and send target action at nonce 6.
			tx1, err := buildTx()
			if err != nil {
				t.Fatal(err)
			}
			if tx1.Nonce() != 6 {
				t.Fatalf("tx1 nonce = %d, want 6", tx1.Nonce())
			}
			signed1, err := etypes.SignTx(tx1, signer, key)
			if err != nil {
				t.Fatal(err)
			}
			if err := client.SendTransaction(signed1); err != nil {
				t.Fatal(err)
			}

			// Simulate predecessor at nonce 5 being evicted from mempool: pendingNonce drops to 5
			// leaving nonce 6 stranded behind a nonce gap.
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
