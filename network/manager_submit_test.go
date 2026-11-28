package network

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/zenon-network/go-zenon/chain/nom"

	"orchestrator/common/config"
	"orchestrator/rpc"
)

func assertSubmitOrder(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}

// A publication that does not answer before the submit bound is cancelled:
// the send observes the cancellation (so a context-aware publish RPC aborts
// instead of outliving the state it was submitted under) and returns before
// submitContext does, and the caller gets the deadline error.
func TestSubmitContextAbortsADelayedPublication(t *testing.T) {
	m := &NetworksManager{submitTimeout: 100 * time.Millisecond}

	var order []string
	start := time.Now()
	sent, err := m.submitContext(context.Background(), nil, func(pubCtx context.Context) error {
		order = append(order, "send-start")
		<-pubCtx.Done() // a delayed publication that only the deadline ends
		order = append(order, "send-end")
		return pubCtx.Err()
	})
	elapsed := time.Since(start)

	if !sent {
		t.Fatal("submitContext reported the submission as not run")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("submitContext err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed < 100*time.Millisecond {
		t.Fatalf("submitContext returned after %v, before the 100ms bound", elapsed)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("submitContext returned only after %v, long past the 100ms bound", elapsed)
	}
	// Reading order without synchronization is safe precisely because the
	// send ran to completion inside submitContext: nothing is left running.
	assertSubmitOrder(t, order, []string{"send-start", "send-end"})
}

// A publication that answers before the bound returns its own result, and the
// gate (while) is held until the publication has finished.
func TestSubmitContextHoldsTheGateUntilSendReturns(t *testing.T) {
	m := &NetworksManager{submitTimeout: 30 * time.Second}

	var order []string
	while := func(send func() error) (bool, error) {
		order = append(order, "gate-enter")
		err := send()
		order = append(order, "gate-exit")
		return true, err
	}
	sent, err := m.submitContext(context.Background(), while, func(pubCtx context.Context) error {
		order = append(order, "send-start")
		if _, ok := pubCtx.Deadline(); !ok {
			t.Error("the publication context has no deadline")
		}
		order = append(order, "send-end")
		return nil
	})
	if err != nil || !sent {
		t.Fatalf("submitContext = (%v, %v), want (true, nil)", sent, err)
	}
	assertSubmitOrder(t, order, []string{"gate-enter", "send-start", "send-end", "gate-exit"})
}

// On the bound the same holds: the gate is released only after the send has
// returned, so the state it guards cannot change under a publication that is
// still in flight.
func TestSubmitContextHoldsTheGateUntilSendReturnsOnTimeout(t *testing.T) {
	m := &NetworksManager{submitTimeout: 100 * time.Millisecond}

	var order []string
	while := func(send func() error) (bool, error) {
		order = append(order, "gate-enter")
		err := send()
		order = append(order, "gate-exit")
		return true, err
	}
	sent, err := m.submitContext(context.Background(), while, func(pubCtx context.Context) error {
		order = append(order, "send-start")
		<-pubCtx.Done() // a delayed publication that only the deadline ends
		order = append(order, "send-end")
		return pubCtx.Err()
	})
	if !sent {
		t.Fatal("submitContext reported the submission as not run")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("submitContext err = %v, want context.DeadlineExceeded", err)
	}
	assertSubmitOrder(t, order, []string{"gate-enter", "send-start", "send-end", "gate-exit"})
}

// A publication failure before the bound reaches the caller unchanged.
func TestSubmitContextPropagatesSendError(t *testing.T) {
	m := &NetworksManager{submitTimeout: 30 * time.Second}

	publicationErr := errors.New("the node rejected the transaction")
	sent, err := m.submitContext(context.Background(), nil, func(pubCtx context.Context) error {
		return publicationErr
	})
	if !sent {
		t.Fatal("submitContext reported the submission as not sent")
	}
	if !errors.Is(err, publicationErr) {
		t.Fatalf("submitContext err = %v, want %v", err, publicationErr)
	}
}

// The transfer publication routes through the context-aware settings
// connection and runs inside the gate: when the submit bound passes, the
// in-flight ledger.publishRawTransaction RPC is aborted instead of being
// abandoned, the caller gets the deadline error, and the gate is released
// only after the send has returned. The httptest node speaks HTTP, where the
// abort cancels the request on the wire, as publishCanceled observes;
// production Zenon defaults to WebSocket, where the client deregisters the
// waiter on the deadline without cancelling the server-side call, so the
// publication may yet land — the deadline error means "may still land", not
// a clean failure.
func TestPublishTransferAbortsADelayedPublicationOnTheWire(t *testing.T) {
	publishEntered := make(chan struct{})
	publishCanceled := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Method == "ledger.publishRawTransaction" {
			close(publishEntered)
			<-r.Context().Done()
			close(publishCanceled)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "result": nil})
	}))
	t.Cleanup(srv.Close)

	mgr, err := rpc.NewRpcManager(config.BaseNetworkConfig{Urls: []string{srv.URL}}, make(chan os.Signal, 1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mgr.Znn().Stop() })
	m := &NetworksManager{znnNetwork: &znnNetwork{rpcManager: mgr}, submitTimeout: 100 * time.Millisecond}

	var order []string
	gate := func(send func() error) (bool, error) {
		order = append(order, "gate-enter")
		err := send()
		order = append(order, "gate-exit")
		return true, err
	}

	start := time.Now()
	err = m.publishTransfer(&nom.AccountBlock{}, gate)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("publishTransfer err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed < 100*time.Millisecond {
		t.Fatalf("publishTransfer returned after %v, before the 100ms bound", elapsed)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("publishTransfer returned only after %v, long past the 100ms bound", elapsed)
	}
	// The publication ran inside the gate, and the gate was released only
	// after the send had returned.
	assertSubmitOrder(t, order, []string{"gate-enter", "gate-exit"})
	select {
	case <-publishEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("publishTransfer did not reach ledger.publishRawTransaction")
	}
	select {
	case <-publishCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight ledger.publishRawTransaction RPC was left running after the submit bound")
	}
}

// The administrator's halt against a chain whose eth_sendRawTransaction hangs
// is aborted at the submit bound: the caller gets the deadline error, the
// gate is held until the send has returned, and the EVM client stays open —
// reads still answer afterwards.
func TestSendHaltEvmAdministratorAbortsAHungSendOnTheWire(t *testing.T) {
	h := newHeldStart(t)
	h.evm.open()
	h.m.submitTimeout = 100 * time.Millisecond

	hold := make(chan struct{})
	h.evm.sendHold.Store(&hold)
	t.Cleanup(func() { close(hold) })

	batch, _, err := h.m.GetHaltMessages()
	if err != nil {
		t.Fatal(err)
	}
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)

	var order []string
	gate := func(send func() error) (bool, error) {
		order = append(order, "gate-enter")
		err := send()
		order = append(order, "gate-exit")
		return true, err
	}

	start := time.Now()
	halted, err := h.m.SendHaltEvmAdministrator(batch, 0, key, sender, gate)
	elapsed := time.Since(start)

	if halted {
		t.Fatal("a hung halt send reported the network as halted")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SendHaltEvmAdministrator err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed < 100*time.Millisecond {
		t.Fatalf("SendHaltEvmAdministrator returned after %v, before the 100ms bound", elapsed)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("SendHaltEvmAdministrator returned only after %v, long past the bound", elapsed)
	}
	// The send reached the wire and was aborted there, inside the gate.
	if attempts := h.evm.sendAttempts.Load(); attempts != 1 {
		t.Fatalf("%d eth_sendRawTransaction attempts, want 1", attempts)
	}
	assertSubmitOrder(t, order, []string{"gate-enter", "gate-exit"})
	// The client survives the aborted send: the chain still answers reads.
	halted, err = h.net.EvmRpc().HaltFlagSet()
	if err != nil || halted {
		t.Fatalf("halt flag read after the aborted send: halted %v, err %v", halted, err)
	}
}
