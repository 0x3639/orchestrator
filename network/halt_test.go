package network

import (
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"

	"orchestrator/common"
)

// A halt round goes through the networks one by one. One whose halt flag
// cannot be read must not keep the networks after it from getting their halt:
// an endpoint that is down during an emergency would otherwise leave every
// other bridge live for as long as it stays down.
func TestHaltGoesPastANetworkWhoseFlagCannotBeRead(t *testing.T) {
	h := newHeldStart(t)
	h.evm.open()
	h.evm.haltedFails.Store(true)
	second := newHeldEvm(t, h.bridge)
	second.open()
	h.addEvmNetwork(2, second.url)
	batch, messages, err := h.m.GetHaltMessages()
	if err != nil || len(messages) != 2 {
		t.Fatalf("%d messages, err %v", len(messages), err)
	}
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	noPanic(t, "sending the halt", func() {
		halted, err := h.m.SendHaltEvm(batch, [][]byte{{1}, {2}}, key, crypto.PubkeyToAddress(key.PublicKey), nil)
		if err != nil || halted {
			t.Fatalf("halted %v, err %v; want a halt sent past the unreadable network", halted, err)
		}
	})
	if sent := second.sent.Load(); sent != 1 {
		t.Fatalf("%d halt transactions sent to the readable network, want 1", sent)
	}
	if sent := h.evm.sent.Load(); sent != 0 {
		t.Fatalf("%d halt transactions sent to the network whose flag could not be read", sent)
	}
}

// The senders of a halt end at different times. The halt is recorded only
// when one read shows every flag set, on Zenon and on every EVM network the
// bridge lists then, so a bridge unhalted after its sender ended, or a network
// added after the EVM sender ended, keeps the signer in EmergencyState.
func TestHaltIsRecordedOnlyWhenEveryBridgeShowsItsFlag(t *testing.T) {
	h := newHeldStart(t)
	h.evm.open()
	h.setState(common.EmergencyState)
	state := func() uint8 {
		t.Helper()
		current, err := h.n.state.GetState()
		if err != nil {
			t.Fatal(err)
		}
		return current
	}
	record := func(want bool, wantErr error) {
		t.Helper()
		recorded, err := h.m.RecordConfirmedHalt(h.n.state)
		if recorded != want || (wantErr == nil) != (err == nil) || wantErr != nil && !errors.Is(err, wantErr) {
			t.Fatalf("recorded %v, err %v; want %v, %v", recorded, err, want, wantErr)
		}
		if wantState := map[bool]uint8{true: common.HaltedState, false: common.EmergencyState}[want]; state() != wantState {
			t.Fatalf("state %d, want %d", state(), wantState)
		}
	}

	// Zenon is live while the EVM bridge is halted.
	h.evm.halted.Store(true)
	record(false, nil)
	// Zenon is only inside the wait after an unhalt: its flag is clear.
	h.zenon.change(func(z *heldZenon) { z.unhaltWait = true })
	record(false, nil)
	// A failed read is never a halt.
	h.zenon.change(func(z *heldZenon) { z.unhaltWait, z.bridgeInfoFails = false, true })
	record(false, ErrHaltFlagsUnreadable)
	h.zenon.change(func(z *heldZenon) { z.bridgeInfoFails, z.halted = false, true })
	h.evm.haltedFails.Store(true)
	record(false, ErrHaltFlagsUnreadable)
	h.evm.haltedFails.Store(false)

	// A network added after the EVM sender ended is live.
	added := newHeldEvm(t, h.bridge)
	added.open()
	h.addEvmNetwork(2, added.url)
	record(false, nil)
	// The EVM bridge was unhalted after its sender ended.
	added.halted.Store(true)
	h.evm.halted.Store(false)
	record(false, nil)

	h.evm.halted.Store(true)
	record(true, nil)
}

// A halt that one network rejects does not end the round there: the next
// network still gets its halt, and the round is not reported as complete.
func TestHaltGoesPastANetworkThatRejectsIt(t *testing.T) {
	h := newHeldStart(t)
	h.evm.open()
	h.evm.sendFails.Store(true)
	second := newHeldEvm(t, h.bridge)
	second.open()
	h.addEvmNetwork(2, second.url)
	batch, messages, err := h.m.GetHaltMessages()
	if err != nil || len(messages) != 2 {
		t.Fatalf("%d messages, err %v", len(messages), err)
	}
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	halted, err := h.m.SendHaltEvm(batch, [][]byte{{1}, {2}}, key, crypto.PubkeyToAddress(key.PublicKey), nil)
	if err != nil || halted {
		t.Fatalf("halted %v, err %v; want a halt sent past the network that rejected it", halted, err)
	}
	if rejected := h.evm.rejected.Load(); rejected != 1 {
		t.Fatalf("%d halts rejected by the first network, want 1", rejected)
	}
	if sent := second.sent.Load(); sent != 1 {
		t.Fatalf("%d halt transactions sent to the second network, want 1", sent)
	}
}

// The bridge adds a network while the halt flags are read. The flags read
// all show a halt, but the new network is live, so nothing is recorded.
func TestHaltIsNotRecordedWhenTheNetworksChangeDuringTheRead(t *testing.T) {
	h := newHeldStart(t)
	h.evm.open()
	h.setState(common.EmergencyState)
	h.zenon.change(func(z *heldZenon) { z.halted = true })
	h.evm.halted.Store(true)
	added := newHeldEvm(t, h.bridge)
	added.open()
	addNetwork := func() { h.addEvmNetwork(2, added.url) }
	h.evm.onFlagRead.Store(&addNetwork)

	recorded, err := h.m.RecordConfirmedHalt(h.n.state)
	if err != nil || recorded {
		t.Fatalf("recorded %v, err %v; want nothing recorded", recorded, err)
	}
	if h.evm.onFlagRead.Load() != nil {
		t.Fatal("the network was not added during the read")
	}
	if state, err := h.n.state.GetState(); err != nil || state != common.EmergencyState {
		t.Fatalf("state %d, err %v; want EmergencyState", state, err)
	}
}

// Only an emergency becomes a halt. A signer whose emergency ended before
// the flags were read records nothing, even with every flag set.
func TestHaltIsRecordedOnlyInAnEmergency(t *testing.T) {
	h := newHeldStart(t)
	h.evm.open()
	h.zenon.change(func(z *heldZenon) { z.halted = true })
	h.evm.halted.Store(true)
	recorded, err := h.m.RecordConfirmedHalt(h.n.state)
	if err != nil || recorded {
		t.Fatalf("recorded %v, err %v; want nothing recorded", recorded, err)
	}
	if state, err := h.n.state.GetState(); err != nil || state != common.LiveState {
		t.Fatalf("state %d, err %v; want LiveState", state, err)
	}
}
