package common

import (
	"fmt"
	"strings"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"
)

func FuzzParseRedeemLogID(f *testing.F) {
	f.Add("1:0x" + strings.Repeat("ab", 32) + ":0")
	f.Add("56:" + strings.Repeat("cd", 32) + ":42")
	f.Add("")
	f.Add("1:0x1234:0")
	f.Add("-1:0x" + strings.Repeat("00", 32) + ":0")
	f.Add("4294967296:0x" + strings.Repeat("00", 32) + ":0")
	f.Add("1:0x" + strings.Repeat("00", 32) + ":4294967296")
	f.Add("1:0x" + strings.Repeat("00", 32) + ":0:extra")

	f.Fuzz(func(t *testing.T, input string) {
		id, err := ParseRedeemLogID(input)
		if err != nil {
			return
		}
		// Any accepted input must cleanly round-trip via canonical format.
		canonical := fmt.Sprintf("%d:%s:%d", id.ChainID, id.TxHash.Hex(), id.Index)
		id2, err := ParseRedeemLogID(canonical)
		if err != nil {
			t.Fatalf("canonical %q failed to re-parse: %v", canonical, err)
		}
		if id != id2 {
			t.Fatalf("round-trip mismatch: %+v != %+v", id, id2)
		}
	})
}

func FuzzSecretRedactor(f *testing.F) {
	f.Add("super-secret-api-key", "https://eth.example/v3/super-secret-api-key", "failed dialing https://eth.example/v3/super-secret-api-key: 429")
	f.Add("key", "key-extended", "error using key-extended and key together")
	f.Add("", "abc", "abc")

	f.Fuzz(func(t *testing.T, secret1, secret2, message string) {
		var r SecretRedactor
		r.Register(secret1, RedactedPlaceholder)
		r.Register(secret2, RedactedPlaceholder)
		out := r.Redact(message)
		if secret1 != "" && !strings.Contains(RedactedPlaceholder, secret1) && strings.Contains(out, secret1) {
			t.Fatalf("secret1 %q leaked in %q", secret1, out)
		}
		if secret2 != "" && !strings.Contains(RedactedPlaceholder, secret2) && strings.Contains(out, secret2) {
			t.Fatalf("secret2 %q leaked in %q", secret2, out)
		}
	})
}

type fuzzIncidentStore struct {
	pending map[RedeemLogID]RedeemIncident
	handled map[RedeemLogID]bool
}

func newFuzzIncidentStore() *fuzzIncidentStore {
	return &fuzzIncidentStore{
		pending: make(map[RedeemLogID]RedeemIncident),
		handled: make(map[RedeemLogID]bool),
	}
}

func (s *fuzzIncidentStore) RecordRedeemIncident(ev RedeemIncident) (bool, error) {
	id := redeemLogID(ev)
	if s.handled[id] {
		return false, nil
	}
	s.pending[id] = ev
	return true, nil
}
func (s *fuzzIncidentStore) HandledRedeemLog(chain uint32, log etypes.Log) (bool, error) {
	return s.handled[RedeemLogID{ChainID: chain, TxHash: log.TxHash, Index: log.Index}], nil
}
func (s *fuzzIncidentStore) HasPendingRedeemIncident() (bool, error) { return len(s.pending) > 0, nil }
func (s *fuzzIncidentStore) PendingRedeemIncidents() ([]RedeemIncident, error) {
	out := make([]RedeemIncident, 0, len(s.pending))
	for _, ev := range s.pending {
		out = append(out, ev)
	}
	return out, nil
}
func (s *fuzzIncidentStore) ResolveRedeemIncident(ev RedeemIncident, _ RedeemResolution) error {
	id := redeemLogID(ev)
	delete(s.pending, id)
	s.handled[id] = true
	return nil
}
func (s *fuzzIncidentStore) ConfirmRedeemHalt(map[uint32]bool) error {
	for id := range s.pending {
		s.handled[id] = true
		delete(s.pending, id)
	}
	return nil
}

// FuzzGlobalStateMachine drives arbitrary sequences of state transitions,
// emergency incidents, halts, resign requests, and restart latches and asserts
// that core safety invariants NEVER break at any step.
func FuzzGlobalStateMachine(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
	f.Add([]byte{3, 0, 1, 4, 9, 0, 2, 0})
	f.Add([]byte{7, 0, 5, 8, 0, 10, 0})

	f.Fuzz(func(t *testing.T, ops []byte) {
		raw := LiveState
		gs := NewGlobalState(&raw)
		store := newFuzzIncidentStore()
		if err := gs.RestoreRedeemIncidents(store); err != nil {
			t.Fatal(err)
		}

		restartLatched := false
		for step, op := range ops {
			prevState, err := gs.GetState()
			if err != nil {
				t.Fatalf("step %d: GetState: %v", step, err)
			}

			switch op % 12 {
			case 0:
				_ = gs.SetState(LiveState)
			case 1:
				_ = gs.SetState(KeyGenState)
			case 2:
				_ = gs.SetState(HaltedState)
			case 3:
				_ = gs.SetState(EmergencyState)
			case 4:
				_ = gs.SetState(ReSignState)
			case 5:
				_, _ = gs.HaltSigning()
			case 6:
				_, _ = gs.ActivateResign(2, 1)
			case 7:
				_ = gs.ReportRedeemIncident(RedeemIncident{
					ChainID: 1,
					Log:     etypes.Log{TxHash: ecommon.BigToHash(ecommon.Big1), Index: uint(step + 1)},
				})
			case 8:
				if gen := gs.RedeemHaltGeneration(); gen != 0 {
					_, _ = gs.CompleteEmergencyHalt(gen, map[uint32]bool{1: true})
				}
			case 9:
				_ = gs.RequireRestart()
				restartLatched = true
			case 10:
				_, _ = gs.DeactivateResign()
			case 11:
				_, _ = gs.SetStateIf(prevState, LiveState)
			}

			curState, err := gs.GetState()
			if err != nil {
				t.Fatalf("step %d: GetState after op %d: %v", step, op%12, err)
			}

			// Invariant 1: Emergency NEVER jumps directly to Live/KeyGen/ReSign.
			if prevState == EmergencyState && (curState == LiveState || curState == KeyGenState || curState == ReSignState) {
				t.Fatalf("step %d: illegal transition Emergency -> %s", step, StateToText(curState))
			}

			// Invariant 2: Active redeem halt pins effective state to Emergency and blocks transfer signing.
			if gs.RedeemHaltGeneration() != 0 {
				if curState != EmergencyState {
					t.Fatalf("step %d: redeem halt pending but GetState=%s", step, StateToText(curState))
				}
				if gs.WhileSigningTransfers(func() {}) {
					t.Fatalf("step %d: WhileSigningTransfers ran during pending redeem halt", step)
				}
			}

			// Invariant 3: RestartRequired latch permanently blocks Live/KeyGen/ReSign and transfer signing.
			if restartLatched {
				if !gs.RestartRequired() {
					t.Fatalf("step %d: RestartRequired cleared after latch", step)
				}
				if curState != HaltedState && curState != EmergencyState {
					t.Fatalf("step %d: non-protected state %s after RequireRestart", step, StateToText(curState))
				}
				if gs.WhileSigningTransfers(func() {}) {
					t.Fatalf("step %d: WhileSigningTransfers ran after RequireRestart", step)
				}
				if ran, _ := gs.WhileLive(func() error { return nil }); ran {
					t.Fatalf("step %d: WhileLive ran after RequireRestart", step)
				}
			}

			// Invariant 4: Halted or Emergency NEVER allows transfer signing or WhileLive.
			if curState == HaltedState || curState == EmergencyState {
				if gs.WhileSigningTransfers(func() {}) {
					t.Fatalf("step %d: WhileSigningTransfers ran in %s", step, StateToText(curState))
				}
				if ran, _ := gs.WhileLive(func() error { return nil }); ran {
					t.Fatalf("step %d: WhileLive ran in %s", step, StateToText(curState))
				}
			}

			// Invariant 5: WhileEmergency runs ONLY in EmergencyState.
			emRan, _ := gs.WhileEmergency(func() error { return nil })
			if emRan != (curState == EmergencyState) {
				t.Fatalf("step %d: WhileEmergency=%v but state=%s", step, emRan, StateToText(curState))
			}
		}
	})
}
