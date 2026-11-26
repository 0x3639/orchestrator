//go:build bend

package common

import (
	"fmt"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"

	"orchestrator/verification"
)

type bendIncidentStore struct {
	pending map[RedeemLogID]RedeemIncident
}

func newBendIncidentStore() *bendIncidentStore {
	return &bendIncidentStore{pending: make(map[RedeemLogID]RedeemIncident)}
}

func (s *bendIncidentStore) RecordRedeemIncident(ev RedeemIncident) (bool, error) {
	s.pending[redeemLogID(ev)] = ev
	return true, nil
}

func (s *bendIncidentStore) HandledRedeemLog(uint32, etypes.Log) (bool, error) { return false, nil }
func (s *bendIncidentStore) HasPendingRedeemIncident() (bool, error)          { return len(s.pending) > 0, nil }
func (s *bendIncidentStore) PendingRedeemIncidents() ([]RedeemIncident, error) {
	out := make([]RedeemIncident, 0, len(s.pending))
	for _, ev := range s.pending {
		out = append(out, ev)
	}
	return out, nil
}
func (s *bendIncidentStore) ResolveRedeemIncident(ev RedeemIncident, _ RedeemResolution) error {
	delete(s.pending, redeemLogID(ev))
	return nil
}
func (s *bendIncidentStore) ConfirmRedeemHalt(map[uint32]bool) error {
	clear(s.pending)
	return nil
}

func bendBridgeState(state uint8) string {
	switch state {
	case LiveState:
		return "B.StateLive{}"
	case KeyGenState:
		return "B.StateKeyGen{}"
	case HaltedState:
		return "B.StateHalted{}"
	case EmergencyState:
		return "B.StateEmergency{}"
	case ReSignState:
		return "B.StateReSign{}"
	default:
		panic(fmt.Sprintf("unknown bridge state %d", state))
	}
}

func TestGlobalStateTransitionsAndGuardsAgreeWithBend(t *testing.T) {
	states := []uint8{LiveState, KeyGenState, HaltedState, EmergencyState, ReSignState}
	var cases verification.Comparisons

	// 1. Exhaustively check WhileSigningTransfers and HaltSigning across all
	//    (state, redeemHaltPending, restartRequired) combinations.
	for _, initial := range states {
		for _, redeemHalt := range []bool{false, true} {
			for _, restartReq := range []bool{false, true} {
				raw := initial
				gs := NewGlobalState(&raw)
				if redeemHalt {
					store := newBendIncidentStore()
					if err := gs.RestoreRedeemIncidents(store); err != nil {
						t.Fatal(err)
					}
					if err := gs.ReportRedeemIncident(RedeemIncident{
						ChainID: 1,
						Log:     etypes.Log{TxHash: ecommon.HexToHash("0xdead"), Index: 1},
					}); err != nil {
						t.Fatal(err)
					}
				}
				if restartReq {
					if err := gs.RequireRestart(); err != nil {
						t.Fatal(err)
					}
				}
				ran := gs.WhileSigningTransfers(func() {})
				expr := fmt.Sprintf("B.can_sign_transfers(%s, %s, %s)",
					bendBridgeState(initial), verification.Bool(redeemHalt), verification.Bool(restartReq))
				cases.Equal(expr, verification.Bool(ran), "Bool")
			}
		}

		// HaltSigning from each state (without latches).
		raw := initial
		gs := NewGlobalState(&raw)
		if _, err := gs.HaltSigning(); err != nil {
			t.Fatal(err)
		}
		got, err := gs.GetState()
		if err != nil {
			t.Fatal(err)
		}
		cases.Equal(fmt.Sprintf("B.halt_signing_target(%s)", bendBridgeState(initial)), bendBridgeState(got), "B.BridgeState")
	}

	// 2. Exhaustively check SetState(requested) across all (initial, requested,
	//    redeemHaltPending, restartRequired, pendingResign) combinations.
	for _, initial := range states {
		for _, requested := range states {
			for _, redeemHalt := range []bool{false, true} {
				for _, restartReq := range []bool{false, true} {
					for _, pendingResign := range []bool{false, true} {
						raw := initial
						gs := NewGlobalState(&raw)
						if pendingResign && (initial == KeyGenState || initial == HaltedState || initial == EmergencyState) {
							if _, err := gs.ActivateResign(2, 1); err != nil {
								t.Fatal(err)
							}
						}
						if redeemHalt {
							store := newBendIncidentStore()
							if err := gs.RestoreRedeemIncidents(store); err != nil {
								t.Fatal(err)
							}
							if err := gs.ReportRedeemIncident(RedeemIncident{
								ChainID: 1,
								Log:     etypes.Log{TxHash: ecommon.HexToHash("0xbeef"), Index: 2},
							}); err != nil {
								t.Fatal(err)
							}
						}
						if restartReq {
							// Latch restartRequired without altering the pre-latch state under test
							// when testing direct transition_allowed from initial.
							gs.restartRequired.Store(true)
						}

						err := gs.SetState(requested)
						got, getErr := gs.GetState()
						if getErr != nil {
							t.Fatal(getErr)
						}

						// SetState returns nil on a blocked redeemHalt transition (no-op) and
						// returns an error when blocked by EmergencyState or restartRequired.
						allowed := err == nil && (!redeemHalt || requested == EmergencyState)
						cases.Equal(
							fmt.Sprintf("B.transition_allowed(%s, %s, %s, %s)",
								bendBridgeState(initial), bendBridgeState(requested),
								verification.Bool(redeemHalt), verification.Bool(restartReq)),
							verification.Bool(allowed),
							"Bool",
						)

						// When pendingResign is tested on states that accept ActivateResign without
						// immediately moving to ReSignState, compare resulting state against next_state.
						if !pendingResign || initial == KeyGenState || initial == HaltedState || initial == EmergencyState {
							cases.Equal(
								fmt.Sprintf("B.next_state(%s, %s, %s, %s, %s)",
									bendBridgeState(initial), bendBridgeState(requested),
									verification.Bool(redeemHalt), verification.Bool(restartReq),
									verification.Bool(pendingResign)),
								bendBridgeState(got),
								"B.BridgeState",
							)
						}
					}
				}
			}
		}
	}

	cases.Check(t)
}
