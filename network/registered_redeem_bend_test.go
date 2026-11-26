//go:build bend

package network

import (
	"fmt"
	"math/big"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"orchestrator/common"
	"orchestrator/verification"
)

func TestRegisteredRedeemDecisionAgreesWithBend(t *testing.T) {
	var cases verification.Comparisons

	type redeemCase struct {
		name          string
		matched       bool
		live          bool
		acknowledged  bool
		evidenceOK    bool
		settledLatest bool
		settledFinal  bool
	}

	testCases := []redeemCase{
		{"matched_live", true, true, false, true, false, false},
		{"matched_live_even_if_evidence_down", true, true, false, false, false, false},
		{"matched_historical_evidence_ok", true, false, false, true, false, false},
		{"matched_historical_evidence_disagreed", true, false, false, false, false, false},
		{"unmatched_live_evidence_disagreed", false, true, false, false, false, false},
		{"unmatched_historical_evidence_disagreed", false, false, false, false, false, false},
		{"unmatched_live_acknowledged", false, true, true, true, false, false},
		{"unmatched_historical_acknowledged", false, false, true, true, false, false},
		{"unmatched_live_active_fraud_halts", false, true, false, true, false, false},
		{"unmatched_live_settled_waits_finality", false, true, false, true, true, false},
		{"unmatched_historical_settled_archives", false, false, false, true, true, true},
		{"unmatched_historical_unsettled_halts", false, false, false, true, false, false},
	}

	for _, tc := range testCases {
		var src *redeemSource
		if tc.matched {
			src = &redeemSource{
				synced: true,
				wrap: &definition.WrapTokenRequest{
					NetworkClass: definition.EvmClass,
					ChainId:      1,
					Id:           types.Hash(ecommon.HexToHash("0x31")),
					ToAddress:    ecommon.HexToAddress("0x32").Hex(),
					TokenAddress: ecommon.HexToAddress("0x33").Hex(),
					Amount:       big.NewInt(25),
					Fee:          big.NewInt(6), // net = 19 matching log.Data
				},
			}
		} else {
			src = &redeemSource{err: constants.ErrDataNonExistent.Error(), synced: true}
		}

		net, log, secondary := redeemFixture(t, src)
		if tc.acknowledged {
			net.state.AcknowledgeRedeemLogs([]common.RedeemLogID{{
				ChainID: net.ChainId(),
				TxHash:  log.TxHash,
				Index:   log.Index,
			}})
		}
		if !tc.evidenceOK {
			// Byzantine secondary endpoint returns a different block hash at the event height.
			secondary.replaced.Store(log.BlockNumber, ecommon.HexToHash("0xbadc0ffe"))
		}
		if tc.settledFinal {
			// Closed and unhalted before confirmation-deep height (head=140, conf=10 -> check at 130).
			secondary.contract.closedAt.Store(105)
			secondary.contract.unhaltedAt.Store(110)
		} else if tc.settledLatest {
			// Closed and unhalted only at latest state (block 138 > 130).
			secondary.contract.closedAt.Store(135)
			secondary.contract.unhaltedAt.Store(138)
		}

		err := net.registerRedeem(log, tc.live)
		handled, handledErr := net.state.HandledRedeemLog(net.ChainId(), log)
		if handledErr != nil {
			t.Fatalf("%s: HandledRedeemLog: %v", tc.name, handledErr)
		}
		state, stateErr := net.state.GetState()
		if stateErr != nil {
			t.Fatalf("%s: GetState: %v", tc.name, stateErr)
		}

		var observed string
		switch {
		case err != nil:
			observed = "B.RedeemRetry{}"
		case state == common.EmergencyState || net.state.RedeemHaltGeneration() != 0:
			observed = "B.RedeemHalt{}"
		case handled:
			observed = "B.RedeemArchive{}"
		default:
			observed = "B.RedeemRecordMatch{}"
		}

		expr := fmt.Sprintf("B.decide_redeem(%s, %s, %s, %s, %s, %s)",
			verification.Bool(tc.matched),
			verification.Bool(tc.live),
			verification.Bool(tc.acknowledged),
			verification.Bool(tc.evidenceOK),
			verification.Bool(tc.settledLatest),
			verification.Bool(tc.settledFinal),
		)
		cases.Equal(expr, observed, "B.RedeemAction")
	}

	cases.Check(t)
}
