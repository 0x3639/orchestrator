package network

import (
	"errors"
	"math/big"
	"testing"
	"time"

	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"orchestrator/common"
	"orchestrator/common/events"
)

// TestAdversarialRegisteredRedeemFraudAndGriefing attacks registerRedeem with:
// 1. Subtle field-tampering fraud (amount/fee mismatch, recipient swap, token swap, wrong chainId) -> MUST HALT.
// 2. Single-endpoint Byzantine griefing (lying secondary EVM or failing Zenon RPC) -> MUST NOT FALSE-HALT.
func TestAdversarialRegisteredRedeemFraudAndGriefing(t *testing.T) {
	validWrap := func() *definition.WrapTokenRequest {
		return &definition.WrapTokenRequest{
			NetworkClass: definition.EvmClass,
			ChainId:      1,
			Id:           types.Hash(ecommon.HexToHash("0x31")),
			ToAddress:    ecommon.HexToAddress("0x32").Hex(),
			TokenAddress: ecommon.HexToAddress("0x33").Hex(),
			Amount:       big.NewInt(25),
			Fee:          big.NewInt(6), // net = 19 matching fixture log.Data
		}
	}

	fraudMutations := []struct {
		name   string
		mutate func(*definition.WrapTokenRequest)
	}{
		{"inflated_amount", func(w *definition.WrapTokenRequest) { w.Amount = big.NewInt(24) }}, // net 18 != 19
		{"recipient_swap", func(w *definition.WrapTokenRequest) { w.ToAddress = ecommon.HexToAddress("0xdead").Hex() }},
		{"token_swap", func(w *definition.WrapTokenRequest) { w.TokenAddress = ecommon.HexToAddress("0xbeef").Hex() }},
		{"wrong_chain_id", func(w *definition.WrapTokenRequest) { w.ChainId = 56 }},
		{"wrong_network_class", func(w *definition.WrapTokenRequest) { w.NetworkClass = 99 }},
	}

	for _, tc := range fraudMutations {
		t.Run("fraud_"+tc.name+"_triggers_emergency", func(t *testing.T) {
			w := validWrap()
			tc.mutate(w)
			net, log, _ := redeemFixture(t, &redeemSource{synced: true, wrap: w})
			if err := net.registerRedeem(log, true); err != nil {
				t.Fatalf("live fraud %s returned error instead of emergency halt: %v", tc.name, err)
			}
			state, err := net.state.GetState()
			if err != nil {
				t.Fatal(err)
			}
			if state != common.EmergencyState || net.state.RedeemHaltGeneration() == 0 {
				t.Fatalf("fraud %s failed to trigger EmergencyState (got state=%s gen=%d)",
					tc.name, common.StateToText(state), net.state.RedeemHaltGeneration())
			}
		})
	}

	// Griefing attack 1: Byzantine secondary EVM lies about block hash -> MUST NOT false-halt.
	t.Run("byzantine_evm_disagreement_never_false_halts", func(t *testing.T) {
		missing := &redeemSource{err: constants.ErrDataNonExistent.Error(), synced: true}
		net, log, secondary := redeemFixture(t, missing)
		secondary.replaced.Store(log.BlockNumber, ecommon.HexToHash("0x11e00000"))

		if err := net.registerRedeem(log, true); err == nil {
			t.Fatal("expected retry error when EVM endpoints disagree")
		}
		if state, _ := net.state.GetState(); state == common.EmergencyState || net.state.RedeemHaltGeneration() != 0 {
			t.Fatalf("Byzantine EVM disagreement caused false emergency halt (state=%s)", common.StateToText(state))
		}
	})

	// Griefing attack 2: Unsynced or transiently failing Zenon RPC -> MUST NOT false-halt.
	t.Run("unsynced_or_failing_zenon_never_false_halts", func(t *testing.T) {
		unsynced := &redeemSource{err: constants.ErrDataNonExistent.Error(), synced: false}
		net, log, _ := redeemFixture(t, unsynced)
		if err := net.registerRedeem(log, true); err == nil {
			t.Fatal("expected error when Zenon endpoint is unsynced")
		}
		if state, _ := net.state.GetState(); state == common.EmergencyState || net.state.RedeemHaltGeneration() != 0 {
			t.Fatalf("unsynced Zenon endpoint caused false emergency halt (state=%s)", common.StateToText(state))
		}

		transientErr := &redeemSource{err: "connection reset by peer", synced: true}
		net2, log2, _ := redeemFixture(t, transientErr)
		if err := net2.registerRedeem(log2, true); err == nil {
			t.Fatal("expected error on transient Zenon RPC failure")
		}
		if state, _ := net2.state.GetState(); state == common.EmergencyState || net2.state.RedeemHaltGeneration() != 0 {
			t.Fatalf("transient Zenon RPC error caused false emergency halt (state=%s)", common.StateToText(state))
		}
	})

	// Griefing attack 3: Incomplete / malformed wrap reply (fee > amount or non-hex address) -> retry, never panic or false-halt.
	t.Run("malformed_zenon_wrap_reply_retries_without_halt", func(t *testing.T) {
		badFee := validWrap()
		badFee.Fee = big.NewInt(999) // fee > amount!
		net, log, _ := redeemFixture(t, &redeemSource{synced: true, wrap: badFee})
		if err := net.registerRedeem(log, true); err == nil {
			t.Fatal("expected error when Zenon returns fee > amount")
		}
		if state, _ := net.state.GetState(); state == common.EmergencyState {
			t.Fatal("malformed Zenon reply caused false emergency halt")
		}
	})
}

type flappingChainReader struct {
	head     uint64
	headErr  error
	canon    ecommon.Hash
	canonErr error
	logs     []etypes.Log
	logsErr  error
}

func (f *flappingChainReader) BlockNumber() (uint64, error)             { return f.head, f.headErr }
func (f *flappingChainReader) CanonicalHash(uint64) (ecommon.Hash, error) { return f.canon, f.canonErr }
func (f *flappingChainReader) FilterBlockLogs(ecommon.Hash) ([]etypes.Log, error) {
	return f.logs, f.logsErr
}

// TestAdversarialConfirmationBoundaryAndReorgFlap verifies confirmQueuedEvent
// and classifyEvent never falsely confirm or falsely discard under hostile RPC behavior.
func TestAdversarialConfirmationBoundaryAndReorgFlap(t *testing.T) {
	origHash := ecommon.HexToHash("0x1111")
	altHash := ecommon.HexToHash("0x2222")
	contract := ecommon.HexToAddress("0xbeef")
	ev := &events.UnwrapRequestEvm{
		BlockNumber:     500,
		BlockHash:       origHash,
		TransactionHash: ecommon.HexToHash("0xaaaa"),
		LogIndex:        2,
	}

	// 1. Regressing head (provider head < event block) -> MUST retry.
	d := confirmQueuedEvent(&flappingChainReader{head: 499, canon: origHash}, ev, 12, time.Second, func() error { return nil })
	if d.outcome != outcomeRetry {
		t.Fatalf("regressing head yielded %s, want retry", d.outcome)
	}

	// 2. Exactly 1 confirmation short (head = 511, need 12) -> MUST retry.
	d = confirmQueuedEvent(&flappingChainReader{head: 511, canon: origHash}, ev, 12, time.Second, func() error { return nil })
	if d.outcome != outcomeRetry {
		t.Fatalf("immature event yielded %s, want retry", d.outcome)
	}

	// 3. Split/inconclusive canonical hash error -> MUST retry (never discard!).
	d = confirmQueuedEvent(&flappingChainReader{head: 600, canonErr: errors.New("endpoints disagree")}, ev, 12, time.Second, func() error { return nil })
	if d.outcome != outcomeRetry {
		t.Fatalf("disagreed canonical hash yielded %s, want retry", d.outcome)
	}

	// 4. Canonical block matches, but source verification fails (tampered/missing log) -> MUST retry.
	d = confirmQueuedEvent(&flappingChainReader{head: 600, canon: origHash}, ev, 12, time.Second, func() error {
		return errors.New("payload mismatch across endpoints")
	})
	if d.outcome != outcomeRetry {
		t.Fatalf("failed source verification yielded %s, want retry", d.outcome)
	}

	// 5. classifyEvent with canonical block match but pruned/missing logs -> inconclusive (never reorged!).
	v, err := classifyEvent(&flappingChainReader{canon: origHash, logs: nil}, ev, contract)
	if err != nil || v != verdictInconclusive {
		t.Fatalf("pruned canonical block yielded %s err=%v, want inconclusive", v, err)
	}

	// 6. classifyEvent with unanimous replacement hash -> verdictReorged.
	v, err = classifyEvent(&flappingChainReader{canon: altHash}, ev, contract)
	if err != nil || v != verdictReorged {
		t.Fatalf("replacement block yielded %s err=%v, want reorged", v, err)
	}
}
