package common

import (
	"strings"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"orchestrator/health/snapshot"
)

func TestReplaceTokensMapUpdatesAffiliateFlagsAndGeneration(t *testing.T) {
	znn := types.ZnnTokenStandard.String()
	qsr := types.QsrTokenStandard.String()
	gs := NewGlobalState(new(uint8))

	gs.SetTokensMap(1, znn, "0xAA")
	gs.SetTokensMap(1, qsr, "0xBB")
	gs.SetIsAffiliateProgram(AffiliateProgram{
		Networks: map[uint32]AffiliateNetwork{
			1: {StartingHeight: 10, ZNN: true, WZNN: true, QSR: true, WQSR: true},
		},
	})

	policy, gen0 := gs.AffiliatePolicyWithGeneration(1, "0xaa", 10)
	if policy != (SplitAffiliate | VerifyAffiliate) {
		t.Fatalf("initial policy = %d, want Split|Verify", policy)
	}

	// Replace chain 1 pairs: rotate wZNN to 0xCC and drop QSR.
	gs.ReplaceTokensMap(1, map[string]string{znn: "0xCC"})

	ran := false
	if current, err := gs.WhileAffiliateSettingsAre(gen0, func() error { ran = true; return nil }); err != nil || current || ran {
		t.Fatalf("expected generation bump after ReplaceTokensMap, got current=%v ran=%v err=%v", current, ran, err)
	}
	if got := gs.GetTokensMap(1, "0xaa"); got != "" {
		t.Fatalf("stale wZNN address still mapped to %q", got)
	}
	if got := gs.GetTokensMap(1, "0xbb"); got != "" {
		t.Fatalf("removed wQSR address still mapped to %q", got)
	}
	if got := gs.GetTokensMap(1, "0xcc"); got != znn {
		t.Fatalf("new wZNN address mapped to %q, want %q", got, znn)
	}
	if got := gs.AffiliatePolicyAt(1, "0xaa", 10); got != 0 {
		t.Fatalf("stale wZNN policy = %d, want 0", got)
	}
	if got := gs.AffiliatePolicyAt(1, "0xbb", 10); got != 0 {
		t.Fatalf("removed wQSR policy = %d, want 0", got)
	}
	policy1, gen1 := gs.AffiliatePolicyWithGeneration(1, "0xcc", 10)
	if policy1 != (SplitAffiliate | VerifyAffiliate) {
		t.Fatalf("new wZNN policy = %d, want Split|Verify", policy1)
	}

	// Re-applying identical pairs (case-insensitively) must not bump generation.
	gs.ReplaceTokensMap(1, map[string]string{znn: "0xcc"})
	if current, err := gs.WhileAffiliateSettingsAre(gen1, func() error { return nil }); err != nil || !current {
		t.Fatalf("no-op ReplaceTokensMap bumped generation: current=%v err=%v", current, err)
	}

	// RemoveTokenPair removes both directions and bumps generation.
	gs.RemoveTokenPair(1, znn, "0xcc")
	if current, err := gs.WhileAffiliateSettingsAre(gen1, func() error { return nil }); err != nil || current {
		t.Fatalf("RemoveTokenPair did not bump generation: current=%v err=%v", current, err)
	}
	if got := gs.GetTokensMap(1, znn); got != "" {
		t.Fatalf("ZNN still mapped to %q after removal", got)
	}
	if got := gs.AffiliatePolicyAt(1, "0xcc", 10); got != 0 {
		t.Fatalf("policy after removal = %d, want 0", got)
	}
}

func TestSetTokensMapCleansUpStaleReverseMapping(t *testing.T) {
	znn := types.ZnnTokenStandard.String()
	gs := NewGlobalState(new(uint8))
	gs.SetTokensMap(1, znn, "0xAA")
	gs.SetTokensMap(1, znn, "0xBB")

	if got := gs.GetTokensMap(1, "0xaa"); got != "" {
		t.Fatalf("old wrapped token 0xaa still mapped to %q", got)
	}
	if got := gs.GetTokensMap(1, "0xbb"); got != znn {
		t.Fatalf("new wrapped token 0xbb mapped to %q, want %q", got, znn)
	}
	if got := gs.GetTokensMap(1, znn); got != "0xbb" {
		t.Fatalf("ZNN mapped to %q, want 0xbb", got)
	}
}

func TestAffiliateMetadataAsymmetryLogsError(t *testing.T) {
	origLogger := GlobalLogger
	core, logs := observer.New(zapcore.InfoLevel)
	GlobalLogger = zap.New(core).Sugar()
	t.Cleanup(func() { GlobalLogger = origLogger })

	gs := NewGlobalState(new(uint8))
	gs.SetTokensMap(1, types.ZnnTokenStandard.String(), "0xAA")
	gs.SetTokensMap(1, types.QsrTokenStandard.String(), "0xBB")

	// Dangerous asymmetry: wZNN enabled without ZNN.
	gs.SetIsAffiliateProgram(AffiliateProgram{
		Networks: map[uint32]AffiliateNetwork{
			1: {StartingHeight: 5, WZNN: true, ZNN: false},
		},
	})
	if got := gs.AffiliatePolicyAt(1, "0xaa", 5); got != SplitAffiliate {
		t.Fatalf("policy = %d, want SplitAffiliate", got)
	}
	errEntries := logs.FilterLevelExact(zapcore.ErrorLevel).All()
	if len(errEntries) != 1 || !strings.Contains(errEntries[0].Message, "wZNN") || !strings.Contains(errEntries[0].Message, "VerifyAffiliate") {
		t.Fatalf("expected 1 error log for wZNN asymmetry, got %d: %+v", len(errEntries), errEntries)
	}

	// Dangerous asymmetry: wQSR enabled without QSR.
	gs.SetIsAffiliateProgram(AffiliateProgram{
		Networks: map[uint32]AffiliateNetwork{
			1: {StartingHeight: 5, WQSR: true, QSR: false},
		},
	})
	errEntries = logs.FilterLevelExact(zapcore.ErrorLevel).All()
	if len(errEntries) != 2 || !strings.Contains(errEntries[1].Message, "wQSR") {
		t.Fatalf("expected 2nd error log for wQSR asymmetry, got %d: %+v", len(errEntries), errEntries)
	}

	// Safe configurations (VerifyAffiliate only, or both enabled) log no errors.
	gs.SetIsAffiliateProgram(AffiliateProgram{
		Networks: map[uint32]AffiliateNetwork{
			1: {StartingHeight: 5, ZNN: true, WZNN: false, QSR: true, WQSR: true},
		},
	})
	if got := logs.FilterLevelExact(zapcore.ErrorLevel).Len(); got != 2 {
		t.Fatalf("safe affiliate settings logged unexpected errors: got %d total", got)
	}
}

func TestReceiveHoldAndPendingRedeemIncidentStatus(t *testing.T) {
	gs := NewGlobalState(new(uint8))
	if hold := gs.ReceiveHold(); hold != nil {
		t.Fatalf("initial ReceiveHold = %+v, want nil", hold)
	}
	count, haltPending, err := gs.PendingRedeemIncidentStatus()
	if err != nil || count != 0 || haltPending {
		t.Fatalf("initial PendingRedeemIncidentStatus = (%d, %v, %v), want (0, false, nil)", count, haltPending, err)
	}

	// Works without a snapshot store attached.
	since := time.Now().UTC().Truncate(time.Second)
	gs.SetReceiveHold(&snapshot.ReceiveHold{Height: 42, Since: since, Attempts: 3})
	got := gs.ReceiveHold()
	if got == nil || got.Height != 42 || !got.Since.Equal(since) || got.Attempts != 3 {
		t.Fatalf("ReceiveHold = %+v, want height 42 attempts 3", got)
	}
	// Mutating the returned copy must not affect stored state.
	got.Attempts = 999
	if check := gs.ReceiveHold(); check.Attempts != 3 {
		t.Fatalf("mutating copy changed stored ReceiveHold: %+v", check)
	}

	// Also mirrors into snapshot.Store when configured.
	store := snapshot.New("v1", "")
	t.Cleanup(store.Close)
	gs.Health = store
	gs.SetReceiveHold(&snapshot.ReceiveHold{Height: 43, Since: since, Attempts: 4})
	if snapHold := store.Read().Zenon.ReceiveHold; snapHold == nil || snapHold.Height != 43 || snapHold.Attempts != 4 {
		t.Fatalf("snapshot ReceiveHold = %+v, want height 43 attempts 4", snapHold)
	}

	gs.SetReceiveHold(nil)
	if gs.ReceiveHold() != nil || store.Read().Zenon.ReceiveHold != nil {
		t.Fatal("clearing ReceiveHold left state or snapshot populated")
	}
}
