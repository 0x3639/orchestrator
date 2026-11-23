package network

import (
	"math/big"
	"testing"
	"time"

	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"orchestrator/common"
	"orchestrator/db/unwrap"
	"orchestrator/db/wrap"
)

func sourceRequestFor(log etypes.Log) *definition.WrapTokenRequest {
	return &definition.WrapTokenRequest{Id: types.Hash(log.Topics[1]), NetworkClass: definition.EvmClass, ChainId: 1,
		ToAddress: ecommon.BytesToAddress(log.Topics[2][:]).Hex(), TokenAddress: ecommon.BytesToAddress(log.Topics[3][:]).Hex(),
		Amount: big.NewInt(20), Fee: big.NewInt(1)}
}

// The range scan checks a registration that is not final yet, the way the
// subscription would, and neither path decides on an answer it cannot confirm.
func TestCatchUpChecksYoungRegistrations(t *testing.T) {
	notFound := constants.ErrDataNonExistent.Error()
	for _, tc := range []struct {
		name     string
		sources  func(etypes.Log) []*redeemSource
		lagging  bool // one EVM endpoint does not serve the log
		wantHalt bool
	}{
		{name: "no source request", wantHalt: true,
			sources: func(etypes.Log) []*redeemSource { return []*redeemSource{{synced: true, err: notFound}} }},
		{name: "matching source request",
			sources: func(log etypes.Log) []*redeemSource {
				return []*redeemSource{{synced: true, wrap: sourceRequestFor(log)}}
			}},
		{name: "source request for another chain", wantHalt: true,
			sources: func(log etypes.Log) []*redeemSource {
				request := sourceRequestFor(log)
				request.ChainId = 2
				return []*redeemSource{{synced: true, wrap: request}}
			}},
		{name: "source request for another network class", wantHalt: true,
			sources: func(log etypes.Log) []*redeemSource {
				request := sourceRequestFor(log)
				request.NetworkClass = definition.NoMClass
				return []*redeemSource{{synced: true, wrap: request}}
			}},
		{name: "source endpoints disagree",
			sources: func(log etypes.Log) []*redeemSource {
				return []*redeemSource{{synced: true, err: notFound}, {synced: true, wrap: sourceRequestFor(log)}}
			}},
		{name: "one source endpoint is not synced",
			sources: func(etypes.Log) []*redeemSource {
				return []*redeemSource{{synced: true, err: notFound}, {synced: false, err: notFound}}
			}},
		{name: "one EVM endpoint does not serve the log", lagging: true,
			sources: func(etypes.Log) []*redeemSource { return []*redeemSource{{synced: true, err: notFound}} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The fixture log is fixed, so the sources can be built before it.
			probe := etypes.Log{Topics: []ecommon.Hash{common.RegisteredRedeemSigHash, ecommon.HexToHash("0x31"), ecommon.HexToHash("0x32"), ecommon.HexToHash("0x33")}}
			n, reg, chain := redeemFixture(t, tc.sources(probe)...)
			head := reg.BlockNumber + n.ConfirmationsToFinality() - 1
			chain.setChain(head, reg)
			if tc.lagging {
				chain.set(nil, nil, head)
			}
			if err := n.Sync(); err != nil {
				t.Fatal(err)
			}
			if halted := n.state.RedeemHaltGeneration() != 0; halted != tc.wantHalt {
				t.Fatalf("halt pending=%t, want %t", halted, tc.wantHalt)
			}
			// Durable status and the redeem checkpoint wait for finality.
			row, err := n.dbManager.ZnnStorage().GetWrapRequestById(types.Hash(reg.Topics[1]))
			if err != nil || row != nil {
				t.Fatalf("young registration wrote a wrap row: %+v %v", row, err)
			}
			through, from, _, err := n.eventsStore().GetScanHeights(n.ConfirmationsToFinality())
			if err != nil || from > reg.BlockNumber {
				t.Fatalf("redeem checkpoint %d passed the young registration: %v", from, err)
			}
			// A pending halt stops the pass before its range is checkpointed.
			if wantThrough := map[bool]uint64{true: reg.BlockNumber, false: head}[tc.wantHalt]; through != wantThrough {
				t.Fatalf("scan height %d, want %d", through, wantThrough)
			}
		})
	}
}

// A live check that gave no answer is repeated until it gives one. Here the
// subscription delivered the log while another endpoint was one block behind,
// and no range scan is due before that endpoint catches up.
func TestUncheckedLiveRegistrationIsRetried(t *testing.T) {
	src := &redeemSource{synced: true, err: constants.ErrDataNonExistent.Error()}
	n, reg, chain := redeemFixture(t, src)
	head := reg.BlockNumber + n.ConfirmationsToFinality() - 2
	// Receipts only: the range scan returns no log, so it cannot be the path
	// that finds this registration.
	chain.primary.set(evidenceReceipt(reg), nil, head)
	chain.set(nil, nil, reg.BlockNumber-1)
	if err := n.InterpretLog(reg, true); err == nil || n.state.RedeemHaltGeneration() != 0 {
		t.Fatalf("lagging endpoint: err=%v pending=%d", err, n.state.RedeemHaltGeneration())
	}
	stopped := make(chan struct{})
	go func() { defer close(stopped); n.SubscribeToEvents() }()
	defer func() { close(n.done); <-stopped }()
	chain.set(evidenceReceipt(reg), nil, head)
	deadline := time.Now().Add(15 * time.Second)
	for n.state.RedeemHaltGeneration() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the registration was not checked again after every endpoint served it")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A signer that scans, from empty databases, a registration that was halted
// for, closed, and followed by an administrator unhalt archives it as evidence
// and keeps working.
func TestSettledHistoryDoesNotHaltAFreshSigner(t *testing.T) {
	src := &redeemSource{synced: true, err: constants.ErrDataNonExistent.Error()}
	n, reg, chain := redeemFixture(t, src)

	// First life: the registration is still payable, so it holds the bridge
	// until the halt flags are confirmed.
	if err := n.Sync(); err != nil {
		t.Fatal(err)
	}
	generation := n.state.RedeemHaltGeneration()
	if generation == 0 {
		t.Fatal("a payable unmatched registration did not open a halt")
	}
	if err := n.state.CompleteEmergencyHalt(generation, map[uint32]bool{1: true}); err != nil {
		t.Fatal(err)
	}

	// The administrator revoked the redeem and unhalted at block 120.
	chain.contract.closed.Store(true)
	chain.contract.unhaltedAt.Store(120)

	// Second life: same chain, empty databases.
	stores := n.dbManager.(unwrapTestStores)
	stores.znn = wrap.NewZnnStorage(zdb.NewMemDB(), n.stopChan)
	stores.evm = unwrap.NewEvmStorage(zdb.NewMemDB(), n.stopChan, 100)
	n.dbManager = stores
	fresh, err := leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	n.state = common.NewGlobalState(new(uint8))
	if err := n.state.RestoreRedeemIncidents(wrap.NewRedeemIncidentStore(fresh)); err != nil {
		t.Fatal(err)
	}
	for pass := 0; pass < 2; pass++ {
		if err := n.Sync(); err != nil {
			t.Fatal(err)
		}
		// A halt that began and ended would have left the stored state halted.
		state, _ := n.state.GetState()
		cursor, _ := n.eventsStore().GetLastUpdateHeight()
		if n.state.RedeemHaltGeneration() != 0 || state != common.LiveState || cursor != 140 {
			t.Fatalf("pass %d: pending=%d state=%d cursor=%d", pass, n.state.RedeemHaltGeneration(), state, cursor)
		}
	}
	if archived, err := n.state.HandledRedeemLog(1, reg); err != nil || !archived {
		t.Fatalf("settled registration was not kept as evidence: %t %v", archived, err)
	}
}

// Only a closed redeem followed by a finalized unhalt is archived. Anything
// else still holds the bridge, and an answer that is not unanimous or not
// final decides nothing.
func TestOnlySettledHistoryIsArchived(t *testing.T) {
	for _, tc := range []struct {
		name       string
		closed     bool
		unhaltedAt uint64
		unreadable bool                  // no endpoint can read the contract
		secondary  func(*redeemContract) // nil: both endpoints report the same state
		wantHalt   bool
		wantErr    bool
		wantStored bool
	}{
		{name: "payable after an unhalt", unhaltedAt: 120, wantHalt: true},
		{name: "closed without a later unhalt", closed: true, wantHalt: true},
		{name: "closed, unhalt in the registration block", closed: true, unhaltedAt: 100, wantHalt: true},
		{name: "closed, unhalt one block later", closed: true, unhaltedAt: 101, wantStored: true},
		{name: "closed, unhalt at the final height", closed: true, unhaltedAt: 128, wantStored: true},
		{name: "closed, unhalt not final", closed: true, unhaltedAt: 129, wantErr: true},
		{name: "endpoints disagree", closed: true, unhaltedAt: 120, wantErr: true,
			secondary: func(c *redeemContract) { c.unhaltedAt.Store(120) }},
		{name: "one endpoint cannot read the contract", closed: true, unhaltedAt: 120, wantErr: true,
			secondary: func(c *redeemContract) { c.failing.Store(true) }},
		{name: "no endpoint can read the contract", closed: true, unhaltedAt: 120, unreadable: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &redeemSource{synced: true, err: constants.ErrDataNonExistent.Error()}
			n, reg, chain := redeemFixture(t, src)
			chain.primary.contract.closed.Store(tc.closed)
			chain.primary.contract.unhaltedAt.Store(tc.unhaltedAt)
			chain.primary.contract.failing.Store(tc.unreadable)
			if tc.secondary != nil {
				chain.contract = new(redeemContract)
				tc.secondary(chain.contract)
			}
			err := n.InterpretLog(reg, false)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v, want error %t", err, tc.wantErr)
			}
			if halted := n.state.RedeemHaltGeneration() != 0; halted != tc.wantHalt {
				t.Fatalf("halt pending=%t, want %t", halted, tc.wantHalt)
			}
			stored, err := n.state.HandledRedeemLog(1, reg)
			if err != nil || stored != tc.wantStored {
				t.Fatalf("archived=%t err=%v, want %t", stored, err, tc.wantStored)
			}
		})
	}
}
