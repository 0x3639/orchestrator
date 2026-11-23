package network

import (
	"math/big"
	"sync"
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
		wantHeld bool // no answer yet: kept for the recheck worker
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
		{name: "source endpoints disagree", wantHeld: true,
			sources: func(log etypes.Log) []*redeemSource {
				return []*redeemSource{{synced: true, err: notFound}, {synced: true, wrap: sourceRequestFor(log)}}
			}},
		{name: "one source endpoint is not synced", wantHeld: true,
			sources: func(etypes.Log) []*redeemSource {
				return []*redeemSource{{synced: true, err: notFound}, {synced: false, err: notFound}}
			}},
		{name: "one EVM endpoint does not serve the log", lagging: true, wantHeld: true,
			sources: func(etypes.Log) []*redeemSource { return []*redeemSource{{synced: true, err: notFound}} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The fixture log is fixed, so the sources can be built before it.
			probe := etypes.Log{Topics: []ecommon.Hash{common.RegisteredRedeemSigHash, ecommon.HexToHash("0x31"), ecommon.HexToHash("0x32"), ecommon.HexToHash("0x33")}}
			sources := tc.sources(probe)
			n, reg, chain := redeemFixture(t, sources...)
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
			if lookups := sources[0].lookups(); lookups == 0 {
				t.Fatal("the source was never asked about the young registration")
			}
			if held, want := len(n.heldRedeemLogs()), map[bool]int{true: 1}[tc.wantHeld]; held != want {
				t.Fatalf("held=%d, want %d", held, want)
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

// A registration log without its data cannot be compared with anything. It
// is an unanswered check, not a crash and not a match.
func TestRegistrationWithoutDataIsNotDecided(t *testing.T) {
	src := &redeemSource{synced: true}
	n, reg, _ := redeemFixture(t, src)
	src.wrap = sourceRequestFor(reg)
	reg.Data = nil
	for _, live := range []bool{true, false} {
		if err := n.InterpretLog(reg, live); err == nil || n.state.RedeemHaltGeneration() != 0 {
			t.Fatalf("live=%t: err=%v pending=%d", live, err, n.state.RedeemHaltGeneration())
		}
	}
	if row, err := n.dbManager.ZnnStorage().GetWrapRequestById(src.wrap.Id); err != nil || row != nil {
		t.Fatalf("a log without data wrote a wrap row: %+v %v", row, err)
	}
	if len(n.heldRedeemLogs()) != 1 {
		t.Fatal("the unanswered live check was not held")
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

	// The administrator revoked the redeem at block 110 and unhalted at 120.
	chain.contract.closedAt.Store(110)
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

// Only a redeem that, at the final height, is closed and followed by an
// unhalt is archived. Anything else still holds the bridge, and an answer that
// is not unanimous or rests on blocks that are not final decides nothing.
// The fixture head is 140 with 12 confirmations, so the final height is 128.
func TestOnlySettledHistoryIsArchived(t *testing.T) {
	for _, tc := range []struct {
		name       string
		closedAt   uint64                // 0: still payable
		unhaltedAt uint64                // 0: never unhalted
		primary    func(*redeemContract) // extra condition on the first endpoint
		secondary  func(*redeemContract) // nil: both endpoints report the same contract
		lagging    uint64                // head of the second endpoint, when it is behind
		wantHalt   bool
		wantErr    bool
		wantStored bool
	}{
		{name: "payable after an unhalt", unhaltedAt: 120, wantHalt: true},
		{name: "payable, unhalt above the final height", unhaltedAt: 135, wantHalt: true},
		{name: "closed without a later unhalt", closedAt: 110, wantHalt: true},
		{name: "closed, unhalt in the registration block", closedAt: 110, unhaltedAt: 100, wantHalt: true},
		{name: "closed, unhalt one block later", closedAt: 110, unhaltedAt: 101, wantStored: true},
		{name: "closed and unhalted at the final height", closedAt: 128, unhaltedAt: 128, wantStored: true},
		{name: "unhalt above the final height", closedAt: 110, unhaltedAt: 129, wantErr: true},
		{name: "closed above the final height", closedAt: 129, unhaltedAt: 120, wantErr: true},
		{name: "closed in the head block", closedAt: 140, unhaltedAt: 120, wantErr: true},
		{name: "endpoints disagree on the entry", closedAt: 110, unhaltedAt: 120, wantErr: true,
			secondary: func(c *redeemContract) { c.unhaltedAt.Store(120) }},
		{name: "endpoints disagree on the unhalt height", closedAt: 110, unhaltedAt: 120, wantErr: true,
			secondary: func(c *redeemContract) { c.closedAt.Store(110); c.unhaltedAt.Store(119) }},
		{name: "only one endpoint sees the settlement, in its latest state", wantErr: true,
			secondary: func(c *redeemContract) { c.closedAt.Store(130); c.unhaltedAt.Store(131) }},
		{name: "one endpoint cannot read the contract", closedAt: 110, unhaltedAt: 120, wantErr: true,
			secondary: func(c *redeemContract) { c.failing.Store(true) }},
		{name: "no endpoint can read the contract", closedAt: 110, unhaltedAt: 120, wantErr: true,
			primary: func(c *redeemContract) { c.failing.Store(true) }},
		{name: "one endpoint has no state at the final height", closedAt: 110, unhaltedAt: 120, wantErr: true,
			secondary: func(c *redeemContract) { c.closedAt.Store(110); c.unhaltedAt.Store(120); c.pruned.Store(true) }},
		// The final height is measured from the endpoint that is furthest
		// behind, so it is confirmation-deep for every one of them: with a
		// second endpoint at 139 it is 127, at 131 it is 119, at 120 it is 108.
		{name: "one endpoint is a block behind", closedAt: 110, unhaltedAt: 120, lagging: 139, wantStored: true},
		{name: "the unhalt is not final for the endpoint that is behind", closedAt: 110, unhaltedAt: 120, lagging: 131, wantErr: true},
		{name: "the closure is not final for the endpoint that is behind", closedAt: 110, unhaltedAt: 120, lagging: 120, wantErr: true},
		// What can still be paid is reported without any older state.
		{name: "payable, no endpoint has state at the final height", unhaltedAt: 120, wantHalt: true,
			primary: func(c *redeemContract) { c.pruned.Store(true) }},
		{name: "closed without a later unhalt, no endpoint has state at the final height", closedAt: 110, unhaltedAt: 90, wantHalt: true,
			primary: func(c *redeemContract) { c.pruned.Store(true) }},
		{name: "settled, no endpoint has state at the final height", closedAt: 110, unhaltedAt: 120, wantErr: true,
			primary: func(c *redeemContract) { c.pruned.Store(true) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &redeemSource{synced: true, err: constants.ErrDataNonExistent.Error()}
			n, reg, chain := redeemFixture(t, src)
			chain.primary.contract.closedAt.Store(tc.closedAt)
			chain.primary.contract.unhaltedAt.Store(tc.unhaltedAt)
			if tc.primary != nil {
				tc.primary(chain.primary.contract)
			}
			if tc.secondary != nil {
				chain.contract = new(redeemContract)
				tc.secondary(chain.contract)
			}
			if tc.lagging != 0 {
				chain.set(evidenceReceipt(reg), []etypes.Log{reg}, tc.lagging)
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

// A signer that restarts with a stored incident the chain settled while it
// was down releases it in its first scan, before anything acts on the
// restored emergency, and then scans on.
func TestRestoredIncidentIsReleasedByTheFirstScan(t *testing.T) {
	src := &redeemSource{synced: true, err: constants.ErrDataNonExistent.Error()}
	n, reg, chain := redeemFixture(t, src)
	db, err := leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	restart := func() {
		t.Helper()
		n.state = common.NewGlobalState(new(uint8))
		if err := n.state.RestoreRedeemIncidents(wrap.NewRedeemIncidentStore(db)); err != nil {
			t.Fatal(err)
		}
	}
	restart()
	if err := n.Sync(); err != nil || n.state.RedeemHaltGeneration() == 0 {
		t.Fatalf("payable unmatched registration: err=%v pending=%d", err, n.state.RedeemHaltGeneration())
	}

	// Still payable after a restart: the first scan must keep the emergency.
	restart()
	if err := n.Sync(); err != nil {
		t.Fatal(err)
	}
	if state, _ := n.state.GetState(); n.state.RedeemHaltGeneration() == 0 || state != common.EmergencyState {
		t.Fatalf("a payable incident did not survive the restart: pending=%d state=%d", n.state.RedeemHaltGeneration(), state)
	}

	// The halt, the revocation and the unhalt all happened while it was down.
	chain.contract.closedAt.Store(110)
	chain.contract.unhaltedAt.Store(120)
	restart()
	if state, _ := n.state.GetState(); state != common.EmergencyState {
		t.Fatalf("restored state = %d", state)
	}
	if err := n.Sync(); err != nil {
		t.Fatal(err)
	}
	state, _ := n.state.GetState()
	cursor, _ := n.eventsStore().GetLastUpdateHeight()
	if n.state.RedeemHaltGeneration() != 0 || state == common.EmergencyState || cursor != 140 {
		t.Fatalf("settled incident after the first scan: pending=%d state=%d cursor=%d", n.state.RedeemHaltGeneration(), state, cursor)
	}
	if archived, err := n.state.HandledRedeemLog(1, reg); err != nil || !archived {
		t.Fatalf("released incident lost its evidence: %t %v", archived, err)
	}
}

// In a long pass the checkpoint limit is fixed by the first range. A
// registration above that limit that is final under the current head is
// decided by the finalized rules, so settled history is archived there too.
func TestSettledRegistrationAboveTheCheckpointLimit(t *testing.T) {
	src := &redeemSource{synced: true, err: constants.ErrDataNonExistent.Error()}
	n, reg, chain := redeemFixture(t, src)
	reg.BlockNumber = 110
	reg.BlockHash = canonicalHeader(110).Hash()
	chain.setChain(112, reg) // the pass starts here: checkpoint limit 100
	chain.contract.closedAt.Store(115)
	chain.contract.unhaltedAt.Store(120)
	n.queryRange = newQueryRangeAdapter(5)
	chain.primary.afterRange = sync.OnceFunc(func() { chain.setChain(150, reg) })
	if err := n.Sync(); err != nil {
		t.Fatal(err)
	}
	through, from, _, err := n.eventsStore().GetScanHeights(n.ConfirmationsToFinality())
	if err != nil || through != 150 || from != 100 {
		t.Fatalf("through=%d redeem=%d err=%v", through, from, err)
	}
	if state, _ := n.state.GetState(); n.state.RedeemHaltGeneration() != 0 || state != common.LiveState || len(n.heldRedeemLogs()) != 0 {
		t.Fatalf("settled registration above the limit: pending=%d state=%d held=%d", n.state.RedeemHaltGeneration(), state, len(n.heldRedeemLogs()))
	}
	if archived, err := n.state.HandledRedeemLog(1, reg); err != nil || !archived {
		t.Fatalf("settled registration was not kept as evidence: %t %v", archived, err)
	}
}

// The live rules are for a registration that can still be paid. A registration
// that an endpoint already shows closed after a completed halt is history,
// even when a stale head or a late delivery puts it on the live path. It is
// held and then decided by the finalized rules.
func TestLiveRulesDoNotReportSettledHistory(t *testing.T) {
	for _, tc := range []struct {
		name   string
		behind func(*redeemProvider) // makes one endpoint miss the settlement
	}{
		{name: "every endpoint shows it settled"},
		{name: "one endpoint has not seen the settlement yet",
			behind: func(p *redeemProvider) { p.contract = new(redeemContract) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &redeemSource{synced: true, err: constants.ErrDataNonExistent.Error()}
			n, reg, chain := redeemFixture(t, src)
			chain.contract.closedAt.Store(105)
			chain.contract.unhaltedAt.Store(110)
			if tc.behind != nil {
				tc.behind(chain.primary)
			}
			// A late delivery through the subscription.
			if err := n.InterpretLog(reg, true); err == nil || n.state.RedeemHaltGeneration() != 0 {
				t.Fatalf("late delivery: err=%v pending=%d", err, n.state.RedeemHaltGeneration())
			}
			// A range scan whose endpoint reports a head that makes it look young.
			n.heldRedeems = nil
			chain.primary.set(evidenceReceipt(reg), []etypes.Log{reg}, reg.BlockNumber+n.ConfirmationsToFinality()-1)
			if err := n.Sync(); err != nil {
				t.Fatal(err)
			}
			if n.state.RedeemHaltGeneration() != 0 || len(n.heldRedeemLogs()) != 1 {
				t.Fatalf("stale head: pending=%d held=%d", n.state.RedeemHaltGeneration(), len(n.heldRedeemLogs()))
			}
		})
	}
}

// While the redeem behind every stored incident is closed after a completed
// halt on every endpoint, nothing can be paid on it, so dispatching the halt
// can wait for the final evidence. One payable redeem, one disagreement, one
// unreadable contract or one newer incident ends the wait.
func TestRestoredIncidentWaitsOnlyWhileItsRedeemCannotBePaid(t *testing.T) {
	for _, tc := range []struct {
		name       string
		closedAt   uint64
		unhaltedAt uint64
		secondary  func(*redeemContract)
		unreadable bool
		wantWait   bool
	}{
		{name: "payable", unhaltedAt: 120},
		{name: "closed without a later unhalt", closedAt: 110, unhaltedAt: 90},
		{name: "closed after a completed halt", closedAt: 110, unhaltedAt: 120, wantWait: true},
		{name: "one endpoint shows it payable", closedAt: 110, unhaltedAt: 120,
			secondary: func(c *redeemContract) { c.unhaltedAt.Store(120) }},
		{name: "the contract cannot be read", closedAt: 110, unhaltedAt: 120, unreadable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &redeemSource{synced: true, err: constants.ErrDataNonExistent.Error()}
			n, reg, chain := redeemFixture(t, src)
			if err := n.Sync(); err != nil || n.state.RedeemHaltGeneration() == 0 {
				t.Fatalf("payable unmatched registration: err=%v pending=%d", err, n.state.RedeemHaltGeneration())
			}
			if n.state.RedeemHaltCanWait() {
				t.Fatal("the halt for a payable redeem can wait")
			}
			// No endpoint keeps the older state that would archive the incident.
			chain.primary.contract.pruned.Store(true)
			chain.primary.contract.closedAt.Store(tc.closedAt)
			chain.primary.contract.unhaltedAt.Store(tc.unhaltedAt)
			chain.primary.contract.failing.Store(tc.unreadable)
			if tc.secondary != nil {
				chain.contract = new(redeemContract)
				chain.contract.pruned.Store(true)
				tc.secondary(chain.contract)
			}
			if err := n.Sync(); err != nil { // the first scan after a restart
				t.Fatal(err)
			}
			if state, _ := n.state.GetState(); n.state.RedeemHaltGeneration() == 0 || state != common.EmergencyState {
				t.Fatalf("the incident was released without final evidence: pending=%d state=%d", n.state.RedeemHaltGeneration(), state)
			}
			if wait := n.state.RedeemHaltCanWait(); wait != tc.wantWait {
				t.Fatalf("halt can wait=%t, want %t", wait, tc.wantWait)
			}
			if !tc.wantWait {
				return
			}
			// A newer incident is not covered by what was found for the old one.
			other := reg
			other.Index++
			if err := n.state.ReportRedeemIncident(common.RedeemIncident{ChainID: 1, Log: other}); err != nil {
				t.Fatal(err)
			}
			if n.state.RedeemHaltCanWait() {
				t.Fatal("the halt for a newer incident can wait")
			}
		})
	}
}

// A scan that stopped for a pending halt resumes soon after the halt is
// released, not at the next ordinary refresh.
func TestPausedScanResumesWhenTheHaltIsReleased(t *testing.T) {
	src := &redeemSource{synced: true}
	n, reg, _ := redeemFixture(t, src)
	src.wrap = sourceRequestFor(reg)
	// An incident on another chain holds the bridge.
	if err := n.state.ReportRedeemIncident(common.RedeemIncident{ChainID: 56, Log: reg}); err != nil {
		t.Fatal(err)
	}
	startRedeemWatch(t, n)
	waitFor(t, "the scan was never attempted", n.scanPaused.Load)
	if cursor, _ := n.eventsStore().GetLastUpdateHeight(); cursor != 100 {
		t.Fatalf("the scan ran while the halt was pending: cursor=%d", cursor)
	}
	if err := n.state.CompleteEmergencyHalt(n.state.RedeemHaltGeneration(), map[uint32]bool{56: true}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the scan did not resume after the halt was released", func() bool {
		cursor, err := n.eventsStore().GetLastUpdateHeight()
		return err == nil && cursor == 140
	})
}
