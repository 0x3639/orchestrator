package network

import (
	"errors"
	"math/big"
	"slices"
	"strings"
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
	"go.uber.org/zap"

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

// Zenon stores a destination as the user wrote it, with or without the 0x
// prefix and in any case. Both spell the same address and match.
func TestRegistrationMatchesAnUnprefixedSourceAddress(t *testing.T) {
	src := &redeemSource{synced: true}
	n, reg, chain := redeemFixture(t, src)
	// Addresses with letters, so that their case matters.
	reg.Topics = append([]ecommon.Hash(nil), reg.Topics...)
	reg.Topics[2] = ecommon.HexToHash("0x00000000000000000000000000000000000000000000000000000000abcdef01")
	reg.Topics[3] = ecommon.HexToHash("0x000000000000000000000000000000000000000000000000000000000fedcba9")
	chain.setChain(140, reg)
	src.wrap = sourceRequestFor(reg)
	src.wrap.ToAddress = strings.ToLower(src.wrap.ToAddress[2:])
	src.wrap.TokenAddress = strings.ToUpper(src.wrap.TokenAddress[2:])
	for _, live := range []bool{true, false} {
		if err := n.InterpretLog(reg, live); err != nil || n.state.RedeemHaltGeneration() != 0 || len(n.heldRedeemLogs()) != 0 {
			t.Fatalf("live=%t: err=%v pending=%d held=%d", live, err, n.state.RedeemHaltGeneration(), len(n.heldRedeemLogs()))
		}
	}
	row, err := n.dbManager.ZnnStorage().GetWrapRequestById(src.wrap.Id)
	if err != nil || row == nil || row.RedeemStatus != common.PendingRedeemStatus {
		t.Fatalf("matched registration row=%+v err=%v", row, err)
	}
	// Another address still does not match.
	src.wrap.ToAddress = "ff" + src.wrap.ToAddress[2:]
	if err := n.InterpretLog(reg, true); err != nil || n.state.RedeemHaltGeneration() == 0 {
		t.Fatalf("other destination: err=%v pending=%d", err, n.state.RedeemHaltGeneration())
	}
}

// A source request for another token is not a match.
func TestRegistrationForAnotherTokenDoesNotMatch(t *testing.T) {
	src := &redeemSource{synced: true}
	n, reg, _ := redeemFixture(t, src)
	src.wrap = sourceRequestFor(reg)
	src.wrap.TokenAddress = "ff" + src.wrap.TokenAddress[4:]
	if err := n.InterpretLog(reg, true); err != nil || n.state.RedeemHaltGeneration() == 0 {
		t.Fatalf("other token: err=%v pending=%d", err, n.state.RedeemHaltGeneration())
	}
}

// The same holds for the reply of an endpoint that is asked to confirm a
// mismatch the first endpoint reported.
func TestConfirmingReplyWithoutAFeeDecidesNothing(t *testing.T) {
	first := &redeemSource{synced: true}
	second := &redeemSource{synced: true, mangle: func(r map[string]any) { delete(r, "fee") }}
	n, reg, _ := redeemFixture(t, first, second)
	// Both hold the same request, for another amount than the registration.
	first.wrap = sourceRequestFor(reg)
	first.wrap.Amount = big.NewInt(30)
	second.wrap = first.wrap
	for _, live := range []bool{true, false} {
		if err := n.InterpretLog(reg, live); err == nil || n.state.RedeemHaltGeneration() != 0 {
			t.Fatalf("live=%t: err=%v pending=%d", live, err, n.state.RedeemHaltGeneration())
		}
	}
	// With the fee present the mismatch is confirmed.
	second.mangle = nil
	if err := n.InterpretLog(reg, false); err != nil || n.state.RedeemHaltGeneration() == 0 {
		t.Fatalf("complete confirmation: err=%v pending=%d", err, n.state.RedeemHaltGeneration())
	}
}

// A source reply without a usable amount or fee is not a request with other
// numbers. It decides nothing.
func TestSourceReplyWithoutAmountOrFeeDecidesNothing(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mangle func(map[string]any)
		amount int64 // gross amount in the source request; the log carries 19
		fee    int64
		wantOK bool
	}{
		{name: "fee missing", amount: 20, fee: 1, mangle: func(r map[string]any) { delete(r, "fee") }},
		{name: "fee not a number", amount: 20, fee: 1, mangle: func(r map[string]any) { r["fee"] = "one" }},
		{name: "fee negative", amount: 20, fee: 1, mangle: func(r map[string]any) { r["fee"] = "-1" }},
		{name: "amount missing", amount: 20, fee: 1, mangle: func(r map[string]any) { delete(r, "amount") }},
		{name: "amount not a string", amount: 20, fee: 1, mangle: func(r map[string]any) { r["amount"] = 20 }},
		{name: "chain id missing", amount: 19, fee: 0, mangle: func(r map[string]any) { delete(r, "chainId") }},
		{name: "network class missing", amount: 19, fee: 0, mangle: func(r map[string]any) { delete(r, "networkClass") }},
		{name: "chain id not a number", amount: 19, fee: 0, mangle: func(r map[string]any) { r["chainId"] = "1" }},
		{name: "explicit zero fee", amount: 19, fee: 0, mangle: func(map[string]any) {}, wantOK: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &redeemSource{synced: true, mangle: tc.mangle}
			n, reg, _ := redeemFixture(t, src)
			src.wrap = sourceRequestFor(reg)
			src.wrap.Amount, src.wrap.Fee = big.NewInt(tc.amount), big.NewInt(tc.fee)
			for _, live := range []bool{true, false} {
				err := n.InterpretLog(reg, live)
				if (err == nil) != tc.wantOK || n.state.RedeemHaltGeneration() != 0 {
					t.Fatalf("live=%t: err=%v pending=%d", live, err, n.state.RedeemHaltGeneration())
				}
			}
			row, err := n.dbManager.ZnnStorage().GetWrapRequestById(src.wrap.Id)
			if err != nil || (row != nil) != tc.wantOK {
				t.Fatalf("row=%+v err=%v", row, err)
			}
		})
	}
}

// Halt signatures are matched to networks by position and cover the
// network's contract. When the networks are not the ones the messages were
// built for, sending must stop and say so instead of using a signature for
// another network or none at all.
func TestHaltSignaturesNeedTheNetworksTheyWereSignedFor(t *testing.T) {
	src := &redeemSource{synced: true}
	n, _, chain := redeemFixture(t, src)
	m := &NetworksManager{evmNetworks: []*evmNetwork{n}, logger: zap.NewNop().Sugar()}
	// Signed while there was no EVM network; one was published since.
	if _, err := m.SendHaltEvm(nil, nil, ecommon.Address{}); !errors.Is(err, ErrHaltMembershipChanged) {
		t.Fatalf("a network without a signature: %v", err)
	}
	messages, err := m.GetHaltMessages()
	if err != nil || len(messages) != 1 {
		t.Fatalf("halt messages: %d %v", len(messages), err)
	}
	signatures := [][]byte{{1}}
	// The same networks: sending goes on, here to a chain already halted.
	*chain.halted = true
	if halted, err := m.SendHaltEvm(signatures, nil, ecommon.Address{}); err != nil || !halted {
		t.Fatalf("matching networks: halted=%t err=%v", halted, err)
	}
	// The network was removed and published again under the same chain id.
	replacement := &evmNetwork{EvmParams: n.EvmParams, rpcManager: n.rpcManager, logger: n.logger, done: make(chan struct{})}
	if replacement.ChainId() != n.ChainId() {
		t.Fatal("fixture: the replacement must share the chain id")
	}
	m.RemoveEvmNetwork(n.ChainId())
	m.AddEvmNetwork(replacement)
	if _, err := m.SendHaltEvm(signatures, nil, ecommon.Address{}); !errors.Is(err, ErrHaltMembershipChanged) {
		t.Fatalf("a replaced network: %v", err)
	}
	// New messages are for the current networks.
	if _, err := m.GetHaltMessages(); err != nil {
		t.Fatal(err)
	}
	if halted, err := m.SendHaltEvm(signatures, nil, ecommon.Address{}); err != nil || !halted {
		t.Fatalf("after signing again: halted=%t err=%v", halted, err)
	}
}

// Finalized evidence is checked against the confirmation depth before the
// endpoints are read. An endpoint that no longer serves that depth by the time
// the reads are done decides nothing.
func TestFinalizedRedeemNeedsTheDepthAfterTheReads(t *testing.T) {
	src := &redeemSource{synced: true}
	n, reg, chain := redeemFixture(t, src)
	src.wrap = sourceRequestFor(reg)
	chain.afterReceipt = sync.OnceFunc(func() {
		chain.set(evidenceReceipt(reg), []etypes.Log{reg}, reg.BlockNumber+n.ConfirmationsToFinality()-1)
	})
	if err := n.InterpretLog(reg, false); err == nil {
		t.Fatal("decided on evidence that was no longer final on every endpoint")
	}
	if row, err := n.dbManager.ZnnStorage().GetWrapRequestById(src.wrap.Id); err != nil || row != nil {
		t.Fatalf("a wrap row was written: %+v %v", row, err)
	}
	chain.set(evidenceReceipt(reg), []etypes.Log{reg}, 140)
	if err := n.InterpretLog(reg, false); err != nil {
		t.Fatalf("with the depth back: %v", err)
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
		reorgAt    uint64                // height whose block is replaced while the older state is read
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
		{name: "the block at the final height changes during the read", closedAt: 110, unhaltedAt: 120, reorgAt: 128, wantErr: true},
		{name: "the block at the lowest head changes during the read", closedAt: 110, unhaltedAt: 120, reorgAt: 140, wantErr: true},
		{name: "one endpoint does not hold the registration", closedAt: 110, unhaltedAt: 120, wantErr: true,
			secondary: func(c *redeemContract) { c.registered.Store(0); c.unhaltedAt.Store(120) }},
		{name: "no endpoint holds the registration", wantErr: true,
			primary: func(c *redeemContract) { c.registered.Store(0) }},
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
				chain.contract = contractWith(reg.BlockNumber)
				tc.secondary(chain.contract)
			}
			if tc.lagging != 0 {
				chain.set(evidenceReceipt(reg), []etypes.Log{reg}, tc.lagging)
			}
			if tc.reorgAt != 0 {
				chain.primary.contract.onOldRead = sync.OnceFunc(func() {
					chain.primary.replaced.Store(tc.reorgAt, ecommon.HexToHash("0xbeef"))
					chain.replaced.Store(tc.reorgAt, ecommon.HexToHash("0xbeef"))
				})
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

// Before the signing loop exists the scan settles what it can, but it does not
// work through the whole backlog once one incident is known to stay pending:
// that incident needs its halt.
func TestStartupSweepStopsAtTheFirstIncidentThatStaysPending(t *testing.T) {
	src := &redeemSource{synced: true, err: constants.ErrDataNonExistent.Error()}
	n, reg, chain := redeemFixture(t, src)
	// Three stored incidents, in store order: settled, one whose log no
	// endpoint serves any more, and settled again.
	gone, last := reg, reg
	gone.Index, last.Index = reg.Index+1, reg.Index+2
	for _, log := range []etypes.Log{reg, gone, last} {
		if err := n.state.ReportRedeemIncident(common.RedeemIncident{ChainID: 1, Log: log}); err != nil {
			t.Fatal(err)
		}
	}
	chain.setChain(140, reg, last)
	chain.contract.closedAt.Store(110)
	chain.contract.unhaltedAt.Store(120)
	pendingIndexes := func() []uint {
		t.Helper()
		pending, err := n.state.PendingRedeemIncidents()
		if err != nil {
			t.Fatal(err)
		}
		indexes := make([]uint, 0, len(pending))
		for _, incident := range pending {
			indexes = append(indexes, incident.Log.Index)
		}
		return indexes
	}
	if err := n.Sync(); err != nil {
		t.Fatal(err)
	}
	// The first was settled; the sweep stopped at the second.
	if left := pendingIndexes(); !slices.Equal(left, []uint{gone.Index, last.Index}) {
		t.Fatalf("pending after the startup sweep: %v", left)
	}
	// The worker checks the rest.
	startRedeemWatch(t, n)
	waitUntil(t, "the worker did not settle the incident behind it", func() bool {
		return slices.Equal(pendingIndexes(), []uint{gone.Index})
	})
	if n.state.RedeemHaltGeneration() == 0 {
		t.Fatal("the incident that stays pending no longer holds the bridge")
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
	chain.contract.registered.Store(110)
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
			behind: func(p *redeemProvider) { p.contract = contractWith(100) }},
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
	waitUntil(t, "the scan was never attempted", n.scanPaused.Load)
	if cursor, _ := n.eventsStore().GetLastUpdateHeight(); cursor != 100 {
		t.Fatalf("the scan ran while the halt was pending: cursor=%d", cursor)
	}
	if err := n.state.CompleteEmergencyHalt(n.state.RedeemHaltGeneration(), map[uint32]bool{56: true}); err != nil {
		t.Fatal(err)
	}
	// The ordinary first retry of a failed subscription is five seconds away.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if cursor, err := n.eventsStore().GetLastUpdateHeight(); err == nil && cursor == 140 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the scan did not resume soon after the halt was released")
		}
		time.Sleep(2 * time.Millisecond)
	}
}
