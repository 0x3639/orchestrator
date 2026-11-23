package network

import (
	"encoding/json"
	"errors"
	"math/big"
	"sync/atomic"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/protocol"
	"github.com/zenon-network/go-zenon/rpc/api"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"orchestrator/common"
)

// evmSource is what the EVM endpoint answers for the transaction an unwrap
// request names as its source.
type evmSource struct {
	receipt func() (interface{}, *replayRPCError)
	logs    func() (interface{}, *replayRPCError)
}

// lateNode is a signer with one EVM network, whose ledger, sync status and EVM
// endpoint a test changes between passes. Its receives confirm at momentum
// 100 and finality is 5 momentums.
type lateNode struct {
	t        *testing.T
	bridge   *replayBridge
	ledger   atomic.Pointer[replayLedger]
	source   atomic.Pointer[evmSource]
	syncing  atomic.Bool
	evmCalls atomic.Int32
	// Momentums left before the bridge lets the request be redeemed, and
	// whether the node's reply leaves that out.
	redeemableIn      atomic.Uint64
	redeemDelayAbsent atomic.Bool
	// Called when the listener asks the node for its sync status.
	onSyncStatus atomic.Pointer[func()]
	// Reads of the unwrap request, and whether the node fails to return the
	// send paired with a receive.
	requestReads   atomic.Int32
	sendUnreadable atomic.Bool
	n              *znnNetwork
	stores         replayStores
}

func newLateNode(t *testing.T, register func(*replayBridge)) *lateNode {
	t.Helper()
	l := &lateNode{t: t, bridge: newReplayBridge(t)}
	if register != nil {
		register(l.bridge)
	}
	l.ledger.Store(newReplayLedger(t, l.bridge, 100))
	l.source.Store(l.noTransaction())
	url := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "stats.syncInfo":
			if hook := l.onSyncStatus.Load(); hook != nil {
				(*hook)()
			}
			if l.syncing.Load() {
				return &protocol.SyncInfo{State: protocol.Syncing}, nil
			}
		case "ledger.getAccountBlockByHash":
			var hash types.Hash
			if len(params) == 1 && json.Unmarshal(params[0], &hash) == nil && hash[1] == 1 && l.sendUnreadable.Load() {
				return nil, &replayRPCError{-32000, "upstream timeout"}
			}
		case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
			l.requestReads.Add(1)
			if l.redeemDelayAbsent.Load() {
				return l.bridge.request, nil
			}
			return withRedeemDelay(l.bridge.request, l.redeemableIn.Load()), nil
		case "eth_getTransactionReceipt":
			l.evmCalls.Add(1)
			return l.source.Load().receipt()
		case "eth_getLogs":
			l.evmCalls.Add(1)
			return l.source.Load().logs()
		}
		if result, ok := l.ledger.Load().answer(t, method, params); ok {
			return result, nil
		}
		t.Errorf("unexpected method %s", method)
		return nil, nil
	})
	l.n, l.stores = replayNetwork(t, l.bridge, url, map[uint32]string{1: url})
	l.n.SetConfirmationsToFinality(5)
	return l
}

// at moves the frontier and replaces the bridge account's receives, one per
// status, from account height 2.
func (l *lateNode) at(frontier uint64, statuses ...[]byte) {
	l.t.Helper()
	l.ledger.Store(newReplayLedger(l.t, l.bridge, frontier, statuses...))
	if err := l.n.state.SetFrontierMomentum(frontier); err != nil {
		l.t.Fatal(err)
	}
}

// restart is a new process on the same databases.
func (l *lateNode) restart() {
	old := l.n
	l.n = &znnNetwork{listenerCtx: old.listenerCtx, cancelListener: old.cancelListener, dbManager: old.dbManager,
		rpcManager: old.rpcManager, state: old.state, stopChan: old.stopChan, logger: old.logger}
	l.n.SetConfirmationsToFinality(5)
}

func (l *lateNode) cursor() uint64 {
	l.t.Helper()
	height, err := l.stores.znn.GetLastUpdateHeight()
	if err != nil {
		l.t.Fatal(err)
	}
	return height
}

// listen is one pass of the running listener from its checkpoint.
func (l *lateNode) listen() error {
	_, _, err := l.n.processBridgeAccountPage(l.n.listenerCtx, l.cursor(), l.n.refreshSettings)
	return err
}

func (l *lateNode) row() *eventsRow {
	l.t.Helper()
	row, err := l.stores.evm[1].GetUnwrapRequestByHashAndLog(l.bridge.txHash, l.bridge.request.LogIndex)
	if err != nil {
		l.t.Fatal(err)
	}
	if row == nil {
		return nil
	}
	return &eventsRow{status: row.RedeemStatus, signature: row.Signature}
}

type eventsRow struct {
	status    uint32
	signature string
}

func (l *lateNode) expect(step string, state uint8, cursor uint64) {
	l.t.Helper()
	if got := replayState(l.t, l.n); got != state {
		l.t.Fatalf("%s: state %s, want %s", step, common.StateToText(got), common.StateToText(state))
	}
	if got := l.cursor(); got != cursor {
		l.t.Fatalf("%s: checkpoint %d, want %d", step, got, cursor)
	}
}

func (l *lateNode) noTransaction() *evmSource {
	return &evmSource{receipt: func() (interface{}, *replayRPCError) { return nil, nil }}
}

func (l *lateNode) matchingEvent() *evmSource {
	log := l.bridge.sourceLog(l.t, l.bridge.recipient.String())
	return &evmSource{
		receipt: func() (interface{}, *replayRPCError) { return l.bridge.receipt(), nil },
		logs:    func() (interface{}, *replayRPCError) { return []etypes.Log{log}, nil },
	}
}

// besideAnotherContract is the matching event in a transaction that also
// emitted a log from another contract. The endpoint's log query is filtered to
// the bridge, so its answer rightly leaves that log out.
func (l *lateNode) besideAnotherContract() *evmSource {
	log := l.bridge.sourceLog(l.t, l.bridge.recipient.String())
	log.Index = 1
	other := etypes.Log{Address: ecommon.HexToAddress("0x0f"), TxHash: l.bridge.txHash, BlockHash: l.bridge.blockHash, BlockNumber: 100, Index: 0,
		Topics: []ecommon.Hash{ecommon.HexToHash("0x0e")}}
	receipt := l.bridge.receipt()
	receipt.Logs = []*etypes.Log{&other, &log}
	return &evmSource{
		receipt: func() (interface{}, *replayRPCError) { return receipt, nil },
		logs:    func() (interface{}, *replayRPCError) { return []etypes.Log{log}, nil },
	}
}

// anotherEvent is a transaction the endpoint serves in full, whose bridge log
// pays someone else.
func (l *lateNode) anotherEvent() *evmSource {
	log := l.bridge.sourceLog(l.t, "z1qpp8g8v4hrd5ernfrgsn86jmv09ra6zrvhvmr7")
	return &evmSource{
		receipt: func() (interface{}, *replayRPCError) { return l.bridge.receipt(), nil },
		logs:    func() (interface{}, *replayRPCError) { return []etypes.Log{log}, nil },
	}
}

func (l *lateNode) receiptFails(message string) *evmSource {
	return &evmSource{receipt: func() (interface{}, *replayRPCError) { return nil, &replayRPCError{-32000, message} }}
}

func (l *lateNode) logsFail(message string) *evmSource {
	return &evmSource{
		receipt: func() (interface{}, *replayRPCError) { return l.bridge.receipt(), nil },
		logs:    func() (interface{}, *replayRPCError) { return nil, &replayRPCError{-32000, message} },
	}
}

// alteredLogs is an endpoint whose receipt lists the matching bridge log and
// whose log query returns that slot with another payload.
func (l *lateNode) alteredLogs() *evmSource {
	log := l.bridge.sourceLog(l.t, l.bridge.recipient.String())
	receipt := l.bridge.receipt()
	receipt.Logs = []*etypes.Log{&log}
	altered := l.bridge.sourceLog(l.t, "z1qpp8g8v4hrd5ernfrgsn86jmv09ra6zrvhvmr7")
	return &evmSource{
		receipt: func() (interface{}, *replayRPCError) { return receipt, nil },
		logs:    func() (interface{}, *replayRPCError) { return []etypes.Log{altered}, nil },
	}
}

// anotherHeight is an endpoint whose log query returns the receipt's bridge
// log under another block number.
func (l *lateNode) anotherHeight() *evmSource {
	log := l.bridge.sourceLog(l.t, l.bridge.recipient.String())
	receipt := l.bridge.receipt()
	receipt.Logs = []*etypes.Log{&log}
	moved := log
	moved.BlockNumber++
	return &evmSource{
		receipt: func() (interface{}, *replayRPCError) { return receipt, nil },
		logs:    func() (interface{}, *replayRPCError) { return []etypes.Log{moved}, nil },
	}
}

// incompleteLogs is an endpoint whose receipt lists the bridge log and whose
// log query for the same block returns nothing.
func (l *lateNode) incompleteLogs() *evmSource {
	log := l.bridge.sourceLog(l.t, l.bridge.recipient.String())
	receipt := l.bridge.receipt()
	receipt.Logs = []*etypes.Log{&log}
	return &evmSource{
		receipt: func() (interface{}, *replayRPCError) { return receipt, nil },
		logs:    func() (interface{}, *replayRPCError) { return []etypes.Log{}, nil },
	}
}

var acceptedReceive, rejectedReceive = receiveStatus(vmResultSuccess), receiveStatus(vmResultFail)

// The listener starts at the tip of the bridge account, so a registration it
// reads was made while it was running. How far the frontier has moved by the
// time it reads it says nothing about the registration.
func TestRunningListenerChecksARegistrationHoweverLateItReadsIt(t *testing.T) {
	for _, tc := range []struct {
		name     string
		frontier uint64
		source   func(*lateNode) *evmSource
		want     uint8
		stored   bool
	}{
		{"within finality, no transaction", 104, (*lateNode).noTransaction, common.EmergencyState, false},
		{"at finality, no transaction", 105, (*lateNode).noTransaction, common.EmergencyState, false},
		{"long after finality, no transaction", 5000, (*lateNode).noTransaction, common.EmergencyState, false},
		{"long after finality, another event", 5000, (*lateNode).anotherEvent, common.EmergencyState, false},
		{"long after finality, matching event", 5000, (*lateNode).matchingEvent, common.LiveState, true},
		{"long after finality, matching event beside another contract's log", 5000, (*lateNode).besideAnotherContract, common.LiveState, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := newLateNode(t, func(b *replayBridge) {
				if tc.name == "long after finality, matching event beside another contract's log" {
					b.request.LogIndex = 1
				}
			})
			l.source.Store(tc.source(l))
			l.at(tc.frontier, acceptedReceive)
			if err := l.listen(); err != nil {
				t.Fatal(err)
			}
			l.expect("listener", tc.want, 2)
			if row := l.row(); (row != nil) != tc.stored {
				t.Fatalf("stored row %v, want stored %v", row, tc.stored)
			} else if tc.stored && row.status != common.PendingRedeemStatus {
				t.Fatalf("stored status %d", row.status)
			}
		})
	}
}

// A node that reports it is syncing cannot say how old a receive is. The
// listener waits for it; it does not read the receive as history.
func TestListenerWaitsWhileTheNodeIsSyncing(t *testing.T) {
	l := newLateNode(t, nil)
	l.syncing.Store(true)
	l.at(104, acceptedReceive)
	if err := l.listen(); err == nil {
		t.Fatal("the listener read a receive from a node that is syncing")
	}
	l.expect("syncing", common.LiveState, 1)
	if l.evmCalls.Load() != 0 || l.row() != nil {
		t.Fatalf("the receive was interpreted while the node was syncing: %d source lookups, row %v", l.evmCalls.Load(), l.row())
	}
	// The node catches up long after the receive became final.
	l.syncing.Store(false)
	l.at(5000, acceptedReceive)
	if err := l.listen(); err != nil {
		t.Fatal(err)
	}
	l.expect("synced", common.EmergencyState, 2)
}

// While the listener holds one receive it reads none of the receives behind
// it. They are checked when it reaches them.
func TestRegistrationBehindAHeldReceiveIsChecked(t *testing.T) {
	l := newLateNode(t, nil)
	l.at(104, nil, acceptedReceive) // the first receive has no readable status
	if err := l.listen(); err == nil {
		t.Fatal("the listener passed a receive without a status")
	}
	l.expect("held", common.LiveState, 1)
	l.at(5000, rejectedReceive, acceptedReceive)
	if err := l.listen(); err != nil {
		t.Fatal(err)
	}
	l.expect("released", common.EmergencyState, 3)
}

// A database that has followed the bridge account to its tip owes a check to
// every receive after that point, also when a restart reads it long after it
// became final. A database reading the account for the first time cannot tell
// a transaction that never existed from one its endpoint no longer serves.
func TestRestartChecksReceivesMadeAfterTheDatabaseReachedTheTip(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reachedTip bool
		source     func(*lateNode) *evmSource
		want       uint8
		fails      bool
	}{
		{"reached the tip before, no transaction", true, (*lateNode).noTransaction, common.EmergencyState, false},
		{"reached the tip before, another event", true, (*lateNode).anotherEvent, common.EmergencyState, false},
		{"reached the tip before, matching event", true, (*lateNode).matchingEvent, common.LiveState, false},
		{"reached the tip before, endpoint has pruned the block", true,
			func(l *lateNode) *evmSource { return l.logsFail("pruned history unavailable") }, common.LiveState, true},
		{"first read of the account, no transaction", false, (*lateNode).noTransaction, common.LiveState, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			restartAfterHold(t, tc.reachedTip, 5000, tc.source, tc.want, tc.fails)
		})
	}
	// A receive that is still within finality when startup reads it is live,
	// on a first read as on any other.
	t.Run("first read of the account, receive within finality, no transaction", func(t *testing.T) {
		restartAfterHold(t, false, 104, (*lateNode).noTransaction, common.EmergencyState, false)
	})
}

// restartAfterHold starts a signer on a database at account height 1 and reads
// one accepted registration at the given frontier. With reachedTip, an earlier
// run followed the account to its tip and then held that receive until it
// stopped.
func restartAfterHold(t *testing.T, reachedTip bool, frontier uint64, source func(*lateNode) *evmSource, want uint8, fails bool) {
	t.Helper()
	l := newLateNode(t, nil)
	if reachedTip {
		l.at(100)
		if err := l.n.Sync(); err != nil {
			t.Fatal(err)
		}
		l.source.Store(l.receiptFails("upstream timeout"))
		l.at(104, acceptedReceive)
		if err := l.listen(); err == nil {
			t.Fatal("the listener passed a receive whose source it could not read")
		}
		l.expect("held", common.LiveState, 1)
	}
	l.restart()
	l.source.Store(source(l))
	l.at(frontier, acceptedReceive)
	if err := l.n.Sync(); (err != nil) != fails {
		t.Fatalf("startup error %v, want failure %v", err, fails)
	}
	wantCursor := uint64(2)
	if fails {
		wantCursor = 1
	}
	l.expect("restart", want, wantCursor)
}

// A database reading the bridge account for the first time checks a request
// the bridge still withholds, as it would a live one. It asks the source chain
// nothing about a request that is past its redeem delay, redeemed or revoked:
// its transaction may be older than the endpoint's history.
func TestFirstReadOfTheAccountChecksRequestsInsideTheirRedeemDelay(t *testing.T) {
	redeemed := func(b *replayBridge) { b.request.Redeemed = 1 }
	revoked := func(b *replayBridge) { b.request.Revoked = 1 }
	timesOut := func(l *lateNode) *evmSource { return l.receiptFails("upstream timeout") }
	for _, tc := range []struct {
		name         string
		register     func(*replayBridge)
		redeemableIn uint64
		source       func(*lateNode) *evmSource
		want         uint8
		fails        bool
		stored       bool
		lookups      bool
	}{
		{"inside the delay, matching event", nil, 30, (*lateNode).matchingEvent, common.LiveState, false, true, true},
		{"inside the delay, another event", nil, 30, (*lateNode).anotherEvent, common.EmergencyState, false, false, true},
		{"inside the delay, no transaction", nil, 30, (*lateNode).noTransaction, common.EmergencyState, false, false, true},
		{"last momentum of the delay, no transaction", nil, 1, (*lateNode).noTransaction, common.EmergencyState, false, false, true},
		{"inside the delay, receipt lookup times out", nil, 30, timesOut, common.LiveState, true, false, true},
		{"inside the delay, endpoint omits the block's logs", nil, 30, (*lateNode).incompleteLogs, common.LiveState, true, false, true},
		{"past the delay, no transaction", nil, 0, (*lateNode).noTransaction, common.LiveState, false, true, false},
		{"past the delay, another event", nil, 0, (*lateNode).anotherEvent, common.LiveState, false, true, false},
		{"past the delay, lookups would time out", nil, 0, timesOut, common.LiveState, false, true, false},
		{"redeemed, lookups would time out", redeemed, 0, timesOut, common.LiveState, false, true, false},
		{"redeemed, delay lengthened since, no transaction", redeemed, 30, (*lateNode).noTransaction, common.LiveState, false, true, false},
		{"revoked inside the delay, no transaction", revoked, 30, (*lateNode).noTransaction, common.LiveState, false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := newLateNode(t, tc.register)
			l.redeemableIn.Store(tc.redeemableIn)
			l.source.Store(tc.source(l))
			l.at(5000, acceptedReceive)
			if err := l.n.Sync(); (err != nil) != tc.fails {
				t.Fatalf("startup error %v, want failure %v", err, tc.fails)
			}
			wantCursor := uint64(2)
			if tc.fails {
				wantCursor = 1
			}
			l.expect("startup", tc.want, wantCursor)
			if row := l.row(); (row != nil) != tc.stored {
				t.Fatalf("stored row %v, want stored %v", row, tc.stored)
			} else if tc.stored && row.signature != "test-signature" {
				t.Fatalf("stored signature %q", row.signature)
			}
			if lookups := l.evmCalls.Load(); (lookups != 0) != tc.lookups {
				t.Fatalf("%d source lookups, want lookups %v", lookups, tc.lookups)
			}
			// The request and its delay come from one reply. Two reads could
			// see the delay run out between them.
			if reads := l.requestReads.Load(); !tc.fails && reads != 1 {
				t.Fatalf("%d reads of the request, want 1", reads)
			}
		})
	}
}

// Whether a request is still inside its redeem delay decides if it is checked.
// A reply that does not say holds the receive; it is not read as past the delay.
func TestFirstReadWaitsForTheRedeemDelay(t *testing.T) {
	l := newLateNode(t, nil)
	l.redeemableIn.Store(30)
	l.redeemDelayAbsent.Store(true)
	l.at(5000, acceptedReceive)
	if err := l.n.Sync(); err == nil {
		t.Fatal("startup passed a request without knowing its redeem delay")
	}
	l.expect("delay unknown", common.LiveState, 1)
	if l.evmCalls.Load() != 0 || l.row() != nil {
		t.Fatalf("the request was handled without its redeem delay: %d source lookups, row %v", l.evmCalls.Load(), l.row())
	}
	l.redeemDelayAbsent.Store(false)
	if err := l.n.Sync(); err != nil {
		t.Fatal(err)
	}
	l.expect("delay known", common.EmergencyState, 2)
}

// The node works a request's remaining delay out from its own frontier and
// settings. While it is syncing neither a remaining delay nor an expired one
// says anything, and startup waits. It does not take the receive, or every
// later one, as new on that node's word.
func TestFirstReadDoesNotDecideOnASyncingNode(t *testing.T) {
	for _, tc := range []struct {
		name         string
		frontier     uint64
		redeemableIn uint64
	}{
		{"delay remaining", 5000, 30},
		{"delay expired", 5000, 0},
		{"receive within finality", 104, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := newLateNode(t, nil)
			l.redeemableIn.Store(tc.redeemableIn)
			l.syncing.Store(true)
			l.at(tc.frontier, acceptedReceive)
			if err := l.n.Sync(); err == nil {
				t.Fatal("startup decided about a request on a node that is syncing")
			}
			l.expect("syncing", common.LiveState, 1)
			started, err := l.stores.znn.BridgeWatchStarted()
			if err != nil || started || l.evmCalls.Load() != 0 || l.row() != nil {
				t.Fatalf("on a node that is syncing: watch recorded %v (err %v), %d source lookups, row %v", started, err, l.evmCalls.Load(), l.row())
			}
		})
	}
}

// The bridge returned the request, so its network exists there. A node with
// no client for that network cannot check a request it has to vouch for, and
// holds it. Old history on a first read is passed over as before.
func TestCheckedRegistrationWaitsForItsNetworkClient(t *testing.T) {
	elsewhere := func(b *replayBridge) { b.request.ChainId = 7 }
	t.Run("read by the listener", func(t *testing.T) {
		l := newLateNode(t, elsewhere)
		l.at(5000, acceptedReceive)
		if err := l.listen(); err == nil {
			t.Fatal("the listener passed a request on a network it has no client for")
		}
		l.expect("listener", common.LiveState, 1)
	})
	t.Run("first read, past the delay", func(t *testing.T) {
		l := newLateNode(t, elsewhere)
		l.at(5000, acceptedReceive)
		if err := l.n.Sync(); err != nil {
			t.Fatal(err)
		}
		l.expect("startup", common.LiveState, 2)
	})
	t.Run("first read, inside the delay", func(t *testing.T) {
		l := newLateNode(t, elsewhere)
		l.redeemableIn.Store(30)
		l.at(5000, acceptedReceive)
		if err := l.n.Sync(); err == nil {
			t.Fatal("startup passed a request it has to check on a network it has no client for")
		}
		l.expect("startup", common.LiveState, 1)
	})
}

// A request the node already has to vouch for is not decided by its delay.
func TestCheckedRegistrationDoesNotDependOnTheRedeemDelay(t *testing.T) {
	l := newLateNode(t, nil)
	l.redeemDelayAbsent.Store(true)
	l.at(5000, acceptedReceive)
	if err := l.listen(); err != nil {
		t.Fatal(err)
	}
	l.expect("listener", common.EmergencyState, 2)
}

// A first read that has to check a request and cannot finish keeps the
// obligation. The next start checks the request although it is by then past
// finality and past its redeem delay.
func TestFirstReadKeepsACheckItCouldNotFinish(t *testing.T) {
	for _, tc := range []struct {
		name         string
		frontier     uint64
		redeemableIn uint64
		sendFails    bool
	}{
		{"request inside its redeem delay", 5000, 30, false},
		{"receive within finality", 104, 0, false},
		{"receive within finality whose send cannot be read", 104, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := newLateNode(t, nil)
			l.redeemableIn.Store(tc.redeemableIn)
			l.source.Store(l.receiptFails("upstream timeout"))
			l.sendUnreadable.Store(tc.sendFails)
			l.at(tc.frontier, acceptedReceive)
			if err := l.n.Sync(); err == nil {
				t.Fatal("startup passed a request whose source it could not read")
			}
			l.expect("first start", common.LiveState, 1)

			l.restart()
			l.sendUnreadable.Store(false)
			l.redeemableIn.Store(0)
			l.source.Store(l.noTransaction())
			l.at(9000, acceptedReceive)
			if err := l.n.Sync(); err != nil {
				t.Fatal(err)
			}
			l.expect("second start", common.EmergencyState, 2)
		})
	}
}

// An empty page may be the tip of a node that was still catching up. Startup
// reads the tip again after the node says it is synced, and what appears then
// is still history to a database on its first read.
func TestStartupReadsTheTipAgainOnceTheNodeIsSynced(t *testing.T) {
	l := newLateNode(t, nil)
	l.at(5000)
	arrived := false
	hook := func() {
		if !arrived {
			arrived = true
			l.ledger.Store(newReplayLedger(t, l.bridge, 5000, acceptedReceive))
		}
	}
	l.onSyncStatus.Store(&hook)
	if err := l.n.Sync(); err != nil {
		t.Fatal(err)
	}
	l.expect("startup", common.LiveState, 2)
	if l.row() == nil || l.evmCalls.Load() != 0 {
		t.Fatalf("the late block was not read as first-read history: row %v, %d source lookups", l.row(), l.evmCalls.Load())
	}
}

// For a registration the node must vouch for, a source it cannot read is not
// an answer, whatever the reason. The listener holds the receive without
// declaring anything and checks it once the endpoint serves the block.
func TestCheckedRegistrationWaitsForAReadableSource(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source func(*lateNode) *evmSource
	}{
		{"receipt lookup times out", func(l *lateNode) *evmSource { return l.receiptFails("upstream timeout") }},
		{"endpoint has pruned the receipt", func(l *lateNode) *evmSource { return l.receiptFails("pruned history unavailable") }},
		{"endpoint has no archive logs", func(l *lateNode) *evmSource { return l.logsFail("archive data is not supported on this plan") }},
		{"endpoint omits the block's logs", (*lateNode).incompleteLogs},
		{"endpoint's logs differ from its receipt", (*lateNode).alteredLogs},
		{"endpoint's logs put the event at another height", (*lateNode).anotherHeight},
	} {
		for name, frontier := range map[string]uint64{"live": 104, "late": 5000} {
			t.Run(name+", "+tc.name, func(t *testing.T) {
				l := newLateNode(t, nil)
				l.source.Store(tc.source(l))
				l.at(frontier, acceptedReceive)
				if err := l.listen(); err == nil {
					t.Fatal("the listener passed a registration whose source it could not read")
				}
				l.expect("unreadable", common.LiveState, 1)
				l.source.Store(l.noTransaction())
				if err := l.listen(); err != nil {
					t.Fatal(err)
				}
				l.expect("readable", common.EmergencyState, 2)
			})
		}
	}
}

// keyGenNetwork is a signer in key generation whose ledger holds one
// administrator send at account height 2, confirmed at momentum 100, and
// serves the bridge's current settings.
func keyGenNetwork(t *testing.T, frontier uint64, data []byte, info func() (*definition.BridgeInfoVariable, error), syncing ...bool) *znnNetwork {
	t.Helper()
	receive := startupReceive(2)
	receive.Data = acceptedReceive
	send := startupSend(receive)
	send.Data = data
	ledger := &startupLedger{
		page: func(height uint64) *api.AccountBlockListMarshal {
			if height > receive.Height {
				return startupPage()
			}
			return startupPage(receive)
		},
		lookup: func(hash types.Hash) (*api.AccountBlockMarshal, error) {
			if hash == receive.Hash {
				return receive, nil
			}
			return send, nil
		},
		tip: func() *api.Momentum {
			return &api.Momentum{Momentum: &nom.Momentum{Height: frontier, Hash: types.Hash{1}}}
		},
		bridgeInfo: info,
	}
	if len(syncing) == 1 && syncing[0] {
		ledger.syncStatus = func() *protocol.SyncInfo { return &protocol.SyncInfo{State: protocol.Syncing} }
	}
	if info == nil {
		ledger.bridgeInfo = func() (*definition.BridgeInfoVariable, error) {
			t.Error("the handler read the bridge's settings for a send it does not act on")
			return nil, errors.New("unexpected bridge info read")
		}
	}
	n := startupNetwork(t, ledger)
	n.SetConfirmationsToFinality(5)
	n.administrator = send.Address.String()
	if err := n.state.SetState(common.KeyGenState); err != nil {
		t.Fatal(err)
	}
	if err := n.state.SetFrontierMomentum(frontier); err != nil {
		t.Fatal(err)
	}
	return n
}

// Nothing but these two receives takes a signer out of key generation. Read
// late, by the listener or by startup, they follow what the bridge allows now.
func TestLateKeyGenExitFollowsTheBridge(t *testing.T) {
	const key, otherKey = "A2V5IEdlbmVyYXRlZCBieSBUU1M=", "A090aGVyIEtleQ=="
	pack := func(method string, args ...interface{}) []byte {
		data, err := definition.ABIBridge.PackMethod(method, args...)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	allowOff := pack(definition.SetAllowKeygenMethodName, false)
	allowOn := pack(definition.SetAllowKeygenMethodName, true)
	changeKey := pack(definition.ChangeTssECDSAPubKeyMethodName, key, "old-signature", "new-signature")
	bridge := func(allow bool, pubKey string) func() (*definition.BridgeInfoVariable, error) {
		return func() (*definition.BridgeInfoVariable, error) {
			return &definition.BridgeInfoVariable{AllowKeyGen: allow, CompressedTssECDSAPubKey: pubKey}, nil
		}
	}
	unavailable := func() (*definition.BridgeInfoVariable, error) { return nil, errors.New("bridge info unavailable") }
	const live, late = 104, 105
	for _, tc := range []struct {
		name     string
		frontier uint64
		startup  bool
		data     []byte
		stranger bool
		info     func() (*definition.BridgeInfoVariable, error)
		want     uint8
		held     bool
		from     uint8 // the signer's state before the receive; key generation unless set
	}{
		{"live, keygen switched off", live, false, allowOff, false, nil, common.LiveState, false, 0},
		{"live, keygen switched on", live, false, allowOn, false, nil, common.KeyGenState, false, 0},
		{"late, keygen switched off, still off", late, false, allowOff, false, bridge(false, key), common.LiveState, false, 0},
		{"late, keygen switched off, on again since", late, false, allowOff, false, bridge(true, key), common.KeyGenState, false, 0},
		{"late, keygen switched on, off since", late, false, allowOn, false, bridge(false, key), common.LiveState, false, 0},
		{"late, keygen switched off, settings unreadable", late, false, allowOff, false, unavailable, common.KeyGenState, true, 0},
		{"live, keygen switched off by another account", live, false, allowOff, true, nil, common.KeyGenState, false, 0},
		{"late, sent by a former administrator, keygen off", late, false, allowOff, true, bridge(false, key), common.LiveState, false, 0},
		{"late, sent by another account, keygen still on", late, false, allowOff, true, bridge(true, key), common.KeyGenState, false, 0},
		{"startup, keygen switched off, still off", 5000, true, allowOff, false, bridge(false, key), common.LiveState, false, 0},
		{"startup, keygen switched off, on again since", 5000, true, allowOff, false, bridge(true, key), common.KeyGenState, false, 0},
		{"live, key changed to this key", live, false, changeKey, false, bridge(false, key), common.LiveState, false, 0},
		{"live, bridge holds another key", live, false, changeKey, false, bridge(false, otherKey), common.KeyGenState, false, 0},
		{"late, key changed, keygen over", late, false, changeKey, false, bridge(false, otherKey), common.LiveState, false, 0},
		{"late, key changed, keygen allowed again", late, false, changeKey, false, bridge(true, key), common.KeyGenState, false, 0},
		{"late, key changed, settings unreadable", late, false, changeKey, false, unavailable, common.KeyGenState, true, 0},
		{"startup, key changed, keygen over", 5000, true, changeKey, false, bridge(false, otherKey), common.LiveState, false, 0},
		{"late, keygen switched off, signer is halted", late, false, allowOff, false, nil, common.HaltedState, false, common.HaltedState},
		{"live, keygen switched off, signer is halted", live, false, allowOff, false, nil, common.HaltedState, false, common.HaltedState},
		{"late, key changed, signer is halted", late, false, changeKey, false, nil, common.HaltedState, false, common.HaltedState},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := keyGenNetwork(t, tc.frontier, tc.data, tc.info)
			if tc.stranger {
				n.administrator = types.BridgeContract.String()
			}
			if tc.from != 0 {
				if err := n.state.SetState(tc.from); err != nil {
					t.Fatal(err)
				}
			}
			var err error
			if tc.startup {
				err = n.Sync()
			} else {
				_, _, err = n.processBridgeAccountPage(n.listenerCtx, 1, n.refreshSettings)
			}
			if (err != nil) != tc.held {
				t.Fatalf("error %v, want held %v", err, tc.held)
			}
			if got := replayState(t, n); got != tc.want {
				t.Fatalf("state %s, want %s", common.StateToText(got), common.StateToText(tc.want))
			}
			want := uint64(2)
			if tc.held {
				want = 1
			}
			checkStartupCursor(t, n, want)
		})
	}
}

// The bridge accepts any JSON document as metadata. One that does not decode
// into the orchestrator's settings will not decode on a retry either.
func TestBridgeMetadataThatDoesNotDecodeDoesNotHoldTheListener(t *testing.T) {
	for _, tc := range []struct {
		name     string
		frontier uint64
	}{{"live", 104}, {"late", 105}} {
		t.Run(tc.name, func(t *testing.T) {
			metadata := `{"partyTimeout":"soon"}`
			data, err := definition.ABIBridge.PackMethod(definition.SetBridgeMetadataMethodName, metadata)
			if err != nil {
				t.Fatal(err)
			}
			current := func() (*definition.BridgeInfoVariable, error) {
				return &definition.BridgeInfoVariable{Metadata: metadata}, nil
			}
			n := keyGenNetwork(t, tc.frontier, data, current)
			applied := 0
			n.setBridgeMetadata = func(*common.BridgeMetadata) { applied++ }
			height, _, err := n.processBridgeAccountPage(n.listenerCtx, 1, n.refreshSettings)
			if err != nil || height != 2 {
				t.Fatalf("height %d, err %v: the listener is held by metadata it can never apply", height, err)
			}
			if applied != 0 {
				t.Fatal("applied metadata that does not decode")
			}
			checkStartupCursor(t, n, 2)
		})
	}
}

// An amount that differs from the source event is as much a mismatch for a
// request checked on the first read of the account as it is live.
func TestFirstReadComparesTheRegisteredAmount(t *testing.T) {
	l := newLateNode(t, func(b *replayBridge) { b.request.Amount = big.NewInt(101) })
	l.redeemableIn.Store(30)
	l.source.Store(l.matchingEvent()) // the event carries 100
	l.at(5000, acceptedReceive)
	if err := l.n.Sync(); err != nil {
		t.Fatal(err)
	}
	l.expect("startup", common.EmergencyState, 2)
}

// A signer that stopped in key generation may find the receive that ended it
// behind its checkpoint. Reaching the tip, startup follows the bridge.
func TestStartupLeavesKeyGenerationTheBridgeHasEnded(t *testing.T) {
	for _, tc := range []struct {
		name  string
		allow bool
		from  uint8
		want  uint8
	}{
		{"bridge no longer allows key generation", false, common.KeyGenState, common.LiveState},
		{"bridge still allows key generation", true, common.KeyGenState, common.KeyGenState},
		{"signer is halted", false, common.HaltedState, common.HaltedState},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ledger := &startupLedger{
				page: func(uint64) *api.AccountBlockListMarshal { return startupPage() },
				bridgeInfo: func() (*definition.BridgeInfoVariable, error) {
					return &definition.BridgeInfoVariable{AllowKeyGen: tc.allow}, nil
				},
			}
			n := startupNetwork(t, ledger)
			if err := n.state.SetState(tc.from); err != nil {
				t.Fatal(err)
			}
			if err := n.Sync(); err != nil {
				t.Fatal(err)
			}
			if got := replayState(t, n); got != tc.want {
				t.Fatalf("state %s, want %s", common.StateToText(got), common.StateToText(tc.want))
			}
		})
	}
}

// The bridge's settings as a syncing node reports them may be old ones. A
// late receive about key generation waits for the node.
func TestLateKeyGenExitWaitsForASyncedNode(t *testing.T) {
	data, err := definition.ABIBridge.PackMethod(definition.SetAllowKeygenMethodName, false)
	if err != nil {
		t.Fatal(err)
	}
	ended := func() (*definition.BridgeInfoVariable, error) { return &definition.BridgeInfoVariable{}, nil }
	n := keyGenNetwork(t, 105, data, ended, true)
	if _, _, err := n.processBridgeAccountPage(n.listenerCtx, 1, n.refreshSettings); err == nil {
		t.Fatal("the listener followed the bridge settings of a node that is syncing")
	}
	if got := replayState(t, n); got != common.KeyGenState {
		t.Fatalf("state %s", common.StateToText(got))
	}
	checkStartupCursor(t, n, 1)
}

// A node whose bridge account ends below the checkpoint is behind what this
// signer has already processed. Startup does not take its empty page for the
// tip.
func TestStartupRejectsANodeBehindTheCheckpoint(t *testing.T) {
	ledger := &startupLedger{page: func(uint64) *api.AccountBlockListMarshal {
		return &api.AccountBlockListMarshal{List: []*api.AccountBlockMarshal{}, Count: 3}
	}}
	n := startupNetwork(t, ledger)
	if err := n.eventsStore().SetLastUpdateHeight(5); err != nil {
		t.Fatal(err)
	}
	if err := n.Sync(); err == nil {
		t.Fatal("startup finished on a node whose bridge account is behind the checkpoint")
	}
	if started, err := n.eventsStore().BridgeWatchStarted(); err != nil || started {
		t.Fatalf("watch recorded %v, err %v", started, err)
	}
	// At the checkpoint, the same node is at the tip.
	if err := n.eventsStore().SetLastUpdateHeight(3); err != nil {
		t.Fatal(err)
	}
	if err := n.Sync(); err != nil {
		t.Fatal(err)
	}
}
