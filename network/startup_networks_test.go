package network

import (
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/api"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"orchestrator/common"
	"orchestrator/common/events"
	"orchestrator/common/storage"
	"orchestrator/db"
)

// A start that waited for the EVM scans does not complete startup by itself.
// The node completes it once it has looked at the halt flags, and a halted
// bridge moves every state that signs to HaltedState first, key generation
// and re-signing included.
func TestStartupCompletesOnlyAfterTheHaltFlagsAreRead(t *testing.T) {
	for _, state := range []uint8{common.LiveState, common.KeyGenState, common.ReSignState} {
		t.Run(common.StateToText(state), func(t *testing.T) {
			h := newHeldStart(t)
			h.zenon.change(func(z *heldZenon) { z.allowKeyGen = true })
			h.setState(state)
			h.plainReceive()
			h.evm.open()
			h.evm.halted.Store(true)
			if err := h.m.Start(); err != nil {
				t.Fatal(err)
			}
			if h.m.CompletesStartupLater() {
				t.Fatal("a start that waited for the scans completes later")
			}
			h.expect("start returned", state, 2, false)
			if err := h.m.CompleteStartup(); err != nil {
				t.Fatal(err)
			}
			h.expect("completed", common.HaltedState, 2, true)
		})
	}
}

// A network the bridge listener adds while the halt flags are read is looked
// at too before startup completes.
func TestNetworkAddedWhileTheHaltFlagsAreReadIsLookedAt(t *testing.T) {
	h := newHeldStart(t)
	h.plainReceive()
	h.evm.open()
	if err := h.m.Start(); err != nil {
		t.Fatal(err)
	}
	second := newHeldEvm(t, h.bridge)
	second.open()
	second.halted.Store(true)
	add := func() { h.addEvmNetwork(2, second.url) }
	h.evm.onHaltedRead.Store(&add)
	if err := h.m.CompleteStartup(); err != nil {
		t.Fatal(err)
	}
	if h.m.NetworksLength() != 2 {
		t.Fatalf("%d networks; the second was not added during the read", h.m.NetworksLength())
	}
	h.expect("completed", common.HaltedState, 2, true)
}

// The line that says startup cannot complete names what could not be read.
func TestHaltFlagErrorNamesWhatCouldNotBeRead(t *testing.T) {
	for _, tc := range []struct {
		name string
		fail func(h *heldStart)
		want string
	}{
		{"EVM network", func(h *heldStart) { h.evm.haltedFails.Store(true) }, "EVM network 1: "},
		{"Zenon", func(h *heldStart) { h.zenon.change(func(z *heldZenon) { z.noBridgeInfo = true }) }, "Zenon: missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHeldStart(t)
			h.plainReceive()
			h.evm.open()
			if err := h.m.Start(); err != nil {
				t.Fatal(err)
			}
			tc.fail(h)
			err := h.m.CompleteStartup()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want one that contains %q", err, tc.want)
			}
			if h.complete() {
				t.Fatal("startup completed without the halt flags")
			}
		})
	}
}

// A background start brings each EVM network up on its own. A network whose
// node is still syncing does not keep another from scanning, which may hold
// the event a held read waits for. Startup still waits for both.
func TestEvmNetworkWaitingForItsNodeDoesNotHoldBackTheOthers(t *testing.T) {
	h := newHeldStart(t)
	h.setState(common.EmergencyState)
	h.plainReceive()
	h.evm.syncing.Store(true)
	h.evm.open()
	second := newHeldEvm(t, h.bridge)
	second.open()
	h.addEvmNetwork(2, second.url)
	if err := h.start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the second network to scan to the head", func() bool {
		height, err := h.stores.evm[2].GetLastUpdateHeight()
		return err == nil && height == 200
	})
	if h.scansFinished() || h.complete() {
		t.Fatal("startup counts as complete while the first network waits for its node")
	}
}

// The worker that confirms a removed network's events ends with the removal,
// without stopping the node, and the queue can be opened again.
func TestRemovedNetworksQueueWorkerEnds(t *testing.T) {
	h := newHeldStart(t)
	exited := make(chan struct{})
	go func() {
		h.net.ProcessEvents()
		close(exited)
	}()
	time.Sleep(100 * time.Millisecond) // the worker waits for an event
	h.m.RemoveEvmNetwork(1)
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("the worker of a removed network still waits on its queue")
	}
	if h.stopRequested() {
		t.Fatal("the removal of a network stopped the node")
	}
	queue, err := storage.CreateOrOpenQueue(definition.EvmClass, "held-start-1")
	if err != nil {
		t.Fatalf("the queue of a removed network cannot be opened again: %v", err)
	}
	_ = queue.Close()
}

// noPanic runs call and fails the test if it panics.
func noPanic(t *testing.T, what string, call func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("%s panicked: %v", what, r)
		}
	}()
	call()
}

// A halt is signed for the networks the bridge listed when its messages were
// made. If the bridge lists another one by the time it is sent, the round is
// signed again for every network instead of a signature going elsewhere.
func TestHaltSignedForSomeNetworksIsNotSentToAnother(t *testing.T) {
	h := newHeldStart(t)
	h.evm.open()
	h.evm.halted.Store(true)
	batch, messages, err := h.m.GetHaltMessages()
	if err != nil || len(messages) != 1 {
		t.Fatalf("%d messages, err %v", len(messages), err)
	}
	second := newHeldEvm(t, h.bridge)
	second.open()
	h.addEvmNetwork(2, second.url)
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	noPanic(t, "sending the halt", func() {
		changed, err := h.m.SendHaltEvm(batch, [][]byte{{1}}, key, crypto.PubkeyToAddress(key.PublicKey))
		if !errors.Is(err, ErrNetworksChanged) || changed {
			t.Fatalf("changed %v, err %v; want the round to be signed again", changed, err)
		}
	})
	if calls := second.calls.Load(); calls != 0 {
		t.Fatalf("%d calls to the network the halt was not signed for", calls)
	}
	if h.m.Covers(batch) {
		t.Fatal("a batch without the added network covers it")
	}
}

// A network the bridge removed needs no halt, and the others keep the
// signatures made for them.
func TestSignaturesStayWithTheirNetworksAfterARemoval(t *testing.T) {
	h := newHeldStart(t)
	second := newHeldEvm(t, h.bridge)
	second.open()
	h.addEvmNetwork(2, second.url)
	batch := h.m.EvmBatch()
	h.m.RemoveEvmNetwork(1)
	targets, err := h.m.targets(batch, [][]byte{{1}, {2}})
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 || targets[0].network.ChainId() != 2 || targets[0].index != 1 {
		t.Fatalf("targets %+v, want chain 2 with the signature at index 1", targets)
	}
	if _, err := h.m.targets(batch, [][]byte{{1}}); err == nil {
		t.Fatal("fewer signatures than networks in the round were accepted")
	}
}

// A key change is signed for the networks the bridge listed when its messages
// were made. A network the bridge adds during the round has no signature in
// it: it is named, and the networks the round was signed for still get the
// new key.
func TestKeyChangeReachesItsNetworksWhenTheBridgeAddsOne(t *testing.T) {
	h := newHeldStart(t)
	h.evm.open()
	batch := h.m.EvmBatch()
	second := newHeldEvm(t, h.bridge)
	second.open()
	h.addEvmNetwork(2, second.url)
	tssKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	newKey := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&tssKey.PublicKey))
	newTss := crypto.PubkeyToAddress(tssKey.PublicKey)
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	send := func() (bool, error) {
		var changed bool
		var err error
		noPanic(t, "sending the key change", func() {
			changed, err = h.m.SetTssEcdsaPubKeyEvm(batch, [][]byte{{1}}, [][]byte{{2}}, newKey, key, crypto.PubkeyToAddress(key.PublicKey))
		})
		return changed, err
	}

	changed, err := send()
	if !errors.Is(err, ErrNetworksChanged) || !strings.Contains(err.Error(), "EVM network 2") || changed {
		t.Fatalf("changed %v, err %v; want not changed yet, with network 2 named", changed, err)
	}
	if sent := h.evm.sent.Load(); sent != 1 {
		t.Fatalf("%d key changes sent to the network the round was signed for, want 1", sent)
	}
	if sent := second.sent.Load(); sent != 0 {
		t.Fatalf("%d transactions sent to the network the round was not signed for", sent)
	}

	h.evm.tss.Store(&newTss)
	if changed, err := send(); !errors.Is(err, ErrNetworksChanged) || !changed {
		t.Fatalf("changed %v, err %v; want the round's networks done, with network 2 named", changed, err)
	}
	second.tss.Store(&newTss)
	if changed, err := send(); err != nil || !changed {
		t.Fatalf("changed %v, err %v; want done", changed, err)
	}
	if sent := h.evm.sent.Load() + second.sent.Load(); sent != 1 {
		t.Fatalf("%d transactions sent in all, want 1", sent)
	}
}

// A key change reports only networks the bridge lists. A network of the round
// the bridge removed gets nothing, the others still get theirs, and a network
// the bridge added and removed again during the call is not named.
func TestKeyChangeReportsOnlyNetworksTheBridgeLists(t *testing.T) {
	h := newHeldStart(t)
	h.evm.open()
	second := newHeldEvm(t, h.bridge)
	second.open()
	h.addEvmNetwork(2, second.url)
	batch := h.m.EvmBatch()
	h.m.RemoveEvmNetwork(1)
	third := newHeldEvm(t, h.bridge)
	third.open()
	h.addEvmNetwork(3, third.url)
	removeThird := func() { h.m.RemoveEvmNetwork(3) }
	second.onTssRead.Store(&removeThird)
	tssKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	newKey := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&tssKey.PublicKey))
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}

	noPanic(t, "sending the key change", func() {
		changed, err := h.m.SetTssEcdsaPubKeyEvm(batch, [][]byte{{1}, {2}}, [][]byte{{3}, {4}}, newKey, key, crypto.PubkeyToAddress(key.PublicKey))
		if err != nil || changed {
			t.Fatalf("changed %v, err %v; want not changed yet and no network named", changed, err)
		}
	})
	if sent := [3]int32{h.evm.sent.Load(), second.sent.Load(), third.sent.Load()}; sent != [3]int32{0, 1, 0} {
		t.Fatalf("transactions sent to chains 1, 2, 3: %v; want only chain 2's", sent)
	}
}

// The administrator halts each network of a batch on its own. One the bridge
// removes in the meantime needs no halt, and no other takes its place.
func TestAdministratorHaltKeepsEachWorkerOnItsNetwork(t *testing.T) {
	h := newHeldStart(t)
	h.evm.open()
	second := newHeldEvm(t, h.bridge)
	second.open()
	second.halted.Store(true)
	h.addEvmNetwork(2, second.url)
	batch := h.m.EvmBatch()
	h.m.RemoveEvmNetwork(1)
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	for idx := 0; idx < batch.Len(); idx++ {
		noPanic(t, fmt.Sprintf("the worker for index %d", idx), func() {
			halted, err := h.m.SendHaltEvmAdministrator(batch, idx, key, crypto.PubkeyToAddress(key.PublicKey))
			if err != nil || !halted {
				t.Fatalf("index %d: halted %v, err %v; want halted", idx, halted, err)
			}
		})
	}
}

// hookedEvmStore runs a hook when the scan reads a stored request.
type hookedEvmStore struct {
	db.EvmStorage
	onRead func()
}

func (s *hookedEvmStore) GetUnwrapRequestByHashAndLog(tx ecommon.Hash, index uint32) (*events.UnwrapRequestEvm, error) {
	if hook := s.onRead; hook != nil {
		s.onRead = nil
		hook()
	}
	return s.EvmStorage.GetUnwrapRequestByHashAndLog(tx, index)
}

// The affiliate settings can change while the scan interprets an event, from
// a receive the listener reads live. The policy read before the change is not
// fixed for the event; the event is read again under the new settings.
func TestAffiliateSettingsChangedDuringTheReadAreNotFixed(t *testing.T) {
	h := newHeldStart(t)
	h.n.state.SetTokensMap(1, types.ZnnTokenStandard.String(), h.bridge.token.Hex())
	h.n.state.SetIsAffiliateProgram(common.AffiliateProgram{Networks: map[uint32]common.AffiliateNetwork{1: {StartingHeight: 1, WZNN: true}}})
	read := make(chan struct{})
	close(read)
	h.net.bridgeRead = read
	store := &hookedEvmStore{EvmStorage: h.stores.evm[1]}
	store.onRead = func() {
		h.n.state.SetIsAffiliateProgram(common.AffiliateProgram{Networks: map[uint32]common.AffiliateNetwork{1: {StartingHeight: 1}}})
	}
	h.stores.evm[1] = store
	log := h.bridge.sourceLog(t, testRecipient+common.AffiliateProgramAddressSeparator+testRecipient)

	transfers, err := h.net.unwrapRequestsFromLog(log, nil)
	if !errors.Is(err, errSettingsChanged) {
		t.Fatalf("%d transfers, err %v; want the event read again", len(transfers), err)
	}
	if _, pinned, err := store.UnwrapAffiliatePolicy(affiliateEvidenceKey(log)); err != nil || pinned {
		t.Fatalf("settings fixed %v, err %v; want none fixed", pinned, err)
	}
	transfers, err = h.net.unwrapRequestsFromLog(log, nil)
	policy, pinned, storeErr := store.UnwrapAffiliatePolicy(affiliateEvidenceKey(log))
	if err != nil || storeErr != nil || len(transfers) != 1 || !pinned || policy&common.SplitAffiliate != 0 {
		t.Fatalf("%d transfers, err %v, policy %d fixed %v (%v); want the transfer whole under the new settings", len(transfers), err, policy, pinned, storeErr)
	}
}

// The receive of a wrap request's signature adds the request only if this
// node has no record of it. A record the scan of the destination chain wrote
// keeps its redeem status.
func TestWrapSignatureReceiveKeepsTheRecordTheScanWrote(t *testing.T) {
	for _, tc := range []struct {
		name   string
		stored *uint32
		want   uint32
	}{
		{"no row yet", nil, common.UnredeemedStatus},
		{"redeemed", &[]uint32{common.RedeemedStatus}[0], common.RedeemedStatus},
		{"redeem registered", &[]uint32{common.PendingRedeemStatus}[0], common.PendingRedeemStatus},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newReplayBridge(t)
			id := types.Hash{9}
			request := &definition.WrapTokenRequest{NetworkClass: definition.EvmClass, ChainId: 1, Id: id, ToAddress: "0x04",
				TokenStandard: types.ZnnTokenStandard, TokenAddress: strings.ToLower(b.token.Hex()), Amount: big.NewInt(100), Fee: big.NewInt(1), Signature: "signature"}
			url := replayServer(t, func(method string) interface{} {
				if method == "embedded.bridge.getWrapTokenRequestById" {
					return request
				}
				t.Errorf("unexpected method %s", method)
				return nil
			})
			n, stores := replayNetwork(t, b, url, nil)
			if tc.stored != nil {
				row := common.ZnnWrapToOrchestratorWrap(request)
				row.RedeemStatus = *tc.stored
				if err := stores.znn.AddWrapRequest(row); err != nil {
					t.Fatal(err)
				}
			}
			data, err := definition.ABIBridge.PackMethod(definition.UpdateWrapRequestMethodName, id, request.Signature)
			if err != nil {
				t.Fatal(err)
			}
			send := &api.AccountBlock{AccountBlock: nom.AccountBlock{Hash: types.Hash{8}, Data: data}}
			if err := n.InterpretSendBlockData(send, false, 42); err != nil {
				t.Fatal(err)
			}
			row, err := stores.znn.GetWrapRequestById(id)
			if err != nil || row == nil {
				t.Fatalf("stored row %v, err %v", row, err)
			}
			if row.RedeemStatus != tc.want || row.Signature != request.Signature || !row.SentSignature {
				t.Fatalf("stored status %d, signature %q, sent %v; want status %d with the signature recorded as sent", row.RedeemStatus, row.Signature, row.SentSignature, tc.want)
			}
		})
	}
}

// While the scan is ahead of the bridge read, bridge settings that no longer
// list the event's chain do not confirm the node's policy for it, also when
// that policy is not to split: earlier updates the read has not reached may
// have set the chain otherwise.
func TestUnlistedChainDoesNotConfirmAPolicyOfNoSplit(t *testing.T) {
	h := newHeldStart(t)
	h.n.state.SetTokensMap(1, types.ZnnTokenStandard.String(), h.bridge.token.Hex())
	h.n.state.SetIsAffiliateProgram(common.AffiliateProgram{Networks: map[uint32]common.AffiliateNetwork{1: {StartingHeight: 1}}})
	h.zenon.change(func(z *heldZenon) {
		z.registered = map[uint32]*definition.UnwrapTokenRequest{}
		z.metadata = `{"affiliateProgram":{"networks":{"2":{"startingHeight":1,"wZNN":true}}}}`
	})
	h.net.bridgeRead = make(chan struct{}) // the bridge account is not read yet
	log := h.bridge.sourceLog(t, testRecipient+common.AffiliateProgramAddressSeparator+testRecipient)

	transfers, err := h.net.unwrapRequestsFromLog(log, nil)
	_, pinned, storeErr := h.stores.evm[1].UnwrapAffiliatePolicy(affiliateEvidenceKey(log))
	if storeErr != nil {
		t.Fatal(storeErr)
	}
	if !errors.Is(err, errSettingsStale) || pinned {
		t.Fatalf("%d transfers, err %v, settings fixed %v; want the event deferred", len(transfers), err, pinned)
	}
}

// While the scan is ahead of the bridge read, a program that does not list the
// event's chain confirms the node's policy for it when it is the program the
// node applied last, an empty program included. Such an event is not deferred:
// a deferred event holds its range, and the scan has to pass it to record the
// source of an event a held start waits for.
func TestUnlistedChainUnderTheProgramAppliedLastIsNotDeferred(t *testing.T) {
	for _, tc := range []struct {
		name     string
		metadata string
		applied  map[uint32]common.AffiliateNetwork
	}{
		{"no program", `{}`, nil},
		{"a program for another chain", `{"affiliateProgram":{"networks":{"2":{"startingHeight":1}}}}`,
			map[uint32]common.AffiliateNetwork{2: {StartingHeight: 1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHeldStart(t)
			h.n.state.SetTokensMap(1, types.ZnnTokenStandard.String(), h.bridge.token.Hex())
			h.n.state.SetIsAffiliateProgram(common.AffiliateProgram{Networks: tc.applied})
			h.zenon.change(func(z *heldZenon) {
				z.registered = map[uint32]*definition.UnwrapTokenRequest{}
				z.metadata = tc.metadata
			})
			h.net.bridgeRead = make(chan struct{}) // the bridge account is not read yet
			log := h.bridge.sourceLog(t, testRecipient+common.AffiliateProgramAddressSeparator+testRecipient)

			transfers, err := h.net.unwrapRequestsFromLog(log, nil)
			policy, pinned, storeErr := h.stores.evm[1].UnwrapAffiliatePolicy(affiliateEvidenceKey(log))
			if err != nil || storeErr != nil || len(transfers) != 1 || !pinned || policy != 0 {
				t.Fatalf("%d transfers, err %v, policy %d fixed %v (%v); want the transfer whole with no split fixed", len(transfers), err, policy, pinned, storeErr)
			}
		})
	}
}
