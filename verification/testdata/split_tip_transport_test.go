// Loaded into package node by TestSplitTipBridgeSignatureRequestsAgreeWithBend.
package node

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	tcommon "github.com/HyperCore-Team/go-tss/common"
	"github.com/HyperCore-Team/go-tss/conversion"
	"github.com/HyperCore-Team/go-tss/keysign"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/zenon-network/go-zenon/chain/nom"
	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/api"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"github.com/zenon-network/go-zenon/vm/embedded/implementation"
	"github.com/zenon-network/go-zenon/vm/vm_context"
	"go.uber.org/zap"

	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/tss"
	"orchestrator/verification"
	"orchestrator/verification/testdata/signtransport"
)

func TestRequestsAtSigningTransport(t *testing.T) {
	for _, tc := range []struct {
		name string
		tips []byte
	}{
		{"one to one", []byte{1, 2}},
		{"two to one", []byte{1, 1, 2}},
		{"minority first", []byte{2, 1, 1}},
		{"unanimous", []byte{1, 1, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) { checkBridgeRequests(t, tc.tips) })
	}
}

const testCeremony uint64 = 17

type signingPeer struct {
	chain      *fixtureCeremony
	node       *Node
	messages   []string
	commitment string
	requests   int
}

func newSigningPeer(t *testing.T, tip byte, key string) *signingPeer {
	return buildSigningPeer(t, tip, key, "")
}

func buildSigningPeer(t *testing.T, tip byte, key, activeBridgeKey string) *signingPeer {
	t.Helper()
	events := candidates(6)
	for _, event := range events {
		event.To = types.ZeroAddress.String()
	}
	c := signerWithEvents(t, events)
	c.ceremony = &fixtureCeremony{height: testCeremony * 10, size: 10}
	c.signingKey = activeBridgeKey
	c.head = func() (*api.AccountBlockMarshal, error) {
		return frontierBlock(types.BridgeContract, types.Hash{tip}, uint64(10+tip)), nil
	}
	if tip == 2 {
		c.lookup = func(i uint32) (*definition.UnwrapTokenRequest, error) {
			if i == 0 {
				return nil, constants.ErrDataNonExistent
			}
			return stableLookup(i)
		}
	}
	// All signers receive the same six valid transfers. Independently encode
	// the four expected bridge messages to distinguish them from a commitment.
	order := []int{1, 3, 4, 5}
	if tip == 2 {
		order = []int{0, 1, 3, 4}
	}
	var messages []string
	for _, i := range order {
		msg, err := events[i].GetMessage()
		if err != nil {
			t.Fatal(err)
		}
		messages = append(messages, base64.StdEncoding.EncodeToString(msg))
	}
	state := common.NewGlobalState(new(uint8))
	networks, _ := c.openNetworks(t, state)
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := tss.NewTssManager(config.TssManagerConfig{PublicKey: key, BaseDir: t.TempDir()}, base64.StdEncoding.EncodeToString(priv))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(manager.Stop)
	if err := state.SetFrontierMomentum(testCeremony * networks.WindowSize()); err != nil {
		t.Fatal(err)
	}
	return &signingPeer{
		chain:    c.ceremony,
		node:     &Node{networksManager: networks, tssManager: manager, state: state, logger: zap.NewNop().Sugar()},
		messages: messages,
	}
}

func checkBridgeRequests(t *testing.T, tips []byte) {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	pub := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&key.PublicKey))
	threshold, err := conversion.GetThreshold(len(tips))
	if err != nil {
		t.Fatal(err)
	}
	previous := common.SignCeremonyPoolSize
	common.SignCeremonyPoolSize = 4
	defer func() { common.SignCeremonyPoolSize = previous }()
	var peers []*signingPeer
	identities := map[string]*signingPeer{}
	support := map[byte]int{}
	for _, tip := range tips {
		support[tip]++
		p := newSigningPeer(t, tip, pub)
		peers = append(peers, p)
		identities[p.node.tssManager.GetPeerPubKey()] = p
	}

	// The transport issues a test certificate only after enough distinct peers
	// actually submit the same commitment. Count bridge requests on receipt,
	// before returning a failed response to end the test without signing transfers.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var mu sync.Mutex
	votes := map[string]int{}
	seen := map[string]bool{}
	firstRound := make(chan struct{})
	signtransport.Receive = func(id string, r keysign.Request) (keysign.Response, error) {
		var empty keysign.Response
		p := identities[id]
		if p == nil || r.PoolPubKey != pub || r.Algo != "ecdsa" {
			t.Errorf("unexpected signer, key, or algorithm")
			return empty, errors.New("invalid signer")
		}
		bridge := slices.Equal(p.messages, r.Messages)
		if !bridge && len(r.Messages) != 1 {
			t.Errorf("unexpected ordered bridge messages %v", r.Messages)
			return empty, errors.New("unexpected messages")
		}
		mu.Lock()
		if bridge {
			p.requests++
		} else {
			if seen[id] {
				mu.Unlock()
				t.Errorf("duplicate commitment from %s", id)
				return empty, errors.New("duplicate commitment")
			}
			p.commitment = r.Messages[0]
			votes[p.commitment]++
		}
		if !seen[id] {
			seen[id] = true
			if len(seen) == len(peers) {
				close(firstRound)
			}
		}
		mu.Unlock()
		if bridge {
			return keysign.Response{Status: tcommon.Fail}, nil
		}
		select {
		case <-firstRound:
		case <-ctx.Done():
			return empty, ctx.Err()
		}
		if votes[p.commitment] <= threshold {
			return empty, errors.New("no signing quorum")
		}
		digest, err := base64.StdEncoding.DecodeString(p.commitment)
		if err != nil {
			return empty, err
		}
		sig, err := crypto.Sign(digest, key)
		if err != nil {
			return empty, err
		}
		return keysign.Response{Status: tcommon.Success, Signatures: []keysign.Signature{{Msg: p.commitment, Signature: base64.StdEncoding.EncodeToString(sig[:64])}}}, nil
	}
	defer func() { signtransport.Receive = nil }()
	var wg sync.WaitGroup
	for _, p := range peers {
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()
			_, _ = runScheduledUnwrap(t, n, testCeremony)
		}(p.node)
	}
	wg.Wait()
	if ctx.Err() != nil {
		t.Fatal("timed out waiting for signer requests")
	}
	if len(seen) != len(peers) {
		t.Fatalf("only %d of %d signers reached the signing transport", len(seen), len(peers))
	}
	var checks verification.Comparisons
	for i, p := range peers {
		// The oracle uses only the stable views assigned by the scenario.
		supporters := support[tips[i]]
		t.Logf("signer %d, view %d, support %d/%d: %d bridge-signature requests", i, tips[i], supporters, len(peers), p.requests)
		if p.requests > 1 {
			t.Errorf("duplicate bridge-signature request from signer %d", i)
		}
		checks.Equal(fmt.Sprintf("B.can_certify(%dn, %dn)", supporters, len(peers)-supporters), verification.Bool(p.requests > 0), "Bool")
	}
	checks.Check(t)
}

func runScheduledUnwrap(t *testing.T, n *Node, number uint64) (error, bool) {
	t.Helper()
	// The same fixture can also be run on dev, before certificates existed.
	switch run := any(n.processSignaturesUnwrap).(type) {
	case func() (error, bool):
		return run()
	case func(uint64) (error, bool):
		return run(number)
	case func(uint64, uint64) (error, bool):
		return run(number, n.networksManager.WindowSize())
	default:
		t.Fatal("unsupported unwrap entry point")
		return nil, false
	}
}

type windowTransport struct {
	first, later *signingPeer
	commitments  []string
	bridgeCalls  int
	onAgreement  func()
}

func newWindowTransport(t *testing.T) *windowTransport {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	pub := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&key.PublicKey))
	first := newSigningPeer(t, 1, pub)
	later := newSigningPeer(t, 2, pub)
	// Two different chain views, but one signer, signing key and vote database.
	later.node.tssManager = first.node.tssManager
	f := &windowTransport{first: first, later: later}
	signtransport.Receive = func(_ string, r keysign.Request) (keysign.Response, error) {
		if len(r.Messages) != 1 {
			f.bridgeCalls++
			return keysign.Response{Status: tcommon.Fail}, nil
		}
		f.commitments = append(f.commitments, r.Messages[0])
		if f.onAgreement != nil {
			f.onAgreement()
		}
		d, err := base64.StdEncoding.DecodeString(r.Messages[0])
		if err != nil {
			return keysign.Response{}, err
		}
		sig, err := crypto.Sign(d, key)
		if err != nil {
			return keysign.Response{}, err
		}
		return keysign.Response{Status: tcommon.Success, Signatures: []keysign.Signature{{Msg: r.Messages[0], Signature: base64.StdEncoding.EncodeToString(sig[:64])}}}, nil
	}
	t.Cleanup(func() { signtransport.Receive = nil })
	return f
}

func (p *signingPeer) attempt(t *testing.T, height, size uint64) error {
	t.Helper()
	p.chain.set(height, size)
	p.node.networksManager.Znn().SetWindowSize(size)
	if err := p.node.state.SetFrontierMomentum(height); err != nil {
		t.Fatal(err)
	}
	err, _ := runScheduledUnwrap(t, p.node, height/size)
	return err
}

func TestWindowResizeAtSigningTransport(t *testing.T) {
	f := newWindowTransport(t)
	if err := f.first.attempt(t, 1750, 100); err != nil {
		t.Fatal(err)
	}
	if err := f.later.attempt(t, 3500, 200); err != nil {
		t.Fatalf("later window reused number 17 and blocked a new pool: %v", err)
	}
	if len(f.commitments) != 2 || f.bridgeCalls != 2 {
		t.Fatalf("got %d agreement requests and %d bridge requests, want two of each", len(f.commitments), f.bridgeCalls)
	}
	if err := f.first.attempt(t, 3501, 200); err == nil || len(f.commitments) != 2 || f.bridgeCalls != 2 {
		t.Fatal("signer could change its vote within the resized window")
	}

	// An already-issued certificate must also expire when governance resizes
	// the window while the round number stays 17 at the observed height.
	f.onAgreement = func() { f.later.chain.set(3502, 201); f.later.node.networksManager.Znn().SetWindowSize(201) }
	if err := f.later.attempt(t, 3502, 200); err == nil || len(f.commitments) != 3 || f.bridgeCalls != 2 {
		t.Fatalf("round changed during agreement: err=%v, agreements=%d, bridge requests=%d", err, len(f.commitments), f.bridgeCalls)
	}
}

// The bridge setter uses only contract storage. Exercise its real receive
// handler so the window sequence below comes from contract state.
type bridgeWindowContext struct {
	vm_context.AccountVmContext
	store zdb.DB
}

func (c bridgeWindowContext) Storage() zdb.DB { return c.store }

func TestRestoredWindowAtSigningTransport(t *testing.T) {
	f := newWindowTransport(t)
	ctx := bridgeWindowContext{store: zdb.DisableNotFound(zdb.NewMemDB())}
	method := implementation.SetOrchestratorInfoMethod{MethodName: definition.SetOrchestratorInfoMethodName}
	setSize := func(size uint64) definition.OrchestratorInfo {
		t.Helper()
		data, err := definition.ABIBridge.PackMethod(method.MethodName, size, uint32(1), uint32(1), uint32(1))
		if err != nil {
			t.Fatal(err)
		}
		send := &nom.AccountBlock{Address: constants.InitialBridgeAdministrator, Amount: new(big.Int), Data: data}
		if _, err := method.ReceiveBlock(ctx, send); err != nil {
			t.Fatal(err)
		}
		info, err := definition.GetOrchestratorInfoVariable(ctx.Storage())
		if err != nil {
			t.Fatal(err)
		}
		if info.WindowSize != size {
			t.Fatalf("contract stored window size %d, want %d", info.WindowSize, size)
		}
		return *info
	}

	original := setSize(100)
	if err := f.first.attempt(t, 1710, original.WindowSize); err != nil {
		t.Fatal(err)
	}
	shorter := setSize(50)
	if err := f.first.attempt(t, 1740, shorter.WindowSize); err != nil {
		t.Fatal(err)
	}
	restored := setSize(100)
	if restored != original {
		t.Fatalf("restored contract settings differ: original=%+v restored=%+v", original, restored)
	}

	// One signer's persisted vote, two possible pools. The contract stores
	// no activation identifier. Under the PR's interval-based signing rule,
	// both heights 1710 and 1760 are in [1700, 1800) after size 100 returns.
	if err := f.later.attempt(t, 1760, restored.WindowSize); err == nil || !strings.Contains(err.Error(), "already voted for another unwrap pool") || len(f.commitments) != 2 || f.bridgeCalls != 2 {
		t.Fatalf("conflicting pool in restored interval: err=%v agreements=%d bridge calls=%d", err, len(f.commitments), f.bridgeCalls)
	}
	if err := f.first.attempt(t, 1761, restored.WindowSize); err != nil {
		t.Fatalf("retry original pool: %v", err)
	}
	if len(f.commitments) != 3 || f.commitments[0] != f.commitments[2] || f.bridgeCalls != 3 {
		t.Fatal("retry of the original pool did not reuse its commitment and reach bridge signing")
	}
	if err := f.later.attempt(t, 1900, restored.WindowSize); err != nil {
		t.Fatalf("new pool in next unwrap-first window: %v", err)
	}
	if len(f.commitments) != 4 || f.bridgeCalls != 4 {
		t.Fatal("new pool never reached bridge signing in window 19")
	}
}

// Observe the first bridge request from the polling loop in its isolated test
// process. A real certificate from the fixture key is required to reach it.
func observeLiveUnwrap(t *testing.T, p *signingPeer, key *ecdsa.PrivateKey, onFirstAgreement func()) []string {
	t.Helper()
	var agreements, batch []string
	signtransport.Receive = func(_ string, r keysign.Request) (keysign.Response, error) {
		if len(r.Messages) != 1 {
			batch = slices.Clone(r.Messages)
			runtime.Goexit()
		}
		agreements = append(agreements, r.Messages[0])
		if len(agreements) == 1 && onFirstAgreement != nil {
			onFirstAgreement()
		}
		digest, err := base64.StdEncoding.DecodeString(r.Messages[0])
		if err != nil {
			return keysign.Response{}, err
		}
		sig, err := crypto.Sign(digest, key)
		if err != nil {
			return keysign.Response{}, err
		}
		return keysign.Response{Status: tcommon.Success, Signatures: []keysign.Signature{{Msg: r.Messages[0], Signature: base64.StdEncoding.EncodeToString(sig[:64])}}}, nil
	}
	done := make(chan struct{})
	go func() { defer close(done); p.node.processSignatures() }()
	select {
	case <-done:
		signtransport.Receive = nil
	case <-time.After(55 * time.Second):
		t.Fatal("live scheduler never requested bridge signatures")
	}
	if !slices.Equal(batch, p.messages) {
		t.Fatalf("unexpected bridge batch: %v", batch)
	}
	return agreements
}

func newLiveSigningPeer(t *testing.T) (*signingPeer, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	pub := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&key.PublicKey))
	return buildSigningPeer(t, 1, pub, pub), key
}

func TestLiveLoopRetriesExpiredUnwrap(t *testing.T) {
	p, key := newLiveSigningPeer(t)
	p.chain.set(1799, 100)
	p.node.networksManager.Znn().SetWindowSize(100)
	if err := p.node.state.SetFrontierMomentum(1799); err != nil {
		t.Fatal(err)
	}
	agreements := observeLiveUnwrap(t, p, key, func() {
		p.chain.set(1800, 100)
		if err := p.node.state.SetFrontierMomentum(1800); err != nil {
			t.Error(err)
		}
	})
	height, err := p.node.state.GetFrontierMomentum()
	if err != nil || height != 1800 || len(agreements) != 2 || agreements[0] == agreements[1] {
		t.Fatalf("expected fresh agreement at height 1800, got height=%d agreements=%v err=%v", height, agreements, err)
	}
}

func TestEmergencyAfterAgreementStopsBridgeSigning(t *testing.T) {
	f := newWindowTransport(t)
	f.onAgreement = func() {
		if err := f.first.node.state.SetState(common.EmergencyState); err != nil {
			t.Fatal(err)
		}
	}
	err := f.first.attempt(t, 1750, 100)
	if err == nil || len(f.commitments) != 1 || f.bridgeCalls != 0 {
		t.Fatalf("emergency during agreement: agreements=%d bridge requests=%d err=%v", len(f.commitments), f.bridgeCalls, err)
	}
	// The state can also change while the signer is fetching its final
	// window snapshot. A state check made before that read is too early.
	if err := f.first.node.state.SetState(common.LiveState); err != nil {
		t.Fatal(err)
	}
	f.onAgreement = func() {
		chain := f.first.chain
		chain.mu.Lock()
		defer chain.mu.Unlock()
		chain.afterTip = func() {
			chain.afterTip = nil
			if err := f.first.node.state.SetState(common.EmergencyState); err != nil {
				t.Error(err)
			}
		}
	}
	if err := f.first.attempt(t, 1751, 100); err == nil || f.bridgeCalls != 0 {
		t.Fatalf("emergency during final chain read: bridge requests=%d err=%v", f.bridgeCalls, err)
	}
	f.onAgreement = nil
	if err := f.first.node.state.SetState(common.LiveState); err != nil {
		t.Fatal(err)
	}
	if err := f.first.attempt(t, 1751, 100); err != nil || f.bridgeCalls != 1 {
		t.Fatalf("live signer could not retry original pool: bridge requests=%d err=%v", f.bridgeCalls, err)
	}
}

func TestChangingFrontierCannotAdmitMixedWindow(t *testing.T) {
	f := newWindowTransport(t)
	p := f.first
	f.onAgreement = func() {
		// While approval is in flight the real chain is at (1799, 50).
		// A size notification can reach the local cache before the next
		// height notification. During a fresh read, deliver the boundary
		// immediately after the endpoint sampled the first frontier.
		p.chain.mu.Lock()
		defer p.chain.mu.Unlock()
		p.chain.height, p.chain.size = 1799, 50
		p.chain.afterTip = func() {
			p.chain.height, p.chain.size = 1800, 100
			p.chain.afterTip = nil
		}
	}
	err := p.attempt(t, 1799, 100)
	if err == nil || len(f.commitments) != 1 || f.bridgeCalls != 0 {
		t.Fatalf("stale certificate for (17,100): agreements=%d bridge requests=%d err=%v", len(f.commitments), f.bridgeCalls, err)
	}
	f.onAgreement = nil
	if err := p.attempt(t, 1800, 100); err != nil || f.bridgeCalls != 1 {
		t.Fatalf("stable new interval did not sign: bridge requests=%d err=%v", f.bridgeCalls, err)
	}
	// A replacement at the same height is still a different tip. If the
	// endpoint cannot supply one stable snapshot, keep the certificate gated.
	f.onAgreement = func() {
		p.chain.mu.Lock()
		defer p.chain.mu.Unlock()
		p.chain.afterTip = func() { p.chain.fork++ }
	}
	if err := p.attempt(t, 1801, 100); err == nil || f.bridgeCalls != 1 {
		t.Fatalf("changing hashes at the same height: bridge requests=%d err=%v", f.bridgeCalls, err)
	}
	// Retain the original local expiry guard if the RPC itself lags behind
	// an already observed subscription height.
	f.onAgreement = func() {
		p.chain.mu.Lock()
		p.chain.afterTip = nil
		p.chain.mu.Unlock()
		if err := p.node.state.SetFrontierMomentum(1800); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.attempt(t, 1799, 100); err == nil || f.bridgeCalls != 1 {
		t.Fatalf("lagging RPC revived an expired certificate: bridge requests=%d err=%v", f.bridgeCalls, err)
	}
}

func TestSigningRecoversAfterMissedResize(t *testing.T) {
	p, key := newLiveSigningPeer(t)
	// Subscription height is current, but neither attempt to read the
	// administrator's resize succeeded. The cache still contains size 100.
	p.node.networksManager.Znn().SetWindowSize(100)
	if err := p.node.state.SetFrontierMomentum(1760); err != nil {
		t.Fatal(err)
	}
	p.chain.set(1760, 50)
	p.chain.mu.Lock()
	p.chain.infoErr = errors.New("settings endpoint unavailable")
	p.chain.mu.Unlock()
	data, err := definition.ABIBridge.PackMethod(definition.SetOrchestratorInfoMethodName, uint64(50), uint32(1), uint32(1), uint32(1))
	if err != nil {
		t.Fatal(err)
	}
	send := &api.AccountBlock{AccountBlock: nom.AccountBlock{Address: types.ZeroAddress, Data: data}}
	for range 2 {
		if err := p.node.networksManager.Znn().InterpretSendBlockData(send, true, 55); err == nil || !strings.Contains(err.Error(), "settings endpoint unavailable") {
			t.Fatalf("did not exercise failed resize lookup: %v", err)
		}
	}
	p.chain.mu.Lock()
	p.chain.infoErr = nil
	p.chain.mu.Unlock()
	// Let the real scheduler select the window. Compare the *outgoing*
	// commitment against the contract's size and height, independently of
	// both the subscription cache and the scheduler's chosen round.
	agreements := observeLiveUnwrap(t, p, key, nil)
	h := sha256.New()
	h.Write([]byte("zenon/orchestrator/unwrap-pool/v2\x00"))
	h.Write(crypto.CompressPubkey(&key.PublicKey))
	h.Write(binary.BigEndian.AppendUint64(nil, 50))
	h.Write(binary.BigEndian.AppendUint64(nil, 35))
	for _, msg := range p.messages {
		raw, err := base64.StdEncoding.DecodeString(msg)
		if err != nil {
			t.Fatal(err)
		}
		h.Write(raw)
	}
	want := base64.StdEncoding.EncodeToString(h.Sum(nil))
	if !slices.Equal(agreements, []string{want}) || p.node.networksManager.WindowSize() != 50 {
		t.Fatalf("after RPC recovery: cached size=%d commitments=%v, want size 50 and commitment %s for window 35", p.node.networksManager.WindowSize(), agreements, want)
	}
}
