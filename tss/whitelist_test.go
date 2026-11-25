package tss

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	tcommon "github.com/HyperCore-Team/go-tss/common"
	"github.com/HyperCore-Team/go-tss/conversion"
	"github.com/HyperCore-Team/go-tss/keysign"
	"github.com/HyperCore-Team/go-tss/messages"
	"github.com/HyperCore-Team/go-tss/p2p"
	gotss "github.com/HyperCore-Team/go-tss/tss"
	"github.com/ethereum/go-ethereum/crypto"
	libp2p "github.com/libp2p/go-libp2p"
	ic "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/tendermint/tendermint/crypto/ed25519"
)

type fakeWhitelistBackend struct {
	mu      sync.RWMutex
	p2p     map[string]bool
	party   map[string]bool
	sig     map[string]bool
	deleted []string
}

func newSharedFakeWhitelistBackend(initial map[string]bool) *fakeWhitelistBackend {
	return &fakeWhitelistBackend{
		p2p:   initial,
		party: initial,
		sig:   initial,
	}
}

func newSeparateFakeWhitelistBackend() *fakeWhitelistBackend {
	return &fakeWhitelistBackend{
		p2p:   make(map[string]bool),
		party: make(map[string]bool),
		sig:   make(map[string]bool),
	}
}

func (f *fakeWhitelistBackend) GetWhitelists() (map[string]bool, map[string]bool, map[string]bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.p2p, f.party, f.sig
}

func (f *fakeWhitelistBackend) AddWhitelistEntries(peerIDs ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range peerIDs {
		if id == "" {
			continue
		}
		f.p2p[id] = true
		f.party[id] = true
		f.sig[id] = true
	}
}

func (f *fakeWhitelistBackend) DeleteWhitelistEntry(peerID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, peerID)
	delete(f.p2p, peerID)
	delete(f.party, peerID)
	delete(f.sig, peerID)
}

func (f *fakeWhitelistBackend) ReplaceWhitelist(peerIDs ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	next := make(map[string]bool, len(peerIDs))
	for _, id := range peerIDs {
		if id != "" {
			next[id] = true
		}
	}
	if &f.p2p == &f.party {
		f.p2p = next
		f.party = next
		f.sig = next
		return
	}
	f.p2p = next
	f.party = make(map[string]bool, len(next))
	f.sig = make(map[string]bool, len(next))
	for k, v := range next {
		f.party[k] = v
		f.sig[k] = v
	}
}

func TestBuildInitialWhitelistClonesFiltersFalseAndIncludesSelf(t *testing.T) {
	configured := map[string]bool{
		"peer-a": true,
		"peer-b": false,
		"":       true,
	}

	got := buildInitialWhitelist(configured, "peer-self")

	if !got["peer-a"] {
		t.Fatalf("expected peer-a to be whitelisted")
	}
	if _, exists := got["peer-b"]; exists {
		t.Fatalf("expected legacy false entry peer-b to be dropped, got map %v", got)
	}
	if _, exists := got[""]; exists {
		t.Fatalf("expected empty peer ID to be dropped, got map %v", got)
	}
	if !got["peer-self"] {
		t.Fatalf("expected self peer ID to always be included")
	}

	// Mutating the configured map must not affect the cloned map.
	configured["peer-c"] = true
	if _, exists := got["peer-c"]; exists {
		t.Fatalf("expected buildInitialWhitelist to clone the configured map")
	}
}

func TestBuildInitialWhitelistEmptyConfigStillNonEmpty(t *testing.T) {
	got := buildInitialWhitelist(nil, "peer-self")
	if len(got) != 1 || !got["peer-self"] {
		t.Fatalf("expected non-empty whitelist containing self, got %v", got)
	}
}

func TestTssManagerWhitelistAddDeleteReplaceAndClone(t *testing.T) {
	selfID := peer.ID("peer-self")
	selfKey := selfID.String()

	t.Run("shared map mirrors go-tss", func(t *testing.T) {
		shared := map[string]bool{
			selfKey:      true,
			"peer-old":   true,
			"peer-false": false,
		}
		backend := newSharedFakeWhitelistBackend(shared)
		m := &TssManager{
			peerId:         selfID,
			whitelistStore: backend,
		}

		m.AddWhitelistEntries("peer-new", "")
		if !shared["peer-new"] {
			t.Fatalf("expected peer-new to be added to shared map")
		}
		if _, exists := shared[""]; exists {
			t.Fatalf("expected empty peer ID to be ignored")
		}

		// Delete must remove the key (not set it to false) because go-tss checks map key existence.
		m.DeleteWhitelistEntry("peer-old")
		if _, exists := shared["peer-old"]; exists {
			t.Fatalf("expected peer-old key to be deleted, not left as false: %v", shared)
		}
		m.DeleteWhitelistEntry("peer-false")
		if _, exists := shared["peer-false"]; exists {
			t.Fatalf("expected peer-false key to be deleted: %v", shared)
		}

		// Self peer ID must never be deleted so whitelist cannot drop to empty (accept-all).
		m.DeleteWhitelistEntry(selfKey)
		if _, exists := shared[selfKey]; !exists {
			t.Fatalf("expected self peer ID to remain whitelisted")
		}

		// GetWhitelists must return cloned maps so caller mutations do not affect backend maps.
		p2pWL, partyWL, sigWL := m.GetWhitelists()
		p2pWL["peer-injected"] = true
		delete(partyWL, selfKey)
		sigWL["peer-other"] = true
		if _, exists := shared["peer-injected"]; exists {
			t.Fatalf("expected GetWhitelists clone isolation for p2p map")
		}
		if _, exists := shared[selfKey]; !exists {
			t.Fatalf("expected GetWhitelists clone isolation for party map")
		}
		if _, exists := shared["peer-other"]; exists {
			t.Fatalf("expected GetWhitelists clone isolation for sig map")
		}

		// ReplaceWhitelist keeps only the requested peers plus self.
		m.ReplaceWhitelist("peer-final")
		p2pWL, _, _ = m.GetWhitelists()
		if len(p2pWL) != 2 || !p2pWL[selfKey] || !p2pWL["peer-final"] {
			t.Fatalf("unexpected whitelist after ReplaceWhitelist: %v", p2pWL)
		}
	})

	t.Run("separate maps updated consistently", func(t *testing.T) {
		backend := newSeparateFakeWhitelistBackend()
		m := &TssManager{
			peerId:         selfID,
			whitelistStore: backend,
		}
		m.AddWhitelistEntries(selfKey, "peer-a")
		for name, wl := range map[string]map[string]bool{"p2p": backend.p2p, "party": backend.party, "sig": backend.sig} {
			if !wl["peer-a"] || !wl[selfKey] {
				t.Fatalf("expected %s whitelist to contain self and peer-a, got %v", name, wl)
			}
		}

		m.DeleteWhitelistEntry("peer-a")
		for name, wl := range map[string]map[string]bool{"p2p": backend.p2p, "party": backend.party, "sig": backend.sig} {
			if _, exists := wl["peer-a"]; exists {
				t.Fatalf("expected peer-a to be deleted from %s whitelist, got %v", name, wl)
			}
			if !wl[selfKey] {
				t.Fatalf("expected self to remain in %s whitelist, got %v", name, wl)
			}
		}
	})
}

func TestTssManagerDeleteLocalPubKeyRemovesAllDuplicates(t *testing.T) {
	m := &TssManager{
		localPubKeys: []string{"k1", "k2", "k1", "k3"},
	}
	m.DeleteLocalPubKey("k1")
	got := m.GetLocalPubKeys()
	if len(got) != 2 || got[0] != "k2" || got[1] != "k3" {
		t.Fatalf("unexpected localPubKeys after delete: %v", got)
	}
}

func TestTssManagerWhitelistConcurrentAccess(t *testing.T) {
	selfID := peer.ID("peer-self")
	selfKey := selfID.String()
	backend := newSharedFakeWhitelistBackend(map[string]bool{selfKey: true})
	m := &TssManager{
		peerId:         selfID,
		whitelistStore: backend,
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			peerID := "peer-" + string(rune('a'+id))
			for j := 0; j < 50; j++ {
				m.AddWhitelistEntries(peerID)
				p2pWL, _, _ := m.GetWhitelists()
				_ = p2pWL[selfKey]
				m.DeleteWhitelistEntry(peerID)
			}
		}(i)
	}
	wg.Wait()

	p2pWL, _, _ := m.GetWhitelists()
	if !p2pWL[selfKey] {
		t.Fatalf("expected self peer ID to remain present after concurrent operations")
	}
}

// TestRealTssHandlersConcurrentWhitelistUpdates exercises all three real go-tss
// libp2p stream handlers (Communication, PartyCoordinator, SignatureNotifier)
// concurrently with live AddWhitelistEntries, DeleteWhitelistEntry, and
// ReplaceWhitelist calls under -race.
func TestRealTssHandlersConcurrentWhitelistUpdates(t *testing.T) {
	serverPriv := ed25519.GenPrivKeyFromSecret([]byte("tss-whitelist-race-server"))
	serverPubB64 := base64.StdEncoding.EncodeToString(serverPriv.PubKey().Bytes())
	serverPeerID, err := conversion.GetPeerIDFromPubKey(serverPubB64)
	if err != nil {
		t.Fatalf("server peer ID: %v", err)
	}

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("close temp listener: %v", err)
	}

	conf := tcommon.TssConfig{PartyTimeout: 5 * time.Second, KeyGenTimeout: 10 * time.Second, KeySignTimeout: 10 * time.Second}
	server, err := gotss.NewTss(nil, port, serverPriv, t.Name(), t.TempDir(), conf, nil, "", messages.ECDSAKEYSIGN, map[string]bool{
		serverPeerID.String(): true,
	})
	if err != nil {
		t.Fatalf("NewTss: %v", err)
	}
	t.Cleanup(server.Stop)

	mgr := &TssManager{
		server:         server,
		whitelistStore: server,
		peerId:         serverPeerID,
	}

	clientPriv, _, err := ic.GenerateEd25519Key(bytes.NewReader(bytes.Repeat([]byte{7}, 64)))
	if err != nil {
		t.Fatalf("client key: %v", err)
	}
	clientHost, err := libp2p.New(context.Background(), libp2p.Identity(clientPriv), libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("client host: %v", err)
	}
	t.Cleanup(func() { _ = clientHost.Close() })

	serverAddr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/p2p/%s", port, serverPeerID.String()))
	if err != nil {
		t.Fatalf("server multiaddr: %v", err)
	}
	info, err := peer.AddrInfoFromP2pAddr(serverAddr)
	if err != nil {
		t.Fatalf("server addrinfo: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := clientHost.Connect(ctx, *info); err != nil {
		t.Fatalf("connect to tss server: %v", err)
	}

	protocols := []protocol.ID{
		"/p2p/tss",
		"/p2p/join-party",
		"/p2p/join-party-leader",
		"/p2p/signatureNotifier",
	}

	// Verify an unwhitelisted peer has its stream closed immediately by every handler.
	for _, proto := range protocols {
		s, err := clientHost.NewStream(ctx, serverPeerID, proto)
		if err != nil {
			t.Fatalf("open stream %s: %v", proto, err)
		}
		_ = s.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 1)
		_, readErr := s.Read(buf)
		_ = s.Close()
		if readErr == nil {
			t.Fatalf("expected unwhitelisted stream on %s to be closed", proto)
		}
	}

	// Concurrently open streams on all four real go-tss protocols while mutating
	// the live whitelist via TssManager. Under -race this immediately catches
	// any unsynchronized map access in go-tss stream handlers.
	clientID := clientHost.ID().String()
	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 60; i++ {
			mgr.AddWhitelistEntries(clientID)
			_, _, _ = mgr.GetWhitelists()
			mgr.DeleteWhitelistEntry(clientID)
			mgr.ReplaceWhitelist(clientID)
			mgr.DeleteWhitelistEntry(clientID)
		}
		close(stop)
	}()

	for _, proto := range protocols {
		proto := proto
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				sCtx, sCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				s, err := clientHost.NewStream(sCtx, serverPeerID, proto)
				sCancel()
				if err != nil {
					continue
				}
				_ = s.SetDeadline(time.Now().Add(100 * time.Millisecond))
				_, _ = s.Write([]byte{0, 0, 0, 0})
				_, _ = io.Copy(io.Discard, s)
				_ = s.Close()
			}
		}()
	}
	wg.Wait()

	// Self peer ID must still be present and client removed after final delete.
	p2pWL, partyWL, sigWL := mgr.GetWhitelists()
	for name, wl := range map[string]map[string]bool{"p2p": p2pWL, "party": partyWL, "sig": sigWL} {
		if !wl[serverPeerID.String()] {
			t.Fatalf("%s whitelist lost self peer ID: %v", name, wl)
		}
		if _, exists := wl[clientID]; exists {
			t.Fatalf("%s whitelist still contains deleted client %s: %v", name, clientID, wl)
		}
	}
}

// TestOldKeyQuorumRetainsPeerExcludedFromNewKeygenRealCoordinator verifies that
// when a peer from the active key is blamed during a keygen attempt and removed
// from pending LocalPubKeys, retaining it in the shared whitelist and deriving
// BulkSign signers from localstate-<key>.json allows the active key's 2-of-3
// quorum (e.g. after an aborted/interrupted keygen or emergency halt) to sign
// with that peer under join-party version 0.14.0, and that ReplaceWhitelist
// prunes it once a rotation succeeds.
func TestOldKeyQuorumRetainsPeerExcludedFromNewKeygenRealCoordinator(t *testing.T) {
	g := realSigningGroup(t)

	rawKey, err := base64.StdEncoding.DecodeString(g.publicKey)
	if err != nil {
		t.Fatal(err)
	}
	rotationMsg := bytes.Repeat([]byte{0x42}, 32)

	// Determine which peer is elected leader for this rotation signing request
	// so we stop a non-leader peer and force the leader to reach 2-of-3 quorum
	// specifically with the peer that was excluded from the new keygen.
	sortedKeys := slices.Clone(g.keys)
	sort.Strings(sortedKeys)
	msgID, err := tcommon.MsgToHashString([]byte(base64.StdEncoding.EncodeToString(rotationMsg) + strings.Join(sortedKeys, "")))
	if err != nil {
		t.Fatal(err)
	}
	leaderPeerID, err := p2p.LeaderNode(msgID, leaderHeight, g.ids)
	if err != nil {
		t.Fatal(err)
	}
	leaderIdx := -1
	var nonLeaders []int
	for i, id := range g.ids {
		if id == leaderPeerID {
			leaderIdx = i
		} else {
			nonLeaders = append(nonLeaders, i)
		}
	}
	if leaderIdx < 0 || len(nonLeaders) != 2 {
		t.Fatalf("unexpected leader election: leader=%s ids=%v", leaderPeerID, g.ids)
	}
	offlineIdx := nonLeaders[0]
	blamedIdx := nonLeaders[1]

	// Take offlineIdx offline so the active key (2-of-3) MUST use leaderIdx
	// and blamedIdx to sign the rotation message.
	g.stop[offlineIdx]()

	// Exclude blamedIdx from leaderIdx's pending LocalPubKeys (simulating blame
	// in a new keygen), while retaining blamedIdx in the shared whitelist until
	// rotation completes.
	g.peers[leaderIdx].DeleteLocalPubKey(g.keys[blamedIdx])
	if slices.Contains(g.peers[leaderIdx].GetLocalPubKeys(), g.keys[blamedIdx]) {
		t.Fatalf("expected blamed peer to be excluded from pending LocalPubKeys")
	}

	type signOutcome struct {
		idx int
		res *keysign.Response
		err error
	}
	online := []int{leaderIdx, blamedIdx}
	results := make(chan signOutcome, len(online))
	for _, idx := range online {
		idx := idx
		go func() {
			res, err := g.peers[idx].BulkSign([][]byte{rotationMsg}, messages.ECDSAKEYSIGN)
			results <- signOutcome{idx: idx, res: res, err: err}
		}()
	}

	for range online {
		out := <-results
		if out.err != nil || out.res == nil || out.res.Status != tcommon.Success || len(out.res.Signatures) != 1 {
			t.Fatalf("active-key rotation sign failed on peer %d: res=%v err=%v", out.idx, out.res, out.err)
		}
		sigBytes, err := base64.StdEncoding.DecodeString(out.res.Signatures[0].Signature)
		if err != nil || !crypto.VerifySignature(rawKey, rotationMsg, sigBytes) {
			t.Fatalf("invalid rotation signature from peer %d", out.idx)
		}
	}

	// Once rotation completes, pruning blamedIdx via ReplaceWhitelist removes it
	// from all three go-tss whitelist views while retaining self.
	g.peers[leaderIdx].peerId, _ = peer.Decode(g.ids[leaderIdx])
	g.peers[leaderIdx].ReplaceWhitelist(g.ids[leaderIdx])
	p2pWL, partyWL, sigWL := g.peers[leaderIdx].GetWhitelists()
	for name, wl := range map[string]map[string]bool{"p2p": p2pWL, "party": partyWL, "sig": sigWL} {
		if _, exists := wl[g.ids[blamedIdx]]; exists {
			t.Fatalf("%s whitelist still contains pruned peer after rotation: %v", name, wl)
		}
		if !wl[g.ids[leaderIdx]] {
			t.Fatalf("%s whitelist missing self peer after rotation: %v", name, wl)
		}
	}
}
