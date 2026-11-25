package node

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/HyperCore-Team/go-tss/blame"
	tcommon "github.com/HyperCore-Team/go-tss/common"
	"github.com/HyperCore-Team/go-tss/keygen"
	"github.com/HyperCore-Team/go-tss/keysign"
	ic "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"github.com/zenon-network/go-zenon/wallet"
	"go.uber.org/zap"

	"orchestrator/common"
	oconfig "orchestrator/common/config"
	"orchestrator/network"
)

type testIdentity struct {
	pubKeyBytes []byte
	pubKeyB64   string
	peerID      string
}

func newTestEd25519Identity(t *testing.T) testIdentity {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 key: %v", err)
	}
	libp2pPub, err := ic.UnmarshalEd25519PublicKey(pub)
	if err != nil {
		t.Fatalf("unmarshal ed25519 pubkey: %v", err)
	}
	pid, err := peer.IDFromPublicKey(libp2pPub)
	if err != nil {
		t.Fatalf("derive peer id: %v", err)
	}
	return testIdentity{
		pubKeyBytes: pub,
		pubKeyB64:   base64.StdEncoding.EncodeToString(pub),
		peerID:      pid.String(),
	}
}

type fakeParticipantUpdater struct {
	mu           sync.Mutex
	localPubKeys []string
	p2pWL        map[string]bool
	partyWL      map[string]bool
	sigWL        map[string]bool
	deletedPeers []string
}

func newFakeParticipantUpdater(initialPeers ...string) *fakeParticipantUpdater {
	shared := make(map[string]bool, len(initialPeers))
	for _, p := range initialPeers {
		shared[p] = true
	}
	return &fakeParticipantUpdater{
		p2pWL:   shared,
		partyWL: shared,
		sigWL:   shared,
	}
}

func (f *fakeParticipantUpdater) DeleteLocalPubKey(pubKey string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.localPubKeys = slices.DeleteFunc(f.localPubKeys, func(k string) bool { return k == pubKey })
}

func (f *fakeParticipantUpdater) SetNewLocalPubKeys(newPubKeys []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.localPubKeys = slices.Clone(newPubKeys)
}

func (f *fakeParticipantUpdater) GetLocalPubKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.localPubKeys)
}

func (f *fakeParticipantUpdater) AddWhitelistEntries(peerIDs ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range peerIDs {
		if id == "" {
			continue
		}
		f.p2pWL[id] = true
		f.partyWL[id] = true
		f.sigWL[id] = true
	}
}

func (f *fakeParticipantUpdater) DeleteWhitelistEntry(peerID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletedPeers = append(f.deletedPeers, peerID)
	delete(f.p2pWL, peerID)
	delete(f.partyWL, peerID)
	delete(f.sigWL, peerID)
}

func (f *fakeParticipantUpdater) ReplaceWhitelist(peerIDs ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	next := make(map[string]bool, len(peerIDs))
	for _, id := range peerIDs {
		if id != "" {
			next[id] = true
		}
	}
	f.p2pWL = next
	f.partyWL = next
	f.sigWL = next
}

func TestPubKeyToPeerIDConversion(t *testing.T) {
	id := newTestEd25519Identity(t)

	got, err := pubKeyToPeerID(id.pubKeyB64)
	if err != nil {
		t.Fatalf("pubKeyToPeerID valid key: %v", err)
	}
	if got.String() != id.peerID {
		t.Fatalf("pubKeyToPeerID = %q, want %q", got.String(), id.peerID)
	}

	if _, err := pubKeyToPeerID("not-valid-base64!!!"); err == nil {
		t.Fatalf("expected error for invalid base64")
	}
	if _, err := pubKeyToPeerID(base64.StdEncoding.EncodeToString([]byte("short"))); err == nil {
		t.Fatalf("expected error for invalid ed25519 key length")
	}
}

func TestRemoveParticipantConvertsPubKeyToPeerIDAndDeletesFromWhitelists(t *testing.T) {
	selfID := newTestEd25519Identity(t)
	blamedID := newTestEd25519Identity(t)
	otherID := newTestEd25519Identity(t)

	updater := newFakeParticipantUpdater(selfID.peerID, blamedID.peerID, otherID.peerID)
	updater.SetNewLocalPubKeys([]string{selfID.pubKeyB64, blamedID.pubKeyB64, otherID.pubKeyB64})

	n := &Node{
		config: &oconfig.Config{
			TssConfig: oconfig.TssManagerConfig{
				LocalPubKeys: []string{selfID.pubKeyB64, blamedID.pubKeyB64, blamedID.pubKeyB64, otherID.pubKeyB64},
				PubKeyWhitelist: map[string]bool{
					selfID.peerID:   true,
					blamedID.peerID: true,
					otherID.peerID:  true,
				},
			},
		},
		producerKeyPair:    &wallet.KeyPair{Public: selfID.pubKeyBytes},
		participantUpdater: updater,
		logger:             zap.NewNop().Sugar(),
	}

	n.removeParticipant(blamedID.pubKeyB64)

	// Blamed pubkey must be removed from both config and TSS local pubkeys (including duplicates).
	if slices.Contains(n.config.TssConfig.LocalPubKeys, blamedID.pubKeyB64) {
		t.Fatalf("blamed pubkey still present in config LocalPubKeys: %v", n.config.TssConfig.LocalPubKeys)
	}
	if slices.Contains(updater.GetLocalPubKeys(), blamedID.pubKeyB64) {
		t.Fatalf("blamed pubkey still present in TSS LocalPubKeys: %v", updater.GetLocalPubKeys())
	}

	// Blamed peer ID must be DELETED (key absent, not set to false) from both config and TSS whitelists.
	if _, exists := n.config.TssConfig.PubKeyWhitelist[blamedID.peerID]; exists {
		t.Fatalf("blamed peer ID still exists in config PubKeyWhitelist: %v", n.config.TssConfig.PubKeyWhitelist)
	}
	if _, exists := updater.p2pWL[blamedID.peerID]; exists {
		t.Fatalf("blamed peer ID still exists in TSS whitelist: %v", updater.p2pWL)
	}
	if len(updater.deletedPeers) != 1 || updater.deletedPeers[0] != blamedID.peerID {
		t.Fatalf("expected DeleteWhitelistEntry(%q), got calls %v", blamedID.peerID, updater.deletedPeers)
	}

	// Other participants and self remain intact.
	if !n.config.TssConfig.PubKeyWhitelist[otherID.peerID] || !updater.p2pWL[otherID.peerID] {
		t.Fatalf("expected other peer to remain whitelisted")
	}
	if !n.config.TssConfig.PubKeyWhitelist[selfID.peerID] || !updater.p2pWL[selfID.peerID] {
		t.Fatalf("expected self peer to remain whitelisted")
	}

	// Removing self pubkey must never delete self peer ID from whitelist (avoiding empty accept-all map).
	n.removeParticipant(selfID.pubKeyB64)
	if !n.config.TssConfig.PubKeyWhitelist[selfID.peerID] || !updater.p2pWL[selfID.peerID] {
		t.Fatalf("expected self peer ID to stay whitelisted even if self pubkey is removed")
	}

	// Malformed pubkey must not panic and must not call DeleteWhitelistEntry with raw invalid string.
	n.removeParticipant("bad-pubkey!!!")
	if len(updater.deletedPeers) != 1 {
		t.Fatalf("unexpected DeleteWhitelistEntry call for invalid pubkey: %v", updater.deletedPeers)
	}
}

func TestBlamedActiveKeySignerRetainedThroughRotationThenPruned(t *testing.T) {
	selfID := newTestEd25519Identity(t)
	activePeerB := newTestEd25519Identity(t)
	activePeerC := newTestEd25519Identity(t)
	newPeerD := newTestEd25519Identity(t)

	// Start with an active key whose participants are {self, B, C}.
	updater := newFakeParticipantUpdater(selfID.peerID, activePeerB.peerID, activePeerC.peerID)
	updater.SetNewLocalPubKeys([]string{selfID.pubKeyB64, activePeerB.pubKeyB64, activePeerC.pubKeyB64})
	n := &Node{
		config: &oconfig.Config{
			TssConfig: oconfig.TssManagerConfig{
				PublicKey:    "old-active-key",
				LocalPubKeys: []string{selfID.pubKeyB64, activePeerB.pubKeyB64, activePeerC.pubKeyB64},
				PubKeyWhitelist: map[string]bool{
					selfID.peerID:      true,
					activePeerB.peerID: true,
					activePeerC.peerID: true,
				},
			},
		},
		activeKeyPeerIDs: map[string]bool{
			selfID.peerID:      true,
			activePeerB.peerID: true,
			activePeerC.peerID: true,
		},
		producerKeyPair:    &wallet.KeyPair{Public: selfID.pubKeyBytes},
		participantUpdater: updater,
		logger:             zap.NewNop().Sugar(),
	}

	// Keygen adds candidate D.
	n.allowNewParticipants(map[string]string{newPeerD.pubKeyB64: newPeerD.peerID})
	if !updater.p2pWL[newPeerD.peerID] {
		t.Fatalf("expected new candidate D in TSS whitelist")
	}

	// Blaming candidate D (not in active key) removes D from both LocalPubKeys and whitelists immediately.
	n.removeParticipant(newPeerD.pubKeyB64)
	if slices.Contains(n.config.TssConfig.LocalPubKeys, newPeerD.pubKeyB64) {
		t.Fatalf("expected blamed candidate D removed from LocalPubKeys")
	}
	if _, exists := n.config.TssConfig.PubKeyWhitelist[newPeerD.peerID]; exists {
		t.Fatalf("expected blamed non-active peer D removed from config whitelist")
	}
	if _, exists := updater.p2pWL[newPeerD.peerID]; exists {
		t.Fatalf("expected blamed non-active peer D removed from TSS whitelist")
	}

	// Blaming activePeerC removes C from the pending keygen's LocalPubKeys,
	// but RETAINS C in the shared whitelist so the active key can still reach
	// quorum with {self, C} to authorize key rotation.
	n.removeParticipant(activePeerC.pubKeyB64)
	if slices.Contains(n.config.TssConfig.LocalPubKeys, activePeerC.pubKeyB64) {
		t.Fatalf("expected blamed activePeerC removed from pending keygen LocalPubKeys")
	}
	if slices.Contains(updater.GetLocalPubKeys(), activePeerC.pubKeyB64) {
		t.Fatalf("expected blamed activePeerC removed from TSS LocalPubKeys")
	}
	if !n.config.TssConfig.PubKeyWhitelist[activePeerC.peerID] {
		t.Fatalf("expected activePeerC retained in config whitelist while old key is active")
	}
	if !updater.p2pWL[activePeerC.peerID] {
		t.Fatalf("expected activePeerC retained in TSS whitelist while old key is active")
	}

	// Once rotation completes with {self, B}, activePeerC is pruned from both
	// config and TSS whitelists and activeKeyPeerIDs is updated.
	n.finalizeKeyRotation("new-rotated-key", "new-decompressed-key")
	if n.config.TssConfig.PublicKey != "new-rotated-key" {
		t.Fatalf("PublicKey = %q, want new-rotated-key", n.config.TssConfig.PublicKey)
	}
	if _, exists := n.config.TssConfig.PubKeyWhitelist[activePeerC.peerID]; exists {
		t.Fatalf("expected old blamed peer C pruned from config whitelist after rotation")
	}
	if _, exists := updater.p2pWL[activePeerC.peerID]; exists {
		t.Fatalf("expected old blamed peer C pruned from TSS whitelist after rotation")
	}
	if !updater.p2pWL[selfID.peerID] || !updater.p2pWL[activePeerB.peerID] {
		t.Fatalf("expected new active key peers {self, B} whitelisted after rotation, got %v", updater.p2pWL)
	}
	if n.activeKeyPeerIDs[activePeerC.peerID] || !n.activeKeyPeerIDs[activePeerB.peerID] {
		t.Fatalf("unexpected activeKeyPeerIDs after rotation: %v", n.activeKeyPeerIDs)
	}
}

func TestAbortedKeygenRestoresActiveKeyParticipantsAndRetainsActiveWhitelist(t *testing.T) {
	selfID := newTestEd25519Identity(t)
	activePeerB := newTestEd25519Identity(t)
	activePeerC := newTestEd25519Identity(t)
	candidateD := newTestEd25519Identity(t)

	initialKeys := []string{selfID.pubKeyB64, activePeerB.pubKeyB64, activePeerC.pubKeyB64}
	slices.Sort(initialKeys)

	updater := newFakeParticipantUpdater(selfID.peerID, activePeerB.peerID, activePeerC.peerID)
	updater.SetNewLocalPubKeys(initialKeys)

	n := &Node{
		config: &oconfig.Config{
			TssConfig: oconfig.TssManagerConfig{
				PublicKey:    "old-active-key",
				LocalPubKeys: slices.Clone(initialKeys),
				PubKeyWhitelist: map[string]bool{
					selfID.peerID:      true,
					activePeerB.peerID: true,
					activePeerC.peerID: true,
				},
			},
		},
		activeKeyPeerIDs: map[string]bool{
			selfID.peerID:      true,
			activePeerB.peerID: true,
			activePeerC.peerID: true,
		},
		activeKeyPubKeys:   slices.Clone(initialKeys),
		producerKeyPair:    &wallet.KeyPair{Public: selfID.pubKeyBytes},
		participantUpdater: updater,
		logger:             zap.NewNop().Sugar(),
	}

	// Simulate keygen starting with a new candidate D and then blaming activePeerC.
	n.allowNewParticipants(map[string]string{candidateD.pubKeyB64: candidateD.peerID})
	n.removeParticipant(activePeerC.pubKeyB64)
	if slices.Contains(n.config.TssConfig.LocalPubKeys, activePeerC.pubKeyB64) {
		t.Fatalf("expected activePeerC removed during pending keygen")
	}

	// When keygen exits (e.g., interrupted by EmergencyState or AllowKeyGen=false),
	// restoreActiveKeyParticipants restores LocalPubKeys to the active key set and
	// prunes non-active candidate D while retaining all active-key signers.
	n.restoreActiveKeyParticipants(true)

	if !slices.Equal(n.config.TssConfig.LocalPubKeys, initialKeys) {
		t.Fatalf("config LocalPubKeys = %v, want %v", n.config.TssConfig.LocalPubKeys, initialKeys)
	}
	if !slices.Equal(updater.GetLocalPubKeys(), initialKeys) {
		t.Fatalf("TSS LocalPubKeys = %v, want %v", updater.GetLocalPubKeys(), initialKeys)
	}
	if _, exists := n.config.TssConfig.PubKeyWhitelist[candidateD.peerID]; exists {
		t.Fatalf("expected temporary keygen candidate D pruned from config whitelist")
	}
	if _, exists := updater.p2pWL[candidateD.peerID]; exists {
		t.Fatalf("expected temporary keygen candidate D pruned from TSS whitelist")
	}
	for _, id := range []string{selfID.peerID, activePeerB.peerID, activePeerC.peerID} {
		if !n.config.TssConfig.PubKeyWhitelist[id] || !updater.p2pWL[id] {
			t.Fatalf("expected active signer %s retained after aborted keygen", id)
		}
	}
}

func TestKeyGenInterruptedByEmergencyAbortsRotationAndRestoresActiveSet(t *testing.T) {
	selfID := newTestEd25519Identity(t)
	activePeerB := newTestEd25519Identity(t)
	activePeerC := newTestEd25519Identity(t)
	candidateD := newTestEd25519Identity(t)

	initialKeys := []string{selfID.pubKeyB64, activePeerB.pubKeyB64, activePeerC.pubKeyB64}
	slices.Sort(initialKeys)

	baseDir := t.TempDir()
	generatedPubKey := base64.StdEncoding.EncodeToString(make([]byte, 33))
	generatedKeyFile, err := common.GetPublicKeyFilePath(baseDir, generatedPubKey)
	if err != nil {
		t.Fatalf("GetPublicKeyFilePath: %v", err)
	}
	if err := os.WriteFile(generatedKeyFile, []byte(`{"participant_keys":[]}`), 0600); err != nil {
		t.Fatalf("write generated key file: %v", err)
	}

	stateVal := uint8(common.KeyGenState)
	state := common.NewPersistentGlobalState(&stateVal, nil)
	updater := newFakeParticipantUpdater(selfID.peerID, activePeerB.peerID, activePeerC.peerID)
	updater.SetNewLocalPubKeys(initialKeys)

	n := &Node{
		config: &oconfig.Config{
			TssConfig: oconfig.TssManagerConfig{
				BaseDir:      baseDir,
				PublicKey:    "old-active-key",
				LocalPubKeys: slices.Clone(initialKeys),
				PubKeyWhitelist: map[string]bool{
					selfID.peerID:      true,
					activePeerB.peerID: true,
					activePeerC.peerID: true,
				},
			},
		},
		activeKeyPeerIDs: map[string]bool{
			selfID.peerID:      true,
			activePeerB.peerID: true,
			activePeerC.peerID: true,
		},
		activeKeyPubKeys:   slices.Clone(initialKeys),
		producerKeyPair:    &wallet.KeyPair{Public: selfID.pubKeyBytes},
		participantUpdater: updater,
		state:              state,
		logger:             zap.NewNop().Sugar(),
	}

	n.allowNewParticipants(map[string]string{candidateD.pubKeyB64: candidateD.peerID})

	// Simulate an emergency starting while KeyGen is executing and returning Success.
	resp := n.runKeyGenLoop(
		func() uint32 { return 3 },
		func() (*keygen.Response, error) {
			if err := state.SetState(common.EmergencyState); err != nil {
				t.Fatalf("SetState(EmergencyState): %v", err)
			}
			return &keygen.Response{
				PubKey: generatedPubKey,
				Status: tcommon.Success,
			}, nil
		},
	)
	if resp != nil {
		t.Fatalf("expected interrupted keygen to abort rotation and return nil, got %+v", resp)
	}
	if _, err := os.Stat(generatedKeyFile); !os.IsNotExist(err) {
		t.Fatalf("expected unused generated key file to be removed, stat err=%v", err)
	}
	if !slices.Equal(n.config.TssConfig.LocalPubKeys, initialKeys) || !slices.Equal(updater.GetLocalPubKeys(), initialKeys) {
		t.Fatalf("expected active LocalPubKeys restored: config=%v tss=%v", n.config.TssConfig.LocalPubKeys, updater.GetLocalPubKeys())
	}
	if _, exists := n.config.TssConfig.PubKeyWhitelist[candidateD.peerID]; exists {
		t.Fatalf("expected temporary candidate D pruned from config whitelist")
	}
	if _, exists := updater.p2pWL[candidateD.peerID]; exists {
		t.Fatalf("expected temporary candidate D pruned from TSS whitelist")
	}
	for _, id := range []string{selfID.peerID, activePeerB.peerID, activePeerC.peerID} {
		if !n.config.TssConfig.PubKeyWhitelist[id] || !updater.p2pWL[id] {
			t.Fatalf("expected active signer %s retained for emergency halt signing", id)
		}
	}
}

func TestKeyGenThresholdFailureAfterAllowKeyGenClearedPrunesTempPeers(t *testing.T) {
	selfID := newTestEd25519Identity(t)
	activePeerB := newTestEd25519Identity(t)
	activePeerC := newTestEd25519Identity(t)
	candidateD := newTestEd25519Identity(t)
	candidateE := newTestEd25519Identity(t)

	initialKeys := []string{selfID.pubKeyB64, activePeerB.pubKeyB64, activePeerC.pubKeyB64}
	slices.Sort(initialKeys)

	stateVal := uint8(common.KeyGenState)
	state := common.NewPersistentGlobalState(&stateVal, nil)
	updater := newFakeParticipantUpdater(selfID.peerID, activePeerB.peerID, activePeerC.peerID)
	updater.SetNewLocalPubKeys(initialKeys)

	n := &Node{
		config: &oconfig.Config{
			TssConfig: oconfig.TssManagerConfig{
				BaseDir:      t.TempDir(),
				PublicKey:    "old-active-key",
				LocalPubKeys: slices.Clone(initialKeys),
				PubKeyWhitelist: map[string]bool{
					selfID.peerID:      true,
					activePeerB.peerID: true,
					activePeerC.peerID: true,
				},
			},
		},
		activeKeyPeerIDs: map[string]bool{
			selfID.peerID:      true,
			activePeerB.peerID: true,
			activePeerC.peerID: true,
		},
		activeKeyPubKeys:   slices.Clone(initialKeys),
		producerKeyPair:    &wallet.KeyPair{Public: selfID.pubKeyBytes},
		participantUpdater: updater,
		state:              state,
		logger:             zap.NewNop().Sugar(),
	}

	// Add two temporary keygen candidates D and E.
	n.allowNewParticipants(map[string]string{
		candidateD.pubKeyB64: candidateD.peerID,
		candidateE.pubKeyB64: candidateE.peerID,
	})

	// Blame drops participant count below threshold (5 -> 3 < 4), leaving
	// temporary candidate E unblamed while the loop exits. Even if AllowKeyGen
	// clears immediately after (transitioning to LiveState on the next loop),
	// BOTH D and surviving temporary candidate E must already be pruned.
	resp := n.runKeyGenLoop(
		func() uint32 { return 4 },
		func() (*keygen.Response, error) {
			return &keygen.Response{
				Status: tcommon.Fail,
				Blame: blame.Blame{
					BlameNodes: []blame.Node{
						{Pubkey: activePeerC.pubKeyB64},
						{Pubkey: candidateD.pubKeyB64},
					},
				},
			}, nil
		},
	)
	if resp != nil {
		t.Fatalf("expected nil response on threshold failure, got %+v", resp)
	}
	if err := state.SetState(common.LiveState); err != nil {
		t.Fatalf("SetState(LiveState): %v", err)
	}
	if !slices.Equal(n.config.TssConfig.LocalPubKeys, initialKeys) || !slices.Equal(updater.GetLocalPubKeys(), initialKeys) {
		t.Fatalf("expected active LocalPubKeys restored after threshold failure: config=%v tss=%v", n.config.TssConfig.LocalPubKeys, updater.GetLocalPubKeys())
	}
	for _, tempID := range []string{candidateD.peerID, candidateE.peerID} {
		if _, exists := n.config.TssConfig.PubKeyWhitelist[tempID]; exists {
			t.Fatalf("expected temporary peer %s pruned from config whitelist", tempID)
		}
		if _, exists := updater.p2pWL[tempID]; exists {
			t.Fatalf("expected temporary peer %s pruned from TSS whitelist", tempID)
		}
	}
	for _, activeID := range []string{selfID.peerID, activePeerB.peerID, activePeerC.peerID} {
		if !n.config.TssConfig.PubKeyWhitelist[activeID] || !updater.p2pWL[activeID] {
			t.Fatalf("expected active signer %s retained after failed keygen", activeID)
		}
	}
}

func TestPrepareKeyRotationAbortsOnEmergencyAndCleansUpPreSubmissionFailures(t *testing.T) {
	oldKey := compressedKey(t, false)
	newKey := compressedKey(t, false)
	decompressedNew, err := common.DecompressPubKey(newKey)
	if err != nil {
		t.Fatalf("DecompressPubKey: %v", err)
	}

	newFixture := func(t *testing.T) (*Node, *fakeParticipantUpdater, *common.GlobalState, testIdentity, testIdentity, testIdentity, testIdentity, string, *string) {
		t.Helper()
		selfID := newTestEd25519Identity(t)
		activeB := newTestEd25519Identity(t)
		activeC := newTestEd25519Identity(t)
		tempD := newTestEd25519Identity(t)

		initialKeys := []string{selfID.pubKeyB64, activeB.pubKeyB64, activeC.pubKeyB64}
		slices.Sort(initialKeys)

		baseDir := t.TempDir()
		genFile, err := common.GetPublicKeyFilePath(baseDir, newKey)
		if err != nil {
			t.Fatalf("GetPublicKeyFilePath: %v", err)
		}
		if err := os.WriteFile(genFile, []byte(`{"participant_keys":[]}`), 0600); err != nil {
			t.Fatalf("write generated key file: %v", err)
		}

		stateVal := uint8(common.KeyGenState)
		state := common.NewPersistentGlobalState(&stateVal, nil)
		updater := newFakeParticipantUpdater(selfID.peerID, activeB.peerID, activeC.peerID)
		updater.SetNewLocalPubKeys(initialKeys)

		n := &Node{
			config: &oconfig.Config{
				TssConfig: oconfig.TssManagerConfig{
					BaseDir:      baseDir,
					PublicKey:    oldKey,
					LocalPubKeys: slices.Clone(initialKeys),
					PubKeyWhitelist: map[string]bool{
						selfID.peerID:  true,
						activeB.peerID: true,
						activeC.peerID: true,
					},
				},
			},
			activeKeyPeerIDs: map[string]bool{
				selfID.peerID:  true,
				activeB.peerID: true,
				activeC.peerID: true,
			},
			activeKeyPubKeys:   slices.Clone(initialKeys),
			producerKeyPair:    &wallet.KeyPair{Public: selfID.pubKeyBytes},
			participantUpdater: updater,
			state:              state,
			logger:             zap.NewNop().Sugar(),
		}
		n.allowNewParticipants(map[string]string{tempD.pubKeyB64: tempD.peerID})
		activePubKey := oldKey
		return n, updater, state, selfID, activeB, activeC, tempD, genFile, &activePubKey
	}

	assertCleanedUp := func(t *testing.T, n *Node, updater *fakeParticipantUpdater, selfID, activeB, activeC, tempD testIdentity, activePubKey string) {
		t.Helper()
		if activePubKey != oldKey {
			t.Fatalf("active pubkey = %q, want restored oldKey %q", activePubKey, oldKey)
		}
		wantKeys := []string{selfID.pubKeyB64, activeB.pubKeyB64, activeC.pubKeyB64}
		slices.Sort(wantKeys)
		if !slices.Equal(n.config.TssConfig.LocalPubKeys, wantKeys) || !slices.Equal(updater.GetLocalPubKeys(), wantKeys) {
			t.Fatalf("expected active LocalPubKeys restored: config=%v tss=%v", n.config.TssConfig.LocalPubKeys, updater.GetLocalPubKeys())
		}
		if _, exists := n.config.TssConfig.PubKeyWhitelist[tempD.peerID]; exists {
			t.Fatalf("expected temporary peer D pruned from config whitelist")
		}
		if _, exists := updater.p2pWL[tempD.peerID]; exists {
			t.Fatalf("expected temporary peer D pruned from TSS whitelist")
		}
		for _, id := range []string{selfID.peerID, activeB.peerID, activeC.peerID} {
			if !n.config.TssConfig.PubKeyWhitelist[id] || !updater.p2pWL[id] {
				t.Fatalf("expected active signer %s retained", id)
			}
		}
	}

	t.Run("emergency during new-key signing aborts before old-key signing", func(t *testing.T) {
		n, updater, state, selfID, activeB, activeC, tempD, genFile, activePubKey := newFixture(t)
		signCalls := 0
		plan, ok := n.prepareKeyRotation(oldKey, &keygen.Response{PubKey: newKey, Status: tcommon.Success}, decompressedNew, rotationPrepOps{
			getTssNonce:        func() (uint64, error) { return 7, nil },
			getChangePubKeyMsg: func(uint64, string) ([]byte, error) { return []byte("znn-msg"), nil },
			getEvmMessages:     func(string) (network.EvmBatch, [][]byte, error) { return network.EvmBatch{}, [][]byte{[]byte("evm-msg")}, nil },
			setActivePubKey:    func(k string) { *activePubKey = k },
			signMessages: func([][]byte, map[string]int) (*keysign.Response, error) {
				signCalls++
				if signCalls == 1 {
					// Emergency arrives during new-key signing; old-key signing must never start.
					if err := state.SetState(common.EmergencyState); err != nil {
						t.Fatalf("SetState(EmergencyState): %v", err)
					}
				}
				return &keysign.Response{Status: tcommon.Success}, nil
			},
			validateSignatures: func(*keysign.Response, string, [][]byte) (string, [][]byte, error) {
				return "sig", [][]byte{[]byte("evm-sig")}, nil
			},
		})
		if ok || plan != nil {
			t.Fatalf("expected rotation aborted before old-key signing, got ok=%v plan=%+v", ok, plan)
		}
		if signCalls != 1 {
			t.Fatalf("expected old-key signing to be skipped after emergency, signCalls=%d", signCalls)
		}
		if _, err := os.Stat(genFile); !os.IsNotExist(err) {
			t.Fatalf("expected unused generated key file removed, stat err=%v", err)
		}
		assertCleanedUp(t, n, updater, selfID, activeB, activeC, tempD, *activePubKey)
	})

	t.Run("emergency during old-key signing aborts before sender dispatch", func(t *testing.T) {
		n, updater, state, selfID, activeB, activeC, tempD, genFile, activePubKey := newFixture(t)
		signCalls := 0
		plan, ok := n.prepareKeyRotation(oldKey, &keygen.Response{PubKey: newKey, Status: tcommon.Success}, decompressedNew, rotationPrepOps{
			getTssNonce:        func() (uint64, error) { return 7, nil },
			getChangePubKeyMsg: func(uint64, string) ([]byte, error) { return []byte("znn-msg"), nil },
			getEvmMessages:     func(string) (network.EvmBatch, [][]byte, error) { return network.EvmBatch{}, [][]byte{[]byte("evm-msg")}, nil },
			setActivePubKey:    func(k string) { *activePubKey = k },
			signMessages: func([][]byte, map[string]int) (*keysign.Response, error) {
				signCalls++
				if signCalls == 2 {
					// Emergency arrives during old-key signing; rotation senders must never start.
					if err := state.SetState(common.EmergencyState); err != nil {
						t.Fatalf("SetState(EmergencyState): %v", err)
					}
				}
				return &keysign.Response{Status: tcommon.Success}, nil
			},
			validateSignatures: func(*keysign.Response, string, [][]byte) (string, [][]byte, error) {
				return "sig", [][]byte{[]byte("evm-sig")}, nil
			},
		})
		if ok || plan != nil {
			t.Fatalf("expected rotation aborted before sender dispatch, got ok=%v plan=%+v", ok, plan)
		}
		if signCalls != 2 {
			t.Fatalf("expected both signing calls before final abort, signCalls=%d", signCalls)
		}
		if _, err := os.Stat(genFile); !os.IsNotExist(err) {
			t.Fatalf("expected unused generated key file removed, stat err=%v", err)
		}
		assertCleanedUp(t, n, updater, selfID, activeB, activeC, tempD, *activePubKey)
	})

	t.Run("nonce RPC error restores active participants and prunes temporary peers", func(t *testing.T) {
		n, updater, _, selfID, activeB, activeC, tempD, _, activePubKey := newFixture(t)
		plan, ok := n.prepareKeyRotation(oldKey, &keygen.Response{PubKey: newKey, Status: tcommon.Success}, decompressedNew, rotationPrepOps{
			getTssNonce:     func() (uint64, error) { return 0, errors.New("nonce rpc timeout") },
			setActivePubKey: func(k string) { *activePubKey = k },
		})
		if ok || plan != nil {
			t.Fatalf("expected nonce error to abort preparation, got ok=%v plan=%+v", ok, plan)
		}
		assertCleanedUp(t, n, updater, selfID, activeB, activeC, tempD, *activePubKey)
	})
}

func TestRestoreActiveKeyParticipantsRestoresBootstrapBaselineWhenNoActiveKey(t *testing.T) {
	selfID := newTestEd25519Identity(t)
	pillarA := newTestEd25519Identity(t)
	tempCandidate := newTestEd25519Identity(t)

	updater := newFakeParticipantUpdater(selfID.peerID, pillarA.peerID)
	n := &Node{
		config: &oconfig.Config{
			TssConfig: oconfig.TssManagerConfig{
				BaseDir: t.TempDir(),
			},
		},
		producerKeyPair:    &wallet.KeyPair{Public: selfID.pubKeyBytes},
		participantUpdater: updater,
		logger:             zap.NewNop().Sugar(),
	}

	// Bootstrap with no local key share: seeds whitelist from pillarA + self.
	if err := n.configurePubKeyFrom(&definition.BridgeInfoVariable{}, func() (map[string]string, error) {
		return map[string]string{pillarA.pubKeyB64: pillarA.peerID}, nil
	}); err != nil {
		t.Fatalf("configurePubKeyFrom: %v", err)
	}

	// Keygen adds a temporary candidate, then aborts.
	n.allowNewParticipants(map[string]string{tempCandidate.pubKeyB64: tempCandidate.peerID})
	if !updater.p2pWL[tempCandidate.peerID] {
		t.Fatalf("expected temporary candidate in TSS whitelist during keygen")
	}

	n.restoreActiveKeyParticipants(true)

	if len(n.config.TssConfig.LocalPubKeys) != 0 || len(updater.GetLocalPubKeys()) != 0 {
		t.Fatalf("expected empty bootstrap LocalPubKeys restored, got config=%v tss=%v", n.config.TssConfig.LocalPubKeys, updater.GetLocalPubKeys())
	}
	if _, exists := n.config.TssConfig.PubKeyWhitelist[tempCandidate.peerID]; exists {
		t.Fatalf("expected temporary candidate pruned from bootstrap config whitelist")
	}
	if _, exists := updater.p2pWL[tempCandidate.peerID]; exists {
		t.Fatalf("expected temporary candidate pruned from bootstrap TSS whitelist")
	}
	for _, id := range []string{selfID.peerID, pillarA.peerID} {
		if !n.config.TssConfig.PubKeyWhitelist[id] || !updater.p2pWL[id] {
			t.Fatalf("expected bootstrap baseline peer %s retained", id)
		}
	}
}

func TestSelectParticipantEmptyDoesNotPanic(t *testing.T) {
	n := &Node{
		config: &oconfig.Config{
			TssConfig: oconfig.TssManagerConfig{
				LocalPubKeys: nil,
			},
		},
	}
	if selected, ok := n.selectParticipant(42); ok || selected != "" {
		t.Fatalf("selectParticipant on empty set = (%q, %v), want (\"\", false)", selected, ok)
	}
	if got := n.getParticipant(42); got != "" {
		t.Fatalf("getParticipant on empty set = %q, want empty", got)
	}

	n.config.TssConfig.LocalPubKeys = []string{"pk-a", "pk-b"}
	if selected, ok := n.selectParticipant(3); !ok || selected != "pk-b" {
		t.Fatalf("selectParticipant(3) = (%q, %v), want (\"pk-b\", true)", selected, ok)
	}
}

func TestAllowNewParticipantsUpdatesConfigAndTssWhitelists(t *testing.T) {
	oldID := newTestEd25519Identity(t)
	newID := newTestEd25519Identity(t)

	updater := newFakeParticipantUpdater(oldID.peerID)
	updater.SetNewLocalPubKeys([]string{oldID.pubKeyB64})

	n := &Node{
		config: &oconfig.Config{
			TssConfig: oconfig.TssManagerConfig{
				LocalPubKeys: []string{oldID.pubKeyB64},
				PubKeyWhitelist: map[string]bool{
					oldID.peerID: true,
				},
			},
		},
		participantUpdater: updater,
		logger:             zap.NewNop().Sugar(),
	}

	n.allowNewParticipants(map[string]string{
		newID.pubKeyB64: newID.peerID,
	})

	// Both old and new participants must be present and sorted.
	wantKeys := []string{oldID.pubKeyB64, newID.pubKeyB64}
	slices.Sort(wantKeys)
	if !slices.Equal(n.config.TssConfig.LocalPubKeys, wantKeys) {
		t.Fatalf("config LocalPubKeys = %v, want %v", n.config.TssConfig.LocalPubKeys, wantKeys)
	}
	if !slices.Equal(updater.GetLocalPubKeys(), wantKeys) {
		t.Fatalf("TSS LocalPubKeys = %v, want %v", updater.GetLocalPubKeys(), wantKeys)
	}

	// Both config and TSS backend whitelists must include old and new peer IDs (regression for SA4006).
	for _, id := range []string{oldID.peerID, newID.peerID} {
		if !n.config.TssConfig.PubKeyWhitelist[id] {
			t.Fatalf("config PubKeyWhitelist missing %s", id)
		}
		if !updater.p2pWL[id] || !updater.partyWL[id] || !updater.sigWL[id] {
			t.Fatalf("TSS whitelists missing %s: p2p=%v party=%v sig=%v", id, updater.p2pWL, updater.partyWL, updater.sigWL)
		}
	}
}

func TestConfigurePubKeySeedsWhitelistWhenNoLocalKeyShareExists(t *testing.T) {
	selfID := newTestEd25519Identity(t)
	pillarA := newTestEd25519Identity(t)
	pillarB := newTestEd25519Identity(t)

	t.Run("seeds from pillar pubkeys plus self", func(t *testing.T) {
		n := &Node{
			config: &oconfig.Config{
				TssConfig: oconfig.TssManagerConfig{
					BaseDir: t.TempDir(),
				},
			},
			producerKeyPair: &wallet.KeyPair{Public: selfID.pubKeyBytes},
			logger:          zap.NewNop().Sugar(),
		}

		bridgeInfo := &definition.BridgeInfoVariable{
			CompressedTssECDSAPubKey:   "",
			DecompressedTssECDSAPubKey: "",
		}
		err := n.configurePubKeyFrom(bridgeInfo, func() (map[string]string, error) {
			return map[string]string{
				pillarA.pubKeyB64: pillarA.peerID,
				pillarB.pubKeyB64: pillarB.peerID,
			}, nil
		})
		if err != nil {
			t.Fatalf("configurePubKeyFrom returned error: %v", err)
		}
		if n.config.TssConfig.PublicKey != "" {
			t.Fatalf("expected empty PublicKey when localstate missing, got %q", n.config.TssConfig.PublicKey)
		}
		if len(n.config.TssConfig.PubKeyWhitelist) != 3 {
			t.Fatalf("expected 3 whitelisted peers (2 pillars + self), got %v", n.config.TssConfig.PubKeyWhitelist)
		}
		for _, id := range []string{selfID.peerID, pillarA.peerID, pillarB.peerID} {
			if !n.config.TssConfig.PubKeyWhitelist[id] {
				t.Fatalf("expected %s in initial whitelist %v", id, n.config.TssConfig.PubKeyWhitelist)
			}
		}
	})

	t.Run("falls back to self peer ID when pillar query fails or returns empty", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			fn   func() (map[string]string, error)
		}{
			{"empty", func() (map[string]string, error) { return map[string]string{}, nil }},
			{"rpc error", func() (map[string]string, error) { return nil, errors.New("rpc unavailable") }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				n := &Node{
					config: &oconfig.Config{
						TssConfig: oconfig.TssManagerConfig{
							BaseDir: t.TempDir(),
						},
					},
					producerKeyPair: &wallet.KeyPair{Public: selfID.pubKeyBytes},
					logger:          zap.NewNop().Sugar(),
				}
				err := n.configurePubKeyFrom(&definition.BridgeInfoVariable{}, tc.fn)
				if err != nil {
					t.Fatalf("configurePubKeyFrom returned error: %v", err)
				}
				if len(n.config.TssConfig.PubKeyWhitelist) != 1 || !n.config.TssConfig.PubKeyWhitelist[selfID.peerID] {
					t.Fatalf("expected non-empty self-only whitelist, got %v", n.config.TssConfig.PubKeyWhitelist)
				}
			})
		}
	})

	t.Run("loads participant_keys from existing localstate file", func(t *testing.T) {
		baseDir := t.TempDir()
		compressedPubKey := base64.StdEncoding.EncodeToString(make([]byte, 33))
		pubKeyPath, err := common.GetPublicKeyFilePath(baseDir, compressedPubKey)
		if err != nil {
			t.Fatalf("GetPublicKeyFilePath: %v", err)
		}
		payload, err := json.Marshal(map[string]any{
			"participant_keys": []string{pillarB.pubKeyB64, pillarA.pubKeyB64},
		})
		if err != nil {
			t.Fatalf("marshal localstate: %v", err)
		}
		if err := os.WriteFile(pubKeyPath, payload, 0600); err != nil {
			t.Fatalf("write localstate: %v", err)
		}

		n := &Node{
			config: &oconfig.Config{
				TssConfig: oconfig.TssManagerConfig{
					BaseDir: baseDir,
				},
			},
			producerKeyPair: &wallet.KeyPair{Public: selfID.pubKeyBytes},
			logger:          zap.NewNop().Sugar(),
		}
		bridgeInfo := &definition.BridgeInfoVariable{
			CompressedTssECDSAPubKey:   compressedPubKey,
			DecompressedTssECDSAPubKey: "decompressed",
		}
		pillarCalled := false
		err = n.configurePubKeyFrom(bridgeInfo, func() (map[string]string, error) {
			pillarCalled = true
			return nil, nil
		})
		if err != nil {
			t.Fatalf("configurePubKeyFrom returned error: %v", err)
		}
		if pillarCalled {
			t.Fatalf("did not expect GetPillarPubKeys call when localstate file exists")
		}
		if n.config.TssConfig.PublicKey != compressedPubKey {
			t.Fatalf("PublicKey = %q, want %q", n.config.TssConfig.PublicKey, compressedPubKey)
		}
		wantKeys := []string{pillarA.pubKeyB64, pillarB.pubKeyB64}
		slices.Sort(wantKeys)
		if !slices.Equal(n.config.TssConfig.LocalPubKeys, wantKeys) {
			t.Fatalf("LocalPubKeys = %v, want %v", n.config.TssConfig.LocalPubKeys, wantKeys)
		}
		for _, id := range []string{selfID.peerID, pillarA.peerID, pillarB.peerID} {
			if !n.config.TssConfig.PubKeyWhitelist[id] {
				t.Fatalf("expected %s in loaded whitelist %v", id, n.config.TssConfig.PubKeyWhitelist)
			}
		}
		for _, id := range []string{pillarA.peerID, pillarB.peerID} {
			if !n.activeKeyPeerIDs[id] {
				t.Fatalf("expected %s tracked in activeKeyPeerIDs %v", id, n.activeKeyPeerIDs)
			}
		}
	})
}

func TestParticipantAccessConcurrentWithSaveConfig(t *testing.T) {
	selfID := newTestEd25519Identity(t)
	otherID := newTestEd25519Identity(t)
	dataDir := filepath.Join(t.TempDir(), "data")
	stateVal := uint8(common.LiveState)
	cfg := &oconfig.Config{
		DataPath: dataDir,
		TssConfig: oconfig.TssManagerConfig{
			LocalPubKeys:    []string{selfID.pubKeyB64},
			PubKeyWhitelist: map[string]bool{selfID.peerID: true},
		},
	}
	if err := oconfig.WriteConfig(*cfg); err != nil {
		t.Fatalf("WriteConfig: %v", err)
	}

	updater := newFakeParticipantUpdater(selfID.peerID)
	n := &Node{
		config:             cfg,
		producerKeyPair:    &wallet.KeyPair{Public: selfID.pubKeyBytes},
		participantUpdater: updater,
		state:              common.NewPersistentGlobalState(&stateVal, nil),
		logger:             zap.NewNop().Sugar(),
	}

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 40; i++ {
			n.allowNewParticipants(map[string]string{otherID.pubKeyB64: otherID.peerID})
			n.removeParticipant(otherID.pubKeyB64)
			n.removeParticipant(selfID.pubKeyB64)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 40; i++ {
			_, _ = n.selectParticipant(uint32(i))
			_ = n.getParticipantIndex(selfID.pubKeyB64)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if err := n.SaveConfig(); err != nil {
				t.Errorf("SaveConfig failed: %v", err)
				return
			}
		}
	}()
	wg.Wait()
}
