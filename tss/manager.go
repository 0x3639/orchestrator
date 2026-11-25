package tss

import (
	"encoding/base64"
	tcommon "github.com/HyperCore-Team/go-tss/common"
	"github.com/HyperCore-Team/go-tss/conversion"
	"github.com/HyperCore-Team/go-tss/keygen"
	"github.com/HyperCore-Team/go-tss/keysign"
	"github.com/HyperCore-Team/go-tss/messages"
	"github.com/HyperCore-Team/go-tss/storage"
	"github.com/HyperCore-Team/go-tss/tss"
	ic "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-peerstore/addr"
	maddr "github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"maps"
	"orchestrator/common"
	"orchestrator/common/config"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
)

type whitelistBackend interface {
	GetWhitelists() (map[string]bool, map[string]bool, map[string]bool)
	AddWhitelistEntries(peerIDs ...string)
	DeleteWhitelistEntry(peerID string)
	ReplaceWhitelist(peerIDs ...string)
}

type TssManager struct {
	settingsMu        sync.RWMutex
	whitelistMu       sync.RWMutex
	voteMu            sync.Mutex
	unwrapVotes       *leveldb.DB
	keyShareDir       string
	server            *tss.TssServer
	whitelistStore    whitelistBackend
	port              int
	privateKey        string
	publicKey         string
	peerPubKey        string
	peerId            peer.ID
	localPubKeys      []string
	joinPartyVersion  string
	leaderBlockHeight int64
}

func NewTssManager(conf config.TssManagerConfig, privateKey string) (*TssManager, error) {
	priKey, err := conversion.GetPriKey(privateKey)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(conf.BaseDir); os.IsNotExist(err) {
		err := os.MkdirAll(conf.BaseDir, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}

	var peerIDs []maddr.Multiaddr
	if len(conf.Bootstrap) > 0 {
		multiAddr, err := maddr.NewMultiaddr(conf.Bootstrap)
		if err != nil {
			panic(err)
		}
		peerIDs = []maddr.Multiaddr{multiAddr}
	} else {
		peerIDs = nil
	}

	priv, err := ic.UnmarshalEd25519PrivateKey(priKey.Bytes())
	if err != nil {
		return nil, err
	}

	pub, err := ic.UnmarshalEd25519PublicKey(priKey.PubKey().Bytes())
	if err != nil {
		return nil, err
	}
	pubB, _ := pub.Raw()

	peerPubKey := base64.StdEncoding.EncodeToString(pubB)
	common.GlobalLogger.Infof("peerPubKey: %s\n", peerPubKey)
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		return nil, err
	}

	idPriv, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return nil, err
	}
	if id.String() != idPriv.String() {
		return nil, errors.New("peerId from pubKey different from privKey")
	}

	common.GlobalLogger.Infof("peerId: %s\n", id.String())

	votes, err := leveldb.OpenFile(filepath.Join(conf.BaseDir, "unwrap-pool-votes"), nil)
	if err != nil {
		return nil, err
	}
	// Clone the configured whitelist so go-tss never shares the map reference
	// stored in node.config, drop any legacy false entries (go-tss checks map
	// key existence, not value), and always include self so the whitelist is
	// never empty (an empty map disables p2p/communication.go's check).
	whitelist := buildInitialWhitelist(conf.PubKeyWhitelist, id.String())

	var server *tss.TssServer
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var algo messages.Algo
		if conf.PublicKey == "" {
			algo = messages.ECDSAKEYGEN
		} else {
			algo = messages.ECDSAKEYSIGN
		}

		// we call pre params with nil because we want to recompute it when we key gen, at key signs it takes it from the pubKey file
		server, err = tss.NewTss(peerIDs, conf.Port, priKey, "Zenon", conf.BaseDir, conf.BaseConfig, nil, "", algo, whitelist)
	}()
	wg.Wait()
	if err != nil {
		votes.Close()
		return nil, err
	}

	return &TssManager{
		unwrapVotes:       votes,
		keyShareDir:       conf.BaseDir,
		port:              conf.Port,
		server:            server,
		whitelistStore:    server,
		privateKey:        privateKey,
		publicKey:         conf.PublicKey,
		peerPubKey:        peerPubKey,
		peerId:            id,
		localPubKeys:      slices.Clone(conf.LocalPubKeys),
		joinPartyVersion:  common.DefaultKeyGenVersion,
		leaderBlockHeight: common.DefaultLeaderBlockHeight,
	}, nil
}

func (m *TssManager) Start() error {
	tcommon.InitLog("info", true, "orchestrator")
	return m.server.Start()
}

func (m *TssManager) Stop() {
	m.server.Stop()
	m.unwrapVotes.Close()
}

func (m *TssManager) BulkSign(messagesBytes [][]byte, algo messages.Algo) (*keysign.Response, error) {
	key := m.GetPubKey()
	signers := m.signersForKey(key)
	return m.bulkSignWithKey(key, messagesBytes, algo, signers, m.GetJoinPartyVersion())
}

// signersForKey returns the canonical participant set stored with key's local
// share so signing with an active key (including old-key rotation signing after
// a new keygen updates LocalPubKeys) produces a consistent go-tss msgID across
// all holders of that key. Falls back to m.GetLocalPubKeys() when no localstate
// entry is available.
func (m *TssManager) signersForKey(key string) []string {
	if key != "" && m.keyShareDir != "" {
		if store, err := storage.NewFileStateMgr(m.keyShareDir); err == nil {
			if state, err := store.GetLocalState(key, messages.ECDSAKEYSIGN); err == nil && len(state.ParticipantKeys) > 0 {
				return slices.Clone(state.ParticipantKeys)
			}
		}
	}
	return m.GetLocalPubKeys()
}

func (m *TssManager) bulkSignWithKey(key string, messagesBytes [][]byte, algo messages.Algo, signers []string, version string) (*keysign.Response, error) {
	var keySignReq keysign.Request
	messagesStr := make([]string, len(messagesBytes))
	for idx, msg := range messagesBytes {
		messagesStr[idx] = base64.StdEncoding.EncodeToString(msg)
		common.GlobalLogger.Info("encoded msg:", messagesStr[idx])
	}
	common.GlobalLogger.Info("before sending sign request")
	common.GlobalLogger.Info("PublicKey: ", key)
	if algo == messages.ECDSAKEYSIGN {
		keySignReq = keysign.NewRequest(key, messagesStr, m.GetLeaderBlockHeight(), slices.Clone(signers), version, "ecdsa")
		resSign, err := m.server.KeySign(keySignReq)
		return &resSign, err
	} else {
		return nil, errors.New("invalid algorithm")
	}
}

func (m *TssManager) KeyGen(algo messages.Algo) (*keygen.Response, error) {
	var algorithm string
	if algo == messages.ECDSAKEYGEN {
		algorithm = "ecdsa"
	} else {
		return nil, errors.New("invalid algorithm")
	}

	start := time.Now()
	// we reset pre params so we always generate it before a keyGen
	if errPrecompute := m.server.GeneratePreParams(); errPrecompute != nil {
		return nil, errPrecompute
	}
	elapsed := time.Since(start)
	common.GlobalLogger.Infof("preParams took %f", elapsed.Seconds())

	sleepDuration := m.Config().PartyTimeout * 2 / 3
	if sleepDuration > elapsed {
		sleepDuration = sleepDuration - elapsed
		time.Sleep(sleepDuration)
		common.GlobalLogger.Infof("finished sleep after preParams")
	}

	var req keygen.Request
	if algo == messages.ECDSAKEYGEN {
		req = keygen.NewRequest(m.GetLocalPubKeys(), m.GetLeaderBlockHeight(), m.GetJoinPartyVersion(), algorithm)
		response, err := m.server.Keygen(req)
		if err != nil {
			return nil, err
		}

		return &response, nil
	} else {
		return nil, errors.New("invalid algorithm")
	}
}

func (m *TssManager) SetPartyTimeout(partyTimeout time.Duration) {
	m.server.SetPartyTimeout(partyTimeout)
}

func (m *TssManager) SetKeyGenTimeout(keyGenTimeout time.Duration) {
	m.server.SetKeyGenTimeout(keyGenTimeout)
}

func (m *TssManager) SetKeySignTimeout(keySignTimeout time.Duration) {
	m.server.SetKeySignTimeout(keySignTimeout)
}

func (m *TssManager) SetPreParamsTimeout(preParamsTimeout time.Duration) {
	m.server.SetPreParamsTimeout(preParamsTimeout)
}

func (m *TssManager) SetJoinPartyVersion(version string) {
	m.settingsMu.Lock()
	defer m.settingsMu.Unlock()
	m.joinPartyVersion = version
}

func (m *TssManager) GetJoinPartyVersion() string {
	m.settingsMu.RLock()
	defer m.settingsMu.RUnlock()
	return m.joinPartyVersion
}

func (m *TssManager) SetLeaderBlockHeight(height int64) {
	m.settingsMu.Lock()
	defer m.settingsMu.Unlock()
	m.leaderBlockHeight = height
}

func (m *TssManager) GetLeaderBlockHeight() int64 {
	m.settingsMu.RLock()
	defer m.settingsMu.RUnlock()
	return m.leaderBlockHeight
}

func (m *TssManager) SetPubKey(pubKey string) {
	m.settingsMu.Lock()
	defer m.settingsMu.Unlock()
	m.publicKey = pubKey
}

func (m *TssManager) Config() tcommon.TssConfig {
	return m.server.Config()
}

func buildInitialWhitelist(configured map[string]bool, selfPeerID string) map[string]bool {
	whitelist := make(map[string]bool, len(configured)+1)
	for peerID, allowed := range configured {
		if allowed && peerID != "" {
			whitelist[peerID] = true
		}
	}
	if selfPeerID != "" {
		whitelist[selfPeerID] = true
	}
	return whitelist
}

func (m *TssManager) DeleteLocalPubKey(pubKey string) {
	m.settingsMu.Lock()
	defer m.settingsMu.Unlock()
	m.localPubKeys = slices.DeleteFunc(m.localPubKeys, func(localPubKey string) bool {
		return localPubKey == pubKey
	})
}

func (m *TssManager) GetPeerId() peer.ID {
	return m.peerId
}

func (m *TssManager) GetPeerPubKey() string {
	return m.peerPubKey
}

func (m *TssManager) GetLocalPubKeys() []string {
	m.settingsMu.RLock()
	defer m.settingsMu.RUnlock()
	return slices.Clone(m.localPubKeys)
}

func (m *TssManager) SetNewLocalPubKeys(newPubKeys []string) {
	m.settingsMu.Lock()
	defer m.settingsMu.Unlock()
	m.localPubKeys = slices.Clone(newPubKeys)
}

func (m *TssManager) GetPubKey() string {
	m.settingsMu.RLock()
	defer m.settingsMu.RUnlock()
	return m.publicKey
}

func (m *TssManager) CanProcessSignatures() bool {
	return m.GetPubKey() != ""
}

func (m *TssManager) whitelists() whitelistBackend {
	if m.whitelistStore != nil {
		return m.whitelistStore
	}
	if m.server != nil {
		return m.server
	}
	return nil
}

// GetWhitelists returns isolated snapshots of the go-tss whitelist maps.
func (m *TssManager) GetWhitelists() (map[string]bool, map[string]bool, map[string]bool) {
	m.whitelistMu.RLock()
	defer m.whitelistMu.RUnlock()
	backend := m.whitelists()
	if backend == nil {
		return nil, nil, nil
	}
	p2pWL, partyWL, sigWL := backend.GetWhitelists()
	return maps.Clone(p2pWL), maps.Clone(partyWL), maps.Clone(sigWL)
}

// AddWhitelistEntries adds libp2p peer IDs to the synchronized go-tss whitelist.
func (m *TssManager) AddWhitelistEntries(peerIDs ...string) {
	filtered := make([]string, 0, len(peerIDs))
	for _, peerID := range peerIDs {
		if peerID != "" {
			filtered = append(filtered, peerID)
		}
	}
	if len(filtered) == 0 {
		return
	}
	m.whitelistMu.Lock()
	defer m.whitelistMu.Unlock()
	backend := m.whitelists()
	if backend == nil {
		return
	}
	backend.AddWhitelistEntries(filtered...)
}

// DeleteWhitelistEntry removes a libp2p peer ID from the synchronized go-tss
// whitelist. go-tss checks map key existence, so entries are deleted rather
// than set to false. The local node's own peer ID is never deleted so the
// whitelist cannot become empty and flip p2p/communication.go into accept-all mode.
func (m *TssManager) DeleteWhitelistEntry(peerID string) {
	if peerID == "" {
		return
	}
	if self := m.peerId.String(); self != "" && peerID == self {
		return
	}
	m.whitelistMu.Lock()
	defer m.whitelistMu.Unlock()
	backend := m.whitelists()
	if backend == nil {
		return
	}
	backend.DeleteWhitelistEntry(peerID)
}

// ReplaceWhitelist atomically replaces the synchronized go-tss whitelist with
// peerIDs, always retaining this node's own peer ID so the whitelist stays non-empty.
func (m *TssManager) ReplaceWhitelist(peerIDs ...string) {
	next := make([]string, 0, len(peerIDs)+1)
	for _, peerID := range peerIDs {
		if peerID != "" {
			next = append(next, peerID)
		}
	}
	if self := m.peerId.String(); self != "" {
		next = append(next, self)
	}
	m.whitelistMu.Lock()
	defer m.whitelistMu.Unlock()
	backend := m.whitelists()
	if backend == nil {
		return
	}
	backend.ReplaceWhitelist(next...)
}

func (m *TssManager) ExportPeersStore() map[peer.ID]addr.AddrList {
	return m.server.ExportPeersStore()
}
