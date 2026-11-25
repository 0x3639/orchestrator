package tss

import (
	"encoding/base64"
	"fmt"
	tcommon "github.com/HyperCore-Team/go-tss/common"
	"github.com/HyperCore-Team/go-tss/conversion"
	"github.com/HyperCore-Team/go-tss/keygen"
	"github.com/HyperCore-Team/go-tss/keysign"
	"github.com/HyperCore-Team/go-tss/messages"
	tstorage "github.com/HyperCore-Team/go-tss/storage"
	"github.com/HyperCore-Team/go-tss/tss"
	ic "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-peerstore/addr"
	maddr "github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"maps"
	"orchestrator/common"
	oconfig "orchestrator/common/config"
	"os"
	"slices"
	"sync"
	"time"
)

// ErrKeyShareSecurity indicates that TSS key share directory or file
// ownership/permission hardening failed after key generation.
var ErrKeyShareSecurity = errors.New("key share file security check failed")

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
	stateDir          *oconfig.OwnedDir
	votesDir          *oconfig.OwnedDir
	logsDir           *oconfig.OwnedDir
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

	// Test hooks; nil in production.
	preParamsFn func() error
	keygenFn    func(keygen.Request) (keygen.Response, error)
	partyConfig func() tcommon.TssConfig
	bulkSignFn  func(key string, msgs [][]byte, algo messages.Algo, signers []string, version string) (*keysign.Response, error)
}

func NewTssManager(conf oconfig.TssManagerConfig, privateKey string) (_ *TssManager, retErr error) {
	priKey, err := conversion.GetPriKey(privateKey)
	if err != nil {
		return nil, err
	}
	stateDir, err := oconfig.OpenOwnedDir(conf.BaseDir)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			_ = stateDir.Close()
		}
	}()
	if err := stateDir.SecureKeyShareFiles(""); err != nil {
		return nil, err
	}
	secureStateMgr := newSecureLocalStateManager(stateDir)
	savedPeers, err := secureStateMgr.RetrieveP2PAddresses()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		if errors.Is(err, errInvalidAddressBook) {
			common.GlobalLogger.Warnf("ignoring malformed TSS address book cache and falling back to configured bootstrap peers: %v", err)
			savedPeers = nil
		} else {
			return nil, fmt.Errorf("failed to read anchored TSS address book: %w", err)
		}
	}

	var peerIDs []maddr.Multiaddr
	if len(savedPeers) > 0 {
		peerIDs = append(peerIDs, savedPeers...)
	}
	if len(conf.Bootstrap) > 0 {
		multiAddr, err := maddr.NewMultiaddr(conf.Bootstrap)
		if err != nil {
			return nil, fmt.Errorf("invalid bootstrap multiaddr: %w", err)
		}
		peerIDs = append(peerIDs, multiAddr)
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

	votesDir, err := stateDir.OpenSubDir("unwrap-pool-votes")
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			_ = votesDir.Close()
		}
	}()
	votesPath, err := votesDir.AnchoredPath()
	if err != nil {
		return nil, err
	}
	votes, err := leveldb.OpenFile(votesPath, nil)
	if err != nil {
		return nil, err
	}
	// Pass a durable, private 0700 subdirectory anchored to stateDir to go-tss's
	// NewTss so go-tss never reads BaseDir/address_book.seed through its
	// pathname-based FileStateMgr (saved peers were already read above through
	// secureStateMgr), while hardlinking go-tss's log files to owned 0600 files
	// in BaseDir so startup/runtime diagnostics remain durable at their expected
	// paths even after startup failure or shutdown.
	logsDir, err := stateDir.OpenSubDir("logs")
	if err != nil {
		votes.Close()
		return nil, err
	}
	defer func() {
		if retErr != nil {
			_ = logsDir.Close()
		}
	}()
	if err := logsDir.EnsureAbsent(addressBookFileName); err != nil {
		votes.Close()
		return nil, err
	}
	for _, logName := range []string{"tss.communication.log", "tss.party.log", "tss.server.log"} {
		if err := stateDir.LinkOwnedFileTo(logsDir, logName, oconfig.ConfigFilePerm); err != nil {
			votes.Close()
			return nil, err
		}
	}
	tssBaseDir, err := logsDir.AnchoredPath()
	if err != nil {
		votes.Close()
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
		server, err = tss.NewTss(peerIDs, conf.Port, priKey, "Zenon", tssBaseDir, conf.BaseConfig, nil, "", algo, whitelist)
	}()
	wg.Wait()
	if err != nil {
		votes.Close()
		return nil, err
	}
	if err := installSecureStateManager(server, secureStateMgr); err != nil {
		server.Stop()
		votes.Close()
		return nil, err
	}

	return &TssManager{
		unwrapVotes:       votes,
		stateDir:          stateDir,
		votesDir:          votesDir,
		logsDir:           logsDir,
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
	if m.votesDir != nil {
		_ = m.votesDir.Close()
	}
	if m.logsDir != nil {
		_ = m.logsDir.Close()
	}
	if m.stateDir != nil {
		_ = m.stateDir.Close()
	}
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
	if key != "" && (m.stateDir != nil || m.keyShareDir != "") {
		if state, err := m.readLocalState(key, messages.ECDSAKEYSIGN); err == nil && len(state.ParticipantKeys) > 0 {
			return slices.Clone(state.ParticipantKeys)
		}
	}
	return m.GetLocalPubKeys()
}

func (m *TssManager) bulkSignWithKey(key string, messagesBytes [][]byte, algo messages.Algo, signers []string, version string) (*keysign.Response, error) {
	if m.bulkSignFn != nil {
		return m.bulkSignFn(key, messagesBytes, algo, slices.Clone(signers), version)
	}
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
	var errPrecompute error
	if m.preParamsFn != nil {
		errPrecompute = m.preParamsFn()
	} else {
		errPrecompute = m.server.GeneratePreParams()
	}
	if errPrecompute != nil {
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
		var response keygen.Response
		var err error
		if m.keygenFn != nil {
			response, err = m.keygenFn(req)
		} else {
			response, err = m.server.Keygen(req)
		}
		if err != nil {
			return nil, err
		}
		if response.Status == tcommon.Success {
			if secErr := m.VerifyKeyShare(response.PubKey); secErr != nil {
				return nil, secErr
			}
		}

		return &response, nil
	} else {
		return nil, errors.New("invalid algorithm")
	}
}

// readLocalState reads the key share for pubKey through the manager's anchored
// open TSS directory descriptor so directory substitution cannot redirect or
// break signing reads.
func (m *TssManager) readLocalState(pubKey string, algo messages.Algo) (tstorage.KeygenLocalState, error) {
	if m.stateDir != nil {
		return newSecureLocalStateManager(m.stateDir).GetLocalState(pubKey, algo)
	}
	ownedDir, err := oconfig.OpenOwnedDir(m.keyShareDir)
	if err != nil {
		return tstorage.KeygenLocalState{}, err
	}
	defer ownedDir.Close()
	return newSecureLocalStateManager(ownedDir).GetLocalState(pubKey, algo)
}

// VerifyKeyShare checks ownership and exact 0600 permissions on all key share
// files and verifies the share for pubKey exists, anchored to the manager's
// open TSS directory descriptor when available.
func (m *TssManager) VerifyKeyShare(pubKey string) error {
	expectedFile, nameErr := common.GetPublicKeyFileName(pubKey)
	if nameErr != nil {
		return fmt.Errorf("%w: invalid generated pubKey: %w", ErrKeyShareSecurity, nameErr)
	}
	var secErr error
	if m.stateDir != nil {
		secErr = m.stateDir.SecureKeyShareFiles(expectedFile)
	} else {
		secErr = oconfig.SecureKeyShareFilesExpecting(m.keyShareDir, expectedFile)
	}
	if secErr != nil {
		return fmt.Errorf("%w: %w", ErrKeyShareSecurity, secErr)
	}
	return nil
}

// DeleteKeyShare removes the key share file for pubKey through the manager's
// open TSS directory descriptor so pathname substitution cannot redirect
// rollback cleanup.
func (m *TssManager) DeleteKeyShare(pubKey string) error {
	fileName, err := common.GetPublicKeyFileName(pubKey)
	if err != nil {
		return err
	}
	if m.stateDir != nil {
		return m.stateDir.RemoveFile(fileName)
	}
	return common.DeletePubKeyFile(m.keyShareDir, pubKey)
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
	if m.partyConfig != nil {
		return m.partyConfig()
	}
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
