package tss

import (
	"encoding/base64"
	"fmt"
	tcommon "github.com/HyperCore-Team/go-tss/common"
	"github.com/HyperCore-Team/go-tss/conversion"
	"github.com/HyperCore-Team/go-tss/keygen"
	"github.com/HyperCore-Team/go-tss/keysign"
	"github.com/HyperCore-Team/go-tss/messages"
	"github.com/HyperCore-Team/go-tss/tss"
	ic "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-peerstore/addr"
	maddr "github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"orchestrator/common"
	oconfig "orchestrator/common/config"
	"path/filepath"
	"slices"
	"sync"
	"time"
)

type TssManager struct {
	settingsMu        sync.RWMutex
	voteMu            sync.Mutex
	unwrapVotes       *leveldb.DB
	keyShareDir       string
	server            *tss.TssServer
	port              int
	privateKey        string
	publicKey         string
	peerPubKey        string
	peerId            peer.ID
	localPubKeys      []string
	joinPartyVersion  string
	leaderBlockHeight int64
}

func NewTssManager(conf oconfig.TssManagerConfig, privateKey string) (*TssManager, error) {
	priKey, err := conversion.GetPriKey(privateKey)
	if err != nil {
		return nil, err
	}
	if err := oconfig.EnsureDataDir(conf.BaseDir); err != nil {
		return nil, err
	}
	if err := oconfig.SecureKeyShareFiles(conf.BaseDir); err != nil {
		return nil, err
	}

	var peerIDs []maddr.Multiaddr
	if len(conf.Bootstrap) > 0 {
		multiAddr, err := maddr.NewMultiaddr(conf.Bootstrap)
		if err != nil {
			return nil, fmt.Errorf("invalid bootstrap multiaddr: %w", err)
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
		server, err = tss.NewTss(peerIDs, conf.Port, priKey, "Zenon", conf.BaseDir, conf.BaseConfig, nil, "", algo, conf.PubKeyWhitelist)
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
	return m.bulkSignWithKey(m.GetPubKey(), messagesBytes, algo, m.GetLocalPubKeys(), m.GetJoinPartyVersion())
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
		if response.Status == tcommon.Success {
			if err := oconfig.SecureKeyShareFiles(m.keyShareDir); err != nil {
				return nil, err
			}
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

func (m *TssManager) DeleteLocalPubKey(pubKey string) {
	m.settingsMu.Lock()
	defer m.settingsMu.Unlock()
	for idx, localPubKey := range m.localPubKeys {
		if localPubKey == pubKey {
			m.localPubKeys = append(m.localPubKeys[:idx], m.localPubKeys[idx+1:]...)
		}
	}
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

func (m *TssManager) GetWhitelists() (map[string]bool, map[string]bool, map[string]bool) {
	return m.server.GetWhitelists()
}

func (m *TssManager) DeleteWhitelistEntry(pubKey string) {
	m.server.DeleteWhitelistEntry(pubKey)
}

func (m *TssManager) ExportPeersStore() map[peer.ID]addr.AddrList {
	return m.server.ExportPeersStore()
}
