package tss

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"

	tcommon "github.com/HyperCore-Team/go-tss/common"
	"github.com/HyperCore-Team/go-tss/conversion"
	"github.com/HyperCore-Team/go-tss/keysign"
	"github.com/HyperCore-Team/go-tss/messages"
	"github.com/HyperCore-Team/go-tss/storage"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"orchestrator/common"
)

// go-tss versions before 0.14 use peer discovery without an elected leader.
const unwrapLeaderlessVersion = "0.13.0"

// Both rounds use the same key. Only a verified signature on the commitment
// releases the exact ordered messages for a bridge signing request.
func (m *TssManager) SignUnwrapPool(ceremony common.CeremonyWindow, pool [][]byte, checkCeremony func() error) (*keysign.Response, error) {
	key := m.GetPubKey()
	// Leader-based go-tss selects from everyone who generated the key, including
	// dissenters who never join this pool. Its leaderless protocol lets matching
	// peers discover each other. Keep the original signing electorate in either
	// mode; go-tss enforces the threshold from the key shares in both rounds.
	version := m.GetJoinPartyVersion()
	legacy, err := conversion.VersionLTCheck(version, messages.NEWJOINPARTYVERSION)
	if err != nil {
		return nil, err
	}
	signers := m.GetLocalPubKeys()
	if !legacy {
		store, err := storage.NewFileStateMgr(m.keyShareDir)
		if err != nil {
			return nil, err
		}
		state, err := store.GetLocalState(key, messages.ECDSAKEYSIGN)
		if err != nil {
			return nil, err
		}
		signers = state.ParticipantKeys
		version = unwrapLeaderlessVersion
	}
	return m.signUnwrapPool(key, ceremony, pool, checkCeremony, func(msgs [][]byte, algo messages.Algo) (*keysign.Response, error) {
		return m.bulkSignWithKey(key, msgs, algo, signers, version)
	})
}

type signRound func([][]byte, messages.Algo) (*keysign.Response, error)

func (m *TssManager) signUnwrapPool(key string, ceremony common.CeremonyWindow, pool [][]byte, checkCeremony func() error, sign signRound) (*keysign.Response, error) {
	if err := m.approveUnwrapPool(key, ceremony, pool, sign); err != nil {
		return nil, err
	}
	if err := checkCeremony(); err != nil {
		return nil, err
	}
	if m.GetPubKey() != key {
		return nil, errors.New("unwrap signing key changed during pool agreement")
	}
	return sign(pool, messages.ECDSAKEYSIGN)
}

func (m *TssManager) approveUnwrapPool(encodedKey string, ceremony common.CeremonyWindow, pool [][]byte, sign signRound) error {
	key, err := base64.StdEncoding.DecodeString(encodedKey)
	if err != nil || len(key) != 33 {
		return errors.New("invalid compressed TSS public key")
	}
	digest, err := unwrapPoolDigest(key, ceremony, pool)
	if err != nil {
		return err
	}
	if err := m.reserveUnwrapVote(key, ceremony, digest, pool); err != nil {
		return err
	}
	response, err := sign([][]byte{digest}, messages.ECDSAKEYSIGN)
	if err != nil {
		return fmt.Errorf("unwrap pool agreement: %w", err)
	}
	return verifyUnwrapAgreement(key, digest, response)
}

func unwrapPoolDigest(key []byte, ceremony common.CeremonyWindow, pool [][]byte) ([]byte, error) {
	if ceremony.Size == 0 {
		return nil, errors.New("invalid zero unwrap window")
	}
	return hashUnwrapPool("zenon/orchestrator/unwrap-pool/v2\x00", key, ceremonyBytes(ceremony), pool)
}

func ceremonyBytes(window common.CeremonyWindow) []byte {
	return binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(nil, window.Size), window.Number)
}

func hashUnwrapPool(domain string, key, window []byte, pool [][]byte) ([]byte, error) {
	if len(pool) == 0 {
		return nil, errors.New("cannot approve an empty unwrap pool")
	}
	h := sha256.New()
	h.Write([]byte(domain))
	h.Write(key)
	h.Write(window)
	for _, msg := range pool {
		if len(msg) != 32 {
			return nil, errors.New("invalid unwrap message length")
		}
		h.Write(msg)
	}
	return h.Sum(nil), nil
}

// Persist before contacting any peers. A timeout may mean that other peers
// already have the certificate. A restart can retry only the same vote.
func (m *TssManager) reserveUnwrapVote(key []byte, ceremony common.CeremonyWindow, digest []byte, pool [][]byte) error {
	m.voteMu.Lock()
	defer m.voteMu.Unlock()
	window := ceremonyBytes(ceremony)
	if err := m.checkLegacyUnwrapVote(key, ceremony, window, pool); err != nil {
		return err
	}
	id := append(bytes.Clone(key), window...)
	prev, err := m.unwrapVotes.Get(id, nil)
	switch {
	case err == leveldb.ErrNotFound:
		return m.unwrapVotes.Put(id, digest, &opt.WriteOptions{Sync: true})
	case err != nil:
		return err
	case !bytes.Equal(prev, digest):
		return fmt.Errorf("already voted for another unwrap pool in ceremony %d", ceremony.Number)
	default:
		return nil
	}
}

// Called under voteMu so concurrent attempts agree on the upgrade window.
func (m *TssManager) checkLegacyUnwrapVote(key []byte, ceremony common.CeremonyWindow, window []byte, pool [][]byte) error {
	// Older PR builds stored only the ceremony number. A coordinated upgrade
	// can overlap one live window. Pin that window durably, including when a
	// legacy vote vetoes the first attempt. Later windows have independent IDs.
	marker := []byte("v2-cutover-window")
	cutover, err := m.unwrapVotes.Get(marker, nil)
	if err == leveldb.ErrNotFound {
		if err = m.unwrapVotes.Put(marker, window, &opt.WriteOptions{Sync: true}); err != nil {
			return err
		}
		cutover = window
	} else if err != nil {
		return err
	}
	if len(cutover) != len(window) {
		return errors.New("invalid legacy vote cutover window")
	}
	if !bytes.Equal(cutover, window) {
		return nil
	}
	number := binary.BigEndian.AppendUint64(nil, ceremony.Number)
	legacy, err := m.unwrapVotes.Get(append(bytes.Clone(key), number...), nil)
	if err == leveldb.ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	old, err := hashUnwrapPool("zenon/orchestrator/unwrap-pool/v1\x00", key, number, pool)
	if err != nil {
		return err
	}
	if !bytes.Equal(old, legacy) {
		return fmt.Errorf("already voted for another unwrap pool in ceremony %d", ceremony.Number)
	}
	return nil
}

func verifyUnwrapAgreement(key, digest []byte, response *keysign.Response) error {
	if response == nil || response.Status != tcommon.Success || len(response.Signatures) != 1 {
		return errors.New("no quorum certificate for unwrap pool")
	}
	vote := response.Signatures[0]
	sig, err := base64.StdEncoding.DecodeString(vote.Signature)
	if err != nil || vote.Msg != base64.StdEncoding.EncodeToString(digest) || !crypto.VerifySignature(key, digest, sig) {
		return errors.New("invalid unwrap pool quorum certificate")
	}
	return nil
}
