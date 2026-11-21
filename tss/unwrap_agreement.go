package tss

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"

	tcommon "github.com/HyperCore-Team/go-tss/common"
	"github.com/HyperCore-Team/go-tss/keysign"
	"github.com/HyperCore-Team/go-tss/messages"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// A vote signs only a domain-separated commitment. Bridge signatures are
// requested separately, after a certificate for the exact ordered pool exists.
func (m *TssManager) ApproveUnwrapPool(ceremony uint64, pool [][]byte) error {
	return m.approveUnwrapPool(ceremony, pool, m.BulkSign)
}

func (m *TssManager) approveUnwrapPool(ceremony uint64, pool [][]byte, sign func([][]byte, messages.Algo) (*keysign.Response, error)) error {
	key, err := base64.StdEncoding.DecodeString(m.GetPubKey())
	if err != nil || len(key) != 33 {
		return errors.New("invalid compressed TSS public key")
	}
	digest, err := unwrapPoolDigest(key, ceremony, pool)
	if err != nil {
		return err
	}
	if err := m.reserveUnwrapVote(key, ceremony, digest); err != nil {
		return err
	}
	response, err := sign([][]byte{digest}, messages.ECDSAKEYSIGN)
	if err != nil {
		return fmt.Errorf("unwrap pool agreement: %w", err)
	}
	return verifyUnwrapAgreement(key, digest, response)
}

func unwrapPoolDigest(key []byte, ceremony uint64, pool [][]byte) ([]byte, error) {
	if len(pool) == 0 {
		return nil, errors.New("cannot approve an empty unwrap pool")
	}
	h := sha256.New()
	h.Write([]byte("zenon/orchestrator/unwrap-pool/v1\x00"))
	h.Write(key)
	h.Write(binary.BigEndian.AppendUint64(nil, ceremony))
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
func (m *TssManager) reserveUnwrapVote(key []byte, ceremony uint64, digest []byte) error {
	m.voteMu.Lock()
	defer m.voteMu.Unlock()
	id := binary.BigEndian.AppendUint64(bytes.Clone(key), ceremony)
	prev, err := m.unwrapVotes.Get(id, nil)
	switch {
	case err == leveldb.ErrNotFound:
		return m.unwrapVotes.Put(id, digest, &opt.WriteOptions{Sync: true})
	case err != nil:
		return err
	case !bytes.Equal(prev, digest):
		return fmt.Errorf("already voted for another unwrap pool in ceremony %d", ceremony)
	default:
		return nil
	}
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
