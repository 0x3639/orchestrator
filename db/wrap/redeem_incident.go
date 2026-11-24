package wrap

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"slices"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"

	"orchestrator/common"
)

// Where earlier builds kept the incident keys; see keys.go.
const (
	oldRedeemEvidencePrefix      byte = 4
	oldRedeemHaltConfirmedPrefix byte = 5
)

// prefix, chain id, block hash, transaction hash, log index
const redeemEvidenceKeyLength = 1 + 4 + 32 + 32 + 8

type RedeemIncidentStore struct{ db *leveldb.DB }

func NewRedeemIncidentStore(db *leveldb.DB) *RedeemIncidentStore {
	return &RedeemIncidentStore{db: db}
}

func redeemEvidenceKey(ev common.RedeemIncident) []byte {
	key := binary.BigEndian.AppendUint32([]byte{redeemEvidencePrefix}, ev.ChainID)
	key = append(key, ev.Log.BlockHash[:]...)
	key = append(key, ev.Log.TxHash[:]...)
	return binary.BigEndian.AppendUint64(key, uint64(ev.Log.Index))
}

func confirmedKey(evidenceKey []byte) []byte {
	return append([]byte{redeemHaltConfirmedPrefix}, evidenceKey[1:]...)
}

func (s *RedeemIncidentStore) HandledRedeemLog(chainID uint32, log etypes.Log) (bool, error) {
	key := redeemEvidenceKey(common.RedeemIncident{ChainID: chainID, Log: log})
	handled, err := s.db.Has(confirmedKey(key), nil)
	if err != nil || !handled {
		return false, err
	}
	data, err := s.db.Get(key, nil)
	if err != nil {
		return false, err
	}
	var stored common.RedeemIncident
	if err := json.Unmarshal(data, &stored); err != nil {
		return false, err
	}
	original := stored.Log
	if stored.ChainID != chainID || original.BlockNumber != log.BlockNumber || original.BlockHash != log.BlockHash ||
		original.TxHash != log.TxHash || original.Index != log.Index || original.Address != log.Address ||
		!slices.Equal(original.Topics, log.Topics) || !slices.Equal(original.Data, log.Data) {
		return false, errors.New("replayed redeem differs from the archived evidence")
	}
	return true, nil
}

// GlobalState serializes incident updates with acknowledgment of a halt.
func (s *RedeemIncidentStore) RecordRedeemIncident(ev common.RedeemIncident) (bool, error) {
	key := redeemEvidenceKey(ev)
	exists, err := s.db.Has(key, nil)
	if err != nil {
		return false, err
	}
	if exists {
		done, err := s.db.Has(confirmedKey(key), nil)
		return !done, err
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return false, err
	}
	return true, s.db.Put(key, data, &opt.WriteOptions{Sync: true})
}

func (s *RedeemIncidentStore) HasPendingRedeemIncident() (bool, error) {
	it := s.db.NewIterator(util.BytesPrefix([]byte{redeemEvidencePrefix}), nil)
	defer it.Release()
	for it.Next() {
		done, err := s.db.Has(confirmedKey(it.Key()), nil)
		if err != nil || !done {
			return !done, err
		}
	}
	return false, it.Error()
}

func (s *RedeemIncidentStore) ConfirmRedeemHalt(checkedChains map[uint32]bool) error {
	it := s.db.NewIterator(util.BytesPrefix([]byte{redeemEvidencePrefix}), nil)
	defer it.Release()
	batch := new(leveldb.Batch)
	for it.Next() {
		key := it.Key()
		if len(key) < 5 {
			return errors.New("invalid redeem incident key")
		}
		marker := confirmedKey(key)
		confirmed, err := s.db.Has(marker, nil)
		if err != nil {
			return err
		}
		if confirmed {
			continue
		}
		chainID := binary.BigEndian.Uint32(key[1:5])
		if !checkedChains[chainID] {
			return fmt.Errorf("pending redeem incident on chain %d has no confirmed halt flag", chainID)
		}
		batch.Put(marker, []byte{byte(common.RedeemHaltObserved)})
	}
	if err := it.Error(); err != nil {
		return err
	}
	return s.db.Write(batch, &opt.WriteOptions{Sync: true})
}

// PendingRedeemIncidents returns the evidence that still holds the bridge.
func (s *RedeemIncidentStore) PendingRedeemIncidents() ([]common.RedeemIncident, error) {
	it := s.db.NewIterator(util.BytesPrefix([]byte{redeemEvidencePrefix}), nil)
	defer it.Release()
	var pending []common.RedeemIncident
	for it.Next() {
		done, err := s.db.Has(confirmedKey(it.Key()), nil)
		if err != nil {
			return nil, err
		}
		if done {
			continue
		}
		var ev common.RedeemIncident
		if err := json.Unmarshal(it.Value(), &ev); err != nil {
			return nil, err
		}
		pending = append(pending, ev)
	}
	return pending, it.Error()
}

// ResolveRedeemIncident archives the evidence, if it is new, together with the
// reason it does not hold the bridge. An earlier resolution is kept.
func (s *RedeemIncidentStore) ResolveRedeemIncident(ev common.RedeemIncident, reason common.RedeemResolution) error {
	if reason != common.RedeemSettledOnChain && reason != common.RedeemOperatorAcknowledged {
		return errors.New("invalid redeem incident resolution")
	}
	key := redeemEvidenceKey(ev)
	batch := new(leveldb.Batch)
	exists, err := s.db.Has(key, nil)
	if err != nil {
		return err
	}
	if !exists {
		data, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		batch.Put(key, data)
	}
	marker := confirmedKey(key)
	done, err := s.db.Has(marker, nil)
	if err != nil || done {
		return err
	}
	batch.Put(marker, []byte{byte(reason)})
	return s.db.Write(batch, &opt.WriteOptions{Sync: true})
}

// MoveRedeemIncidentKeys moves incident evidence and resolutions that earlier
// builds wrote under bytes 4 and 5 to the current prefixes. Call it before
// the incident store is used.
//
// Only a key that is exactly such evidence is moved: it has the evidence
// layout and its value is the incident the key names. Other keys under 4 and
// 5 are left alone. A key with the evidence layout whose value is not that
// incident is an error, so the signer does not start without evidence it
// cannot account for. The move is one synced batch: a crash leaves the old
// layout or the new one, and running it again moves nothing.
func MoveRedeemIncidentKeys(db *leveldb.DB) (int, error) {
	it := db.NewIterator(util.BytesPrefix([]byte{oldRedeemEvidencePrefix}), nil)
	defer it.Release()
	batch := new(leveldb.Batch)
	moved := 0
	for it.Next() {
		old := slices.Clone(it.Key())
		if len(old) != redeemEvidenceKeyLength {
			continue
		}
		var ev common.RedeemIncident
		if err := json.Unmarshal(it.Value(), &ev); err != nil {
			return 0, fmt.Errorf("unreadable redeem incident evidence from an earlier build under key %x: %w", old, err)
		}
		key := redeemEvidenceKey(ev)
		if !bytes.Equal(key[1:], old[1:]) {
			return 0, fmt.Errorf("redeem incident evidence from an earlier build under key %x names another incident", old)
		}
		if has, err := db.Has(key, nil); err != nil {
			return 0, err
		} else if !has {
			batch.Put(key, slices.Clone(it.Value()))
		}
		oldMarker := append([]byte{oldRedeemHaltConfirmedPrefix}, old[1:]...)
		resolution, err := db.Get(oldMarker, nil)
		switch {
		case errors.Is(err, leveldb.ErrNotFound):
		case err != nil:
			return 0, err
		default:
			if has, err := db.Has(confirmedKey(key), nil); err != nil {
				return 0, err
			} else if !has {
				batch.Put(confirmedKey(key), resolution)
			}
			batch.Delete(oldMarker)
		}
		batch.Delete(old)
		moved++
	}
	if err := it.Error(); err != nil {
		return 0, err
	}
	if moved == 0 {
		return 0, nil
	}
	return moved, db.Write(batch, &opt.WriteOptions{Sync: true})
}
