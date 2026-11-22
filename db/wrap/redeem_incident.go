package wrap

import (
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

// Reserved outside the wrap, cursor, resign and health keys in the Zenon DB.
// Keep the original evidence immutable after the bridge is halted.
const redeemEvidencePrefix byte = 4
const redeemHaltConfirmedPrefix byte = 5

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
		batch.Put(marker, []byte{1})
	}
	if err := it.Error(); err != nil {
		return err
	}
	return s.db.Write(batch, &opt.WriteOptions{Sync: true})
}
