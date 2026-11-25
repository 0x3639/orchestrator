package wrap

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"sync"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"orchestrator/common"
)

// UnwrapCorrobStore durably persists secondary-endpoint corroboration cursors
// in the Zenon LevelDB so multi-million-block verification resumes across
// receive-hold restarts instead of restarting from the chain head.
type UnwrapCorrobStore struct {
	db *leveldb.DB
	mu sync.Mutex
}

func NewUnwrapCorrobStore(db *leveldb.DB) *UnwrapCorrobStore {
	if db == nil {
		return nil
	}
	return &UnwrapCorrobStore{db: db}
}

func unwrapCorrobKey(chainID uint32, contract ecommon.Address, txHash ecommon.Hash, logIndex uint32) []byte {
	key := make([]byte, 0, 1+4+ecommon.AddressLength+ecommon.HashLength+4)
	key = append(key, unwrapCorrobCursorPrefix)
	key = binary.BigEndian.AppendUint32(key, chainID)
	key = append(key, contract.Bytes()...)
	key = append(key, txHash.Bytes()...)
	key = binary.BigEndian.AppendUint32(key, logIndex)
	return key
}

func (s *UnwrapCorrobStore) LoadUnwrapCorrobCursor(chainID uint32, contract ecommon.Address, txHash ecommon.Hash, logIndex uint32) (*common.UnwrapCorrobCursor, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := s.db.Get(unwrapCorrobKey(chainID, contract, txHash, logIndex), nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var cur common.UnwrapCorrobCursor
	if err := json.Unmarshal(raw, &cur); err != nil {
		return nil, err
	}
	return &cur, nil
}

func (s *UnwrapCorrobStore) SaveUnwrapCorrobCursor(cur *common.UnwrapCorrobCursor) error {
	if s == nil || s.db == nil || cur == nil {
		return nil
	}
	raw, err := json.Marshal(cur)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Put(unwrapCorrobKey(cur.ChainID, cur.Contract, cur.TxHash, cur.LogIndex), raw, &opt.WriteOptions{Sync: true})
}

func (s *UnwrapCorrobStore) DeleteUnwrapCorrobCursor(chainID uint32, contract ecommon.Address, txHash ecommon.Hash, logIndex uint32) error {
	if s == nil || s.db == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.db.Delete(unwrapCorrobKey(chainID, contract, txHash, logIndex), &opt.WriteOptions{Sync: true})
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil
	}
	return err
}
