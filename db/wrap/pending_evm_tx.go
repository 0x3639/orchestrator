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

// PendingEvmTxStore durably persists unconfirmed EVM halt and key-rotation
// transaction ownership and fee watermarks in the Zenon LevelDB so they survive
// both process restarts and runtime EVM network removals/re-additions.
type PendingEvmTxStore struct {
	db *leveldb.DB
	mu sync.Mutex
}

// NewPendingEvmTxStore creates a durable PendingEvmTxStore backed by db.
func NewPendingEvmTxStore(db *leveldb.DB) *PendingEvmTxStore {
	return &PendingEvmTxStore{db: db}
}

// ForChain returns a common.PendingEvmTxStore scoped to chainID.
func (s *PendingEvmTxStore) ForChain(chainID uint32) common.PendingEvmTxStore {
	if s == nil || s.db == nil {
		return nil
	}
	return &chainPendingEvmTxStore{parent: s, chainID: chainID}
}

type chainPendingEvmTxStore struct {
	parent  *PendingEvmTxStore
	chainID uint32
}

func pendingEvmTxKey(chainID uint32, sender, contract ecommon.Address, kind string) []byte {
	key := make([]byte, 0, 1+4+ecommon.AddressLength+ecommon.AddressLength+len(kind))
	key = append(key, pendingEvmTxPrefix)
	key = binary.BigEndian.AppendUint32(key, chainID)
	key = append(key, sender.Bytes()...)
	key = append(key, contract.Bytes()...)
	key = append(key, kind...)
	return key
}

func (c *chainPendingEvmTxStore) LoadPendingEvmTx(sender, contract ecommon.Address, kind string) (*common.PendingEvmTx, error) {
	c.parent.mu.Lock()
	defer c.parent.mu.Unlock()
	raw, err := c.parent.db.Get(pendingEvmTxKey(c.chainID, sender, contract, kind), nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var rec common.PendingEvmTx
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (c *chainPendingEvmTxStore) SavePendingEvmTx(sender, contract ecommon.Address, kind string, rec *common.PendingEvmTx) error {
	if rec == nil {
		return nil
	}
	return c.ApplyPendingEvmTxBatch([]common.PendingEvmTxOp{{
		Sender:   sender,
		Contract: contract,
		Kind:     kind,
		Record:   rec,
	}})
}

func (c *chainPendingEvmTxStore) DeletePendingEvmTx(sender, contract ecommon.Address, kind string) error {
	return c.ApplyPendingEvmTxBatch([]common.PendingEvmTxOp{{
		Sender:   sender,
		Contract: contract,
		Kind:     kind,
		Record:   nil,
	}})
}

// ApplyPendingEvmTxBatch commits all puts and deletes in a single fsynced
// LevelDB batch so ownership transfer and rollback are crash-atomic.
func (c *chainPendingEvmTxStore) ApplyPendingEvmTxBatch(ops []common.PendingEvmTxOp) error {
	if len(ops) == 0 {
		return nil
	}
	batch := new(leveldb.Batch)
	for _, op := range ops {
		key := pendingEvmTxKey(c.chainID, op.Sender, op.Contract, op.Kind)
		if op.Record == nil {
			batch.Delete(key)
			continue
		}
		raw, err := json.Marshal(op.Record)
		if err != nil {
			return err
		}
		batch.Put(key, raw)
	}
	c.parent.mu.Lock()
	defer c.parent.mu.Unlock()
	return c.parent.db.Write(batch, &opt.WriteOptions{Sync: true})
}
