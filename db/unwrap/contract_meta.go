package unwrap

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	zcommon "github.com/zenon-network/go-zenon/common"

	"orchestrator/common"
)

// BindContractAddress verifies or binds ldb to contractAddress and reports
// whether ldb may be reused for contractAddress.
//
// Reuse requires verified provenance:
//   - If ldb already carries a contract-address marker, it matches iff the
//     stored address equals contractAddress.
//   - If ldb has no marker (a fresh directory or a pre-upgrade database):
//     - A brand-new empty DB with no pre-existing queue is stamped with
//       contractAddress and reused.
//     - A populated unmarked DB or one with an existing unmarked queue has no
//       verified contract provenance, so it is left untouched on disk (not
//       stamped) and returns false so the caller isolates contractAddress in a
//       contract-scoped directory.
func BindContractAddress(ldb *leveldb.DB, contractAddress ecommon.Address, hasExistingQueue bool) (bool, error) {
	if ldb == nil {
		return false, errors.New("nil leveldb")
	}
	if contractAddress == (ecommon.Address{}) {
		return true, nil
	}
	stored, err := ldb.Get(contractAddressPrefix, nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		if hasExistingQueue {
			return false, nil
		}
		iter := ldb.NewIterator(nil, nil)
		hasAny := iter.First()
		iterErr := iter.Error()
		iter.Release()
		if iterErr != nil {
			return false, iterErr
		}
		if hasAny {
			return false, nil
		}
		if putErr := ldb.Put(contractAddressPrefix, contractAddress.Bytes(), &opt.WriteOptions{Sync: true}); putErr != nil {
			return false, putErr
		}
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if len(stored) != ecommon.AddressLength {
		return false, fmt.Errorf("corrupt stored EVM contract address length %d", len(stored))
	}
	return bytes.Equal(stored, contractAddress.Bytes()), nil
}

func pendingEvmTxKey(sender, contract ecommon.Address, kind string) []byte {
	return zcommon.JoinBytes(pendingTxPrefix, sender.Bytes(), contract.Bytes(), []byte(kind))
}

func (es *evmStorage) LoadPendingEvmTx(sender, contract ecommon.Address, kind string) (*common.PendingEvmTx, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	key := pendingEvmTxKey(sender, contract, kind)
	var (
		raw []byte
		err error
	)
	if es.rawLdb != nil {
		raw, err = es.rawLdb.Get(key, nil)
	} else {
		raw, err = es.DB.Get(key)
	}
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

func (es *evmStorage) SavePendingEvmTx(sender, contract ecommon.Address, kind string, rec *common.PendingEvmTx) error {
	if rec == nil {
		return nil
	}
	es.mu.Lock()
	defer es.mu.Unlock()
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	key := pendingEvmTxKey(sender, contract, kind)
	if es.rawLdb != nil {
		return es.rawLdb.Put(key, raw, &opt.WriteOptions{Sync: true})
	}
	return es.DB.Put(key, raw)
}

func (es *evmStorage) DeletePendingEvmTx(sender, contract ecommon.Address, kind string) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	key := pendingEvmTxKey(sender, contract, kind)
	var err error
	if es.rawLdb != nil {
		err = es.rawLdb.Delete(key, &opt.WriteOptions{Sync: true})
	} else {
		err = es.DB.Delete(key)
	}
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil
	}
	return err
}
