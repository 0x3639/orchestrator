package unwrap

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/syndtr/goleveldb/leveldb"
	zcommon "github.com/zenon-network/go-zenon/common"

	"orchestrator/common"
)

// BindContractAddress binds ldb to contractAddress if unbound, and reports
// whether ldb belongs to contractAddress. A mismatch indicates a same-name
// contract rotation; the caller keeps ldb untouched on disk to retain old
// evidence and opens a contract-scoped directory for the new bridge address.
func BindContractAddress(ldb *leveldb.DB, contractAddress ecommon.Address) (bool, error) {
	if ldb == nil {
		return false, errors.New("nil leveldb")
	}
	if contractAddress == (ecommon.Address{}) {
		return true, nil
	}
	stored, err := ldb.Get(contractAddressPrefix, nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		if putErr := ldb.Put(contractAddressPrefix, contractAddress.Bytes(), nil); putErr != nil {
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
	raw, err := es.DB.Get(pendingEvmTxKey(sender, contract, kind))
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
	return es.DB.Put(pendingEvmTxKey(sender, contract, kind), raw)
}

func (es *evmStorage) DeletePendingEvmTx(sender, contract ecommon.Address, kind string) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	err := es.DB.Delete(pendingEvmTxKey(sender, contract, kind))
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil
	}
	return err
}
