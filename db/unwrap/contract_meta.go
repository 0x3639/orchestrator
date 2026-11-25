package unwrap

import (
	"bytes"
	"errors"
	"fmt"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
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
