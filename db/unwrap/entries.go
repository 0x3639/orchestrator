package unwrap

import (
	"encoding/binary"
	"errors"
	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/syndtr/goleveldb/leveldb"
	zcommon "github.com/zenon-network/go-zenon/common"
	"orchestrator/common"
	"orchestrator/common/events"
)

func getUnwrapRequestPrefix() []byte {
	return zcommon.JoinBytes(unwrapRequestPrefix)
}

func getUnwrapRequestKey(txHash ecommon.Hash, logIndex uint32) []byte {
	logNumberBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(logNumberBytes, logIndex)
	return zcommon.JoinBytes(getUnwrapRequestPrefix(), txHash.Bytes(), logNumberBytes)
}

func (es *evmStorage) AddUnwrapRequest(event events.UnwrapRequestEvm) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	return es.putUnwrapRequest(event)
}

// putUnwrapRequest writes the record. Callers must hold es.mu.
func (es *evmStorage) putUnwrapRequest(event events.UnwrapRequestEvm) error {
	if eventBytes, err := event.Serialize(); err != nil {
		es.SendSigInt()
		return err
	} else {
		if err := es.DB.Put(getUnwrapRequestKey(event.TransactionHash, event.LogIndex), eventBytes); err != nil {
			es.SendSigInt()
			return err
		}
	}
	return nil
}

func (es *evmStorage) AddUnwrapRequestIfMissing(event events.UnwrapRequestEvm) (bool, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	existing, err := es.GetUnwrapRequestByHashAndLog(event.TransactionHash, event.LogIndex)
	if err != nil {
		return false, err
	}
	if existing != nil {
		return false, nil
	}
	return true, es.putUnwrapRequest(event)
}

func (es *evmStorage) DeleteUnwrapRequestIfUnsigned(txHash ecommon.Hash, logIndex uint32, blockHash ecommon.Hash) (bool, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	current, err := es.GetUnwrapRequestByHashAndLog(txHash, logIndex)
	if err != nil {
		return false, err
	}
	if current == nil {
		return false, nil
	}
	// The record may have been signed, marked by the Zenon listener, or
	// refreshed to another block since the caller inspected it.
	if current.Signature != "" || current.RedeemStatus != common.UnredeemedStatus || current.BlockHash != blockHash {
		return false, nil
	}
	if err := es.DB.Delete(getUnwrapRequestKey(txHash, logIndex)); err != nil {
		es.SendSigInt()
		return false, err
	}
	return true, nil
}

func (es *evmStorage) UpdateUnwrapRequestBlockNumber(event events.UnwrapRequestEvm) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	localEvent, err := es.GetUnwrapRequestByHashAndLog(event.TransactionHash, event.LogIndex)
	if err != nil {
		es.SendSigInt()
		return err
	}
	if localEvent == nil {
		return es.putUnwrapRequest(event)
	}
	localEvent.BlockNumber = event.BlockNumber
	localEvent.BlockHash = event.BlockHash
	if localEvent.From == (ecommon.Address{}) && localEvent.SameTransfer(&event) {
		localEvent.From = event.From
	}
	return es.putUnwrapRequest(*localEvent)
}

// Fill a legacy row only if it is still the unsigned candidate that was checked.
func (es *evmStorage) HydrateUnsignedUnwrap(before, verified events.UnwrapRequestEvm) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	current, err := es.GetUnwrapRequestByHashAndLog(before.TransactionHash, before.LogIndex)
	if err != nil {
		return err
	}
	if current == nil || current.Signature != "" || current.RedeemStatus != common.UnredeemedStatus ||
		!current.SameTransfer(&verified) || current.From != before.From || current.BlockHash != before.BlockHash || current.BlockNumber != before.BlockNumber {
		return errors.New("stored unwrap changed during source verification")
	}
	current.From, current.BlockHash, current.BlockNumber = verified.From, verified.BlockHash, verified.BlockNumber
	return es.putUnwrapRequest(*current)
}

func (es *evmStorage) UnwrapAffiliatePolicy(key ecommon.Hash) (common.AffiliatePolicy, bool, error) {
	value, err := es.DB.Get(zcommon.JoinBytes(affiliatePolicyPrefix, key.Bytes()))
	if errors.Is(err, leveldb.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if len(value) != 1 || value[0] > byte(common.SplitAffiliate|common.VerifyAffiliate) {
		return 0, false, errors.New("invalid stored unwrap affiliate policy")
	}
	return common.AffiliatePolicy(value[0]), true, nil
}

func (es *evmStorage) PinUnwrapAffiliatePolicy(key ecommon.Hash, policy common.AffiliatePolicy) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	stored, exists, err := es.UnwrapAffiliatePolicy(key)
	if err != nil {
		return err
	}
	if exists {
		if policy != stored {
			return errors.New("unwrap affiliate policy changed during observation; retry")
		}
		return nil
	}
	return es.DB.Put(zcommon.JoinBytes(affiliatePolicyPrefix, key.Bytes()), []byte{byte(policy)})
}

func (es *evmStorage) GetUnwrapRequestByHashAndLog(txHash ecommon.Hash, logIndex uint32) (*events.UnwrapRequestEvm, error) {
	data, err := es.DB.Get(getUnwrapRequestKey(txHash, logIndex))
	if err == leveldb.ErrNotFound {
		return nil, nil
	}

	if err != nil {
		es.SendSigInt()
		return nil, err
	}

	event, err := events.DeserializeEvmUnwrapRequest(data)
	if err != nil {
		es.SendSigInt()
		return nil, err
	}
	return event, nil
}

func (es *evmStorage) SetUnwrapRequestStatus(txHash ecommon.Hash, logIndex, status uint32) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	if event, err := es.GetUnwrapRequestByHashAndLog(txHash, logIndex); err != nil {
		es.SendSigInt()
		return err
	} else if event == nil {
		return leveldb.ErrNotFound
	} else {
		event.RedeemStatus = status
		if eventBytes, err := event.Serialize(); err != nil {
			es.SendSigInt()
			return err
		} else {
			if err := es.DB.Put(getUnwrapRequestKey(event.TransactionHash, event.LogIndex), eventBytes); err != nil {
				es.SendSigInt()
				return err
			}
		}
	}
	return nil
}

func (es *evmStorage) SetUnwrapRequestSignature(txHash ecommon.Hash, logIndex uint32, signature string) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	if event, err := es.GetUnwrapRequestByHashAndLog(txHash, logIndex); err != nil {
		es.SendSigInt()
		return err
	} else if event == nil {
		return leveldb.ErrNotFound
	} else {
		event.Signature = signature
		if eventBytes, err := event.Serialize(); err != nil {
			es.SendSigInt()
			return err
		} else {
			if err := es.DB.Put(getUnwrapRequestKey(event.TransactionHash, event.LogIndex), eventBytes); err != nil {
				es.SendSigInt()
				return err
			}
		}
	}
	return nil
}

func (es *evmStorage) SetUnsentUnwrapRequestAsUnsigned(txHash ecommon.Hash, logIndex uint32) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	if event, err := es.GetUnwrapRequestByHashAndLog(txHash, logIndex); err != nil {
		es.SendSigInt()
		return err
	} else if event == nil {
		return leveldb.ErrNotFound
	} else {
		event.Signature = ""
		event.RedeemStatus = common.UnredeemedStatus
		if eventBytes, err := event.Serialize(); err != nil {
			es.SendSigInt()
			return err
		} else {
			if err := es.DB.Put(getUnwrapRequestKey(event.TransactionHash, event.LogIndex), eventBytes); err != nil {
				es.SendSigInt()
				return err
			}
		}
	}
	return nil
}

func (es *evmStorage) GetUnwrapRequestsByStatus(status uint32) ([]*events.UnwrapRequestEvm, error) {
	iterator := es.DB.NewIterator(getUnwrapRequestPrefix())
	defer iterator.Release()
	result := make([]*events.UnwrapRequestEvm, 0)

	for {
		if !iterator.Next() {
			if iterator.Error() != nil {
				es.SendSigInt()
				return nil, iterator.Error()
			}
			break
		}
		if iterator.Value() == nil {
			continue
		}

		event, err := events.DeserializeEvmUnwrapRequest(iterator.Value())
		if err != nil {
			es.SendSigInt()
			return nil, err
		}
		if event.RedeemStatus != status {
			continue
		}

		result = append(result, event)
	}
	return result, nil
}

func (es *evmStorage) GetUnsignedUnwrapRequests() ([]*events.UnwrapRequestEvm, error) {
	iterator := es.DB.NewIterator(getUnwrapRequestPrefix())
	defer iterator.Release()
	result := make([]*events.UnwrapRequestEvm, 0)

	for {
		if !iterator.Next() {
			if iterator.Error() != nil {
				es.SendSigInt()
				return nil, iterator.Error()
			}
			break
		}
		if iterator.Value() == nil {
			continue
		}

		event, err := events.DeserializeEvmUnwrapRequest(iterator.Value())
		if err != nil {
			es.SendSigInt()
			return nil, err
		}
		if len(event.Signature) > 0 {
			continue
		}
		// An unsigned record that is no longer unredeemed is already known to
		// Zenon (reconciled from the chain), so it must not enter the signing
		// pool: every signer has to build the identical pool for the TSS
		// ceremony to form a party.
		if event.RedeemStatus != common.UnredeemedStatus {
			continue
		}

		result = append(result, event)
	}
	return result, nil
}

func (es *evmStorage) GetUnsentSignedUnwrapRequests() ([]*events.UnwrapRequestEvm, error) {
	iterator := es.DB.NewIterator(getUnwrapRequestPrefix())
	defer iterator.Release()
	result := make([]*events.UnwrapRequestEvm, 0)

	for {
		if !iterator.Next() {
			if iterator.Error() != nil {
				es.SendSigInt()
				return nil, iterator.Error()
			}
			break
		}
		if iterator.Value() == nil {
			continue
		}

		event, err := events.DeserializeEvmUnwrapRequest(iterator.Value())
		if err != nil {
			es.SendSigInt()
			return nil, err
		}
		// unredeemed means unsent
		if len(event.Signature) > 0 && event.RedeemStatus == common.UnredeemedStatus {
			result = append(result, event)
		}
	}
	return result, nil
}

func getLastUpdateKey() []byte {
	return zcommon.JoinBytes(lastUpdatePrefix)
}

func (es *evmStorage) GetLastUpdateHeight() (uint64, error) {
	through, _, _, err := es.GetScanHeights(0)
	return through, err
}

func (es *evmStorage) GetScanHeights(depth uint64) (through, redeem uint64, legacy bool, err error) {
	data, err := es.DB.Get(getLastUpdateKey())
	if errors.Is(err, leveldb.ErrNotFound) {
		return es.contractDeploymentHeight, es.contractDeploymentHeight, false, nil
	}
	if err != nil {
		es.SendSigInt()
		return 0, 0, false, err
	}
	switch len(data) {
	case 8:
		through = binary.LittleEndian.Uint64(data)
		return through, min(through, max(es.contractDeploymentHeight, through-min(through, depth))), true, nil
	case 16:
		through, redeem = binary.LittleEndian.Uint64(data), binary.LittleEndian.Uint64(data[8:])
		if redeem <= through {
			return through, redeem, false, nil
		}
	}
	return 0, 0, false, errors.New("invalid EVM scan checkpoint")
}

func (es *evmStorage) SetScanHeights(through, redeem uint64) error {
	data := binary.LittleEndian.AppendUint64(nil, through)
	err := es.DB.Put(getLastUpdateKey(), binary.LittleEndian.AppendUint64(data, redeem))
	if err != nil {
		es.SendSigInt()
	}
	return err
}

// An explicit rewind resets the finalized checkpoint as well. A redeem
// checkpoint that is already below the target is kept: the blocks between it
// and the target have not been through the finalized redeem scan yet.
func (es *evmStorage) SetLastUpdateHeight(height uint64) error {
	data := binary.LittleEndian.AppendUint64(nil, height)
	stored, err := es.DB.Get(getLastUpdateKey())
	if err != nil && !errors.Is(err, leveldb.ErrNotFound) {
		es.SendSigInt()
		return err
	}
	if len(stored) == 16 {
		if redeem := binary.LittleEndian.Uint64(stored[8:]); redeem < height {
			data = binary.LittleEndian.AppendUint64(data, redeem)
		}
	}
	err = es.DB.Put(getLastUpdateKey(), data)
	if err != nil {
		es.SendSigInt()
	}
	return err
}
