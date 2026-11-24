package wrap

import (
	"encoding/binary"
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	zcommon "github.com/zenon-network/go-zenon/common"
	"github.com/zenon-network/go-zenon/common/types"
	"orchestrator/common"
	"orchestrator/common/events"
)

func getWrapEventPrefix() []byte {
	return zcommon.JoinBytes(wrapEventPrefix)
}

func getWrapEventKey(id types.Hash) []byte {
	return zcommon.JoinBytes(getWrapEventPrefix(), id.Bytes())
}

// AddWrapRequest stores the event only if no record exists for its id. Every
// caller wants a missing record created, and the stored record is newer than
// any copy built from a Zenon receive or RPC response: a late or replayed
// WrapToken receive, or a caller that saw the record missing before another
// goroutine added and updated it, must not reset its signature, sent flag or
// redeem status.
func (es *eventStore) AddWrapRequest(event events.WrapRequestZnn) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	existing, err := es.GetWrapRequestById(event.Id)
	if err != nil {
		return err
	}
	if existing != nil {
		return nil
	}
	return es.putWrapRequest(&event)
}

// putWrapRequest writes the whole record. Callers must hold es.mu.
func (es *eventStore) putWrapRequest(event *events.WrapRequestZnn) error {
	if eventBytes, err := event.Serialize(); err != nil {
		es.SendSigInt()
		return err
	} else {
		if err := es.DB.Put(getWrapEventKey(event.Id), eventBytes); err != nil {
			es.SendSigInt()
			return err
		}
	}
	return nil
}

func (es *eventStore) GetWrapRequestById(id types.Hash) (*events.WrapRequestZnn, error) {
	data, err := es.DB.Get(getWrapEventKey(id))
	if err == leveldb.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		es.SendSigInt()
		return nil, err
	}

	event, err := events.DeserializeWrapEventZnn(data)
	if err != nil {
		es.SendSigInt()
		return nil, err
	}
	return event, nil
}

// updateWrapRequest re-reads the record and writes it back with one field
// changed, all under es.mu. The EVM listener (redeem status), the signing loop
// (signature, sent flag) and the Zenon listener (sent flag, signature) update
// the same record from different goroutines; without the lock a stale read in
// one of them writes back over a field another one set in between.
func (es *eventStore) updateWrapRequest(id types.Hash, change func(*events.WrapRequestZnn)) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	event, err := es.GetWrapRequestById(id)
	if err != nil {
		es.SendSigInt()
		return err
	}
	if event == nil {
		return leveldb.ErrNotFound
	}
	change(event)
	return es.putWrapRequest(event)
}

func (es *eventStore) SetWrapRequestStatus(id types.Hash, status uint32) error {
	return es.updateWrapRequest(id, func(event *events.WrapRequestZnn) {
		event.RedeemStatus = status
	})
}

func (es *eventStore) SetWrapRequestSignature(id types.Hash, signature string) error {
	return es.updateWrapRequest(id, func(event *events.WrapRequestZnn) {
		event.Signature = signature
	})
}

func (es *eventStore) SetWrapRequestSentSignature(id types.Hash, value bool) error {
	return es.updateWrapRequest(id, func(event *events.WrapRequestZnn) {
		event.SentSignature = value
	})
}

func (es *eventStore) GetUnsentSignedWrapRequests() ([]*events.WrapRequestZnn, error) {
	iterator := es.DB.NewIterator(getWrapEventPrefix())
	defer iterator.Release()
	result := make([]*events.WrapRequestZnn, 0)

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

		event, err := events.DeserializeWrapEventZnn(iterator.Value())
		if err != nil {
			es.SendSigInt()
			return nil, err
		}
		if event.SentSignature || len(event.Signature) == 0 {
			continue
		}

		result = append(result, event)
	}
	return result, nil
}

func (es *eventStore) GetResignableWrapRequests() ([]*events.WrapRequestZnn, error) {
	iterator := es.DB.NewIterator(getWrapEventPrefix())
	defer iterator.Release()
	result := make([]*events.WrapRequestZnn, 0)

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

		event, err := events.DeserializeWrapEventZnn(iterator.Value())
		if err != nil {
			es.SendSigInt()
			return nil, err
		}
		resignable, err := es.GetResignStatus(event.Id)
		if err != nil {
			es.SendSigInt()
			return nil, err
		}
		if !resignable {
			continue
		}

		result = append(result, event)
	}
	return result, nil
}

func (es *eventStore) GetUnredeemedWrapRequests() ([]*events.WrapRequestZnn, error) {
	iterator := es.DB.NewIterator(getWrapEventPrefix())
	defer iterator.Release()
	result := make([]*events.WrapRequestZnn, 0)

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

		event, err := events.DeserializeWrapEventZnn(iterator.Value())
		if err != nil {
			es.SendSigInt()
			return nil, err
		}
		if event.RedeemStatus != common.UnredeemedStatus {
			continue
		}

		result = append(result, event)
	}
	return result, nil
}

func (es *eventStore) GetUnsignedWrapRequests() ([]*events.WrapRequestZnn, error) {
	iterator := es.DB.NewIterator(getWrapEventPrefix())
	defer iterator.Release()
	result := make([]*events.WrapRequestZnn, 0)

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

		event, err := events.DeserializeWrapEventZnn(iterator.Value())
		if err != nil {
			es.SendSigInt()
			return nil, err
		}
		if len(event.Signature) > 0 {
			continue
		}

		result = append(result, event)
	}
	return result, nil
}

func getLastUpdateKey() []byte {
	return zcommon.JoinBytes(lastUpdatePrefix)
}

// Monitoring can trust a previously verified receive cursor on a clean restart.
// A missing, mismatched or unreadable checkpoint simply means partial coverage.
func (es *eventStore) HasReceiveCoverageCheckpoint(height uint64) bool {
	data, err := es.DB.Get(receiveCoverageKey)
	return err == nil && len(data) == 8 && binary.LittleEndian.Uint64(data) == height
}

func (es *eventStore) SaveReceiveCoverageCheckpoint(height uint64) error {
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], height)
	return es.DB.Put(receiveCoverageKey, data[:])
}

func (es *eventStore) GetLastUpdateHeight() (uint64, error) {
	data, err := es.DB.Get(getLastUpdateKey())
	if err == leveldb.ErrNotFound {
		return 1, nil
	}

	if err != nil {
		es.SendSigInt()
		return 0, err
	}

	return binary.LittleEndian.Uint64(data), nil
}

func (es *eventStore) SetLastUpdateHeight(accBlHeight uint64) error {
	if _, err := es.GetLastUpdateHeight(); err != nil {
		return err
	} else {
		bytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(bytes, accBlHeight)
		if err := es.DB.Put(getLastUpdateKey(), bytes); err != nil {
			es.SendSigInt()
			return err
		}
	}
	return nil
}

func getResignedStatusKey(id types.Hash) []byte {
	return zcommon.JoinBytes(resignedStatusPrefix, id.Bytes())
}

func (es *eventStore) GetResignStatus(id types.Hash) (bool, error) {
	data, err := es.DB.Get(getResignedStatusKey(id))
	if errors.Is(err, leveldb.ErrNotFound) {
		return false, nil
	}

	if err != nil {
		es.SendSigInt()
		return false, err
	}
	value := binary.LittleEndian.Uint32(data)
	ans := false
	if value > 0 {
		ans = true
	}
	return ans, nil
}

func (es *eventStore) SetResignStatus(id types.Hash, status bool) error {
	bytes := make([]byte, 4)
	value := uint32(0)
	if status {
		value = 1
	}
	binary.LittleEndian.PutUint32(bytes, value)
	if err := es.DB.Put(getResignedStatusKey(id), bytes); err != nil {
		es.SendSigInt()
		return err
	}
	return nil
}
