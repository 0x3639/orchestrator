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

func (es *eventStore) AddWrapRequest(event events.WrapRequestZnn) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	data, err := es.DB.Get(getWrapEventKey(event.Id))
	if errors.Is(err, leveldb.ErrNotFound) {
		return es.putWrap(&event)
	}
	if err != nil {
		es.SendSigInt()
		return err
	}
	// A repeated Zenon observation can supply a missing signature. Other
	// signature changes go through the explicit update and resign paths.
	if event.Signature != "" {
		current, err := events.DeserializeWrapEventZnn(data)
		if err != nil {
			es.SendSigInt()
			return err
		}
		if current.Signature == "" {
			current.Signature = event.Signature
			current.SentSignature = current.SentSignature || event.SentSignature
			return es.putWrap(current)
		}
	}
	return nil
}

func (es *eventStore) putWrap(event *events.WrapRequestZnn) error {
	data, err := event.Serialize()
	if err == nil {
		err = es.putDurable(getWrapEventKey(event.Id), data)
	}
	if err != nil {
		es.SendSigInt()
	}
	return err
}

func (es *eventStore) GetWrapRequestById(id types.Hash) (*events.WrapRequestZnn, error) {
	data, err := es.DB.Get(getWrapEventKey(id))
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil, nil
	}
	if err == nil {
		var event *events.WrapRequestZnn
		event, err = es.decodeWrap(data)
		if err == nil {
			return event, nil
		}
	}
	es.SendSigInt()
	return nil, err
}

// Use the underlying Zenon row here so signature writes never copy an EVM log
// observation into the legacy, unpositioned status field.
func (es *eventStore) updateWrap(id types.Hash, update func(*events.WrapRequestZnn)) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	data, err := es.DB.Get(getWrapEventKey(id))
	if err != nil {
		if !errors.Is(err, leveldb.ErrNotFound) {
			es.SendSigInt()
		}
		return err
	}
	event, err := events.DeserializeWrapEventZnn(data)
	if err != nil {
		es.SendSigInt()
		return err
	}
	update(event)
	return es.putWrap(event)
}

func (es *eventStore) SetWrapRequestStatus(id types.Hash, status uint32) error {
	return es.updateWrap(id, func(event *events.WrapRequestZnn) {
		// Resign reconciliation starts from an unredeemed wrap and reads the
		// contract without a log position. Preserve any known terminal status.
		event.RedeemStatus = max(event.RedeemStatus, status)
	})
}

func (es *eventStore) SetWrapRequestSignature(id types.Hash, signature string) error {
	return es.updateWrap(id, func(event *events.WrapRequestZnn) { event.Signature = signature })
}

func (es *eventStore) SetWrapRequestSentSignature(id types.Hash, value bool) error {
	return es.updateWrap(id, func(event *events.WrapRequestZnn) { event.SentSignature = value })
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

		event, err := es.decodeWrap(iterator.Value())
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

		event, err := es.decodeWrap(iterator.Value())
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

		event, err := es.decodeWrap(iterator.Value())
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

		event, err := es.decodeWrap(iterator.Value())
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

// BridgeWatchFrom returns the first bridge account height this database
// vouches for, or zero while it vouches for none. A receive at or above it is
// new to this node whenever it is read. Startup sets it when it reaches the
// tip, or lower when it meets a receive it has to check on the way there.
func (es *eventStore) BridgeWatchFrom() (uint64, error) {
	data, err := es.DB.Get(bridgeWatchKey)
	if err == leveldb.ErrNotFound {
		return 0, nil
	}
	if err != nil {
		es.SendSigInt()
		return 0, err
	}
	if len(data) != 8 || binary.LittleEndian.Uint64(data) == 0 {
		return 0, errors.New("invalid bridge watch record")
	}
	return binary.LittleEndian.Uint64(data), nil
}

func (es *eventStore) SaveBridgeWatchFrom(height uint64) error {
	if height == 0 {
		return errors.New("bridge watch record needs an account height")
	}
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], height)
	if err := es.DB.Put(bridgeWatchKey, data[:]); err != nil {
		es.SendSigInt()
		return err
	}
	return nil
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
