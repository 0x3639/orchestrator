package wrap

import (
	"encoding/binary"
	"errors"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"orchestrator/common/events"
)

// One record holds the latest EVM log position and status, independent of
// the Zenon wrap and its signature. A finalized log can arrive before the wrap.
type redeemStatus struct {
	block  uint64
	index  uint64
	status uint32
}

// The earlier draft retained only the latest provisional event per nonce.
// Its registration may predate that record, so replay from deployment.
func (es *eventStore) HasLegacyRedeemStatuses(chainID uint32) (bool, error) {
	it := es.DB.NewIterator(binary.BigEndian.AppendUint32([]byte{legacyRedeemStatusPrefix}, chainID))
	defer it.Release()
	found := it.Next()
	return found, it.Error()
}

func redeemStatusKey(chainID uint32, id types.Hash) []byte {
	key := binary.BigEndian.AppendUint32([]byte{redeemStatusPrefix}, chainID)
	return append(key, id.Bytes()...)
}

func (es *eventStore) readRedeemStatus(key []byte) (*redeemStatus, error) {
	b, err := es.DB.Get(key)
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(b) != 20 {
		return nil, errors.New("invalid stored redeem log position and status")
	}
	return &redeemStatus{binary.BigEndian.Uint64(b), binary.BigEndian.Uint64(b[8:]), binary.BigEndian.Uint32(b[16:])}, nil
}

func (es *eventStore) ApplyRedeemLog(chainID uint32, id types.Hash, block uint64, index uint, status uint32) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	key := redeemStatusKey(chainID, id)
	last, err := es.readRedeemStatus(key)
	if err != nil {
		return err
	}
	if last != nil && (block < last.block || (block == last.block && uint64(index) <= last.index)) {
		return nil
	}
	b := binary.BigEndian.AppendUint64(nil, block)
	b = binary.BigEndian.AppendUint64(b, uint64(index))
	return es.DB.Put(key, binary.BigEndian.AppendUint32(b, status))
}

func (es *eventStore) decodeWrap(data []byte) (*events.WrapRequestZnn, error) {
	wrap, err := events.DeserializeWrapEventZnn(data)
	if err != nil || wrap.NetworkClass != definition.EvmClass {
		return wrap, err
	}
	last, err := es.readRedeemStatus(redeemStatusKey(wrap.ChainId, wrap.Id))
	if err != nil {
		return nil, err
	}
	if last != nil {
		// Old rows and resign reconciliation have no log position. The contract
		// forbids registering a spent nonce and permits revoking a redeemed one.
		wrap.RedeemStatus = max(wrap.RedeemStatus, last.status)
	}
	return wrap, nil
}
