package events

import (
	"fmt"
	ecommon "github.com/ethereum/go-ethereum/common"
	zcommon "github.com/zenon-network/go-zenon/common"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"github.com/zenon-network/go-zenon/vm/embedded/implementation"
	"google.golang.org/protobuf/proto"
	"math/big"
)

// WrapRequestZnn znn -> wZnn networkClass and ChainId belong to the destination network
type WrapRequestZnn struct {
	NetworkClass  uint32
	ChainId       uint32
	Id            types.Hash // hash of the send block
	ToAddress     string
	TokenAddress  string
	Amount        *big.Int
	Fee           *big.Int
	Signature     string
	RedeemStatus  uint32
	SentSignature bool
}

func (e *WrapRequestZnn) GetMessage(address *ecommon.Address) ([]byte, error) {
	request := &definition.WrapTokenRequest{
		NetworkClass: e.NetworkClass,
		ChainId:      e.ChainId,
		Id:           e.Id,
		ToAddress:    e.ToAddress,
		TokenAddress: e.TokenAddress,
		Amount:       e.Amount,
		Fee:          e.Fee,
	}
	return implementation.GetWrapTokenRequestMessage(request, address)
}

func (e *WrapRequestZnn) Proto() *WrapRequestZnnProto {
	return &WrapRequestZnnProto{
		NetworkClass:  e.NetworkClass,
		ChainId:       e.ChainId,
		Id:            e.Id.Bytes(),
		ToAddress:     e.ToAddress,
		TokenAddress:  e.TokenAddress,
		Amount:        zcommon.BigIntToBytes(e.Amount),
		Fee:           zcommon.BigIntToBytes(e.Fee),
		Signature:     e.Signature,
		Status:        e.RedeemStatus,
		SentSignature: e.SentSignature,
	}
}

func DeProtoWrapEventZnn(e *WrapRequestZnnProto) *WrapRequestZnn {
	ev := &WrapRequestZnn{
		NetworkClass:  e.NetworkClass,
		ChainId:       e.ChainId,
		Id:            types.BytesToHashPanic(e.Id),
		ToAddress:     e.ToAddress,
		TokenAddress:  e.TokenAddress,
		Amount:        zcommon.BytesToBigInt(e.Amount),
		Fee:           zcommon.BytesToBigInt(e.Fee),
		Signature:     e.Signature,
		RedeemStatus:  e.Status,
		SentSignature: e.SentSignature,
	}
	return ev
}

func (e *WrapRequestZnn) Serialize() ([]byte, error) {
	return proto.Marshal(e.Proto())
}
func DeserializeWrapEventZnn(data []byte) (*WrapRequestZnn, error) {
	ev := &WrapRequestZnnProto{}
	if err := proto.Unmarshal(data, ev); err != nil {
		return nil, err
	}
	id, err := types.BytesToHash(ev.Id)
	if err != nil {
		return nil, fmt.Errorf("invalid wrap request id: %w", err)
	}
	return &WrapRequestZnn{
		NetworkClass:  ev.NetworkClass,
		ChainId:       ev.ChainId,
		Id:            id,
		ToAddress:     ev.ToAddress,
		TokenAddress:  ev.TokenAddress,
		Amount:        zcommon.BytesToBigInt(ev.Amount),
		Fee:           zcommon.BytesToBigInt(ev.Fee),
		Signature:     ev.Signature,
		RedeemStatus:  ev.Status,
		SentSignature: ev.SentSignature,
	}, nil
}
