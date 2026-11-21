package db

import (
	ecommon "github.com/ethereum/go-ethereum/common"
	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
	"orchestrator/common/events"
)

// An event will have 4 stage: unsigned -> signed -> unsent -> sent

type EvmStorage interface {
	Storage() zdb.DB
	Snapshot() EvmStorage
	SendSigInt()

	AddUnwrapRequest(events.UnwrapRequestEvm) error
	UpdateUnwrapRequestBlockNumber(events.UnwrapRequestEvm) error
	GetUnwrapRequestByHashAndLog(ecommon.Hash, uint32) (*events.UnwrapRequestEvm, error)
	// AddUnwrapRequestIfMissing stores the event only when no record exists
	// for its hash and log index, so a duplicate delivery can never reset a
	// signed or sent record. It reports whether a record was added.
	AddUnwrapRequestIfMissing(events.UnwrapRequestEvm) (bool, error)
	// DeleteUnwrapRequestIfUnsigned removes the record only if, re-read under
	// the storage lock, it still has no signature, is still unredeemed and
	// still refers to the given block hash. It reports whether it deleted.
	DeleteUnwrapRequestIfUnsigned(txHash ecommon.Hash, logIndex uint32, blockHash ecommon.Hash) (bool, error)
	SetUnwrapRequestStatus(ecommon.Hash, uint32, uint32) error
	SetUnwrapRequestSignature(ecommon.Hash, uint32, string) error
	GetUnwrapRequestsByStatus(uint32) ([]*events.UnwrapRequestEvm, error)
	GetUnsignedUnwrapRequests() ([]*events.UnwrapRequestEvm, error)
	GetUnsentSignedUnwrapRequests() ([]*events.UnwrapRequestEvm, error)
	SetUnsentUnwrapRequestAsUnsigned(ecommon.Hash, uint32) error

	GetLastUpdateHeight() (uint64, error)
	SetLastUpdateHeight(uint64) error
}

type ZnnStorage interface {
	Storage() zdb.DB
	Snapshot() ZnnStorage
	SendSigInt()

	AddWrapRequest(events.WrapRequestZnn) error
	SetWrapRequestStatus(types.Hash, uint32) error
	SetWrapRequestSignature(types.Hash, string) error
	SetWrapRequestSentSignature(types.Hash, bool) error
	GetWrapRequestById(types.Hash) (*events.WrapRequestZnn, error)
	GetUnsentSignedWrapRequests() ([]*events.WrapRequestZnn, error)
	GetUnredeemedWrapRequests() ([]*events.WrapRequestZnn, error)
	GetResignableWrapRequests() ([]*events.WrapRequestZnn, error)

	GetUnsignedWrapRequests() ([]*events.WrapRequestZnn, error)

	GetLastUpdateHeight() (uint64, error)
	SetLastUpdateHeight(uint64) error

	GetResignStatus(types.Hash) (bool, error)
	SetResignStatus(types.Hash, bool) error
}
