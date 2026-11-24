package db

import (
	ecommon "github.com/ethereum/go-ethereum/common"
	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
	"orchestrator/common"
	"orchestrator/common/events"
)

// An event will have 4 stage: unsigned -> signed -> unsent -> sent

type EvmStorage interface {
	Storage() zdb.DB
	Snapshot() EvmStorage
	SendSigInt()

	AddUnwrapRequest(events.UnwrapRequestEvm) error
	// AddRegisteredUnwrapRequest stores a request read from the bridge and
	// keeps the source block and sender an existing record already holds.
	AddRegisteredUnwrapRequest(events.UnwrapRequestEvm) error
	UpdateUnwrapRequestBlockNumber(events.UnwrapRequestEvm) error
	HydrateUnsignedUnwrap(before, verified events.UnwrapRequestEvm) error
	UnwrapAffiliatePolicy(ecommon.Hash) (common.AffiliatePolicy, bool, error)
	PinUnwrapAffiliatePolicy(ecommon.Hash, common.AffiliatePolicy) error
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

	GetScanHeights(depth uint64) (through, redeem uint64, legacy bool, err error)
	SetScanHeights(through, redeem uint64) error
	GetLastUpdateHeight() (uint64, error)
	SetLastUpdateHeight(uint64) error
}

type ZnnStorage interface {
	Storage() zdb.DB
	Snapshot() ZnnStorage
	SendSigInt()

	AddWrapRequest(events.WrapRequestZnn) error
	HasLegacyRedeemStatuses(chainID uint32) (bool, error)
	ApplyRedeemLog(chainID uint32, id types.Hash, block uint64, index uint, status uint32) error
	SetWrapRequestStatus(types.Hash, uint32) error
	SetWrapRequestSignature(types.Hash, string) error
	SetWrapRequestSentSignature(types.Hash, bool) error
	GetWrapRequestById(types.Hash) (*events.WrapRequestZnn, error)
	GetUnsentSignedWrapRequests() ([]*events.WrapRequestZnn, error)
	GetUnredeemedWrapRequests() ([]*events.WrapRequestZnn, error)
	GetResignableWrapRequests() ([]*events.WrapRequestZnn, error)

	GetUnsignedWrapRequests() ([]*events.WrapRequestZnn, error)

	GetLastUpdateHeight() (uint64, error)
	HasReceiveCoverageCheckpoint(uint64) bool
	SaveReceiveCoverageCheckpoint(uint64) error
	SetLastUpdateHeight(uint64) error
	BridgeWatchFrom() (uint64, error)
	SaveBridgeWatchFrom(uint64) error

	GetResignStatus(types.Hash) (bool, error)
	SetResignStatus(types.Hash, bool) error
}
