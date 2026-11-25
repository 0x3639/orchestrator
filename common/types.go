package common

import (
	"math/big"

	ecommon "github.com/ethereum/go-ethereum/common"
)

type BridgeMetadata struct {
	PartyTimeout         uint64           `json:"partyTimeout"`
	KeyGenTimeout        uint64           `json:"keyGenTimeout"`
	KeySignTimeout       uint64           `json:"keySignTimeout"`
	PreParamTimeout      uint64           `json:"preParamTimeout"`
	JoinPartyVersion     string           `json:"joinPartyVersion"`
	AffiliateProgram     AffiliateProgram `json:"affiliateProgram"`
	ResignState          ResignState      `json:"resignState"`
	SignCeremonyPoolSize int              `json:"signCeremonyPoolSize"`
}

type ResignState struct {
	Active       bool   `json:"active"`
	NetworkClass uint32 `json:"networkClass"`
	ChainId      uint32 `json:"chainId"`
}

type AffiliateProgram struct {
	Networks map[uint32]AffiliateNetwork `json:"networks"`
}

type AffiliateNetwork struct {
	StartingHeight uint64 `json:"startingHeight"`

	// Used on unwrap verification on NoM
	ZNN bool `json:"ZNN"`
	QSR bool `json:"QSR"`

	// Used when splitting an unwrap on evm
	WZNN bool `json:"wZNN"`
	WQSR bool `json:"wQSR"`
}

// PendingEvmTxHash pairs a previously submitted tx hash with its nonce so
// post-restart obsolete-halt cleanup can scope hash lookups and truncation
// checks to unconfirmed nonces (>= confirmedNonce).
type PendingEvmTxHash struct {
	Nonce uint64       `json:"nonce"`
	Hash  ecommon.Hash `json:"hash"`
}

// PendingEvmTx stores the last submitted unconfirmed halt or key-change
// transaction for a (sender, contract, kind) tuple so retries recover nonce
// ownership and fee watermarks across process restarts and network removals.
type PendingEvmTx struct {
	Nonce              uint64             `json:"nonce"`
	TxHash             ecommon.Hash       `json:"txHash,omitempty"`
	PriorTxHashes      []PendingEvmTxHash `json:"priorTxHashes,omitempty"`
	PriorTruncatedUpTo *uint64            `json:"priorTruncatedUpTo,omitempty"`
	SupersededOther    *PendingEvmTx      `json:"supersededOther,omitempty"`
	GasPrice           *big.Int           `json:"gasPrice,omitempty"`
	TipCap             *big.Int           `json:"tipCap,omitempty"`
	FeeCap             *big.Int           `json:"feeCap,omitempty"`
	IsDynamic          bool               `json:"isDynamic"`
}

// PendingEvmTxOp describes one save (Record != nil) or delete (Record == nil)
// within an atomic PendingEvmTxStore batch.
type PendingEvmTxOp struct {
	Sender   ecommon.Address
	Contract ecommon.Address
	Kind     string
	Record   *PendingEvmTx
}

// PendingEvmTxStore persists PendingEvmTx records durably across restarts and
// network removals, supporting atomic multi-key transitions (such as halt
// superseding setTss and rolling back on definitive pre-mempool rejection).
type PendingEvmTxStore interface {
	LoadPendingEvmTx(sender, contract ecommon.Address, kind string) (*PendingEvmTx, error)
	SavePendingEvmTx(sender, contract ecommon.Address, kind string, rec *PendingEvmTx) error
	DeletePendingEvmTx(sender, contract ecommon.Address, kind string) error
	ApplyPendingEvmTxBatch(ops []PendingEvmTxOp) error
}
