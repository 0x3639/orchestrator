package tss

import (
	"github.com/HyperCore-Team/go-tss/keysign"
	"github.com/HyperCore-Team/go-tss/messages"
)

// BulkSignWithKey runs an ECDSA keysign ceremony with an explicit public key
// and participant set instead of reading shared mutable fields from TssManager,
// preventing races with concurrent cancellation or emergency signing.
func (m *TssManager) BulkSignWithKey(pubKey string, signers []string, messagesBytes [][]byte, algo messages.Algo) (*keysign.Response, error) {
	return m.bulkSignWithKey(pubKey, messagesBytes, algo, signers, m.GetJoinPartyVersion())
}
