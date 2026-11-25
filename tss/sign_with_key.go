package tss

import (
	"encoding/base64"
	"errors"
	"slices"

	"github.com/HyperCore-Team/go-tss/keysign"
	"github.com/HyperCore-Team/go-tss/messages"
	"orchestrator/common"
)

// BulkSignWithKey runs an ECDSA keysign ceremony with an explicit public key
// and participant set instead of reading shared mutable fields from TssManager,
// preventing races with concurrent cancellation or emergency signing.
func (m *TssManager) BulkSignWithKey(pubKey string, signers []string, messagesBytes [][]byte, algo messages.Algo) (*keysign.Response, error) {
	if algo != messages.ECDSAKEYSIGN {
		return nil, errors.New("invalid algorithm")
	}
	messagesStr := make([]string, len(messagesBytes))
	for idx, msg := range messagesBytes {
		messagesStr[idx] = base64.StdEncoding.EncodeToString(msg)
		common.GlobalLogger.Info("encoded msg:", messagesStr[idx])
	}
	common.GlobalLogger.Info("before sending sign request")
	common.GlobalLogger.Info("PublicKey: ", pubKey)
	keySignReq := keysign.NewRequest(pubKey, messagesStr, m.GetLeaderBlockHeight(), slices.Clone(signers), m.GetJoinPartyVersion(), "ecdsa")
	resSign, err := m.server.KeySign(keySignReq)
	return &resSign, err
}
