package network

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"go.uber.org/zap"

	"orchestrator/common/config"
)

// A key change has reached every EVM network only when each of them has been
// seen to hold the new key. A network whose key cannot be read has not.
func TestKeyChangeIsNotReportedDoneWhenANetworkCannotBeRead(t *testing.T) {
	b := newReplayBridge(t)
	url := replayServerWithErrors(t, func(method string, _ []json.RawMessage) (interface{}, *replayRPCError) {
		return nil, &replayRPCError{-32000, "upstream timeout"}
	})
	n, _ := replayNetwork(t, b, url, map[uint32]string{1: url})
	params, err := config.NewEvmParams(&definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 1, ContractAddress: b.contract.Hex()})
	if err != nil {
		t.Fatal(err)
	}
	m := &NetworksManager{logger: zap.NewNop().Sugar()}
	m.AddEvmNetwork(&evmNetwork{EvmParams: params, client: n.rpcManager.Evm(1), done: make(chan struct{})})
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	newKey := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&key.PublicKey))

	if changed, err := m.CheckNewTssEcdsaPubKeyEvm(newKey); err != nil || changed {
		t.Fatalf("check: changed %v, err %v; want not changed", changed, err)
	}
	if changed, err := m.SetTssEcdsaPubKeyEvm(nil, nil, newKey, key, crypto.PubkeyToAddress(key.PublicKey)); err != nil || changed {
		t.Fatalf("set: changed %v, err %v; want not changed", changed, err)
	}
}
