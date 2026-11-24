package network

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
)

var isHaltedSelector = hexutil.Encode(crypto.Keccak256([]byte("isHalted()"))[:4])

// haltProvider answers eth_call for the bridge's isHalted() getter on
// rotationContract. Every other method is unknown to the server, so sending a
// halt tx fails at its first RPC.
type haltProvider struct {
	halted bool
	err    error
}

func (p *haltProvider) Call(args map[string]interface{}, block interface{}) (hexutil.Bytes, error) {
	input, _ := args["input"].(string)
	if input == "" {
		input, _ = args["data"].(string)
	}
	to, _ := args["to"].(string)
	if !strings.EqualFold(to, rotationContract.Hex()) || !strings.EqualFold(input, isHaltedSelector) {
		return nil, fmt.Errorf("unexpected eth_call to %q with input %q", to, input)
	}
	if p.err != nil {
		return nil, p.err
	}
	out := make([]byte, 32)
	if p.halted {
		out[31] = 1
	}
	return out, nil
}

func haltNetwork(t *testing.T, chainId uint32, halted bool) *evmNetwork {
	t.Helper()
	n, _ := newRotationNetwork(t, chainId, &haltProvider{halted: halted})
	return n
}

func TestSendHaltEvmMatchesSignaturesByChainId(t *testing.T) {
	sig := [][]byte{{1}, {2}}
	cases := []struct {
		name     string
		nets     []*evmNetwork
		chainIds []uint32
		want     bool
		stale    bool
	}{
		{"all signed and halted", []*evmNetwork{haltNetwork(t, 1, true), haltNetwork(t, 2, true)}, []uint32{1, 2}, true, false},
		{"signed in another order", []*evmNetwork{haltNetwork(t, 2, true), haltNetwork(t, 1, true)}, []uint32{1, 2}, true, false},
		// A network added after signing is never reported as halted, even
		// when every signed network is.
		{"added after signing, unhalted", []*evmNetwork{haltNetwork(t, 1, true), haltNetwork(t, 3, false)}, []uint32{1, 2}, false, true},
		{"added after signing, halted", []*evmNetwork{haltNetwork(t, 1, true), haltNetwork(t, 3, true)}, []uint32{1, 2}, false, true},
		// A signed network that was removed is not waited for.
		{"signed network removed", []*evmNetwork{haltNetwork(t, 2, true)}, []uint32{1, 2}, true, false},
		// Signed networks are halted first; staleness shows once they are.
		{"signed network still live", []*evmNetwork{haltNetwork(t, 1, false), haltNetwork(t, 3, false)}, []uint32{1, 2}, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, _ := rotationManager(tc.nets...)
			got, err := m.SendHaltEvm(sig, tc.chainIds, nil, ecommon.Address{})
			if got != tc.want {
				t.Fatalf("halted = %v, want %v (err %v)", got, tc.want, err)
			}
			if stale := errors.Is(err, ErrHaltSignaturesStale); stale != tc.stale {
				t.Fatalf("stale error = %v, want %v (err %v)", stale, tc.stale, err)
			}
			if !tc.stale && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestSendHaltEvmRejectsMismatchedSignatures(t *testing.T) {
	m, _ := rotationManager(haltNetwork(t, 1, true))
	if halted, err := m.SendHaltEvm([][]byte{{1}}, []uint32{1, 2}, nil, ecommon.Address{}); err == nil || halted {
		t.Fatalf("halted=%v err=%v for two chain ids and one signature", halted, err)
	}
}

func TestSendHaltEvmReadErrorIsNotHalted(t *testing.T) {
	n, _ := newRotationNetwork(t, 1, &haltProvider{err: errors.New("provider down")})
	m, _ := rotationManager(n)
	if halted, err := m.SendHaltEvm([][]byte{{1}}, []uint32{1}, nil, ecommon.Address{}); err == nil || halted {
		t.Fatalf("halted=%v err=%v when isHalted() fails", halted, err)
	}
}

func TestSendHaltEvmAdministratorByChainId(t *testing.T) {
	m, _ := rotationManager(haltNetwork(t, 1, true), haltNetwork(t, 2, false))
	if halted, err := m.SendHaltEvmAdministrator(1, nil, ecommon.Address{}); err != nil || !halted {
		t.Fatalf("halted=%v err=%v for a halted network", halted, err)
	}
	// Removing network 1 shifts live network 2 to position 0. Its sender
	// still reads chain 2 and does not report it halted; an index-based
	// sender for position 1 would have found nothing and reported success.
	m.RemoveEvmNetwork(1)
	if halted, _ := m.SendHaltEvmAdministrator(2, nil, ecommon.Address{}); halted {
		t.Fatal("live network reported halted after the list shifted")
	}
	if halted, err := m.SendHaltEvmAdministrator(1, nil, ecommon.Address{}); err != nil || !halted {
		t.Fatalf("halted=%v err=%v for a removed network", halted, err)
	}
}

func TestHaltedAt(t *testing.T) {
	cases := []struct {
		info definition.BridgeInfoVariable
		mom  uint64
		want bool
	}{
		{definition.BridgeInfoVariable{Halted: true}, 10, true},
		{definition.BridgeInfoVariable{UnhaltedAt: 5, UnhaltDurationInMomentums: 10}, 15, true},
		{definition.BridgeInfoVariable{UnhaltedAt: 5, UnhaltDurationInMomentums: 10}, 16, false},
	}
	for _, tc := range cases {
		if got := haltedAt(&tc.info, tc.mom); got != tc.want {
			t.Fatalf("haltedAt(%+v, %d) = %v, want %v", tc.info, tc.mom, got, tc.want)
		}
	}
}
