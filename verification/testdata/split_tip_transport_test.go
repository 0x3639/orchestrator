// Loaded into package node by TestSplitTipBridgeSignatureRequestsAgreeWithBend.
package node

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	tcommon "github.com/HyperCore-Team/go-tss/common"
	"github.com/HyperCore-Team/go-tss/conversion"
	"github.com/HyperCore-Team/go-tss/keysign"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/api"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"go.uber.org/zap"

	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/tss"
	"orchestrator/verification"
	"orchestrator/verification/testdata/signtransport"
)

func TestRequestsAtSigningTransport(t *testing.T) {
	for _, tc := range []struct {
		name string
		tips []byte
	}{
		{"one to one", []byte{1, 2}},
		{"two to one", []byte{1, 1, 2}},
		{"minority first", []byte{2, 1, 1}},
		{"unanimous", []byte{1, 1, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) { checkBridgeRequests(t, tc.tips) })
	}
}

const testCeremony uint64 = 17

type signingPeer struct {
	node       *Node
	messages   []string
	commitment string
	requests   int
}

func newSigningPeer(t *testing.T, tip byte, key string) *signingPeer {
	t.Helper()
	events := candidates(6)
	for _, event := range events {
		event.To = types.ZeroAddress.String()
	}
	c := signerWithEvents(t, events)
	c.head = func() (*api.AccountBlockMarshal, error) {
		return frontierBlock(types.BridgeContract, types.Hash{tip}, uint64(10+tip)), nil
	}
	if tip == 2 {
		c.lookup = func(i uint32) (*definition.UnwrapTokenRequest, error) {
			if i == 0 {
				return nil, constants.ErrDataNonExistent
			}
			return stableLookup(i)
		}
	}
	// All signers receive the same six valid transfers. Independently encode
	// the four expected bridge messages to distinguish them from a commitment.
	order := []int{1, 3, 4, 5}
	if tip == 2 {
		order = []int{0, 1, 3, 4}
	}
	var messages []string
	for _, i := range order {
		msg, err := events[i].GetMessage()
		if err != nil {
			t.Fatal(err)
		}
		messages = append(messages, base64.StdEncoding.EncodeToString(msg))
	}
	networks, _ := c.openNetworks(t)
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := tss.NewTssManager(config.TssManagerConfig{PublicKey: key, BaseDir: t.TempDir()}, base64.StdEncoding.EncodeToString(priv))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(manager.Stop)
	state := common.NewGlobalState(new(uint8))
	if err := state.SetFrontierMomentum(testCeremony * networks.WindowSize()); err != nil {
		t.Fatal(err)
	}
	state.SetLastCeremony(testCeremony)
	return &signingPeer{
		node:     &Node{networksManager: networks, tssManager: manager, state: state, logger: zap.NewNop().Sugar()},
		messages: messages,
	}
}

func checkBridgeRequests(t *testing.T, tips []byte) {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	pub := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&key.PublicKey))
	threshold, err := conversion.GetThreshold(len(tips))
	if err != nil {
		t.Fatal(err)
	}
	previous := common.SignCeremonyPoolSize
	common.SignCeremonyPoolSize = 4
	defer func() { common.SignCeremonyPoolSize = previous }()
	var peers []*signingPeer
	identities := map[string]*signingPeer{}
	support := map[byte]int{}
	for _, tip := range tips {
		support[tip]++
		p := newSigningPeer(t, tip, pub)
		peers = append(peers, p)
		identities[p.node.tssManager.GetPeerPubKey()] = p
	}

	// The transport issues a test certificate only after enough distinct peers
	// actually submit the same commitment. Count bridge requests on receipt,
	// before returning a failed response to end the test without signing transfers.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var mu sync.Mutex
	votes := map[string]int{}
	seen := map[string]bool{}
	firstRound := make(chan struct{})
	signtransport.Receive = func(id string, r keysign.Request) (keysign.Response, error) {
		var empty keysign.Response
		p := identities[id]
		if p == nil || r.PoolPubKey != pub || r.Algo != "ecdsa" {
			t.Errorf("unexpected signer, key, or algorithm")
			return empty, errors.New("invalid signer")
		}
		bridge := slices.Equal(p.messages, r.Messages)
		if !bridge && len(r.Messages) != 1 {
			t.Errorf("unexpected ordered bridge messages %v", r.Messages)
			return empty, errors.New("unexpected messages")
		}
		mu.Lock()
		if bridge {
			p.requests++
		} else {
			if seen[id] {
				mu.Unlock()
				t.Errorf("duplicate commitment from %s", id)
				return empty, errors.New("duplicate commitment")
			}
			p.commitment = r.Messages[0]
			votes[p.commitment]++
		}
		if !seen[id] {
			seen[id] = true
			if len(seen) == len(peers) {
				close(firstRound)
			}
		}
		mu.Unlock()
		if bridge {
			return keysign.Response{Status: tcommon.Fail}, nil
		}
		select {
		case <-firstRound:
		case <-ctx.Done():
			return empty, ctx.Err()
		}
		if votes[p.commitment] <= threshold {
			return empty, errors.New("no signing quorum")
		}
		digest, err := base64.StdEncoding.DecodeString(p.commitment)
		if err != nil {
			return empty, err
		}
		sig, err := crypto.Sign(digest, key)
		if err != nil {
			return empty, err
		}
		return keysign.Response{Status: tcommon.Success, Signatures: []keysign.Signature{{Msg: p.commitment, Signature: base64.StdEncoding.EncodeToString(sig[:64])}}}, nil
	}
	defer func() { signtransport.Receive = nil }()
	var wg sync.WaitGroup
	for _, p := range peers {
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()
			// dev's unwrap method originally took no argument. Invoke the actual
			// entry point for each checkout with the same scheduled window.
			switch run := any(n.processSignaturesUnwrap).(type) {
			case func() (error, bool):
				run()
			case func(uint64) (error, bool):
				run(testCeremony)
			default:
				t.Error("unsupported unwrap entry point")
			}
		}(p.node)
	}
	wg.Wait()
	if ctx.Err() != nil {
		t.Fatal("timed out waiting for signer requests")
	}
	if len(seen) != len(peers) {
		t.Fatalf("only %d of %d signers reached the signing transport", len(seen), len(peers))
	}
	var checks verification.Comparisons
	for i, p := range peers {
		// The oracle uses only the stable views assigned by the scenario.
		supporters := support[tips[i]]
		t.Logf("signer %d, view %d, support %d/%d: %d bridge-signature requests", i, tips[i], supporters, len(peers), p.requests)
		if p.requests > 1 {
			t.Errorf("duplicate bridge-signature request from signer %d", i)
		}
		checks.Equal(fmt.Sprintf("B.can_certify(%dn, %dn)", supporters, len(peers)-supporters), verification.Bool(p.requests > 0), "Bool")
	}
	checks.Check(t)
}
