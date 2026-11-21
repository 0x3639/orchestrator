package tss

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	tcommon "github.com/HyperCore-Team/go-tss/common"
	"github.com/HyperCore-Team/go-tss/conversion"
	"github.com/HyperCore-Team/go-tss/keygen"
	"github.com/HyperCore-Team/go-tss/keysign"
	"github.com/HyperCore-Team/go-tss/messages"
	"github.com/HyperCore-Team/go-tss/p2p"
	gotss "github.com/HyperCore-Team/go-tss/tss"
	keyfixtures "github.com/HyperCore-Team/tss-lib/ecdsa/keygen"
	"github.com/ethereum/go-ethereum/crypto"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/tendermint/tendermint/crypto/ed25519"

	"orchestrator/common"
)

const leaderHeight = 100

type signingGroup struct {
	peers     []*TssManager
	keys, ids []string
	publicKey string
	stop      []func()
}

// All three servers participate in distributed key generation. Only the
// expensive prime-generation step uses the pinned library's public fixtures.
func realSigningGroup(t *testing.T) signingGroup {
	t.Helper()
	if testing.Short() {
		t.Skip("requires three local TSS servers")
	}
	const n = 3
	const version = "0.14.0"

	fixtures, _, err := keyfixtures.LoadKeygenTestFixtures(n)
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, n)
	identities := make([]ed25519.PrivKey, n)
	ids := make([]string, n)
	whitelist := make(map[string]bool)
	for i := range keys {
		// Reproduce the same peer identities and startup conditions on every
		// run. Key generation still creates fresh threshold shares.
		identities[i] = ed25519.GenPrivKeyFromSecret([]byte(fmt.Sprintf("%s/peer/%d", t.Name(), i)))
		keys[i] = base64.StdEncoding.EncodeToString(identities[i].PubKey().Bytes())
		id, err := conversion.GetPeerIDFromPubKey(keys[i])
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id.String()
		whitelist[ids[i]] = true
	}
	var peers []*TssManager
	var stops []func()
	var bootstrap []ma.Multiaddr
	for i := range keys {
		conf := tcommon.TssConfig{PartyTimeout: 12 * time.Second, KeyGenTimeout: 90 * time.Second, KeySignTimeout: 30 * time.Second}
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		server, err := gotss.NewTss(bootstrap, port, identities[i], t.Name(), dir, conf,
			&fixtures[i].LocalPreParams, "", messages.ECDSAKEYSIGN, whitelist)
		if err != nil {
			t.Fatal(err)
		}
		stop := sync.OnceFunc(server.Stop)
		t.Cleanup(stop)
		stops = append(stops, stop)
		address, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/p2p/%s", port, ids[i]))
		if err != nil {
			t.Fatal(err)
		}
		bootstrap = append(bootstrap, address)
		votes, err := leveldb.OpenFile(t.TempDir(), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { votes.Close() })
		peers = append(peers, &TssManager{server: server, keyShareDir: dir, unwrapVotes: votes, localPubKeys: slices.Clone(keys), joinPartyVersion: version, leaderBlockHeight: leaderHeight})
	}

	// All three participate in keygen, then only two agree on the unwrap pool.
	type generation struct {
		r   keygen.Response
		err error
	}
	generated := make(chan generation, n)
	for _, m := range peers {
		go func() {
			r, err := m.server.Keygen(keygen.NewRequest(slices.Clone(keys), leaderHeight, version, "ecdsa"))
			generated <- generation{r, err}
		}()
	}
	var publicKey string
	for range peers {
		g := <-generated
		if g.err != nil || g.r.Status != tcommon.Success {
			t.Errorf("test key generation: %v, status=%v", g.err, g.r.Status)
			continue
		}
		if publicKey != "" && publicKey != g.r.PubKey {
			t.Error("signers generated different group keys")
		}
		publicKey = g.r.PubKey
	}
	if t.Failed() {
		t.FailNow()
	}
	for _, p := range peers {
		p.SetPubKey(publicKey)
	}
	return signingGroup{peers: peers, keys: keys, ids: ids, publicKey: publicKey, stop: stops}
}

func TestUnwrapTwoOfThreeRealCoordinator(t *testing.T) {
	g := realSigningGroup(t)
	rawKey, err := base64.StdEncoding.DecodeString(g.publicKey)
	if err != nil {
		t.Fatal(err)
	}
	majority := [][]byte{bytes.Repeat([]byte{1}, 32)}
	minority := [][]byte{bytes.Repeat([]byte{2}, 32)}
	digest, err := unwrapPoolDigest(rawKey, common.CeremonyWindow{Number: 17, Size: 100}, majority)
	if err != nil {
		t.Fatal(err)
	}
	// requestToMsgId sorts and concatenates the base64 messages and identity
	// keys. Ask the real leader selector which signer to give the other view.
	sorted := slices.Clone(g.keys)
	sort.Strings(sorted)
	msgID, err := tcommon.MsgToHashString([]byte(base64.StdEncoding.EncodeToString(digest) + strings.Join(sorted, "")))
	if err != nil {
		t.Fatal(err)
	}
	dissent, err := p2p.LeaderNode(msgID, leaderHeight, g.ids)
	if err != nil {
		t.Fatal(err)
	}

	type outcome struct {
		i               int
		response        *keysign.Response
		passedAgreement bool
		err             error
	}
	results := make(chan outcome, len(g.peers))
	for i, m := range g.peers {
		pool := majority
		if g.ids[i] == dissent {
			pool = minority
		}
		go func() {
			passedAgreement := false
			r, err := m.SignUnwrapPool(common.CeremonyWindow{Number: 17, Size: 100}, pool, func() error { passedAgreement = true; return nil })
			results <- outcome{i: i, response: r, passedAgreement: passedAgreement, err: err}
		}()
	}
	for range g.peers {
		result := <-results
		if g.ids[result.i] == dissent {
			if result.err == nil || result.passedAgreement {
				t.Errorf("dissenting signer %d passed the agreement gate", result.i)
			}
			continue
		}
		if result.err != nil || result.response == nil || result.response.Status != tcommon.Success || len(result.response.Signatures) != 1 {
			t.Errorf("agreeing signer %d did not finish bridge signing: response=%v, err=%v", result.i, result.response, result.err)
			continue
		}
		sig := result.response.Signatures[0]
		raw, err := base64.StdEncoding.DecodeString(sig.Signature)
		if err != nil || sig.Msg != base64.StdEncoding.EncodeToString(majority[0]) || !crypto.VerifySignature(rawKey, majority[0], raw) {
			t.Errorf("agreeing signer %d received an invalid bridge signature", result.i)
		} else {
			t.Logf("agreeing signer %d returned a valid bridge signature", result.i)
		}
	}
}

// Start with 14 simulated momentums left. At one momentum per second, the
// first discovery wait crosses the boundary. Window 18 can unwrap when the
// wrap queue is empty. Both attempts retain the same servers, votes and pool.
func TestOfflineSignerRecoversNextWindowRealCoordinator(t *testing.T) {
	g := realSigningGroup(t)
	g.stop[2]()
	online := g.peers[:2]
	for _, p := range online {
		p.SetPartyTimeout(30 * time.Second)
	}
	pub, err := base64.StdEncoding.DecodeString(g.publicKey)
	if err != nil {
		t.Fatal(err)
	}
	pool := [][]byte{bytes.Repeat([]byte{3}, 32)}
	const size = 100
	start := time.Now()
	window := func() common.CeremonyWindow {
		height := 1786 + uint64(time.Since(start)/time.Second)
		return common.CeremonyWindow{Number: height / size, Size: size}
	}
	expired := errors.New("ceremony expired before bridge signing")
	type result struct {
		peer      int
		signature *keysign.Response
		admitted  bool
		err       error
	}
	states := []*common.GlobalState{common.NewGlobalState(new(uint8)), common.NewGlobalState(new(uint8))}
	for _, number := range []uint64{17, 18} {
		current := window()
		if current.Number != number {
			t.Fatalf("missed scheduled window %d, current window is %d", number, current.Number)
		}
		results := make(chan result, len(online))
		for i, p := range online {
			if !states[i].BeginCeremony(current) {
				t.Fatalf("signer %d could not schedule window %d", i, number)
			}
			go func() {
				admitted := false
				sig, err := p.SignUnwrapPool(current, pool, func() error {
					if window() != current {
						return expired
					}
					admitted = true
					return nil
				})
				results <- result{i, sig, admitted, err}
			}()
		}
		for range online {
			r := <-results
			if number == 17 {
				if !errors.Is(r.err, expired) || r.admitted || r.signature != nil {
					t.Errorf("signer %d: expired window admitted=%v response=%v err=%v", r.peer, r.admitted, r.signature, r.err)
				}
				continue
			}
			if r.err != nil || !r.admitted || r.signature == nil || r.signature.Status != tcommon.Success || len(r.signature.Signatures) != 1 {
				t.Errorf("signer %d: next window admitted=%v response=%v err=%v", r.peer, r.admitted, r.signature, r.err)
				continue
			}
			sig := r.signature.Signatures[0]
			raw, err := base64.StdEncoding.DecodeString(sig.Signature)
			if err != nil || sig.Msg != base64.StdEncoding.EncodeToString(pool[0]) || !crypto.VerifySignature(pub, pool[0], raw) {
				t.Errorf("signer %d: invalid bridge signature for the pending pool", r.peer)
			}
		}
		if t.Failed() {
			t.FailNow()
		}
		t.Logf("completed attempt for window %d at %s (current window %d)", number, time.Since(start).Round(time.Millisecond), window().Number)
	}
	if window().Number != 18 {
		t.Fatal("recovery did not finish during the next eligible window")
	}
}
