package tss

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"net"
	"slices"
	"sort"
	"strings"
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

// Use three actual go-tss servers, including distributed key generation, peer
// discovery, party formation and both threshold signatures. Only the expensive
// prime-generation step uses public test fixtures from the pinned library.
func TestUnwrapTwoOfThreeRealCoordinator(t *testing.T) {
	if testing.Short() {
		t.Skip("requires three local TSS servers")
	}
	const n = 3
	const version = "0.14.0"
	const leaderHeight = 100
	fixtures, _, err := keyfixtures.LoadKeygenTestFixtures(n)
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, n)
	identities := make([]ed25519.PrivKey, n)
	ids := make([]string, n)
	whitelist := make(map[string]bool)
	for i := range keys {
		identities[i] = ed25519.GenPrivKey()
		keys[i] = base64.StdEncoding.EncodeToString(identities[i].PubKey().Bytes())
		id, err := conversion.GetPeerIDFromPubKey(keys[i])
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id.String()
		whitelist[ids[i]] = true
	}
	var peers []*TssManager
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
		t.Cleanup(server.Stop)
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
	rawKey, err := base64.StdEncoding.DecodeString(publicKey)
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
	sorted := slices.Clone(keys)
	sort.Strings(sorted)
	msgID, err := tcommon.MsgToHashString([]byte(base64.StdEncoding.EncodeToString(digest) + strings.Join(sorted, "")))
	if err != nil {
		t.Fatal(err)
	}
	dissent, err := p2p.LeaderNode(msgID, leaderHeight, ids)
	if err != nil {
		t.Fatal(err)
	}

	type outcome struct {
		i               int
		response        *keysign.Response
		passedAgreement bool
		err             error
	}
	results := make(chan outcome, n)
	for i, m := range peers {
		m.SetPubKey(publicKey)
		pool := majority
		if ids[i] == dissent {
			pool = minority
		}
		go func() {
			passedAgreement := false
			r, err := m.SignUnwrapPool(common.CeremonyWindow{Number: 17, Size: 100}, pool, func() error { passedAgreement = true; return nil })
			results <- outcome{i: i, response: r, passedAgreement: passedAgreement, err: err}
		}()
	}
	for range peers {
		result := <-results
		if ids[result.i] == dissent {
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
