package node

import (
	"encoding/base64"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
	"go.uber.org/zap"
)

// compressedKey returns a fresh compressed public key, in base64, whose X
// coordinate starts with a zero byte when leadingZero is set and does not
// otherwise. The Y coordinate never starts with one.
func compressedKey(t *testing.T, leadingZero bool) string {
	t.Helper()
	for range 1 << 16 {
		key, err := crypto.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		pub := crypto.FromECDSAPub(&key.PublicKey)
		if (pub[1] == 0) == leadingZero && pub[33] != 0 {
			return base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&key.PublicKey))
		}
	}
	t.Fatal("no key found")
	return ""
}

// A key change is signed with the active key. One the Zenon contract cannot
// verify is refused before any keygen runs, instead of after each one.
func TestActiveKeyThatZenonCannotVerifyIsRefused(t *testing.T) {
	node := &Node{logger: zap.NewNop().Sugar()}
	if !node.activeKeyCanSign(compressedKey(t, false)) {
		t.Fatal("an ordinary active key was refused")
	}
	if node.activeKeyCanSign(compressedKey(t, true)) {
		t.Fatal("an active key with a leading zero coordinate byte was accepted")
	}
	if node.activeKeyCanSign("not-base64!!") {
		t.Fatal("an undecodable active key was accepted")
	}
}
