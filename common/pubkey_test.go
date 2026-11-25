package common

import (
	"bytes"
	"encoding/base64"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
)

// findKey returns the first private scalar from 1 upwards whose public key
// matches want. The search is deterministic.
func findKey(t *testing.T, want func(pub []byte) bool) []byte {
	t.Helper()
	for i := int64(1); i < 1<<16; i++ {
		priv, err := crypto.ToECDSA(scalarBytes(big.NewInt(i)))
		if err != nil {
			t.Fatal(err)
		}
		if pub := crypto.FromECDSAPub(&priv.PublicKey); want(pub) {
			return pub
		}
	}
	t.Fatal("no matching key found")
	return nil
}

func scalarBytes(x *big.Int) []byte {
	b := make([]byte, 32)
	return x.FillBytes(b)
}

func compressed(t *testing.T, uncompressed []byte) string {
	t.Helper()
	key, err := crypto.UnmarshalPubkey(uncompressed)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(crypto.CompressPubkey(key))
}

func TestDecompressPubKeyPadsLeadingZeroCoordinates(t *testing.T) {
	for _, tc := range []struct {
		name string
		want func([]byte) bool
	}{
		{"X starts with zero byte", func(p []byte) bool { return p[1] == 0 }},
		{"Y starts with zero byte", func(p []byte) bool { return p[33] == 0 }},
		{"no zero lead", func(p []byte) bool { return p[1] != 0 && p[33] != 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			full := findKey(t, tc.want)
			got, err := DecompressPubKey(compressed(t, full))
			if err != nil {
				t.Fatal(err)
			}
			raw, err := base64.StdEncoding.DecodeString(got)
			if err != nil {
				t.Fatal(err)
			}
			if len(raw) != 65 || !bytes.Equal(raw, full) {
				t.Fatalf("decompressed %x (%d bytes), want %x", raw, len(raw), full)
			}
			accepted, err := ZenonAcceptsDecompressedPubKey(got)
			if err != nil {
				t.Fatal(err)
			}
			// Expected value from go-zenon's construction: 0x04 || X.Bytes() || Y.Bytes().
			key, _ := crypto.UnmarshalPubkey(full)
			zenonLen := 1 + len(key.X.Bytes()) + len(key.Y.Bytes())
			if wantAccepted := zenonLen == 65; accepted != wantAccepted {
				t.Fatalf("ZenonAcceptsDecompressedPubKey = %v, want %v", accepted, wantAccepted)
			}
		})
	}
}

func TestDecompressPubKeyRejectsInvalidInput(t *testing.T) {
	valid := findKey(t, func([]byte) bool { return true })
	validCompressed, _ := base64.StdEncoding.DecodeString(compressed(t, valid))
	badPrefix := append([]byte{0x05}, validCompressed[1:]...)
	offCurve := append([]byte{0x02}, bytes.Repeat([]byte{0xff}, 32)...) // X >= field prime
	for _, tc := range []struct {
		name, in string
	}{
		{"empty", ""},
		{"not base64", "not-base64!!"},
		{"too short", base64.StdEncoding.EncodeToString(validCompressed[:32])},
		{"uncompressed form", base64.StdEncoding.EncodeToString(valid)},
		{"bad prefix", base64.StdEncoding.EncodeToString(badPrefix)},
		{"not on curve", base64.StdEncoding.EncodeToString(offCurve)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecompressPubKey(tc.in)
			if err == nil {
				t.Fatalf("DecompressPubKey(%q) = %q, nil; want an error", tc.in, got)
			}
			if got != "" {
				t.Fatalf("DecompressPubKey(%q) returned %q with error %v", tc.in, got, err)
			}
		})
	}
}

func TestZenonAcceptsDecompressedPubKeyRejectsMalformed(t *testing.T) {
	for _, in := range []string{"", "!!", base64.StdEncoding.EncodeToString(make([]byte, 64))} {
		if ok, err := ZenonAcceptsDecompressedPubKey(in); err == nil || ok {
			t.Fatalf("ZenonAcceptsDecompressedPubKey(%q) = %v, %v; want false and an error", in, ok, err)
		}
	}
}
