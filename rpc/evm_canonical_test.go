package rpc

import (
	"errors"
	"strings"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
)

func TestAgreeCanonicalHashRequiresEveryEndpoint(t *testing.T) {
	a := ecommon.HexToHash("0xa")
	b := ecommon.HexToHash("0xb")

	if _, err := agreeCanonicalHash(1, nil); err == nil {
		t.Fatal("no endpoints must be an error")
	}

	hash, err := agreeCanonicalHash(1, []headerResult{{0, a, nil}})
	if err != nil || hash != a {
		t.Fatalf("single endpoint answer should be accepted, got %s err=%v", hash.Hex(), err)
	}

	hash, err = agreeCanonicalHash(1, []headerResult{{0, a, nil}, {1, a, nil}, {2, a, nil}})
	if err != nil || hash != a {
		t.Fatalf("unanimous answer should be accepted, got %s err=%v", hash.Hex(), err)
	}

	// Two canonical endpoints time out, one minority endpoint answers: no
	// verdict, even though every answer received agrees.
	_, err = agreeCanonicalHash(1, []headerResult{{0, ecommon.Hash{}, errors.New("timeout")}, {1, ecommon.Hash{}, errors.New("timeout")}, {2, b, nil}})
	if err == nil || !strings.Contains(err.Error(), "inconclusive") {
		t.Fatalf("partial answers must be inconclusive, got %v", err)
	}

	// Split provider: disagreement is an error.
	_, err = agreeCanonicalHash(1, []headerResult{{0, a, nil}, {1, b, nil}})
	if err == nil || !strings.Contains(err.Error(), "disagree") {
		t.Fatalf("disagreement must be an error, got %v", err)
	}
}
