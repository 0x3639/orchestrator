package node

import (
	"errors"
	"math/big"
	"testing"

	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"

	"orchestrator/common"
	"orchestrator/common/events"
	"orchestrator/db"
	"orchestrator/db/wrap"
)

var errInjectedRead = errors.New("injected read failure")

// failingReadDB fails every read, as a leveldb I/O error or corruption would.
type failingReadDB struct{ zdb.DB }

func (failingReadDB) Get([]byte) ([]byte, error) { return nil, errInjectedRead }

func storedWrap(t *testing.T, store db.ZnnStorage, signature string) types.Hash {
	t.Helper()
	id := types.HexToHashPanic("00000000000000000000000000000000000000000000000000000000000000a1")
	if err := store.AddWrapRequest(events.WrapRequestZnn{
		NetworkClass: 2, ChainId: 1, Id: id, Amount: big.NewInt(10), Fee: big.NewInt(1),
		Signature: signature, RedeemStatus: common.UnredeemedStatus,
	}); err != nil {
		t.Fatal(err)
	}
	return id
}

// Regression for the signer checking the outer error instead of the storage
// read error: a failed read returned a nil record that was then dereferenced.
func TestWrapNeedsSignatureReportsReadFailure(t *testing.T) {
	store := wrap.NewZnnStorage(failingReadDB{zdb.NewMemDB()}, nil)
	needed, err := wrapNeedsSignature(store, types.Hash{1})
	if !errors.Is(err, errInjectedRead) || needed {
		t.Fatalf("read failure gave needed=%v err=%v", needed, err)
	}
}

func TestWrapNeedsSignatureReportsMissingRecord(t *testing.T) {
	store := wrap.NewZnnStorage(zdb.NewMemDB(), nil)
	needed, err := wrapNeedsSignature(store, types.Hash{1})
	if err == nil || needed {
		t.Fatalf("missing record gave needed=%v err=%v", needed, err)
	}
}

func TestWrapNeedsSignatureFollowsSignatureAndResignStatus(t *testing.T) {
	for _, tc := range []struct {
		name      string
		signature string
		resign    bool
		want      bool
	}{
		{"unsigned", "", false, true},
		{"signed but unsent is kept", "sig", false, false},
		{"signed and marked for resign", "sig", true, true},
		{"unsigned and marked for resign", "", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := wrap.NewZnnStorage(zdb.NewMemDB(), nil)
			id := storedWrap(t, store, tc.signature)
			if err := store.SetResignStatus(id, tc.resign); err != nil {
				t.Fatal(err)
			}
			needed, err := wrapNeedsSignature(store, id)
			if err != nil || needed != tc.want {
				t.Fatalf("needed=%v err=%v, want %v", needed, err, tc.want)
			}
		})
	}
}
