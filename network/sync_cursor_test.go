package network

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/api"
	"go.uber.org/zap"

	"orchestrator/db"
	"orchestrator/db/unwrap"
	"orchestrator/db/wrap"
)

// Both stores keep the sync cursor under the single-byte key 0x01.
var rawCursorKey = []byte{1}

func checkCorruptCursorStop(t *testing.T, err error, stop chan os.Signal, mem zdb.DB, raw []byte, name string) {
	t.Helper()
	if !errors.Is(err, db.ErrCorruptCursor) || !strings.Contains(err.Error(), name) {
		t.Fatalf("error %v, want a corrupt %s", err, name)
	}
	select {
	case <-stop:
	default:
		t.Fatal("corrupt cursor did not request a stop")
	}
	if got, err := mem.Get(rawCursorKey); err != nil || !bytes.Equal(got, raw) {
		t.Fatalf("stored cursor changed to %x (%v), want %x", got, err, raw)
	}
}

func TestEvmSyncStopsOnCorruptCursor(t *testing.T) {
	raw := []byte{1, 2, 3, 4, 5, 6, 7}
	mem := zdb.NewMemDB()
	if err := mem.Put(rawCursorKey, raw); err != nil {
		t.Fatal(err)
	}
	stop := make(chan os.Signal, 1)
	net := evmNetwork{dbManager: unwrapTestStores{evm: unwrap.NewEvmStorage(mem, stop, 100)}, done: make(chan struct{}), logger: zap.NewNop().Sugar()}
	err := net.Sync()
	checkCorruptCursorStop(t, err, stop, mem, raw, "EVM sync cursor lastUpdateHeight")
	if !strings.Contains(err.Error(), "chainId 0") {
		t.Fatalf("error %q does not name the chain", err)
	}
}

func TestZenonStartupSyncStopsOnCorruptCursor(t *testing.T) {
	raw := []byte{9}
	mem := zdb.NewMemDB()
	if err := mem.Put(rawCursorKey, raw); err != nil {
		t.Fatal(err)
	}
	n := startupNetwork(t, &startupLedger{page: func(uint64) *api.AccountBlockListMarshal {
		t.Error("ledger read with a corrupt cursor")
		return startupPage()
	}})
	stop := make(chan os.Signal, 1)
	n.dbManager = unwrapTestStores{znn: wrap.NewZnnStorage(mem, stop)}
	checkCorruptCursorStop(t, n.Sync(), stop, mem, raw, "Zenon sync cursor lastUpdateHeight")
}

// The cursor is damaged after the listener has read it, so the write that
// follows a processed receive fails. The listener must stop rather than keep
// retrying a write that cannot succeed.
func TestZenonListenerStopsWhenCursorWriteFindsCorruption(t *testing.T) {
	receive := startupReceive(2)
	raw := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}
	mem := zdb.NewMemDB()
	ledger := &startupLedger{
		page: func(height uint64) *api.AccountBlockListMarshal {
			if height == 2 {
				return startupPage(receive)
			}
			return startupPage()
		},
		lookup: func(hash types.Hash) (*api.AccountBlockMarshal, error) {
			if hash != receive.FromBlockHash {
				return nil, errors.New("unexpected lookup")
			}
			if err := mem.Put(rawCursorKey, raw); err != nil {
				return nil, err
			}
			return startupSend(receive), nil
		},
	}
	n := startupNetwork(t, ledger)
	stop := make(chan os.Signal, 2)
	store := wrap.NewZnnStorage(mem, stop)
	if err := store.SetLastUpdateHeight(1); err != nil {
		t.Fatal(err)
	}
	n.dbManager = unwrapTestStores{znn: store}
	n.stopChan = stop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		n.followBridgeAccount(ctx, nil, nil, n.refreshSettings)
	}()
	select {
	case <-exited:
	case <-time.After(3 * time.Second):
		cancel()
		<-exited
		t.Fatal("listener kept retrying a corrupt cursor")
	}
	// Drain the stops the listener requested before reading the cursor again,
	// so the read below cannot supply the signal under test.
	if len(stop) == 0 {
		t.Fatal("listener did not request a stop")
	}
	for len(stop) > 0 {
		<-stop
	}
	_, err := store.GetLastUpdateHeight()
	checkCorruptCursorStop(t, err, stop, mem, raw, "Zenon sync cursor lastUpdateHeight")
}
