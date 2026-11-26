package network

import (
	"fmt"
	"math/big"
	"os"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"go.uber.org/zap"

	"orchestrator/common"
	"orchestrator/common/events"
	"orchestrator/db/manager"
	"orchestrator/health/snapshot"
)

// markSentAfterListener follows sendUnwrapRequests: it reads the unsent
// signed request, lets the Zenon listener store status while the Zenon lookup
// would run, then marks the request as sent. It returns the stored record.
func markSentAfterListener(t *testing.T, status uint32) *events.UnwrapRequestEvm {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	stopChan := make(chan os.Signal, 8)
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	state := common.NewGlobalState(new(uint8))
	state.Health = snapshot.New("test", "")
	t.Cleanup(state.Health.Close)

	info := &definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 1, Name: "eth", ContractAddress: "0x1111111111111111111111111111111111111111"}
	net, err := NewEvmNetwork(info, dbMgr, nil, state, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		net.drainWorkers(false)
		_ = dbMgr.RemoveEvmEventStore(1)
	})
	store := dbMgr.EvmStorage(1)
	ev := events.UnwrapRequestEvm{ChainId: 1, TransactionHash: ecommon.HexToHash("0xfeed"), Amount: big.NewInt(1), RedeemStatus: common.UnredeemedStatus, Signature: "signature"}
	if err := store.AddUnwrapRequest(ev); err != nil {
		t.Fatal(err)
	}
	mgr := &NetworksManager{evmNetworks: []*evmNetwork{net}, health: state.Health, logger: zap.NewNop().Sugar()}

	unsent, err := mgr.GetUnsentSignedUnwrapRequests()
	if err != nil || len(unsent) != 1 {
		t.Fatalf("unsent signed requests %v, err %v; want the stored request", unsent, err)
	}
	// The listener writes through the store, as network/znn.go does.
	if err := store.SetUnwrapRequestStatus(ev.TransactionHash, ev.LogIndex, status); err != nil {
		t.Fatal(err)
	}
	if err := mgr.SetEvmUnwrapRequestAsSent(unsent[0]); err != nil {
		t.Fatalf("marking the request as sent: %v", err)
	}
	got, err := store.GetUnwrapRequestByHashAndLog(ev.TransactionHash, ev.LogIndex)
	if err != nil || got == nil {
		t.Fatalf("reading the request back: %v", err)
	}
	if got.Signature != ev.Signature {
		t.Errorf("signature %q after marking as sent; want %q", got.Signature, ev.Signature)
	}
	if n := len(stopChan); n != 0 {
		t.Errorf("marking as sent sent %d stop requests; want 0", n)
	}
	return got
}

// Marking a request as sent must not move a record the listener has settled
// back to pending, as LAWS.bend requires of apply_pending.
func TestSetEvmUnwrapRequestAsSentKeepsListenerStatus(t *testing.T) {
	for _, tc := range []struct {
		listener, want uint32
	}{
		{common.UnredeemedStatus, common.PendingRedeemStatus},
		{common.PendingRedeemStatus, common.PendingRedeemStatus},
		{common.RedeemedStatus, common.RedeemedStatus},
		{common.RevokedStatus, common.RevokedStatus},
	} {
		t.Run(fmt.Sprint(tc.listener), func(t *testing.T) {
			if got := markSentAfterListener(t, tc.listener).RedeemStatus; got != tc.want {
				t.Errorf("after listener status %d, stored status %d; want %d", tc.listener, got, tc.want)
			}
		})
	}
}
