package network

import (
	"math/big"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"
	"orchestrator/common"
	"orchestrator/common/events"
	"orchestrator/db/manager"
)

func TestNetworksManagerGetUnredeemedWrapRequests(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	dbMan, err := manager.NewDbManager(nil)
	if err != nil {
		t.Fatal(err)
	}
	store := dbMan.ZnnStorage()

	sentUnredeemed := types.Hash{1}
	unsentUnredeemed := types.Hash{2}
	sentRedeemed := types.Hash{3}

	for _, req := range []events.WrapRequestZnn{
		{NetworkClass: 2, ChainId: 1, Id: sentUnredeemed, Amount: big.NewInt(10), Fee: big.NewInt(1), Signature: "sig1", SentSignature: true, RedeemStatus: common.UnredeemedStatus},
		{NetworkClass: 2, ChainId: 1, Id: unsentUnredeemed, Amount: big.NewInt(20), Fee: big.NewInt(1), Signature: "sig2", SentSignature: false, RedeemStatus: common.UnredeemedStatus},
		{NetworkClass: 2, ChainId: 1, Id: sentRedeemed, Amount: big.NewInt(30), Fee: big.NewInt(1), Signature: "sig3", SentSignature: true, RedeemStatus: common.RedeemedStatus},
	} {
		if err := store.AddWrapRequest(req); err != nil {
			t.Fatal(err)
		}
	}

	m := &NetworksManager{
		znnNetwork: &znnNetwork{dbManager: dbMan},
	}

	unredeemed, err := m.GetUnredeemedWrapRequests()
	if err != nil {
		t.Fatalf("GetUnredeemedWrapRequests failed: %v", err)
	}
	got := make(map[types.Hash]bool, len(unredeemed))
	for _, r := range unredeemed {
		got[r.Id] = true
	}
	if !got[sentUnredeemed] {
		t.Fatalf("expected sent+unredeemed request %s to be returned, got %v", sentUnredeemed, got)
	}
	if got[sentRedeemed] {
		t.Fatalf("did not expect redeemed request %s to be returned, got %v", sentRedeemed, got)
	}
}
