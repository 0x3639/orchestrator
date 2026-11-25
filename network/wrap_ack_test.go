package network

import (
	"math/big"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/api"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"orchestrator/common"
	"orchestrator/common/events"
)

func TestUpdateWrapRequestReceivePreservesNewerSignatureAndPendingResign(t *testing.T) {
	b := newReplayBridge(t)

	// Subtest 1: UpdateWrapRequest reads old-sig from Zenon RPC, meanwhile
	// SaveSignedWrapRequest writes new-sig (SentSignature=false). The listener
	// must NOT overwrite new-sig or mark it sent.
	t.Run("preserves newer local signature", func(t *testing.T) {
		id := types.Hash{0x81}
		request := &definition.WrapTokenRequest{
			NetworkClass: definition.EvmClass, ChainId: 1, Id: id,
			ToAddress: "0x04", TokenStandard: types.ZnnTokenStandard,
			TokenAddress: strings.ToLower(b.token.Hex()),
			Amount:       big.NewInt(100), Fee: big.NewInt(1),
			Signature: "old-sig",
		}
		var stores replayStores
		url := replayServer(t, func(method string) interface{} {
			if method == "embedded.bridge.getWrapTokenRequestById" {
				// Simulate SaveSignedWrapRequest storing a newer signature
				// right after the RPC read completes.
				if saved, err := stores.znn.SaveSignedWrapRequest(common.ZnnWrapToOrchestratorWrap(request), "new-sig"); err != nil || !saved {
					t.Errorf("SaveSignedWrapRequest = (%v, %v), want (true, nil)", saved, err)
				}
				return request
			}
			t.Errorf("unexpected method %s", method)
			return nil
		})
		var n *znnNetwork
		n, stores = replayNetwork(t, b, url, nil)
		if err := stores.znn.AddWrapRequest(common.ZnnWrapToOrchestratorWrap(request)); err != nil {
			t.Fatal(err)
		}

		data, err := definition.ABIBridge.PackMethod(definition.UpdateWrapRequestMethodName, id, "old-sig")
		if err != nil {
			t.Fatal(err)
		}
		send := &api.AccountBlock{AccountBlock: nom.AccountBlock{Hash: types.Hash{8}, Data: data}}
		if err := n.InterpretSendBlockData(send, false, 42); err != nil {
			t.Fatal(err)
		}

		row, err := stores.znn.GetWrapRequestById(id)
		if err != nil || row == nil {
			t.Fatalf("stored row %v, err %v", row, err)
		}
		if row.Signature != "new-sig" || row.SentSignature {
			t.Fatalf("listener overwrote newer signature: sig=%q sent=%v, want sig=new-sig sent=false", row.Signature, row.SentSignature)
		}
	})

	// Subtest 2: UpdateWrapRequest reads old-sig from Zenon RPC, meanwhile
	// PublishResignWraps marks the wrap for re-signing. The listener must NOT
	// clear the pending re-sign flag.
	t.Run("preserves newly published resign flag", func(t *testing.T) {
		id := types.Hash{0x82}
		request := &definition.WrapTokenRequest{
			NetworkClass: definition.EvmClass, ChainId: 1, Id: id,
			ToAddress: "0x04", TokenStandard: types.ZnnTokenStandard,
			TokenAddress: strings.ToLower(b.token.Hex()),
			Amount:       big.NewInt(100), Fee: big.NewInt(1),
			Signature: "old-sig",
		}
		var stores replayStores
		url := replayServer(t, func(method string) interface{} {
			if method == "embedded.bridge.getWrapTokenRequestById" {
				if err := stores.znn.PublishResignWraps([]events.WrapRequestZnn{common.ZnnWrapToOrchestratorWrap(request)}); err != nil {
					t.Errorf("PublishResignWraps failed: %v", err)
				}
				return request
			}
			t.Errorf("unexpected method %s", method)
			return nil
		})
		var n *znnNetwork
		n, stores = replayNetwork(t, b, url, nil)
		if err := stores.znn.AddWrapRequest(common.ZnnWrapToOrchestratorWrap(request)); err != nil {
			t.Fatal(err)
		}

		data, err := definition.ABIBridge.PackMethod(definition.UpdateWrapRequestMethodName, id, "old-sig")
		if err != nil {
			t.Fatal(err)
		}
		send := &api.AccountBlock{AccountBlock: nom.AccountBlock{Hash: types.Hash{9}, Data: data}}
		if err := n.InterpretSendBlockData(send, false, 43); err != nil {
			t.Fatal(err)
		}

		if resign, err := stores.znn.GetResignStatus(id); err != nil || !resign {
			t.Fatalf("listener cleared pending resign flag: resign=%v err=%v", resign, err)
		}
	})
}
