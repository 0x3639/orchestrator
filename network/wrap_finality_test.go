package network

import (
	"slices"
	"testing"

	"github.com/zenon-network/go-zenon/rpc/api/embedded"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
)

func TestFinalizedUnsignedWraps(t *testing.T) {
	first, second := new(definition.WrapTokenRequest), new(definition.WrapTokenRequest)
	list := &embedded.WrapTokenRequestList{List: []*embedded.WrapTokenRequest{
		{WrapTokenRequest: first},
		{WrapTokenRequest: new(definition.WrapTokenRequest), ConfirmationsToFinality: 1},
		{WrapTokenRequest: second},
	}}
	if got := finalizedUnsignedWraps(list); !slices.Equal(got, []*definition.WrapTokenRequest{first, second}) {
		t.Fatalf("expected both final requests in RPC order, got %v", got)
	}
	if finalizedUnsignedWraps(nil) != nil || finalizedUnsignedWraps(&embedded.WrapTokenRequestList{}) != nil {
		t.Fatal("empty RPC results must produce an empty signing pool")
	}
}
