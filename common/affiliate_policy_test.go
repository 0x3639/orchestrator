package common

import (
	"github.com/zenon-network/go-zenon/common/types"
	"testing"
)

func TestAffiliatePoliciesKeepOtherNetworks(t *testing.T) {
	state := NewGlobalState(new(uint8))
	for _, id := range []uint32{1, 56} {
		state.SetTokensMap(id, types.ZnnTokenStandard.String(), "0xaBC")
	}
	state.SetIsAffiliateProgram(AffiliateProgram{Networks: map[uint32]AffiliateNetwork{
		1: {WZNN: true, StartingHeight: 5}, 56: {ZNN: true, StartingHeight: 10},
	}})
	if state.AffiliatePolicyAt(1, "0xAbC", 5) != SplitAffiliate || state.AffiliatePolicyAt(56, "0xabc", 10) != VerifyAffiliate {
		t.Fatal("multi-network update lost a setting")
	}
	state.SetIsAffiliateProgram(AffiliateProgram{Networks: map[uint32]AffiliateNetwork{1: {ZNN: true, WZNN: true, StartingHeight: 30}}})
	if state.AffiliatePolicyAt(1, "0xabc", 29) != 0 || state.AffiliatePolicyAt(1, "0xabc", 30) != (SplitAffiliate|VerifyAffiliate) || state.AffiliatePolicyAt(56, "0xabc", 10) != VerifyAffiliate {
		t.Fatal("partial update changed the omitted chain or ignored its threshold")
	}
}
