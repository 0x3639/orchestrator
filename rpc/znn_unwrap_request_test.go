package rpc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"

	"orchestrator/common/config"
)

// The unwrap request read keeps three replies apart: no request, a request
// with its remaining redeem delay, and a request whose reply leaves the delay
// out. A null reply is never a request with zero values.
func TestUnwrapTokenRequestContextKeepsNullAndMissingDelayApart(t *testing.T) {
	const request = `"registrationMomentumHeight":7,"networkClass":2,"chainId":1,` +
		`"transactionHash":"00000000000000000000000000000000000000000000000000000000000000aa","logIndex":3,` +
		`"toAddress":"z1qqjnwjjpnue8xmmpanz6csze6tcmtzzdtfsww7","tokenAddress":"0x02",` +
		`"tokenStandard":"zts1znnxxxxxxxxxxxxx9z4ulx","amount":"100","signature":"s","redeemed":0,"revoked":0`
	for _, tc := range []struct {
		name      string
		reply     string
		found     bool
		wantDelay *uint64
	}{
		{"null", `null`, false, nil},
		{"request with a delay", `{` + request + `,"redeemableIn":30}`, true, ptr(uint64(30))},
		{"request past its delay", `{` + request + `,"redeemableIn":0}`, true, ptr(uint64(0))},
		{"request without the delay", `{` + request + `}`, true, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var call struct {
					ID json.RawMessage `json:"id"`
				}
				_ = json.NewDecoder(r.Body).Decode(&call)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + string(call.ID) + `,"result":` + tc.reply + `}`))
			}))
			defer server.Close()
			manager, err := NewRpcManager(config.BaseNetworkConfig{Urls: []string{server.URL}}, make(chan os.Signal, 1))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = manager.Znn().Stop() }()
			got, delay, err := manager.Znn().UnwrapTokenRequestContext(context.Background(), types.Hash{31: 0xaa}, 3)
			if err != nil {
				t.Fatal(err)
			}
			if (got != nil) != tc.found {
				t.Fatalf("request %+v, want found %v", got, tc.found)
			}
			if tc.found && (got.LogIndex != 3 || got.ChainId != 1 || got.Amount.Int64() != 100) {
				t.Fatalf("decoded request %+v", got)
			}
			if (delay == nil) != (tc.wantDelay == nil) || (delay != nil && *delay != *tc.wantDelay) {
				t.Fatalf("delay %v, want %v", delay, tc.wantDelay)
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }
