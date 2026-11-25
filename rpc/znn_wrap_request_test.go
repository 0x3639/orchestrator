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

const testWrapFields = `"networkClass":2,"chainId":1,` +
	`"id":"00000000000000000000000000000000000000000000000000000000000000bb",` +
	`"toAddress":"0x0000000000000000000000000000000000000001",` +
	`"tokenStandard":"zts1znnxxxxxxxxxxxxx9z4ulx",` +
	`"tokenAddress":"0x0000000000000000000000000000000000000002",` +
	`"amount":"100","fee":"1","signature":"sig","creationMomentumHeight":42`

func newRawZnnRPC(t *testing.T, reply string) *ZnnRpc {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var call struct {
			ID json.RawMessage `json:"id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&call)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + string(call.ID) + `,"result":` + reply + `}`))
	}))
	t.Cleanup(srv.Close)

	mgr, err := NewRpcManager(config.BaseNetworkConfig{Urls: []string{srv.URL}}, make(chan os.Signal, 1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mgr.Znn().Stop() })
	return mgr.Znn()
}

func TestGetAllWrapTokenRequestsContextRejectsMissingNullFinalityAndNullPage(t *testing.T) {
	t.Run("valid page preserves finality", func(t *testing.T) {
		reply := `{"count":2,"list":[{` + testWrapFields + `,"confirmationsToFinality":0},{` + testWrapFields + `,"confirmationsToFinality":3}]}`
		znn := newRawZnnRPC(t, reply)
		page, err := znn.GetAllWrapTokenRequestsContext(context.Background(), 0, 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if page.Count != 2 || len(page.List) != 2 {
			t.Fatalf("page count=%d len=%d, want 2/2", page.Count, len(page.List))
		}
		if page.List[0].ConfirmationsToFinality != 0 || page.List[1].ConfirmationsToFinality != 3 {
			t.Fatalf("confirmations = (%d, %d), want (0, 3)", page.List[0].ConfirmationsToFinality, page.List[1].ConfirmationsToFinality)
		}
	})

	const testWrapFieldsNoID = `"networkClass":2,"chainId":1,` +
		`"toAddress":"0x0000000000000000000000000000000000000001",` +
		`"tokenStandard":"zts1znnxxxxxxxxxxxxx9z4ulx",` +
		`"tokenAddress":"0x0000000000000000000000000000000000000002",` +
		`"amount":"100","fee":"1","signature":"sig","creationMomentumHeight":42,"confirmationsToFinality":0`
	const testWrapFieldsNoHeight = `"networkClass":2,"chainId":1,` +
		`"id":"00000000000000000000000000000000000000000000000000000000000000bb",` +
		`"toAddress":"0x0000000000000000000000000000000000000001",` +
		`"tokenStandard":"zts1znnxxxxxxxxxxxxx9z4ulx",` +
		`"tokenAddress":"0x0000000000000000000000000000000000000002",` +
		`"amount":"100","fee":"1","signature":"sig","confirmationsToFinality":0`

	t.Run("valid empty page accepted", func(t *testing.T) {
		znn := newRawZnnRPC(t, `{"count":0,"list":[]}`)
		page, err := znn.GetAllWrapTokenRequestsContext(context.Background(), 0, 10)
		if err != nil {
			t.Fatalf("unexpected error for empty page: %v", err)
		}
		if page.Count != 0 || len(page.List) != 0 {
			t.Fatalf("empty page count=%d len=%d, want 0/0", page.Count, len(page.List))
		}
	})

	for _, tc := range []struct {
		name  string
		reply string
	}{
		{"null page", `null`},
		{"missing count", `{"list":[]}`},
		{"negative count", `{"count":-1,"list":[]}`},
		{"missing list", `{"count":0}`},
		{"null list", `{"count":0,"list":null}`},
		{"null entry", `{"count":1,"list":[null]}`},
		{"omitted confirmationsToFinality", `{"count":1,"list":[{` + testWrapFields + `}]}`},
		{"null confirmationsToFinality", `{"count":1,"list":[{` + testWrapFields + `,"confirmationsToFinality":null}]}`},
		{"omitted id", `{"count":1,"list":[{` + testWrapFieldsNoID + `}]}`},
		{"null id", `{"count":1,"list":[{` + testWrapFieldsNoID + `,"id":null}]}`},
		{"zero id", `{"count":1,"list":[{` + testWrapFieldsNoID + `,"id":"0000000000000000000000000000000000000000000000000000000000000000"}]}`},
		{"omitted creationMomentumHeight", `{"count":1,"list":[{` + testWrapFieldsNoHeight + `}]}`},
		{"null creationMomentumHeight", `{"count":1,"list":[{` + testWrapFieldsNoHeight + `,"creationMomentumHeight":null}]}`},
		{"missing toAddress", `{"count":1,"list":[{"networkClass":2,"chainId":1,"id":"00000000000000000000000000000000000000000000000000000000000000bb","tokenStandard":"zts1znnxxxxxxxxxxxxx9z4ulx","tokenAddress":"0x0000000000000000000000000000000000000002","amount":"100","fee":"1","creationMomentumHeight":42,"confirmationsToFinality":0}]}`},
		{"missing tokenAddress", `{"count":1,"list":[{"networkClass":2,"chainId":1,"id":"00000000000000000000000000000000000000000000000000000000000000bb","toAddress":"0x0000000000000000000000000000000000000001","tokenStandard":"zts1znnxxxxxxxxxxxxx9z4ulx","amount":"100","fee":"1","creationMomentumHeight":42,"confirmationsToFinality":0}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			znn := newRawZnnRPC(t, tc.reply)
			if got, err := znn.GetAllWrapTokenRequestsContext(context.Background(), 0, 10); err == nil {
				t.Fatalf("expected error for %s, got %+v", tc.name, got)
			}
		})
	}
}

func TestGetWrapTokenRequestByIdContextRejectsMissingAndNullFinality(t *testing.T) {
	t.Run("valid finality preserved", func(t *testing.T) {
		znn := newRawZnnRPC(t, `{`+testWrapFields+`,"confirmationsToFinality":4}`)
		got, err := znn.GetWrapTokenRequestByIdContext(context.Background(), types.Hash{31: 0xbb})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil || got.ConfirmationsToFinality != 4 {
			t.Fatalf("got %+v, want confirmationsToFinality=4", got)
		}
	})

	t.Run("null reply decodes nil", func(t *testing.T) {
		znn := newRawZnnRPC(t, `null`)
		got, err := znn.GetWrapTokenRequestByIdContext(context.Background(), types.Hash{31: 0xbb})
		if err != nil || got != nil {
			t.Fatalf("null reply = (%+v, %v), want (nil, nil)", got, err)
		}
	})

	const testWrapFieldsNoID = `"networkClass":2,"chainId":1,` +
		`"toAddress":"0x0000000000000000000000000000000000000001",` +
		`"tokenStandard":"zts1znnxxxxxxxxxxxxx9z4ulx",` +
		`"tokenAddress":"0x0000000000000000000000000000000000000002",` +
		`"amount":"100","fee":"1","signature":"sig","creationMomentumHeight":42,"confirmationsToFinality":0`
	const testWrapFieldsNoHeight = `"networkClass":2,"chainId":1,` +
		`"id":"00000000000000000000000000000000000000000000000000000000000000bb",` +
		`"toAddress":"0x0000000000000000000000000000000000000001",` +
		`"tokenStandard":"zts1znnxxxxxxxxxxxxx9z4ulx",` +
		`"tokenAddress":"0x0000000000000000000000000000000000000002",` +
		`"amount":"100","fee":"1","signature":"sig","confirmationsToFinality":0`

	for _, tc := range []struct {
		name  string
		reply string
	}{
		{"omitted confirmationsToFinality", `{` + testWrapFields + `}`},
		{"null confirmationsToFinality", `{` + testWrapFields + `,"confirmationsToFinality":null}`},
		{"omitted id", `{` + testWrapFieldsNoID + `}`},
		{"null id", `{` + testWrapFieldsNoID + `,"id":null}`},
		{"zero id", `{` + testWrapFieldsNoID + `,"id":"0000000000000000000000000000000000000000000000000000000000000000"}`},
		{"omitted creationMomentumHeight", `{` + testWrapFieldsNoHeight + `}`},
		{"null creationMomentumHeight", `{` + testWrapFieldsNoHeight + `,"creationMomentumHeight":null}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			znn := newRawZnnRPC(t, tc.reply)
			if got, err := znn.GetWrapTokenRequestByIdContext(context.Background(), types.Hash{31: 0xbb}); err == nil {
				t.Fatalf("expected error for %s, got %+v", tc.name, got)
			}
		})
	}
}
