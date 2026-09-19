package rpc

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestWarnAgreementConfiguration(t *testing.T) {
	cases := map[string]struct {
		urls  []string
		warns int
	}{
		"two independent":  {[]string{"ws://a", "ws://b"}, 0},
		"single":           {[]string{"ws://a"}, 1},
		"duplicate":        {[]string{"ws://a", "ws://a"}, 2},
		"duplicate plus b": {[]string{"ws://a", "ws://a", "ws://b"}, 1},
	}
	for name, tc := range cases {
		core, logs := observer.New(zap.WarnLevel)
		warnAgreementConfiguration(zap.New(core).Sugar(), "Ethereum", tc.urls)
		if logs.Len() != tc.warns {
			t.Fatalf("%s: expected %d warnings, got %d: %v", name, tc.warns, logs.Len(), logs.All())
		}
	}
}
