package common

import (
	"errors"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestRedactingCoreScrubsMessagesAndFields(t *testing.T) {
	LogRedactor.Reset()
	t.Cleanup(LogRedactor.Reset)
	LogRedactor.Register("SECRET-URL-TOKEN", RedactedPlaceholder)
	LogRedactor.Register("SECRET-PASSWORD", RedactedPlaceholder)

	core, logs := observer.New(zap.DebugLevel)
	logger := zap.New(newRedactingCore(core))
	sugar := logger.Sugar()

	logger.Info("dial https://host/v3/SECRET-URL-TOKEN failed",
		zap.String("reason", "auth SECRET-PASSWORD rejected"),
		zap.Error(errors.New("post https://host/v3/SECRET-URL-TOKEN: refused")),
	)
	sugar.Infof("retrying %s", "https://host/v3/SECRET-URL-TOKEN")
	sugar.Info("raw error: ", errors.New("SECRET-PASSWORD leaked"))
	logger.With(zap.String("endpoint", "wss://u:SECRET-PASSWORD@host")).Warn("context field")

	for _, entry := range logs.All() {
		if strings.Contains(entry.Message, "SECRET") {
			t.Fatalf("message leaked secret: %q", entry.Message)
		}
		for _, f := range entry.Context {
			if strings.Contains(f.String, "SECRET") {
				t.Fatalf("field %q leaked secret: %q", f.Key, f.String)
			}
			if f.Interface != nil {
				if err, ok := f.Interface.(error); ok && strings.Contains(err.Error(), "SECRET") {
					t.Fatalf("error field %q leaked secret: %v", f.Key, err)
				}
			}
		}
	}
	if logs.Len() != 4 {
		t.Fatalf("expected 4 entries, got %d", logs.Len())
	}
}

func TestSecretRedactorPrefersLongestMatch(t *testing.T) {
	r := &SecretRedactor{}
	r.Register("abc", "<short>")
	r.Register("abcdef", "<long>")
	if got := r.Redact("x abcdef y abc"); got != "x <long> y <short>" {
		t.Fatalf("got %q", got)
	}
	r.Register("", "ignored")
	if got := r.Redact("plain"); got != "plain" {
		t.Fatalf("got %q", got)
	}
}
