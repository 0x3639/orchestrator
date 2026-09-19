package common

import (
	"sort"
	"strings"
	"sync"
)

// RedactedPlaceholder replaces secret material in log output.
const RedactedPlaceholder = "<redacted>"

type redactPair struct {
	secret      string
	replacement string
}

// SecretRedactor rewrites registered secrets out of strings. It backs the
// logging cores so that any message, field or error that echoes a
// credential-bearing URL is scrubbed at the sink rather than at each call site.
type SecretRedactor struct {
	mu    sync.RWMutex
	pairs []redactPair
}

// LogRedactor is the process-wide redactor used by every logger created in
// this package.
var LogRedactor = &SecretRedactor{}

// Register adds a secret and its replacement. Longer secrets are applied
// first so a full URL is replaced before its components.
func (r *SecretRedactor) Register(secret, replacement string) {
	if secret == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.pairs {
		if p.secret == secret {
			return
		}
	}
	r.pairs = append(r.pairs, redactPair{secret: secret, replacement: replacement})
	sort.SliceStable(r.pairs, func(i, j int) bool {
		return len(r.pairs[i].secret) > len(r.pairs[j].secret)
	})
}

// Redact returns s with every registered secret replaced.
func (r *SecretRedactor) Redact(s string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.pairs {
		if strings.Contains(s, p.secret) {
			s = strings.ReplaceAll(s, p.secret, p.replacement)
		}
	}
	return s
}

// Reset removes every registered secret. Intended for tests.
func (r *SecretRedactor) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pairs = nil
}
