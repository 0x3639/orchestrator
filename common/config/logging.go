package config

import (
	"net/url"
	"orchestrator/common"
	"sort"
	"strconv"
	"strings"
)

// minRegisteredComponentLen avoids registering tiny URL components whose
// replacement would mangle unrelated log text. minRegisteredTokenLen applies
// to individual path segments and query values, which are only worth
// registering when they look like API keys.
const (
	minRegisteredComponentLen = 8
	minRegisteredTokenLen     = 16
)

// RegisterSecretURLs teaches the process-wide log redactor every configured
// RPC URL and its credential-bearing components, so client errors that echo
// the endpoint are scrubbed at the logging sink.
func RegisterSecretURLs(c *Config) {
	for _, network := range c.Networks {
		for _, raw := range network.Urls {
			RegisterSecretURL(raw)
		}
	}
}

// RegisterSecretURL registers one URL with the log redactor, together with
// the forms client libraries emit for it: the canonical rendering, the
// password-masked rendering used by net/http errors, and the userinfo-less
// rendering, plus its credential-bearing components.
func RegisterSecretURL(raw string) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return
	}
	redacted := RedactURL(raw)
	registerURLForm(raw, redacted)
	registerURLForm(parsed.String(), redacted)
	if parsed.User != nil {
		if password, ok := parsed.User.Password(); ok {
			// net/http renders credentials as user:***@host in url.Error by
			// string-replacing the userinfo, so mirror that exactly rather
			// than going through url.UserPassword, which would escape the
			// asterisks as %2A.
			canonical := parsed.String()
			masked := strings.Replace(canonical, parsed.User.String()+"@", parsed.User.Username()+":***@", 1)
			registerURLForm(masked, redacted)
			if len(password) >= minRegisteredComponentLen {
				common.LogRedactor.Register(password, common.RedactedPlaceholder)
			}
		}
		if username := parsed.User.Username(); len(username) >= minRegisteredComponentLen {
			common.LogRedactor.Register(username, common.RedactedPlaceholder)
		}
		stripped := *parsed
		stripped.User = nil
		registerURLForm(stripped.String(), redacted)
	}
	if parsed.Path != "" && parsed.Path != "/" && len(parsed.Path) >= minRegisteredComponentLen {
		common.LogRedactor.Register(parsed.Path, "/"+common.RedactedPlaceholder)
		if escaped := parsed.EscapedPath(); escaped != parsed.Path {
			common.LogRedactor.Register(escaped, "/"+common.RedactedPlaceholder)
		}
	}
	// Fragments are registered with their leading '#' so a short fragment
	// such as "production" cannot scrub ordinary words elsewhere in the log;
	// only key-length fragments are registered bare.
	if fragment := parsed.EscapedFragment(); len(fragment) >= minRegisteredComponentLen {
		common.LogRedactor.Register("#"+fragment, "#"+common.RedactedPlaceholder)
		if fragment != parsed.Fragment {
			common.LogRedactor.Register("#"+parsed.Fragment, "#"+common.RedactedPlaceholder)
		}
	}
	if len(parsed.Fragment) >= minRegisteredTokenLen {
		common.LogRedactor.Register(parsed.Fragment, common.RedactedPlaceholder)
	}
	for _, segment := range strings.Split(parsed.Path, "/") {
		if len(segment) >= minRegisteredTokenLen {
			common.LogRedactor.Register(segment, common.RedactedPlaceholder)
		}
	}
	if len(parsed.RawQuery) >= minRegisteredComponentLen {
		common.LogRedactor.Register(parsed.RawQuery, common.RedactedPlaceholder)
	}
	for _, values := range parsed.Query() {
		for _, value := range values {
			if len(value) >= minRegisteredTokenLen {
				common.LogRedactor.Register(value, common.RedactedPlaceholder)
			}
		}
	}
}

// registerURLForm registers one whole-URL rendering and, when it differs,
// the form fmt's %q verb produces for it. url.Error quotes the URL with %q,
// so a decoded username containing a quote, backslash or control byte is
// escaped again before it reaches the log.
func registerURLForm(form, redacted string) {
	common.LogRedactor.Register(form, redacted)
	quoted := strconv.Quote(form)
	quoted = quoted[1 : len(quoted)-1]
	if quoted != form {
		common.LogRedactor.Register(quoted, redacted)
	}
}

// RedactURLs applies RedactURL to every entry.
func RedactURLs(raw []string) []string {
	out := make([]string, 0, len(raw))
	for _, u := range raw {
		out = append(out, RedactURL(u))
	}
	return out
}

type redactSpan struct {
	start int
	end   int
	repl  string
}

func appendNeedleSpans(spans []redactSpan, msg, needle, repl string) []redactSpan {
	if needle == "" {
		return spans
	}
	for offset := 0; offset < len(msg); {
		idx := strings.Index(msg[offset:], needle)
		if idx < 0 {
			break
		}
		start := offset + idx
		spans = append(spans, redactSpan{start: start, end: start + len(needle), repl: repl})
		offset = start + 1
	}
	return spans
}

// RedactErrorForURL renders err for logging with the raw URL, complete
// request URI / path / query components, credentials, and individual query
// values replaced. Sensitive spans are located on the original error string
// and overlapping spans are merged before a single replacement pass so neither
// path-vs-query overlaps nor replacement text can leak credential suffixes.
func RedactErrorForURL(err error, raw string) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if msg == "" || raw == "" {
		return msg
	}
	var spans []redactSpan
	spans = appendNeedleSpans(spans, msg, raw, RedactURL(raw))
	if parsed, perr := url.Parse(raw); perr == nil {
		if reqURI := parsed.RequestURI(); len(reqURI) > 1 {
			spans = appendNeedleSpans(spans, msg, reqURI, "/"+redactedPlaceholder)
		}
		if escPath := parsed.EscapedPath(); len(escPath) > 1 {
			spans = appendNeedleSpans(spans, msg, escPath, "/"+redactedPlaceholder)
		}
		if len(parsed.Path) > 1 {
			spans = appendNeedleSpans(spans, msg, parsed.Path, "/"+redactedPlaceholder)
		}
		if parsed.RawQuery != "" {
			spans = appendNeedleSpans(spans, msg, parsed.RawQuery, redactedPlaceholder)
		}
		if parsed.User != nil {
			if password, ok := parsed.User.Password(); ok && password != "" {
				spans = appendNeedleSpans(spans, msg, password, redactedPlaceholder)
			}
			if username := parsed.User.Username(); username != "" {
				spans = appendNeedleSpans(spans, msg, username, redactedPlaceholder)
			}
		}
		if parsed.RawQuery != "" {
			for _, vals := range parsed.Query() {
				for _, v := range vals {
					if v != "" {
						spans = appendNeedleSpans(spans, msg, v, redactedPlaceholder)
					}
				}
			}
		}
	}
	if len(spans) == 0 {
		return msg
	}
	sort.Slice(spans, func(i, j int) bool {
		if spans[i].start != spans[j].start {
			return spans[i].start < spans[j].start
		}
		return spans[i].end > spans[j].end
	})
	merged := make([]redactSpan, 0, len(spans))
	for _, sp := range spans {
		if len(merged) == 0 {
			merged = append(merged, sp)
			continue
		}
		last := &merged[len(merged)-1]
		if sp.start < last.end {
			if sp.end > last.end {
				last.end = sp.end
				last.repl = redactedPlaceholder
			}
			continue
		}
		merged = append(merged, sp)
	}
	var out strings.Builder
	out.Grow(len(msg))
	pos := 0
	for _, sp := range merged {
		out.WriteString(msg[pos:sp.start])
		out.WriteString(sp.repl)
		pos = sp.end
	}
	out.WriteString(msg[pos:])
	return out.String()
}

const redactedPlaceholder = common.RedactedPlaceholder

// NetworkSummary is the loggable view of a network configuration.
type NetworkSummary struct {
	Urls                 []string
	FilterQuerySize      uint64
	RpcRequestsPerSecond float64
	RpcBurst             int
}

// HealthSummary is the loggable view of the health RPC configuration.
type HealthSummary struct {
	Address                     string
	Port                        int
	CachedResponseDelay         int64
	ResponsesPerSecond          int
	Burst                       int
	PerClientResponsesPerSecond int
	PerClientBurst              int
}

// TssSummary is the loggable view of the TSS configuration. Bootstrap and the
// peer whitelist are intentionally omitted.
type TssSummary struct {
	Port         int
	PublicKey    string
	BaseDir      string
	BootstrapSet bool
}

// Summary is an explicit allowlist of configuration fields that are safe to
// log. Secrets and secret-bearing URLs never appear here; new fields must be
// added deliberately.
type Summary struct {
	DataPath                      string
	GlobalState                   uint8
	EvmAddress                    string
	Networks                      map[string]NetworkSummary
	Tss                           TssSummary
	Health                        HealthSummary
	ProducerKeyFileName           string
	ProducerKeyFilePassphraseFile string
	ProducerIndex                 uint32
}

// LoggableSummary returns the allowlisted, redacted view of the configuration.
func (c Config) LoggableSummary() Summary {
	networks := make(map[string]NetworkSummary, len(c.Networks))
	for name, network := range c.Networks {
		urls := RedactURLs(network.Urls)
		sort.Strings(urls)
		networks[name] = NetworkSummary{Urls: urls, FilterQuerySize: network.FilterQuerySize, RpcRequestsPerSecond: network.RpcRequestsPerSecond, RpcBurst: network.RpcBurst}
	}

	return Summary{
		DataPath:    c.DataPath,
		GlobalState: c.GlobalState,
		EvmAddress:  c.EvmAddress,
		Networks:    networks,
		Tss: TssSummary{
			Port:         c.TssConfig.Port,
			PublicKey:    c.TssConfig.PublicKey,
			BaseDir:      c.TssConfig.BaseDir,
			BootstrapSet: c.TssConfig.Bootstrap != "",
		},
		Health: HealthSummary{
			Address:                     c.HealthConfig.Address,
			Port:                        c.HealthConfig.Port,
			CachedResponseDelay:         c.HealthConfig.CachedResponseDelay,
			ResponsesPerSecond:          c.HealthConfig.ResponsesPerSecond,
			Burst:                       c.HealthConfig.Burst,
			PerClientResponsesPerSecond: c.HealthConfig.PerClientResponsesPerSecond,
			PerClientBurst:              c.HealthConfig.PerClientBurst,
		},
		ProducerKeyFileName:           c.ProducerKeyFileName,
		ProducerKeyFilePassphraseFile: c.ProducerKeyFilePassphraseFile,
		ProducerIndex:                 c.ProducerIndex,
	}
}

// RedactURL keeps only the scheme and host of a URL. User information, the
// path, the query and the fragment are all dropped because RPC providers
// commonly embed API keys in any of them (for example /v3/<key> or ?apikey=).
// Values that do not parse as URLs are replaced entirely.
func RedactURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return redactedPlaceholder
	}
	var b strings.Builder
	b.WriteString(parsed.Scheme)
	b.WriteString("://")
	if parsed.User != nil {
		b.WriteString(redactedPlaceholder)
		b.WriteString("@")
	}
	b.WriteString(parsed.Host)
	if parsed.Path != "" && parsed.Path != "/" {
		b.WriteString("/")
		b.WriteString(redactedPlaceholder)
	}
	if parsed.RawQuery != "" {
		b.WriteString("?")
		b.WriteString(redactedPlaceholder)
	}
	return b.String()
}
