package config

import (
	"net/url"
	"sort"
	"strings"
)

// RedactURLs applies RedactURL to every entry.
func RedactURLs(raw []string) []string {
	out := make([]string, 0, len(raw))
	for _, u := range raw {
		out = append(out, RedactURL(u))
	}
	return out
}

// RedactErrorForURL renders err for logging with the raw URL, and any
// password embedded in it, replaced. Client libraries frequently echo the
// dial target inside their error strings.
func RedactErrorForURL(err error, raw string) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if raw != "" {
		msg = strings.ReplaceAll(msg, raw, RedactURL(raw))
	}
	if parsed, perr := url.Parse(raw); perr == nil && parsed.User != nil {
		if password, ok := parsed.User.Password(); ok && password != "" {
			msg = strings.ReplaceAll(msg, password, redactedPlaceholder)
		}
		if username := parsed.User.Username(); username != "" {
			msg = strings.ReplaceAll(msg, username, redactedPlaceholder)
		}
	}
	return msg
}

const redactedPlaceholder = "<redacted>"

// NetworkSummary is the loggable view of a network configuration.
type NetworkSummary struct {
	Urls            []string
	FilterQuerySize uint64
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
		networks[name] = NetworkSummary{Urls: urls, FilterQuerySize: network.FilterQuerySize}
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
