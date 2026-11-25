package node

import (
	"math"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// legacyKeyGenWindow is the formula signers ran before keyGenWindowMomentums.
// It is only defined for PartyTimeout of at least one minute: below that it
// returns zero and the scheduling loop panics on the modulo.
func legacyKeyGenWindow(partyTimeout time.Duration) uint64 {
	return uint64(partyTimeout.Minutes()) * 30 * 6
}

// The shipped default must keep the schedule older signers use.
func TestKeyGenWindowMomentumsDefaultConfig(t *testing.T) {
	d := DefaultNodeConfig.TssConfig.BaseConfig.PartyTimeout
	if d != 90*time.Second {
		t.Fatalf("default PartyTimeout is %v; re-check keygen schedule compatibility before changing it", d)
	}
	if got := keyGenWindowMomentums(d); got != 180 {
		t.Fatalf("keyGenWindowMomentums(default %v) = %d, want 180", d, got)
	}
}

func TestKeyGenWindowMomentums(t *testing.T) {
	cases := []struct {
		name    string
		timeout time.Duration
		want    uint64
	}{
		{"default 90s truncates to one minute", 90 * time.Second, 180},
		{"exactly one minute", time.Minute, 180},
		{"just under two minutes", 2*time.Minute - time.Nanosecond, 180},
		{"two minutes", 2 * time.Minute, 360},
		{"sub-minute is clamped up", 59 * time.Second, 180},
		{"one nanosecond is clamped up", time.Nanosecond, 180},
		{"zero is clamped up", 0, 180},
		{"negative is clamped up", -time.Hour, 180},
		{"min int64 is clamped up", math.MinInt64, 180},
		{"maximum accepted timeout", maxTssTimeout, 24 * 60 * 180},
		{"above maximum is clamped down", maxTssTimeout + time.Minute, 24 * 60 * 180},
		{"max int64 is clamped down", math.MaxInt64, 24 * 60 * 180},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := keyGenWindowMomentums(tc.timeout)
			if got != tc.want {
				t.Fatalf("keyGenWindowMomentums(%v) = %d, want %d", tc.timeout, got, tc.want)
			}
			if got == 0 {
				t.Fatalf("keyGenWindowMomentums(%v) returned a zero window", tc.timeout)
			}
		})
	}
}

// Signers on older versions must start keygen at the same heights, so the new
// window has to match the legacy formula wherever that formula worked.
func TestKeyGenWindowMomentumsMatchesLegacySchedule(t *testing.T) {
	for s := int64(60); s <= int64(maxTssTimeout/time.Second); s++ {
		for _, extra := range []time.Duration{0, time.Second - time.Nanosecond} {
			d := time.Duration(s)*time.Second + extra
			if d > maxTssTimeout {
				continue
			}
			if got, want := keyGenWindowMomentums(d), legacyKeyGenWindow(d); got != want {
				t.Fatalf("keyGenWindowMomentums(%v) = %d, legacy schedule %d", d, got, want)
			}
		}
	}
}

func TestTssTimeoutFromMetadata(t *testing.T) {
	maxSeconds := uint64(maxTssTimeout / time.Second)
	cases := []struct {
		name    string
		seconds uint64
		min     time.Duration
		want    time.Duration
		wantErr bool
	}{
		{"default party timeout", 90, minPartyTimeout, 90 * time.Second, false},
		{"one minute party timeout", 60, minPartyTimeout, time.Minute, false},
		{"sub-minute party timeout", 59, minPartyTimeout, 0, true},
		{"one second party timeout", 1, minPartyTimeout, 0, true},
		{"zero party timeout", 0, minPartyTimeout, 0, true},
		{"one second keysign timeout", 1, time.Second, time.Second, false},
		{"maximum", maxSeconds, time.Second, maxTssTimeout, false},
		{"above maximum", maxSeconds + 1, time.Second, 0, true},
		// time.Duration(seconds)*time.Second overflows int64 above this.
		{"duration overflow", uint64(math.MaxInt64/int64(time.Second)) + 1, time.Second, 0, true},
		// uint64 to int64 conversion goes negative here.
		{"sign overflow", 1 << 63, time.Second, 0, true},
		{"max uint64", math.MaxUint64, time.Second, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tssTimeoutFromMetadata("PartyTimeout", tc.seconds, tc.min)
			if (err != nil) != tc.wantErr {
				t.Fatalf("tssTimeoutFromMetadata(%d) error = %v, wantErr %v", tc.seconds, err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("tssTimeoutFromMetadata(%d) = %v, want %v", tc.seconds, got, tc.want)
			}
			if err == nil && keyGenWindowMomentums(got) == 0 {
				t.Fatalf("accepted %d seconds but the keygen window is zero", tc.seconds)
			}
		})
	}
}

func TestMetadataTssTimeout(t *testing.T) {
	core, logs := observer.New(zapcore.WarnLevel)
	n := &Node{logger: zap.New(core).Sugar()}

	if _, ok := n.metadataTssTimeout("PartyTimeout", 0, minPartyTimeout); ok {
		t.Fatal("unset (zero) timeout was applied")
	}
	if logs.Len() != 0 {
		t.Fatalf("unset timeout logged %d warnings", logs.Len())
	}

	if _, ok := n.metadataTssTimeout("PartyTimeout", 30, minPartyTimeout); ok {
		t.Fatal("sub-minute PartyTimeout was applied")
	}
	if _, ok := n.metadataTssTimeout("KeyGenTimeout", math.MaxUint64, time.Second); ok {
		t.Fatal("overflowing KeyGenTimeout was applied")
	}
	if logs.Len() != 2 {
		t.Fatalf("got %d warnings for rejected timeouts, want 2", logs.Len())
	}

	got, ok := n.metadataTssTimeout("PartyTimeout", 90, minPartyTimeout)
	if !ok || got != 90*time.Second {
		t.Fatalf("default PartyTimeout = %v, %v; want 90s, true", got, ok)
	}
}
