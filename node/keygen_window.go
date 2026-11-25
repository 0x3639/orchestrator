package node

import (
	"fmt"
	"math"
	"time"
)

// Key generation starts only when the frontier momentum height is within a few
// momentums of a multiple of the keygen window. Every signer derives the window
// from its PartyTimeout, so every signer must compute the same value for the
// same timeout or they will try to start keygen at different heights.
const (
	// keyGenWindowPartyRounds is the number of party timeouts in one window.
	keyGenWindowPartyRounds = 30
	// keyGenWindowMomentumsPerMinute is the Zenon momentum rate (one per 10s).
	keyGenWindowMomentumsPerMinute = 6
	// keyGenWindowMinPartyMinutes is the smallest whole-minute PartyTimeout the
	// window is computed from. A smaller value would give a zero window and an
	// integer divide by zero in the scheduling loop.
	keyGenWindowMinPartyMinutes = 1
	// maxTssTimeout bounds every TSS timeout accepted from bridge metadata and
	// the PartyTimeout the keygen window is computed from.
	maxTssTimeout = 24 * time.Hour
	// minPartyTimeout is the smallest PartyTimeout accepted from bridge
	// metadata. The keygen window counts PartyTimeout in whole minutes.
	minPartyTimeout = keyGenWindowMinPartyMinutes * time.Minute
)

// keyGenWindowMomentums returns the keygen window in momentums for a PartyTimeout.
//
// It keeps the long-standing schedule: PartyTimeout is truncated to whole
// minutes, so the default 90s timeout gives 1 * 30 * 6 = 180 momentums. Signers
// on older versions compute the same value for every timeout from one minute
// to maxTssTimeout, so they keep starting keygen at the same heights.
//
// Values outside that range used to panic (zero window) or never schedule
// keygen; they are clamped instead. The result is always at least
// keyGenWindowPartyRounds*keyGenWindowMomentumsPerMinute.
func keyGenWindowMomentums(partyTimeout time.Duration) uint64 {
	if partyTimeout > maxTssTimeout {
		partyTimeout = maxTssTimeout
	}
	// Integer division truncates toward zero. Up to maxTssTimeout this matches
	// the old uint64(PartyTimeout.Minutes()) exactly: float64 rounding in
	// Minutes() only moves a value across a minute boundary far above the cap.
	minutes := uint64(keyGenWindowMinPartyMinutes)
	if partyTimeout > 0 {
		if m := uint64(partyTimeout / time.Minute); m > minutes {
			minutes = m
		}
	}
	const perMinute = keyGenWindowPartyRounds * keyGenWindowMomentumsPerMinute
	if minutes > math.MaxUint64/perMinute {
		return math.MaxUint64
	}
	return minutes * perMinute
}

// tssTimeoutFromMetadata converts a TSS timeout in seconds from bridge
// metadata into a duration. It rejects values that would overflow
// time.Duration, values above maxTssTimeout and values below min.
func tssTimeoutFromMetadata(name string, seconds uint64, min time.Duration) (time.Duration, error) {
	if seconds > uint64(maxTssTimeout/time.Second) {
		return 0, fmt.Errorf("%s of %d seconds exceeds the maximum of %d seconds", name, seconds, uint64(maxTssTimeout/time.Second))
	}
	duration := time.Duration(seconds) * time.Second
	if duration < min {
		return 0, fmt.Errorf("%s of %d seconds is below the minimum of %d seconds", name, seconds, uint64(min/time.Second))
	}
	return duration, nil
}

// metadataTssTimeout validates a TSS timeout from bridge metadata. It reports
// false when the field is unset (zero) or invalid; an invalid value is logged
// and the current timeout is kept.
func (node *Node) metadataTssTimeout(name string, seconds uint64, min time.Duration) (time.Duration, bool) {
	if seconds == 0 {
		return 0, false
	}
	duration, err := tssTimeoutFromMetadata(name, seconds, min)
	if err != nil {
		node.logger.Warnf("Ignoring bridge metadata: %v", err)
		return 0, false
	}
	return duration, true
}
