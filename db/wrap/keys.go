package wrap

// Every key in the Zenon database starts with one of the bytes below. Keep
// them distinct, and define new ones in this file.
//
// Earlier builds of the redeem incident change kept incident evidence under 4
// and its resolution under 5. NewDbManager moves such keys to 8 and 9 at
// startup (MoveRedeemIncidentKeys), recognising them by their exact layout,
// so 4 and 5 are free for other keys.

// Keyed by destination EVM chain and Zenon wrap ID.
const legacyRedeemStatusPrefix byte = 6 // Included provisional events in earlier PR builds.
const redeemStatusPrefix byte = 7

// Keyed by EVM chain, block hash, transaction hash and log index. Keep the
// original evidence immutable after the bridge is halted.
const redeemEvidencePrefix byte = 8

// The same key with this prefix holds the common.RedeemResolution that
// released the incident.
const redeemHaltConfirmedPrefix byte = 9

var (
	wrapEventPrefix = []byte{0}

	// We store here the account block height on which the orchestrator updated the blocks
	lastUpdatePrefix = []byte{1}

	// We store the resigned status on wraps
	resignedStatusPrefix = []byte{2}

	// Private monitoring checkpoint, outside the wrap event iterator.
	receiveCoverageKey = []byte{3}

	// The first bridge account height this database vouches for. Outside the
	// wrap event iterator, so a build that does not know the key never reads
	// it.
	bridgeWatchKey = []byte{4}
)
