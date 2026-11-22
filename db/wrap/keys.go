package wrap

// Keyed by destination EVM chain and Zenon wrap ID. Prefixes 4 and 5 store incident evidence.
const legacyRedeemStatusPrefix byte = 6 // Included provisional events in earlier PR builds.
const redeemStatusPrefix byte = 7

var (
	wrapEventPrefix = []byte{0}

	// We store here the account block height on which the orchestrator updated the blocks
	lastUpdatePrefix = []byte{1}

	// We store the resigned status on wraps
	resignedStatusPrefix = []byte{2}

	// Private monitoring checkpoint, outside the wrap event iterator.
	receiveCoverageKey = []byte{3}
)
