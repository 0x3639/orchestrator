package wrap

var (
	wrapEventPrefix = []byte{0}

	// We store here the account block height on which the orchestrator updated the blocks
	lastUpdatePrefix = []byte{1}

	// We store the resigned status on wraps
	resignedStatusPrefix = []byte{2}

	// Private monitoring checkpoint, outside the wrap event iterator.
	receiveCoverageKey = []byte{3}

	// The frontier momentum at which this database first followed the bridge
	// account to its tip. Outside the wrap event iterator, so a build that
	// does not know the key never reads it.
	bridgeWatchKey = []byte{4}
)
