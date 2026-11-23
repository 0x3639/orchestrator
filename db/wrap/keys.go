package wrap

var (
	wrapEventPrefix = []byte{0}

	// We store here the account block height on which the orchestrator updated the blocks
	lastUpdatePrefix = []byte{1}

	// We store the resigned status on wraps
	resignedStatusPrefix = []byte{2}

	// Private monitoring checkpoint, outside the wrap event iterator.
	receiveCoverageKey = []byte{3}

	// Set once this database has reached the part of the bridge account it
	// must vouch for. Outside the wrap event iterator, so a build that does
	// not know the key never reads it.
	bridgeWatchKey = []byte{4}
)
