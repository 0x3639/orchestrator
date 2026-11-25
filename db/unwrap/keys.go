package unwrap

var (
	// events that do not have a send block associated
	unwrapRequestPrefix = []byte{0}

	// We hold here the evm chain block height on which the orchestrator updated the blocks
	lastUpdatePrefix = []byte{1}

	// Per-source affiliate decisions survive queue replay and restart.
	affiliatePolicyPrefix = []byte{2}

	// The block in which this node's scan found the source event of a
	// transaction and log index, kept apart from the record of the request.
	// Nothing that reads the other prefixes sees it.
	sourceLocationPrefix = []byte{3}

	// Bridge contract address bound to this EVM LevelDB directory so a
	// same-name contract rotation isolates new scan cursors and work while
	// retaining old evidence on disk.
	contractAddressPrefix = []byte{4}

	// Persisted unconfirmed halt / key-change transaction metadata per
	// (sender, contract, kind) so retries recover nonce ownership across restarts.
	pendingTxPrefix = []byte{5}
)
