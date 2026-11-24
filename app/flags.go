package app

import (
	"gopkg.in/urfave/cli.v1"
)

var (

	// pprof

	PprofFlag = cli.BoolFlag{
		Name:  "pprof",
		Usage: "Enable the pprof HTTP server",
	}
	PprofPortFlag = cli.Uint64Flag{
		Name:  "pprof.port",
		Usage: "pprof HTTP server listening port",
		Value: 6060,
	}

	PprofAddrFlag = cli.StringFlag{
		Name:  "pprof.addr",
		Usage: "pprof HTTP server listening interface",
		Value: "127.0.0.1",
	}

	// EvmBackfillBlocksFlag rewinds every EVM network's sync cursor once at
	// startup. It is the bounded alternative to deleting the events store
	// when a signer is suspected of having missed events.
	EvmBackfillBlocksFlag = cli.Uint64Flag{
		Name:  "evm.backfill-blocks",
		Usage: "Re-scan this many EVM blocks before the stored sync cursor at startup (one-shot repair; already stored events are kept)",
		Value: 0,
	}

	// AcknowledgeRedeemFlag names unmatched redeem registrations the operator
	// has reviewed. Each is archived as evidence and no longer stops this node.
	AcknowledgeRedeemFlag = cli.StringSliceFlag{
		Name:  "redeem.acknowledge",
		Usage: "Archive a reviewed unmatched redeem so it no longer holds this node in the emergency state, as chainId:transactionHash:logIndex (repeatable; does not change anything on chain)",
	}

	AllFlags = []cli.Flag{

		// pprof
		PprofFlag,
		PprofPortFlag,
		PprofAddrFlag,

		EvmBackfillBlocksFlag,
		AcknowledgeRedeemFlag,
	}
)
