package network

import (
	"fmt"
	"slices"
	"time"

	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"
)

const (
	// heldRedeemRetryInterval paces rechecks of registrations whose check gave
	// no answer. It is short against the shortest configured time between a
	// registration and its payout.
	heldRedeemRetryInterval = 3 * time.Second
	// heldRedeemReportEvery is the time between the repeated lines that tell
	// the operator a registration is still unchecked.
	heldRedeemReportEvery = time.Minute
	// settleIncidentsEvery is how many recheck rounds pass between looks at
	// whether the chain has settled a stored incident.
	settleIncidentsEvery = 10
)

type heldRedeemKey struct {
	block, tx ecommon.Hash
	index     uint
}

type heldRedeem struct {
	log      etypes.Log
	since    time.Time
	reported time.Time
	attempts int
}

func heldKey(log etypes.Log) heldRedeemKey {
	return heldRedeemKey{block: log.BlockHash, tx: log.TxHash, index: log.Index}
}

// checkLiveRedeem runs the live check of a young registration. A check that
// gives no answer is not dropped: the log is held and rechecked by watchRedeems
// until it matches, is reported, or its block is replaced.
func (eN *evmNetwork) checkLiveRedeem(log etypes.Log) error {
	err := eN.registerRedeem(log, true)
	eN.noteRedeemCheck(log, err)
	return err
}

// examineRegistration checks a registration the range scan met above the
// redeem checkpoint limit, and holds it when the check gives no answer.
func (eN *evmNetwork) examineRegistration(log etypes.Log, young bool) {
	if log.Removed || log.Address != *eN.ContractAddress() {
		return // InterpretLog rejects these once the finalized scan reaches them.
	}
	eN.noteRedeemCheck(log, eN.registerRedeem(log, young))
}

func (eN *evmNetwork) noteRedeemCheck(log etypes.Log, err error) {
	key := heldKey(log)
	eN.heldMu.Lock()
	defer eN.heldMu.Unlock()
	entry := eN.heldRedeems[key]
	if err == nil {
		// An answer for other content under the same identity says nothing
		// about the held log.
		if entry != nil && slices.Equal(entry.log.Topics, log.Topics) && slices.Equal(entry.log.Data, log.Data) {
			delete(eN.heldRedeems, key)
			eN.logger.Infof("Redeem registration on chain %d, block %d, transaction %s, log %d was checked after %d retries",
				eN.ChainId(), log.BlockNumber, log.TxHash, log.Index, entry.attempts)
		}
		return
	}
	if entry == nil {
		if eN.heldRedeems == nil {
			eN.heldRedeems = make(map[heldRedeemKey]*heldRedeem)
		}
		eN.heldRedeems[key] = &heldRedeem{log: log, since: time.Now(), reported: time.Now()}
		eN.logger.Warnf("Redeem registration on chain %d, block %d, transaction %s, log %d could not be checked and will be retried: %v",
			eN.ChainId(), log.BlockNumber, log.TxHash, log.Index, err)
		return
	}
	entry.attempts++
	if time.Since(entry.reported) >= heldRedeemReportEvery {
		entry.reported = time.Now()
		eN.logger.Errorf("Redeem registration on chain %d, block %d, transaction %s, log %d is still unchecked after %s and %d retries: %v",
			eN.ChainId(), log.BlockNumber, log.TxHash, log.Index, time.Since(entry.since).Round(time.Second), entry.attempts, err)
	}
}

func (eN *evmNetwork) heldRedeemLogs() []etypes.Log {
	eN.heldMu.Lock()
	defer eN.heldMu.Unlock()
	logs := make([]etypes.Log, 0, len(eN.heldRedeems))
	for _, entry := range eN.heldRedeems {
		logs = append(logs, entry.log)
	}
	return logs
}

// recheckHeldRedeems repeats the check of every held registration. Once a
// registration is finality-deep it is checked as the finalized scan would
// check it, so the same evidence decides it on both paths.
func (eN *evmNetwork) recheckHeldRedeems() {
	for _, log := range eN.heldRedeemLogs() {
		select {
		case <-eN.done:
			return
		default:
		}
		// Without the head the registration's age is unknown, and the age
		// selects the rules. Decide nothing this round.
		head, err := eN.EvmRpc().BlockNumber()
		if err != nil {
			eN.noteRedeemCheck(log, fmt.Errorf("read chain head: %w", err))
			continue
		}
		final := head >= log.BlockNumber && head-log.BlockNumber >= eN.ConfirmationsToFinality()
		err = eN.registerRedeem(log, !final)
		if err != nil && final {
			// A block replaced before finality never reaches the finalized
			// scan, so nothing else would release this entry.
			if canonical, hashErr := eN.EvmRpc().CanonicalHash(log.BlockNumber); hashErr == nil && canonical != log.BlockHash {
				eN.heldMu.Lock()
				delete(eN.heldRedeems, heldKey(log))
				eN.heldMu.Unlock()
				eN.logger.Infof("Redeem registration on chain %d, transaction %s, log %d left the canonical chain at block %d and is no longer held",
					eN.ChainId(), log.TxHash, log.Index, log.BlockNumber)
				continue
			}
		}
		eN.noteRedeemCheck(log, err)
	}
}

// settlePendingIncidents releases stored incidents on this chain once every
// endpoint shows, at the final height, the registration canonical, closed, and
// followed by an administrator unhalt. A signer that was not running during
// that halt would otherwise wait for the next one.
func (eN *evmNetwork) settlePendingIncidents() {
	if eN.state.RedeemHaltGeneration() == 0 {
		return
	}
	defer func() {
		if err := eN.state.ReleaseResolvedRedeemHalt(); err != nil {
			eN.logger.Warnf("Cannot tell whether a redeem incident is still pending: %v", err)
		}
	}()
	pending, err := eN.state.PendingRedeemIncidents()
	if err != nil {
		eN.logger.Warnf("Cannot list pending redeem incidents for chain %d: %v", eN.ChainId(), err)
		return
	}
	for _, incident := range pending {
		if incident.ChainID != eN.ChainId() {
			continue
		}
		log := incident.Log
		if _, err := eN.verifyRedeemLog(log, eN.ConfirmationsToFinality()); err != nil {
			eN.logger.Debugf("Pending redeem incident on chain %d, transaction %s, log %d is not final on every endpoint: %v", eN.ChainId(), log.TxHash, log.Index, err)
			continue
		}
		settled, err := eN.redeemSettled(incident)
		if err != nil {
			eN.logger.Warnf("Cannot read the contract state for the pending redeem incident on chain %d, transaction %s, log %d: %v", eN.ChainId(), log.TxHash, log.Index, err)
			continue
		}
		if !settled {
			continue
		}
		if err := eN.state.ArchiveSettledRedeem(incident); err != nil {
			eN.logger.Errorf("Cannot archive the settled redeem incident on chain %d, transaction %s, log %d: %v", eN.ChainId(), log.TxHash, log.Index, err)
			continue
		}
		eN.logger.Warnf("Released the redeem incident on chain %d, block %d, transaction %s, log %d: the redeem is closed and the bridge was unhalted after it",
			eN.ChainId(), log.BlockNumber, log.TxHash, log.Index)
	}
}

// watchRedeems rechecks held registrations and, while a halt is pending, looks
// for stored incidents the chain has since settled. It ends with the network
// or when stop is closed.
func (eN *evmNetwork) watchRedeems(stop <-chan struct{}) {
	interval := eN.heldRetryEvery
	if interval <= 0 {
		interval = heldRedeemRetryInterval
	}
	for round := 1; ; round++ {
		timer := time.NewTimer(interval)
		select {
		case <-eN.done:
			timer.Stop()
			return
		case <-stop:
			timer.Stop()
			return
		case <-timer.C:
		}
		eN.recheckHeldRedeems()
		if round%settleIncidentsEvery == 0 {
			eN.settlePendingIncidents()
		}
	}
}

// withRedeemWatch runs fn beside the recheck worker and returns once both
// have ended.
func (eN *evmNetwork) withRedeemWatch(fn func()) {
	stop, watching := make(chan struct{}), make(chan struct{})
	go func() { defer close(watching); eN.watchRedeems(stop) }()
	defer func() { close(stop); <-watching }()
	fn()
}
