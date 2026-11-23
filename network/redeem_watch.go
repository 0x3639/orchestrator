package network

import (
	"fmt"
	"time"

	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"orchestrator/common"
)

const (
	// heldRedeemRetryInterval paces rechecks of registrations whose check gave
	// no answer. It is short against the shortest configured time between a
	// registration and its payout.
	heldRedeemRetryInterval = 3 * time.Second
	// heldRedeemReportEvery is the time between the repeated lines that tell
	// the operator a registration is still unchecked.
	heldRedeemReportEvery = time.Minute
	// pausedScanRetryRounds is how many recheck intervals pass before a range
	// scan that stopped for a pending halt is tried again. A released halt
	// then resumes the scan long before the next ordinary refresh.
	pausedScanRetryRounds = 5
)

// A held registration is one observed log: where it was reported and what it
// said. Two observations that differ in either are kept apart, so an answer
// for one never stands in for the other.
type heldRedeemKey struct {
	height    uint64
	block, tx ecommon.Hash
	index     uint
	content   ecommon.Hash
}

type heldRedeem struct {
	log      etypes.Log
	since    time.Time
	reported time.Time
	attempts int
}

func heldKey(log etypes.Log) heldRedeemKey {
	// The topic count keeps topics and data apart in the digest.
	content := make([]byte, 0, 1+len(log.Topics)*ecommon.HashLength+len(log.Data))
	content = append(content, byte(len(log.Topics)))
	for _, topic := range log.Topics {
		content = append(content, topic[:]...)
	}
	content = append(content, log.Data...)
	return heldRedeemKey{height: log.BlockNumber, block: log.BlockHash, tx: log.TxHash, index: log.Index, content: crypto.Keccak256Hash(content)}
}

func (eN *evmNetwork) redeemRetryInterval() time.Duration {
	if eN.heldRetryEvery > 0 {
		return eN.heldRetryEvery
	}
	return heldRedeemRetryInterval
}

// checkLiveRedeem runs the live check of a young registration. A check that
// gives no answer is not dropped: the log is held and rechecked by watchRedeems
// until it matches, is reported, or its block is replaced.
func (eN *evmNetwork) checkLiveRedeem(log etypes.Log) error {
	err := eN.registerRedeem(log, true)
	eN.noteRedeemCheck(log, err, true)
	return err
}

// examineRegistration checks a registration the range scan met above the
// redeem checkpoint limit, and holds it when the check gives no answer.
func (eN *evmNetwork) examineRegistration(log etypes.Log, young bool) {
	if log.Removed || log.Address != *eN.ContractAddress() {
		return // InterpretLog rejects these once the finalized scan reaches them.
	}
	eN.noteRedeemCheck(log, eN.registerRedeem(log, young), true)
}

// noteRedeemCheck records the outcome of a check. A first observation that
// got no answer starts a hold. A recheck passes hold=false: its result only
// counts for an entry that is still held, so a recheck that fails late cannot
// bring back an entry that was answered in the meantime.
func (eN *evmNetwork) noteRedeemCheck(log etypes.Log, err error, hold bool) {
	key := heldKey(log)
	eN.heldMu.Lock()
	defer eN.heldMu.Unlock()
	entry := eN.heldRedeems[key]
	if err == nil {
		if entry != nil {
			delete(eN.heldRedeems, key)
			eN.logger.Infof("Redeem registration on chain %d, block %d, transaction %s, log %d was checked after %d retries",
				eN.ChainId(), log.BlockNumber, log.TxHash, log.Index, entry.attempts)
		}
		return
	}
	if entry == nil {
		if !hold {
			return
		}
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

// stopping reports that the network, or the worker that owns stop, is ending.
// A nil stop never ends.
func (eN *evmNetwork) stopping(stop <-chan struct{}) bool {
	select {
	case <-eN.done:
		return true
	case <-stop:
		return true
	default:
		return false
	}
}

// recheckHeldRedeems repeats the check of every held registration. Once a
// registration is finality-deep it is checked as the finalized scan would
// check it, so the same evidence decides it on both paths.
func (eN *evmNetwork) recheckHeldRedeems(stop <-chan struct{}) {
	for _, log := range eN.heldRedeemLogs() {
		if eN.stopping(stop) {
			return
		}
		// The registration's age selects the rules, and the head gives the
		// age. Without a head at or above the registration, decide nothing
		// this round.
		head, err := eN.EvmRpc().BlockNumber()
		if err == nil && head < log.BlockNumber {
			err = fmt.Errorf("chain head %d is below the registration", head)
		}
		if err != nil {
			eN.noteRedeemCheck(log, fmt.Errorf("read chain head: %w", err), false)
			continue
		}
		depth := eN.ConfirmationsToFinality()
		final := head-log.BlockNumber >= depth
		err = eN.registerRedeem(log, !final)
		if err != nil && final && eN.redeemBlockReplaced(log, depth) {
			eN.heldMu.Lock()
			delete(eN.heldRedeems, heldKey(log))
			eN.heldMu.Unlock()
			eN.logger.Infof("Redeem registration on chain %d, transaction %s, log %d left the canonical chain at block %d and is no longer held",
				eN.ChainId(), log.TxHash, log.Index, log.BlockNumber)
			continue
		}
		eN.noteRedeemCheck(log, err, false)
	}
}

// redeemBlockReplaced reports that every endpoint has the chain depth blocks
// past the log's height and agrees that another block is canonical there. A
// block replaced before finality never reaches the finalized scan, so nothing
// else would release a registration held from it.
func (eN *evmNetwork) redeemBlockReplaced(log etypes.Log, depth uint64) bool {
	canonical, err := eN.EvmRpc().CanonicalHash(log.BlockNumber)
	if err != nil || canonical == log.BlockHash {
		return false
	}
	_, err = eN.EvmRpc().CanonicalHash(log.BlockNumber + depth)
	return err == nil
}

// settlePendingIncidents releases stored incidents of this chain, while a
// halt is pending, once every endpoint shows at the final height the
// registration canonical, closed, and followed by an administrator unhalt. A
// signer that was not running during that halt would otherwise wait for the
// next one. Until then the incident holds the bridge like any other.
//
// With untilPending the sweep ends at the first incident that stays pending.
// The scan uses that before the signing loop exists: once one incident needs
// the halt, checking the others must not delay it.
func (eN *evmNetwork) settlePendingIncidents(stop <-chan struct{}, untilPending bool) {
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
		if eN.stopping(stop) {
			return
		}
		if eN.settleIncident(incident) {
			continue
		}
		if untilPending {
			return
		}
	}
}

// settleIncident archives one stored incident if it is settled, and reports
// whether it did.
func (eN *evmNetwork) settleIncident(incident common.RedeemIncident) bool {
	log := incident.Log
	if _, err := eN.verifyRedeemLog(log, eN.ConfirmationsToFinality()); err != nil {
		eN.logger.Debugf("Pending redeem incident on chain %d, transaction %s, log %d is not final on every endpoint: %v", eN.ChainId(), log.TxHash, log.Index, err)
		return false
	}
	settled, err := eN.redeemSettled(incident)
	if err != nil {
		eN.logger.Warnf("The pending redeem incident on chain %d, transaction %s, log %d cannot be archived: %v", eN.ChainId(), log.TxHash, log.Index, err)
		return false
	}
	if !settled {
		return false
	}
	if err := eN.state.ArchiveSettledRedeem(incident); err != nil {
		eN.logger.Errorf("Cannot archive the settled redeem incident on chain %d, transaction %s, log %d: %v", eN.ChainId(), log.TxHash, log.Index, err)
		return false
	}
	eN.logger.Warnf("Released the redeem incident on chain %d, block %d, transaction %s, log %d: the redeem is closed and the bridge was unhalted after it",
		eN.ChainId(), log.BlockNumber, log.TxHash, log.Index)
	return true
}

// watchRedeems rechecks held registrations and, while a halt is pending, looks
// at the stored incidents. It ends with the network or when stop is closed.
func (eN *evmNetwork) watchRedeems(stop <-chan struct{}) {
	for {
		timer := time.NewTimer(eN.redeemRetryInterval())
		select {
		case <-eN.done:
			timer.Stop()
			return
		case <-stop:
			timer.Stop()
			return
		case <-timer.C:
		}
		eN.recheckHeldRedeems(stop)
		eN.settlePendingIncidents(stop, false)
	}
}

// withRedeemWatch runs fn beside the recheck worker and returns once both
// have ended. The worker stops between two checks, so the wait is at most one
// check long.
func (eN *evmNetwork) withRedeemWatch(fn func()) {
	stop, watching := make(chan struct{}), make(chan struct{})
	go func() { defer close(watching); eN.watchRedeems(stop) }()
	defer func() { close(stop); <-watching }()
	fn()
}
