package common

import (
	"context"
	"errors"
	"maps"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/zenon-network/go-zenon/common/types"
	"golang.org/x/sync/semaphore"
	"math/big"

	"orchestrator/health/snapshot"
)

const (
	LiveState      uint8 = 0
	KeyGenState    uint8 = 1
	HaltedState    uint8 = 2
	EmergencyState uint8 = 3
	ReSignState    uint8 = 4
)

// CeremonyWindow identifies the chain interval even when governance resizes windows.
type CeremonyWindow struct {
	Number uint64
	Size   uint64
}

type GlobalState struct {
	// Health is assigned once before any network workers start.
	Health                *snapshot.Store
	state                 *uint8
	stateMu               sync.Mutex
	persistEmergency      func(uint8) error
	emergencyWritePending bool
	redeemIncidents       RedeemIncidentStore
	acknowledgedRedeems   map[RedeemLogID]bool // fixed before RestoreRedeemIncidents
	redeemHaltPending     atomic.Uint64
	redeemWriteFailed     bool
	redeemGeneration      uint64 // guarded by stateMu
	restoredGeneration    uint64 // the generation restored at startup; guarded by stateMu
	// Held for reading while a halt transaction is submitted, and for
	// writing, before stateMu, by whatever can release a redeem halt.
	haltSendMu sync.RWMutex
	// Held for reading while a transfer is published, and for writing, before
	// haltSendMu and stateMu, by every change that can take the signer out of
	// LiveState. No transfer is published once the signer has left it.
	transferSendMu sync.RWMutex
	// Closed once startup has read the bridge account to its tip and every
	// EVM network has finished its startup scan.
	startupDone chan struct{}
	startupOnce sync.Once
	// Closed once the node has applied what it reads from the bridge when
	// it starts.
	nodeReady     chan struct{}
	nodeReadyOnce sync.Once
	// Set when the node goes on running although it has read, as history, a
	// change to what it took from the bridge when it started.
	settingsMu             sync.RWMutex
	settingsStale          bool
	frontierMomentumHeight uint64
	frontierMomSemaphore   *semaphore.Weighted
	lastCeremony           CeremonyWindow
	isAdministratorActive  bool
	tokensMap              map[uint32]map[string]string
	affiliateMu            sync.RWMutex
	// [chainId][token] -> bool
	isAffiliateProgramActive map[uint32]map[string]bool
	// [chainId] -> height
	affiliateStartingHeight map[uint32]uint64
	// Counts the changes to the affiliate settings and the token map, which
	// decide an event's affiliate policy.
	affiliateGeneration uint64
	// The networks of the affiliate program SetIsAffiliateProgram applied
	// last.
	appliedAffiliate     map[uint32]AffiliateNetwork
	resignNetworkClass   uint32
	resignNetworkChainId uint32
}

func NewGlobalState(state *uint8) *GlobalState {
	return NewPersistentGlobalState(state, nil)
}

// NewPersistentGlobalState orders emergency persistence and receive processing.
func NewPersistentGlobalState(state *uint8, persistEmergency func(uint8) error) *GlobalState {
	return &GlobalState{
		persistEmergency:         persistEmergency,
		startupDone:              make(chan struct{}),
		nodeReady:                make(chan struct{}),
		state:                    state,
		frontierMomentumHeight:   0,
		frontierMomSemaphore:     semaphore.NewWeighted(1),
		isAdministratorActive:    false,
		tokensMap:                make(map[uint32]map[string]string),
		isAffiliateProgramActive: make(map[uint32]map[string]bool),
		affiliateStartingHeight:  make(map[uint32]uint64),
	}
}

func (gs *GlobalState) SetState(newState uint8) error {
	unlock := gs.lockTransfersFor(newState)
	defer unlock()
	gs.stateMu.Lock()
	defer gs.stateMu.Unlock()
	if gs.RedeemHaltGeneration() != 0 && newState != EmergencyState {
		return nil // The incident owns the halt until all explicit flags are set.
	}
	return gs.setStateLocked(newState)
}

// SetStateIf moves to newState only from the expected state and reports
// whether it did. The comparison and the change happen under one lock, so a
// state another goroutine set in between is never overwritten.
func (gs *GlobalState) SetStateIf(expected, newState uint8) (bool, error) {
	unlock := gs.lockTransfersFor(newState)
	defer unlock()
	gs.stateMu.Lock()
	defer gs.stateMu.Unlock()
	if gs.RedeemHaltGeneration() != 0 && newState != EmergencyState {
		return false, nil // The incident owns the halt until all explicit flags are set.
	}
	if *gs.state != expected {
		return false, nil
	}
	err := gs.setStateLocked(newState)
	return expected != newState && *gs.state == newState, err
}

func (gs *GlobalState) setStateLocked(newState uint8) error {
	oldEffective := gs.effectiveStateLocked()
	previous := *gs.state
	if previous == EmergencyState && newState != EmergencyState && newState != HaltedState {
		return errors.New("emergency halt is still pending")
	}
	// A failed write must stop signing in this process and prevent the caller
	// from checkpointing the receive. A repeated emergency retries persistence.
	var persistErr error
	if gs.persistEmergency != nil && (newState == EmergencyState || previous == EmergencyState) {
		persistErr = gs.persistEmergency(newState)

		if persistErr != nil && newState != EmergencyState {
			return persistErr
		}
	}
	gs.emergencyWritePending = persistErr != nil
	*gs.state = newState
	gs.publishStateLocked(oldEffective)
	return persistErr
}

func (gs *GlobalState) effectiveStateLocked() uint8 {
	if gs.RedeemHaltGeneration() != 0 {
		return EmergencyState
	}
	return *gs.state
}

func (gs *GlobalState) publishStateLocked(oldState uint8) {
	current := StateToText(gs.effectiveStateLocked())
	GlobalLogger.Infof("Old state: %s, New state: %s", StateToText(oldState), current)
	gs.Health.SetBridgeState(current)
}

// HaltSigning moves a signer in LiveState, KeyGenState or ReSignState to
// HaltedState, as a halt the bridge listener reads does, and reports whether
// it did. A signer in EmergencyState or HaltedState stays where it is.
func (gs *GlobalState) HaltSigning() (bool, error) {
	gs.transferSendMu.Lock()
	defer gs.transferSendMu.Unlock()
	gs.stateMu.Lock()
	defer gs.stateMu.Unlock()
	switch gs.effectiveStateLocked() {
	case LiveState, KeyGenState, ReSignState:
		return true, gs.setStateLocked(HaltedState)
	}
	return false, nil
}

// A receive retried after a storage error may by then be historical. Even
// if it no longer triggers a live transition, its checkpoint must wait.
func (gs *GlobalState) FlushPendingEmergency() error {
	gs.stateMu.Lock()
	defer gs.stateMu.Unlock()
	if gs.emergencyWritePending {
		if err := gs.persistEmergency(EmergencyState); err != nil {
			return err
		}
		gs.emergencyWritePending = false
	}
	return nil
}

// SaveConfig serializes full config saves with emergency transitions. The
// callback reads the config containing the state pointer supplied at startup.
func (gs *GlobalState) SaveConfig(save func() error) error {
	gs.stateMu.Lock()
	defer gs.stateMu.Unlock()
	return save()
}

// WhileSigningTransfers runs do unless the signer is in EmergencyState or
// HaltedState, and reports whether it ran. The state lock is held throughout,
// so no transition can fall between the check and do. do must not touch the
// state.
func (gs *GlobalState) WhileSigningTransfers(do func()) bool {
	gs.stateMu.Lock()
	defer gs.stateMu.Unlock()
	if state := gs.effectiveStateLocked(); state == EmergencyState || state == HaltedState {
		return false
	}
	do()
	return true
}

// CompleteStartup records that the bridge account has been read to its tip
// and every EVM network has finished its startup scan. Until then the signer
// signs no transfer; see StartupComplete.
func (gs *GlobalState) CompleteStartup() {
	gs.startupOnce.Do(func() { close(gs.startupDone) })
}

// StartupComplete reports whether CompleteStartup has been called. It never
// turns false again in the same process.
func (gs *GlobalState) StartupComplete() bool {
	select {
	case <-gs.startupDone:
		return true
	default:
		return false
	}
}

// StartupDone is closed by CompleteStartup.
func (gs *GlobalState) StartupDone() <-chan struct{} {
	return gs.startupDone
}

// UnlessInEmergency runs do unless the signer is in EmergencyState, and
// reports whether it ran. The state lock is held throughout, as in
// WhileSigningTransfers. do must not touch the state.
func (gs *GlobalState) UnlessInEmergency(do func()) bool {
	gs.stateMu.Lock()
	defer gs.stateMu.Unlock()
	if gs.effectiveStateLocked() == EmergencyState {
		return false
	}
	do()
	return true
}

// MarkNodeReady records that the node has applied the settings and state it
// reads from the bridge when it starts. A startup that completes in the
// background waits for it before it looks at the halt flags.
func (gs *GlobalState) MarkNodeReady() {
	gs.nodeReadyOnce.Do(func() { close(gs.nodeReady) })
}

// NodeReady is closed by MarkNodeReady.
func (gs *GlobalState) NodeReady() <-chan struct{} {
	return gs.nodeReady
}

// MarkSettingsStale records that the settings this process holds may be
// older than the bridge's, for as long as the process runs.
func (gs *GlobalState) MarkSettingsStale() {
	gs.settingsMu.Lock()
	defer gs.settingsMu.Unlock()
	gs.settingsStale = true
}

// SettingsStale reports whether MarkSettingsStale has been called.
func (gs *GlobalState) SettingsStale() bool {
	gs.settingsMu.RLock()
	defer gs.settingsMu.RUnlock()
	return gs.settingsStale
}

// WhileSettingsCurrent runs do unless MarkSettingsStale has been called, and
// reports whether it ran. A call to MarkSettingsStale waits for do, so what do
// writes was decided with settings nothing had marked stale.
func (gs *GlobalState) WhileSettingsCurrent(do func() error) (bool, error) {
	gs.settingsMu.RLock()
	defer gs.settingsMu.RUnlock()
	if gs.settingsStale {
		return false, nil
	}
	return true, do()
}

// lockTransfersFor takes transferSendMu when a move to newState can take the
// signer out of LiveState, and returns what releases it.
func (gs *GlobalState) lockTransfersFor(newState uint8) func() {
	if newState == LiveState {
		return func() {}
	}
	gs.transferSendMu.Lock()
	return gs.transferSendMu.Unlock
}

// WhileLive runs send, which publishes a transfer, if the signer is in
// LiveState, and reports whether it ran. No change of state takes the signer
// out of LiveState while send runs, so no transfer is published after the
// signer left it. send must not touch the state, and has to be bounded in
// time: such a change waits for it.
func (gs *GlobalState) WhileLive(send func() error) (bool, error) {
	gs.transferSendMu.RLock()
	defer gs.transferSendMu.RUnlock()
	if state, err := gs.GetState(); err != nil || state != LiveState {
		return false, err
	}
	return true, send()
}

func (gs *GlobalState) GetState() (uint8, error) {
	gs.stateMu.Lock()
	defer gs.stateMu.Unlock()
	return gs.effectiveStateLocked(), nil
}

func (gs *GlobalState) SetFrontierMomentum(frMom uint64) error {
	err := gs.frontierMomSemaphore.Acquire(context.Background(), 1)
	if err != nil {
		return err
	}
	gs.frontierMomentumHeight = frMom
	gs.Health.UpdateZenon(func(z *snapshot.Zenon) { z.FrontierMomentum = snapshot.Measure(frMom) })
	gs.frontierMomSemaphore.Release(1)
	return nil
}

func (gs *GlobalState) GetFrontierMomentum() (uint64, error) {
	err := gs.frontierMomSemaphore.Acquire(context.Background(), 1)
	if err != nil {
		return 0, err
	}
	defer gs.frontierMomSemaphore.Release(1)
	return gs.frontierMomentumHeight, nil
}

// BeginCeremony is called by the signing loop before attempting either round.
func (gs *GlobalState) BeginCeremony(window CeremonyWindow) bool {
	if window.Size == 0 || window == gs.lastCeremony || (gs.lastCeremony.Size == 0 && window.Number == 0) {
		return false
	}
	gs.lastCeremony = window
	return true
}

func (gs *GlobalState) SetTokensMap(chainId uint32, zts, token string) {
	gs.affiliateMu.Lock()
	defer gs.affiliateMu.Unlock()
	token = strings.ToLower(token)
	if _, found := gs.tokensMap[chainId]; !found {
		gs.tokensMap[chainId] = make(map[string]string)
	}
	if gs.tokensMap[chainId][zts] != token || gs.tokensMap[chainId][token] != zts {
		gs.affiliateGeneration++
	}

	gs.tokensMap[chainId][zts] = token
	gs.tokensMap[chainId][token] = zts
}

func (gs *GlobalState) GetTokensMap(chainId uint32, ztsOrToken string) string {
	gs.affiliateMu.RLock()
	defer gs.affiliateMu.RUnlock()
	if value, found := gs.tokensMap[chainId][strings.ToLower(ztsOrToken)]; found {
		return value
	}
	return ""
}

func (gs *GlobalState) SetIsAdministratorActive(value bool) {
	gs.isAdministratorActive = value
	GlobalLogger.Infof("SetAdministratorActive to : %t", value)
}

func (gs *GlobalState) GetIsAdministratorActive() bool {
	return gs.isAdministratorActive
}

func (gs *GlobalState) SetIsAffiliateProgram(program AffiliateProgram) {
	gs.affiliateMu.Lock()
	defer gs.affiliateMu.Unlock()
	// An update replaces the listed chains. Unlisted chains keep their settings.
	for id, values := range program.Networks {
		znn, qsr := types.ZnnTokenStandard.String(), types.QsrTokenStandard.String()
		flags := map[string]bool{znn: values.ZNN, qsr: values.QSR}
		if addr := gs.tokensMap[id][znn]; addr != "" {
			flags[addr] = values.WZNN
		}
		if addr := gs.tokensMap[id][qsr]; addr != "" {
			flags[addr] = values.WQSR
		}
		if !maps.Equal(gs.isAffiliateProgramActive[id], flags) || gs.affiliateStartingHeight[id] != values.StartingHeight {
			gs.affiliateGeneration++
		}
		gs.isAffiliateProgramActive[id] = flags
		gs.affiliateStartingHeight[id] = values.StartingHeight
		GlobalLogger.Infof("Updated affiliate program for network %d at starting height %d", id, values.StartingHeight)
	}
	gs.appliedAffiliate = maps.Clone(program.Networks)
}

func (gs *GlobalState) GetIsAffiliateProgramActive(id uint32, token string) bool {
	gs.affiliateMu.RLock()
	defer gs.affiliateMu.RUnlock()
	return gs.isAffiliateProgramActive[id][strings.ToLower(token)]
}

func (gs *GlobalState) SetAffiliateStartingHeight(id uint32, height uint64) {
	gs.affiliateMu.Lock()
	defer gs.affiliateMu.Unlock()
	if gs.affiliateStartingHeight[id] != height {
		gs.affiliateGeneration++
	}
	gs.affiliateStartingHeight[id] = height
}

func (gs *GlobalState) GetAffiliateStartingHeight(id uint32) *big.Int {
	gs.affiliateMu.RLock()
	defer gs.affiliateMu.RUnlock()
	return new(big.Int).SetUint64(gs.affiliateStartingHeight[id])
}

// AffiliatePolicyUnder returns the policy AffiliatePolicyAt would return once
// SetIsAffiliateProgram had applied program, and whether program establishes
// it. A program that does not list the chain leaves the chain's settings as
// the updates before it set them. Those are the node's own only when program
// is the one the node applied last; otherwise the policy is not known.
func (gs *GlobalState) AffiliatePolicyUnder(program AffiliateProgram, id uint32, token string, height uint64) (AffiliatePolicy, bool) {
	values, listed := program.Networks[id]
	if !listed {
		gs.affiliateMu.RLock()
		applied := maps.Equal(gs.appliedAffiliate, program.Networks)
		gs.affiliateMu.RUnlock()
		if !applied {
			return 0, false
		}
		return gs.AffiliatePolicyAt(id, token, height), true
	}
	gs.affiliateMu.RLock()
	defer gs.affiliateMu.RUnlock()
	if values.StartingHeight == 0 || height < values.StartingHeight {
		return 0, true
	}
	znn, qsr := types.ZnnTokenStandard.String(), types.QsrTokenStandard.String()
	flags := map[string]bool{znn: values.ZNN, qsr: values.QSR}
	if addr := gs.tokensMap[id][znn]; addr != "" {
		flags[addr] = values.WZNN
	}
	if addr := gs.tokensMap[id][qsr]; addr != "" {
		flags[addr] = values.WQSR
	}
	token = strings.ToLower(token)
	var policy AffiliatePolicy
	if flags[token] {
		policy |= SplitAffiliate
	}
	if flags[gs.tokensMap[id][token]] {
		policy |= VerifyAffiliate
	}
	return policy, true
}

// Wrapped and native token flags govern distinct sides of the bridge.
type AffiliatePolicy byte

const (
	SplitAffiliate AffiliatePolicy = 1 << iota
	VerifyAffiliate
)

func (gs *GlobalState) AffiliatePolicyAt(id uint32, token string, height uint64) AffiliatePolicy {
	policy, _ := gs.AffiliatePolicyWithGeneration(id, token, height)
	return policy
}

// AffiliatePolicyWithGeneration returns the policy AffiliatePolicyAt returns,
// with the generation of the settings it was read from. See
// WhileAffiliateSettingsAre.
func (gs *GlobalState) AffiliatePolicyWithGeneration(id uint32, token string, height uint64) (AffiliatePolicy, uint64) {
	gs.affiliateMu.RLock()
	defer gs.affiliateMu.RUnlock()
	if start := gs.affiliateStartingHeight[id]; start == 0 || height < start {
		return 0, gs.affiliateGeneration
	}
	token = strings.ToLower(token)
	flags := gs.isAffiliateProgramActive[id]
	var policy AffiliatePolicy
	if flags[token] {
		policy |= SplitAffiliate
	}
	if flags[gs.tokensMap[id][token]] {
		policy |= VerifyAffiliate
	}
	return policy, gs.affiliateGeneration
}

// WhileAffiliateSettingsAre runs do unless the affiliate settings or the
// token map changed since generation, and reports whether it ran. A change
// waits for do, so what do writes was decided with the settings in force.
func (gs *GlobalState) WhileAffiliateSettingsAre(generation uint64, do func() error) (bool, error) {
	gs.affiliateMu.RLock()
	defer gs.affiliateMu.RUnlock()
	if gs.affiliateGeneration != generation {
		return false, nil
	}
	return true, do()
}

func (gs *GlobalState) GetResignNetwork() (uint32, uint32) {
	return gs.resignNetworkClass, gs.resignNetworkChainId
}

func (gs *GlobalState) SetResignNetwork(networkClass, chainId uint32) {
	gs.resignNetworkClass = networkClass
	gs.resignNetworkChainId = chainId
}

// HealthStore returns the optional observer for callers that run without node state.
func (gs *GlobalState) HealthStore() *snapshot.Store {
	if gs == nil {
		return nil
	}
	return gs.Health
}
