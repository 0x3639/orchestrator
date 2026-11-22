package common

import (
	"context"
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
	Health                 *snapshot.Store
	state                  *uint8
	stateSemaphore         *semaphore.Weighted
	redeemIncidents        RedeemIncidentStore
	redeemHaltPending      atomic.Uint64
	redeemWriteFailed      bool
	redeemGeneration       uint64 // guarded by stateSemaphore
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
	resignNetworkClass      uint32
	resignNetworkChainId    uint32
}

func NewGlobalState(state *uint8) *GlobalState {
	return &GlobalState{
		state:                    state,
		frontierMomentumHeight:   0,
		stateSemaphore:           semaphore.NewWeighted(1),
		frontierMomSemaphore:     semaphore.NewWeighted(1),
		isAdministratorActive:    false,
		tokensMap:                make(map[uint32]map[string]string),
		isAffiliateProgramActive: make(map[uint32]map[string]bool),
		affiliateStartingHeight:  make(map[uint32]uint64),
	}
}

func (gs *GlobalState) SetState(newState uint8) error {
	err := gs.stateSemaphore.Acquire(context.Background(), 1)
	if err != nil {
		return err
	}
	defer gs.stateSemaphore.Release(1)
	if gs.RedeemHaltGeneration() != 0 && newState != EmergencyState {
		return nil // Only the completed halt procedure can release the incident.
	}
	gs.setStateLocked(newState)
	return nil
}

func (gs *GlobalState) setStateLocked(newState uint8) {
	GlobalLogger.Infof("Old state: %s, New state: %s\n", StateToText(*gs.state), StateToText(newState))
	*gs.state = newState
	gs.Health.SetBridgeState(StateToText(newState))
}

func (gs *GlobalState) GetState() (uint8, error) {
	err := gs.stateSemaphore.Acquire(context.Background(), 1)
	if err != nil {
		return 0, err
	}
	defer gs.stateSemaphore.Release(1)
	return *gs.state, nil
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
		gs.isAffiliateProgramActive[id] = flags
		gs.affiliateStartingHeight[id] = values.StartingHeight
		GlobalLogger.Infof("Updated affiliate program for network %d at starting height %d", id, values.StartingHeight)
	}
}

func (gs *GlobalState) GetIsAffiliateProgramActive(id uint32, token string) bool {
	gs.affiliateMu.RLock()
	defer gs.affiliateMu.RUnlock()
	return gs.isAffiliateProgramActive[id][strings.ToLower(token)]
}

func (gs *GlobalState) SetAffiliateStartingHeight(id uint32, height uint64) {
	gs.affiliateMu.Lock()
	defer gs.affiliateMu.Unlock()
	gs.affiliateStartingHeight[id] = height
}

func (gs *GlobalState) GetAffiliateStartingHeight(id uint32) *big.Int {
	gs.affiliateMu.RLock()
	defer gs.affiliateMu.RUnlock()
	return new(big.Int).SetUint64(gs.affiliateStartingHeight[id])
}

// Wrapped and native token flags govern distinct sides of the bridge.
type AffiliatePolicy byte

const (
	SplitAffiliate AffiliatePolicy = 1 << iota
	VerifyAffiliate
)

func (gs *GlobalState) AffiliatePolicyAt(id uint32, token string, height uint64) AffiliatePolicy {
	gs.affiliateMu.RLock()
	defer gs.affiliateMu.RUnlock()
	if start := gs.affiliateStartingHeight[id]; start == 0 || height < start {
		return 0
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
	return policy
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
