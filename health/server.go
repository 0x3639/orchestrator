package health

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/db/manager"
	"orchestrator/metadata"
	"orchestrator/network"
	"orchestrator/tss"
	"runtime"
	"sort"
	"strconv"
	"time"

	"golang.org/x/crypto/sha3"
	"golang.org/x/time/rate"
)

const (
	// Health requests are tiny JSON objects such as {"method":"getStatus","params":[]}.
	// Anything larger is rejected before it is decoded.
	maxRequestBodyBytes = 4 * 1024
	maxHeaderBytes      = 8 * 1024

	readHeaderTimeout = 5 * time.Second
	readTimeout       = 10 * time.Second
	writeTimeout      = 15 * time.Second
	idleTimeout       = 30 * time.Second
)

// HealthRPCHandler struct
type Handler struct {
	state           *common.GlobalState
	networksManager *network.NetworksManager
	dbManager       *manager.Manager
	tssManager      *tss.TssManager
	identity        Identity
	// limiter is the aggregate safety cap shared by every caller.
	limiter *rate.Limiter
	// clientLimiters holds the per-source limits; nil disables per-client limiting.
	clientLimiters *clientLimiters
	StatusCache    *StatusResults
}

func NewHealthRpcHandler(networksManager *network.NetworksManager, dbManager *manager.Manager, state *common.GlobalState, healthConfig config.HealthRpcConfig) (*Handler, error) {
	handler := &Handler{
		state:           state,
		networksManager: networksManager,
		dbManager:       dbManager,
		tssManager:      nil,
		limiter:         rate.NewLimiter(rate.Limit(healthConfig.ResponsesPerSecond), healthConfig.Burst),
		StatusCache:     NewCachedStatusResults(healthConfig.CachedResponseDelay),
	}
	if healthConfig.PerClientResponsesPerSecond > 0 {
		handler.clientLimiters = newClientLimiters(rate.Limit(healthConfig.PerClientResponsesPerSecond), healthConfig.PerClientBurst)
	}
	return handler, nil
}

// ListenAddress returns the host:port the health server should bind to.
// An empty Address falls back to loopback so the signer is never exposed on
// every interface by accident.
func ListenAddress(healthConfig config.HealthRpcConfig) string {
	address := healthConfig.Address
	if address == "" {
		address = config.DefaultHealthRpcAddress
	}
	return net.JoinHostPort(address, strconv.Itoa(healthConfig.Port))
}

// NewServer builds an http.Server with strict connection deadlines so slow or
// idle clients cannot pin sockets and goroutines indefinitely.
func NewServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}
}

func (s *Handler) SetTssManager(tssManager *tss.TssManager) {
	s.tssManager = tssManager
}

func (s *Handler) SetIdentity(identity Identity) {
	s.identity = identity
}

// GetStatus method
func (s *Handler) GetStatus(params []interface{}) (interface{}, error) {
	if len(params) != 0 {
		return nil, fmt.Errorf("this method does not accept parameters")
	}

	cachedStatus := s.StatusCache.GetStatusResult()
	if cachedStatus != nil {
		return cachedStatus, nil
	}

	state, err := s.state.GetState()
	if err != nil {
		return nil, err
	}

	frontierMomentum, err := s.state.GetFrontierMomentum()
	if err != nil {
		return nil, err
	}

	wrapsData := make(map[uint32][]byte)
	wrapsLen := make(map[uint32]uint32)
	unwrapsData := make(map[uint32][]byte)
	unwrapsLen := make(map[uint32]uint32)
	for _, evmNetwork := range s.networksManager.Networks() {
		wrapsData[evmNetwork.ChainId()] = make([]byte, 0)
		wrapsLen[evmNetwork.ChainId()] = 0
		unwrapsData[evmNetwork.ChainId()] = make([]byte, 0)
		unwrapsLen[evmNetwork.ChainId()] = 0
	}

	wraps, errWraps := s.networksManager.GetUnsignedWrapRequests()
	if errWraps != nil {
		return nil, err
	} else if len(wraps) != 0 {
		sort.Slice(wraps, func(i, j int) bool {
			return wraps[i].Id.String() < wraps[j].Id.String()
		})
		for _, wrap := range wraps {
			wrapsData[wrap.ChainId] = append(wrapsData[wrap.ChainId], wrap.Id.Bytes()...)
			wrapsLen[wrap.ChainId] += 1
		}
	}

	unwraps, err := s.networksManager.GetUnsignedUnwrapRequests()
	if err != nil {
		return nil, err
	} else if len(unwraps) != 0 {
		sort.Slice(unwraps, func(i, j int) bool {
			if unwraps[i].TransactionHash.String() == unwraps[j].TransactionHash.String() {
				return unwraps[i].LogIndex < unwraps[j].LogIndex
			}
			return unwraps[i].TransactionHash.String() < unwraps[j].TransactionHash.String()
		})
		for _, unwrap := range unwraps {
			unwrapsData[unwrap.ChainId] = append(unwrapsData[unwrap.ChainId], unwrap.TransactionHash.Bytes()...)
			logNumberBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(logNumberBytes, unwrap.LogIndex)
			unwrapsData[unwrap.ChainId] = append(unwrapsData[unwrap.ChainId], logNumberBytes...)
			unwrapsLen[unwrap.ChainId] += 1
		}
	}

	networksStatus := make(map[string]StatusNetworkInfo)
	for _, evmNetwork := range s.networksManager.Networks() {
		lastUpdateHeight, err := s.dbManager.EvmStorage(evmNetwork.ChainId()).GetLastUpdateHeight()
		if err != nil {
			return nil, err
		}

		hasher := sha3.NewLegacyKeccak256()
		digestWrapHex := "00"
		if len(wrapsData[evmNetwork.ChainId()]) > 0 {
			hasher.Write(wrapsData[evmNetwork.ChainId()])
			digestWrap := hasher.Sum(nil)
			digestWrapHex = hex.EncodeToString(digestWrap)
		}

		hasher = sha3.NewLegacyKeccak256()
		digestUnwrapHex := "00"
		if len(unwrapsData[evmNetwork.ChainId()]) > 0 {
			hasher.Write(unwrapsData[evmNetwork.ChainId()])
			digestUnwrap := hasher.Sum(nil)
			digestUnwrapHex = hex.EncodeToString(digestUnwrap)
		}

		networksStatus[evmNetwork.NetworkName()] = StatusNetworkInfo{
			ChainId:                  evmNetwork.ChainId(),
			NetworkClass:             evmNetwork.NetworkClass(),
			ContractAddress:          evmNetwork.ContractAddress().String(),
			ContractDeploymentHeight: evmNetwork.ContractDeploymentHeight(),
			EstimatedBlockTime:       uint64(evmNetwork.EstimatedBlockTime().Seconds()),
			ConfirmationsToFinality:  evmNetwork.ConfirmationsToFinality(),
			LatestUpdateHeight:       lastUpdateHeight,
			NetworkSigningStatus: NetworkSigningStatus{
				WrapsTSign:    wrapsLen[evmNetwork.ChainId()],
				WrapsHash:     digestWrapHex,
				UnwrapsToSign: unwrapsLen[evmNetwork.ChainId()],
				UnwrapsHash:   digestUnwrapHex,
			},
		}
	}

	clear(wrapsData)
	clear(wrapsLen)
	clear(unwrapsData)
	clear(unwrapsLen)

	peersLen := uint32(0)
	peers := make([]string, 0)
	addressBook := s.tssManager.ExportPeersStore()
	for k, _ := range addressBook {
		if s.identity.TssPeerId == k.String() {
			continue
		}
		peersLen += 1
		peers = append(peers, k.String())
	}

	status := Status{
		State:            state,
		StateName:        common.StateToText(state),
		FrontierMomentum: frontierMomentum,
		Networks:         networksStatus,
		PeersLen:         peersLen,
		Peers:            peers,
	}

	s.StatusCache.SetStatusResult(status)
	return status, nil
}

func (s *Handler) GetBuildInfo(params []interface{}) (interface{}, error) {
	if len(params) != 0 {
		return nil, fmt.Errorf("this method does not accept parameters")
	}

	buildInfo := BuildInfo{
		Version:   metadata.Version,
		GitCommit: metadata.GitCommit,
		GoVersion: runtime.Version(),
	}
	return buildInfo, nil
}

func (s *Handler) GetIdentity(params []interface{}) (interface{}, error) {
	if len(params) != 0 {
		return nil, fmt.Errorf("this method does not accept parameters")
	}

	return s.identity, nil
}

// ServeHTTP validates the request shape before charging the rate limiter so
// that malformed or trivial requests cannot consume the budget reserved for
// legitimate monitoring probes.
func (s *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "only POST requests are supported")
		return
	}
	if !hasJSONContentType(r) {
		writeError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return
	}
	if r.ContentLength > maxRequestBodyBytes {
		writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("request body must not exceed %d bytes", maxRequestBodyBytes))
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	decoder := json.NewDecoder(r.Body)

	var req Request
	if err := decoder.Decode(&req); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("request body must not exceed %d bytes", maxRequestBodyBytes))
			return
		}
		writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request: %v", err))
		return
	}
	// Exactly one JSON value is allowed per request.
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "Invalid request: body must contain exactly one JSON value")
		return
	}

	var method func([]interface{}) (interface{}, error)
	switch req.Method {
	case "getStatus":
		method = s.GetStatus
	case "getBuildInfo":
		method = s.GetBuildInfo
	case "getIdentity":
		method = s.GetIdentity
	default:
		writeError(w, http.StatusNotFound, fmt.Sprintf("Method %s not found", req.Method))
		return
	}

	// Every exposed method takes no parameters; reject bad shapes before
	// charging the quota so they cannot be used to starve real probes.
	if len(req.Params) != 0 {
		writeError(w, http.StatusBadRequest, "Invalid request: this method does not accept parameters")
		return
	}

	// Only well-formed requests for known methods are charged against the quota.
	if !s.allow(r) {
		writeError(w, http.StatusTooManyRequests, "Too many requests, retry later")
		return
	}

	result, err := method(req.Params)

	var res Response
	if err != nil {
		res.Error = err.Error()
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		res.Result = result
		w.WriteHeader(http.StatusOK)
	}
	json.NewEncoder(w).Encode(res)
}

// allow charges the per-client bucket first so a throttled source does not
// drain the shared budget, then the aggregate cap.
func (s *Handler) allow(r *http.Request) bool {
	if s.clientLimiters != nil && !s.clientLimiters.allow(clientKey(r)) {
		return false
	}
	return s.limiter.Allow()
}

// clientKey identifies the request source by the transport-level remote IP.
// Forwarding headers are deliberately ignored because they are client-controlled.
func clientKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func hasJSONContentType(r *http.Request) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return false
	}
	return mediaType == "application/json"
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(Response{Error: message})
}
