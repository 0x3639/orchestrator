package node

import (
	gotss "github.com/HyperCore-Team/go-tss/common"
	"orchestrator/common"
	"orchestrator/common/config"
	"path"
)

var DefaultNodeConfig = config.Config{
	DataPath: common.DefaultDataDir(),
	// BNB Chain and Ethereum default to dRPC free-tier-safe sync settings:
	// FilterQuerySize 100 stays within dRPC's ~100-block eth_getLogs free-plan
	// ceiling; RpcRequestsPerSecond/RpcBurst 2/3 keeps both networks running
	// concurrently on the same IP/key at <=80 CU/s (at 20 CU/call), within the
	// 210M CU/30-day monthly budget (~81 CU/s average) and well below the
	// 50,400 CU/min (840 CU/s) degraded floor even at 75 CU/call. Omitted
	// fields in config.json inherit these defaults; set RpcRequestsPerSecond
	// explicitly to 0 to run uncapped (for example against a local full node).
	Networks: map[string]config.BaseNetworkConfig{
		common.ZenonNetworkName: {
			Urls: []string{"ws://127.0.0.1:35998"},
		},
		"BNB Chain": {
			Urls:                 []string{"ws://127.0.0.1:8545"},
			FilterQuerySize:      100,
			RpcRequestsPerSecond: 2,
			RpcBurst:             3,
		},
		"Ethereum": {
			Urls:                 []string{"ws://127.0.0.1:8545"},
			FilterQuerySize:      100,
			RpcRequestsPerSecond: 2,
			RpcBurst:             3,
		},
	},
	GlobalState: common.LiveState,
	TssConfig: config.TssManagerConfig{
		Port:            55055,
		PublicKey:       "",
		LocalPubKeys:    nil,
		Bootstrap:       "",
		PubKeyWhitelist: map[string]bool{},
		BaseDir:         path.Join(common.DefaultDataDir(), common.DefaultTssDir),
		BaseConfig: gotss.TssConfig{
			PartyTimeout:      90000000000,  // 1.5 minute
			KeyGenTimeout:     900000000000, // 15 minutes
			KeySignTimeout:    90000000000,  // 1.5 minute
			KeyRegroupTimeout: 60,           // regroup not used
			PreParamTimeout:   900000000000, // 15 minutes
			EnableMonitor:     false,
		},
	},
	HealthConfig: config.HealthRpcConfig{
		Address:                     config.DefaultHealthRpcAddress,
		Port:                        55000,
		CachedResponseDelay:         25,
		ResponsesPerSecond:          2,
		Burst:                       2,
		PerClientResponsesPerSecond: 2,
		PerClientBurst:              2,
	},
	ProducerKeyFileName:           "producer",
	ProducerKeyFilePassphraseFile: "",
	ProducerIndex:                 0,
}
