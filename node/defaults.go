package node

import (
	gotss "github.com/HyperCore-Team/go-tss/common"
	"orchestrator/common"
	"orchestrator/common/config"
	"path"
)

var DefaultNodeConfig = config.Config{
	DataPath: common.DefaultDataDir(),
	// RpcRequestsPerSecond/RpcBurst default to 3/5: at ~75 CU per
	// eth_getLogs that is ~225 CU/s, about a quarter of a typical free
	// provider's degraded floor (50,400 CU/min) and a tenth of its normal
	// budget (120,000 CU/min). Existing config.json files without the fields
	// stay uncapped; add the fields to opt in.
	Networks: map[string]config.BaseNetworkConfig{
		common.ZenonNetworkName: {
			Urls: []string{"ws://127.0.0.1:35998"},
		},
		"BSC": {
			Urls:                 []string{"ws://127.0.0.1:8545"},
			FilterQuerySize:      2000,
			RpcRequestsPerSecond: 3,
			RpcBurst:             5,
		},
		"Ethereum": {
			Urls:                 []string{"ws://127.0.0.1:8545"},
			FilterQuerySize:      2000,
			RpcRequestsPerSecond: 3,
			RpcBurst:             5,
		},
		"Supernova": {
			Urls:                 []string{"wss://rpc.novascan.io"},
			FilterQuerySize:      2000,
			RpcRequestsPerSecond: 3,
			RpcBurst:             5,
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
