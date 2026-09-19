package app

import (
	"encoding/json"
	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/node"
	"os"
)

func MakeConfig() (*config.Config, error) {
	cfg := node.DefaultNodeConfig

	// 1: Load config file.
	err := readConfigFromFile(&cfg)
	if err != nil {
		return nil, err
	}

	// 2: Make dir paths absolute
	if err := cfg.MakePathsAbsolute(); err != nil {
		return nil, err
	}

	// 3: Resolve the producer passphrase (env, passphrase file, config.json, prompt).
	if err := config.LoadProducerPassphrase(&cfg, true); err != nil {
		return nil, err
	}

	// 4: Log only the allowlisted, redacted view of the configuration.
	if j, err := json.MarshalIndent(cfg.LoggableSummary(), "", "    "); err == nil {
		common.GlobalLogger.Info("Using the following orchestrator config: \n", string(j))
	}

	// 5: Write it so a default one is created after the first run
	if err := config.WriteConfig(cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func readConfigFromFile(cfg *config.Config) error {
	// second read default settings
	if err := config.EnsureDataDir(cfg.DataPath); err != nil {
		return err
	}
	configPath := cfg.ConfigPath()

	jsonConf, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			common.GlobalLogger.Infof("Config file missing: you can provide a data path using the --data flag or provide a config file using the --config flag; configPath: %s", configPath)
			return nil
		}
		return err
	}
	if err := config.VerifyOwnedPath(configPath); err != nil {
		return err
	}

	if err := json.Unmarshal(jsonConf, cfg); err != nil {
		common.GlobalLogger.Errorf("Config malformed: please check; error: %v", err)
		return err
	}
	return nil
}
