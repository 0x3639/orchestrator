package app

import (
	"encoding/json"
	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/node"
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

	// 3: Teach the log redactor every configured RPC URL before anything
	// that might dial them can log an error.
	config.RegisterSecretURLs(&cfg)

	// 4: Resolve the producer passphrase (env, passphrase file, config.json, prompt).
	if err := config.LoadProducerPassphrase(&cfg, true); err != nil {
		return nil, err
	}

	// 5: Log only the allowlisted, redacted view of the configuration.
	if j, err := json.MarshalIndent(cfg.LoggableSummary(), "", "    "); err == nil {
		common.GlobalLogger.Info("Using the following orchestrator config: \n", string(j))
	}

	// 6: Write it so a default one is created after the first run
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

	found, err := config.ReadConfigFile(cfg)
	if err != nil {
		common.GlobalLogger.Errorf("Config could not be loaded from %s: %v", cfg.ConfigPath(), err)
		return err
	}
	if !found {
		common.GlobalLogger.Infof("Config file missing: you can provide a data path using the --data flag or provide a config file using the --config flag; configPath: %s", cfg.ConfigPath())
	}
	return nil
}
