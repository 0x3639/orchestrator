package app

import (
	"encoding/json"
	"fmt"
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

	// 3: Load the producer passphrase from a runtime source; it is never
	// serialized with the rest of the configuration.
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
	if hasLegacyPassphrase(jsonConf) {
		return fmt.Errorf("%s contains %s, which is no longer read from disk; remove it from the file and provide the passphrase through %s or ProducerKeyFilePassphraseFile",
			configPath, config.LegacyPassphraseConfigKey, config.ProducerPassphraseEnv)
	}
	return nil
}

// hasLegacyPassphrase reports whether the raw config still carries a
// plaintext passphrase so operators are told to migrate instead of silently
// starting without it.
func hasLegacyPassphrase(jsonConf []byte) bool {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(jsonConf, &raw); err != nil {
		return false
	}
	value, ok := raw[config.LegacyPassphraseConfigKey]
	if !ok {
		return false
	}
	var passphrase string
	if err := json.Unmarshal(value, &passphrase); err != nil {
		return true
	}
	return passphrase != ""
}
