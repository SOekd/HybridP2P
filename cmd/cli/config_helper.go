package main

import (
	"P2P-CDN/pkg/config"
	"fmt"
	"os"
	"path/filepath"
)

func getConfigPath() string {
	if cfgFile != "" {
		return cfgFile
	}
	return filepath.Join(config.ResolveBaseDir(), "client.yaml")
}

func loadConfig() (*config.ClientConfig, string, error) {
	configPath := getConfigPath()

	configExists := false
	if _, err := os.Stat(configPath); err == nil {
		configExists = true
	}

	cfg, err := config.LoadOrCreateClientConfig(configPath)
	if err != nil {
		return nil, "", fmt.Errorf("failed to load config: %w", err)
	}

	if !configExists {
		absPath, _ := filepath.Abs(configPath)
		fmt.Printf("Created config: %s\n\n", absPath)
	}

	if err := config.PromptForTracker(cfg, configPath); err != nil {
		return nil, "", fmt.Errorf("failed to configure tracker: %w", err)
	}

	return cfg, configPath, nil
}
