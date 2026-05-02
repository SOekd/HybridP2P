package config

import (
	"fmt"
	"os"
	"path/filepath"
)

func ResolveBaseDir() string {
	execPath, err := os.Executable()
	if err == nil {
		return filepath.Join(filepath.Dir(execPath), ".p2pcdn")
	}
	return ".p2pcdn"
}

func DefaultClientYAML(baseDir string) string {
	dataDir := filepath.Join(baseDir, "data")
	logFile := filepath.Join(baseDir, "client.log")

	return fmt.Sprintf(`# P2P CDN Client Configuration (Auto-generated)

client:
  listen_port: 4001
  data_dir: %s

tracker:
  # IMPORTANT: Set your tracker URL here
  url: ""
  # Example: url: http://your-tracker.com:8080

p2p:
  enable_relay: true
  enable_hole_punch: true
  bootstrap_peers:
    # Libp2p public bootstrap nodes (also act as relays)
    - /dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN
    - /dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa
    - /dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb
    - /dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt

storage:
  chunk_size: 524288  # 512KB
  blockstore_type: flatfs

api:
  enabled: false
  port: 8081

logging:
  level: info
  file: %s
`, dataDir, logFile)
}

func CreateDefaultConfig(configPath, baseDir string) error {
	dir := filepath.Dir(configPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	if err := os.WriteFile(configPath, []byte(DefaultClientYAML(baseDir)), 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

func LoadOrCreateClientConfig(configPath string) (*ClientConfig, error) {
	baseDir := ResolveBaseDir()

	if configPath == "" {
		configPath = filepath.Join(baseDir, "client.yaml")
	}

	if _, err := os.Stat(configPath); err == nil {
		return LoadClientConfig(configPath)
	}

	// Try to create a default config file. If we can't (e.g. read-only filesystem
	// or restricted container permissions), silently fall back to defaults + env vars.
	if err := CreateDefaultConfig(configPath, baseDir); err != nil {
		return LoadClientConfig("")
	}

	return LoadClientConfig(configPath)
}
