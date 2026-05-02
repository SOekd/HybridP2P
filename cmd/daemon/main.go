package main

import (
	"P2P-CDN/pkg/config"
	"P2P-CDN/pkg/daemon"
	"fmt"
	"os"
	"path/filepath"
)

const (
	serviceName        = "p2pcdn-daemon"
	serviceDisplayName = "P2P CDN Daemon"
	serviceDescription = "P2P Content Distribution Network daemon service"
	defaultListenAddr  = "127.0.0.1:9090"
)

func main() {
	configPath := os.Getenv("P2PCDN_CONFIG")
	if configPath == "" {
		configPath = filepath.Join(config.ResolveBaseDir(), "client.yaml")
	}

	configExists := false
	if _, err := os.Stat(configPath); err == nil {
		configExists = true
	}

	cfg, err := config.LoadOrCreateClientConfig(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	if absPath, err := filepath.Abs(configPath); err == nil {
		if !configExists {
			fmt.Fprintf(os.Stdout, "Created default config: %s\n", absPath)
			fmt.Fprintf(os.Stdout, "Edit this file to customize settings (tracker URL, ports, etc.)\n\n")
		} else {
			fmt.Fprintf(os.Stdout, "Using config: %s\n", absPath)
		}
	}

	listenAddr := os.Getenv("P2PCDN_DAEMON_ADDR")
	if listenAddr == "" {
		listenAddr = defaultListenAddr
	}

	if err := daemon.RunService(cfg, listenAddr, serviceName, serviceDisplayName, serviceDescription); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to run service: %v\n", err)
		os.Exit(1)
	}
}
