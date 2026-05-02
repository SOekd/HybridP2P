package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"P2P-CDN/internal/logger"
	"P2P-CDN/pkg/client"
	"P2P-CDN/pkg/config"
	"P2P-CDN/pkg/p2p"
	"P2P-CDN/pkg/protocol"
	"P2P-CDN/pkg/storage"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	cfgFile     string
	outputPath  string
	autoSeed    bool
	fallbackURL string
	noAutoSeed  bool
)

var registerCmd = &cobra.Command{
	Use:   "register <file-path>",
	Short: "Register a file with the tracker",
	Long:  "Chunks a file and registers it with the tracker for P2P distribution",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		filePath := args[0]

		if err := logger.Init("info", "cli.log"); err != nil {
			return fmt.Errorf("failed to initialize logger: %w", err)
		}
		defer logger.Sync()

		logger.Info("registering file", zap.String("path", filePath))

		chunker := storage.NewChunker(protocol.DefaultChunkSize)

		metadata, err := chunker.ChunkFile(filePath, fallbackURL)
		if err != nil {
			return fmt.Errorf("failed to chunk file: %w", err)
		}

		logger.Info("file chunked",
			zap.String("hash", metadata.Hash.String()),
			zap.Uint32("chunks", metadata.ChunkCount),
			zap.Uint64("size", metadata.Size))

		cfg, _, err := loadConfig()
		if err != nil {
			return err
		}

		ctx := context.Background()
		p2pHost, err := p2p.NewHost(&p2p.HostConfig{
			ListenPort:      cfg.Client.ListenPort,
			DataDir:         cfg.Client.DataDir,
			EnableRelay:     cfg.P2P.EnableRelay,
			EnableHolePunch: cfg.P2P.EnableHolePunch,
			BootstrapPeers:  cfg.P2P.BootstrapPeers,
		})
		if err != nil {
			return fmt.Errorf("failed to create p2p host: %w", err)
		}
		defer p2pHost.Close()

		trackerClient := client.NewTrackerClient(cfg.Tracker.URL, p2pHost.GetPeerInfo())

		req := &protocol.RegisterFileRequest{
			Hash:        metadata.Hash.String(),
			Size:        metadata.Size,
			ChunkSize:   metadata.ChunkSize,
			ChunkCount:  metadata.ChunkCount,
			Chunks:      metadata.Chunks,
			FallbackURL: fallbackURL,
		}

		if err := trackerClient.RegisterFile(ctx, req); err != nil {
			return fmt.Errorf("failed to register file: %w", err)
		}

		logger.Info("file registered successfully",
			zap.String("hash", metadata.Hash.String()))

		fmt.Printf("File registered successfully!\n")
		fmt.Printf("Hash: %s\n", metadata.Hash.String())
		fmt.Printf("Size: %d bytes\n", metadata.Size)
		fmt.Printf("Chunks: %d\n", metadata.ChunkCount)

		if autoSeed {
			fmt.Println("\nStarting seeding...")
			return seedFile(ctx, cfg, p2pHost, filePath, metadata)
		}

		return nil
	},
}

var downloadCmd = &cobra.Command{
	Use:   "download <file-hash>",
	Short: "Download a file by hash",
	Long:  "Downloads a file from peers using P2P with fallback to HTTP",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		fileHash := args[0]

		cfg, _, err := loadConfig()
		if err != nil {
			return err
		}

		if !standaloneMode {
			if client, ok := tryUseDaemon(); ok {
				return downloadViaDaemon(client, fileHash, outputPath, cfg.Tracker.URL, !noAutoSeed, fallbackURL)
			}

			if ensureDaemonRunning() {
				if client, ok := tryUseDaemon(); ok {
					return downloadViaDaemon(client, fileHash, outputPath, cfg.Tracker.URL, !noAutoSeed, fallbackURL)
				}
			}

			fmt.Println("Running in standalone mode")
			fmt.Println()
		}

		if err := logger.Init("info", "cli.log"); err != nil {
			return fmt.Errorf("failed to initialize logger: %w", err)
		}
		defer logger.Sync()

		logger.Info("downloading file", zap.String("hash", fileHash))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		bootstrapPeerAddrs := cfg.P2P.BootstrapPeers
		if cfg.Tracker.URL != "" {
			tempTrackerClient := client.NewTrackerClient(cfg.Tracker.URL, peer.AddrInfo{})
			natInfo, err := tempTrackerClient.GetNATInfo(ctx)
			if err != nil {
				logger.Warn("failed to get NAT info from tracker", zap.Error(err))
			} else if len(natInfo.RelayServers) > 0 {
				for _, relay := range natInfo.RelayServers {
					for _, addr := range relay.Addrs {
						bootstrapPeerAddrs = append(bootstrapPeerAddrs, addr)
					}
				}
				logger.Info("added relay servers from tracker", zap.Int("count", len(natInfo.RelayServers)))
			}
		}

		p2pHost, err := p2p.NewHost(&p2p.HostConfig{
			ListenPort:      cfg.Client.ListenPort,
			DataDir:         cfg.Client.DataDir,
			EnableRelay:     cfg.P2P.EnableRelay,
			EnableHolePunch: cfg.P2P.EnableHolePunch,
			BootstrapPeers:  bootstrapPeerAddrs,
		})
		if err != nil {
			return fmt.Errorf("failed to create p2p host: %w", err)
		}
		defer p2pHost.Close()

		blockstoreDir := fmt.Sprintf("%s/blocks", cfg.Client.DataDir)
		blockstore, err := storage.NewFileBlockstore(blockstoreDir)
		if err != nil {
			return fmt.Errorf("failed to create blockstore: %w", err)
		}
		defer blockstore.Close()

		exchange, err := p2p.NewBitSwapExchange(ctx, p2pHost.Host, blockstore)
		if err != nil {
			return fmt.Errorf("failed to create bitswap exchange: %w", err)
		}
		defer exchange.Close()

		bootstrapPeers, err := parseBootstrapPeers(cfg.P2P.BootstrapPeers)
		if err != nil {
			return fmt.Errorf("failed to parse bootstrap peers: %w", err)
		}

		discovery, err := p2p.NewDiscovery(ctx, p2pHost.Host, bootstrapPeers)
		if err != nil {
			return fmt.Errorf("failed to create discovery: %w", err)
		}
		defer discovery.Close()

		if err := p2pHost.ConnectBootstrap(ctx); err != nil {
			logger.Warn("failed to connect to bootstrap peers", zap.Error(err))
		}

		if err := discovery.Bootstrap(ctx); err != nil {
			logger.Warn("failed to bootstrap DHT", zap.Error(err))
		}

		logger.Info("waiting for DHT to populate routing table...")
		maxWait := 30 * time.Second
		checkInterval := 2 * time.Second
		startTime := time.Now()

		for time.Since(startTime) < maxWait {
			rtInfo := discovery.GetRoutingTable()
			if rtInfo.Size > 0 {
				logger.Info("DHT routing table populated",
					zap.Int("peers", rtInfo.Size),
					zap.Duration("elapsed", time.Since(startTime)))
				break
			}
			logger.Debug("DHT routing table empty, waiting...",
				zap.Duration("elapsed", time.Since(startTime)))
			time.Sleep(checkInterval)
		}

		rtInfo := discovery.GetRoutingTable()
		logger.Info("DHT routing table status",
			zap.Int("peers", rtInfo.Size))

		trackerClient := client.NewTrackerClient(cfg.Tracker.URL, p2pHost.GetPeerInfo())

		fileHashParsed, err := protocol.FileHashFromString(fileHash)
		if err != nil {
			return fmt.Errorf("invalid file hash: %w", err)
		}

		progressChan := make(chan client.DownloadProgress, 10)
		go func() {
			for progress := range progressChan {
				percentage := float64(0)
				if progress.TotalSize > 0 {
					percentage = float64(progress.Downloaded) / float64(progress.TotalSize) * 100
				}
				fmt.Printf("\rProgress: %.2f%% (%d/%d bytes) - %.2f KB/s",
					percentage, progress.Downloaded, progress.TotalSize, float64(progress.DownloadRate)/1024)
			}
		}()

		downloader := client.NewDownloader(p2pHost, exchange, discovery, trackerClient, progressChan)

		metadata := &protocol.FileMetadata{
			Hash: fileHashParsed,
		}

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		errChan := make(chan error, 1)
		go func() {
			errChan <- downloader.DownloadFile(ctx, metadata, outputPath)
		}()

		select {
		case err := <-errChan:
			if err != nil {
				return fmt.Errorf("download failed: %w", err)
			}
			logger.Info("download completed", zap.String("output", outputPath))
			fmt.Printf("\nDownload completed successfully!\n")
			fmt.Printf("File saved to: %s\n", outputPath)
		case sig := <-sigChan:
			logger.Info("received signal, cancelling download", zap.String("signal", sig.String()))
			cancel()
			return fmt.Errorf("download cancelled by user")
		}

		return nil
	},
}

var seedCmd = &cobra.Command{
	Use:   "seed <file-path> [file-path...]",
	Short: "Seed file(s) to peers",
	Long:  "Seeds one or more files to other peers in the network",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, _, err := loadConfig()
		if err != nil {
			return err
		}

		if !standaloneMode {
			if client, ok := tryUseDaemon(); ok {
				for _, filePath := range args {
					if err := seedViaDaemon(client, filePath, fallbackURL, cfg.Tracker.URL); err != nil {
						fmt.Printf("Failed to seed %s: %v\n", filePath, err)
						continue
					}
				}
				return nil
			}

			if ensureDaemonRunning() {
				if client, ok := tryUseDaemon(); ok {
					for _, filePath := range args {
						if err := seedViaDaemon(client, filePath, fallbackURL, cfg.Tracker.URL); err != nil {
							fmt.Printf("Failed to seed %s: %v\n", filePath, err)
							continue
						}
					}
					return nil
				}
			}

			fmt.Println("Running in standalone mode")
			fmt.Println()
		}

		if len(args) > 1 {
			return fmt.Errorf("standalone mode only supports seeding one file at a time. Use daemon mode for multiple files.")
		}

		filePath := args[0]

		if err := logger.Init("info", "cli.log"); err != nil {
			return fmt.Errorf("failed to initialize logger: %w", err)
		}
		defer logger.Sync()

		logger.Info("seeding file", zap.String("path", filePath))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		bootstrapPeerAddrs := cfg.P2P.BootstrapPeers
		if cfg.Tracker.URL != "" {
			tempTrackerClient := client.NewTrackerClient(cfg.Tracker.URL, peer.AddrInfo{})
			natInfo, err := tempTrackerClient.GetNATInfo(ctx)
			if err != nil {
				logger.Warn("failed to get NAT info from tracker", zap.Error(err))
			} else if len(natInfo.RelayServers) > 0 {
				for _, relay := range natInfo.RelayServers {
					for _, addr := range relay.Addrs {
						bootstrapPeerAddrs = append(bootstrapPeerAddrs, addr)
					}
				}
				logger.Info("added relay servers from tracker", zap.Int("count", len(natInfo.RelayServers)))
			}
		}

		p2pHost, err := p2p.NewHost(&p2p.HostConfig{
			ListenPort:      cfg.Client.ListenPort,
			DataDir:         cfg.Client.DataDir,
			EnableRelay:     cfg.P2P.EnableRelay,
			EnableHolePunch: cfg.P2P.EnableHolePunch,
			BootstrapPeers:  bootstrapPeerAddrs,
		})
		if err != nil {
			return fmt.Errorf("failed to create p2p host: %w", err)
		}
		defer p2pHost.Close()

		chunker := storage.NewChunker(protocol.DefaultChunkSize)
		metadata, err := chunker.ChunkFile(filePath, fallbackURL)
		if err != nil {
			return fmt.Errorf("failed to chunk file: %w", err)
		}

		logger.Info("file chunked",
			zap.String("hash", metadata.Hash.String()),
			zap.Uint32("chunks", metadata.ChunkCount))

		fmt.Printf("Seeding file: %s\n", metadata.Hash.String())
		fmt.Printf("Press Ctrl+C to stop seeding\n\n")

		return seedFile(ctx, cfg, p2pHost, filePath, metadata)
	},
}

var unseedCmd = &cobra.Command{
	Use:   "unseed <file-hash> [file-hash...]",
	Short: "Stop seeding file(s)",
	Long:  "Stops seeding one or more files (daemon mode only)",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if !ensureDaemonRunning() {
			return fmt.Errorf("daemon is not running")
		}
		client, err := getDaemonClient()
		if err != nil {
			return fmt.Errorf("could not connect to daemon: %w", err)
		}

		for _, fileHash := range args {
			fmt.Printf("Stopping seed for %s...\n", fileHash)
			if err := client.Unseed(fileHash); err != nil {
				fmt.Printf("Failed to unseed %s: %v\n", fileHash, err)
				continue
			}
			fmt.Printf("Stopped seeding %s\n", fileHash)
		}

		return nil
	},
}

var listSeedsCmd = &cobra.Command{
	Use:   "list-seeds",
	Short: "List all files being seeded",
	Long:  "Shows all files currently being seeded by the daemon",
	RunE: func(cmd *cobra.Command, args []string) error {
		if !ensureDaemonRunning() {
			return fmt.Errorf("daemon is not running")
		}
		client, err := getDaemonClient()
		if err != nil {
			return fmt.Errorf("could not connect to daemon: %w", err)
		}

		seeds, err := client.ListSeeds()
		if err != nil {
			return fmt.Errorf("failed to get seeding list: %w", err)
		}

		if len(seeds.Seeds) == 0 {
			fmt.Println("No files currently being seeded.")
			return nil
		}

		fmt.Printf("Seeding %d file(s):\n\n", len(seeds.Seeds))
		for i, seed := range seeds.Seeds {
			fmt.Printf("[%d] Hash: %s\n", i+1, seed.FileHash)
			fmt.Printf("    Path: %s\n", seed.FilePath)
			if seed.FallbackURL != "" {
				fmt.Printf("    Fallback: %s\n", seed.FallbackURL)
			}
			fmt.Printf("    Size: %d bytes (%d chunks)\n", seed.Size, seed.ChunkCount)

			if startedAt, err := time.Parse(time.RFC3339, seed.StartedAt); err == nil {
				fmt.Printf("    Started: %s\n", startedAt.Format("2006-01-02 15:04:05"))
			} else {
				fmt.Printf("    Started: %s\n", seed.StartedAt)
			}

			if seed.BytesServed > 0 || seed.PeersServed > 0 {
				fmt.Printf("    Stats: %d bytes served to %d peers\n", seed.BytesServed, seed.PeersServed)
			}
			fmt.Println()
		}

		return nil
	},
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show peer status",
	Long:  "Shows information about the local peer and connections",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := logger.Init("info", "cli.log"); err != nil {
			return fmt.Errorf("failed to initialize logger: %w", err)
		}
		defer logger.Sync()

		cfg, err := config.LoadClientConfig(cfgFile)
		if err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}

		p2pHost, err := p2p.NewHost(&p2p.HostConfig{
			ListenPort:      cfg.Client.ListenPort,
			DataDir:         cfg.Client.DataDir,
			EnableRelay:     cfg.P2P.EnableRelay,
			EnableHolePunch: cfg.P2P.EnableHolePunch,
			BootstrapPeers:  cfg.P2P.BootstrapPeers,
		})
		if err != nil {
			return fmt.Errorf("failed to create p2p host: %w", err)
		}
		defer p2pHost.Close()

		fmt.Printf("Peer ID: %s\n", p2pHost.Host.ID())
		fmt.Printf("Addresses:\n")
		for _, addr := range p2pHost.Host.Addrs() {
			fmt.Printf("  %s\n", addr)
		}

		peers := p2pHost.Host.Network().Peers()
		fmt.Printf("\nConnected peers: %d\n", len(peers))
		for _, peerID := range peers {
			fmt.Printf("  - %s\n", peerID)
		}

		return nil
	},
}

func seedFile(ctx context.Context, cfg *config.ClientConfig, p2pHost *p2p.P2PHost, filePath string, metadata *protocol.FileMetadata) error {
	blockstoreDir := fmt.Sprintf("%s/blocks", cfg.Client.DataDir)
	blockstore, err := storage.NewFileBlockstore(blockstoreDir)
	if err != nil {
		return fmt.Errorf("failed to create blockstore: %w", err)
	}
	defer blockstore.Close()

	exchange, err := p2p.NewBitSwapExchange(ctx, p2pHost.Host, blockstore)
	if err != nil {
		return fmt.Errorf("failed to create bitswap exchange: %w", err)
	}
	defer exchange.Close()

	bootstrapPeers, err := parseBootstrapPeers(cfg.P2P.BootstrapPeers)
	if err != nil {
		return fmt.Errorf("failed to parse bootstrap peers: %w", err)
	}

	discovery, err := p2p.NewDiscovery(ctx, p2pHost.Host, bootstrapPeers)
	if err != nil {
		return fmt.Errorf("failed to create discovery: %w", err)
	}
	defer discovery.Close()

	if err := p2pHost.ConnectBootstrap(ctx); err != nil {
		logger.Warn("failed to connect to bootstrap peers", zap.Error(err))
	}

	if err := discovery.Bootstrap(ctx); err != nil {
		logger.Warn("failed to bootstrap DHT", zap.Error(err))
	}

	logger.Info("waiting for DHT to populate routing table...")
	maxWait := 30 * time.Second
	checkInterval := 2 * time.Second
	startTime := time.Now()

	for time.Since(startTime) < maxWait {
		rtInfo := discovery.GetRoutingTable()
		if rtInfo.Size > 0 {
			logger.Info("DHT routing table populated",
				zap.Int("peers", rtInfo.Size),
				zap.Duration("elapsed", time.Since(startTime)))
			break
		}
		logger.Debug("DHT routing table empty, waiting...",
			zap.Duration("elapsed", time.Since(startTime)))
		time.Sleep(checkInterval)
	}

	rtInfo := discovery.GetRoutingTable()
	logger.Info("DHT routing table status",
		zap.Int("peers", rtInfo.Size))

	logger.Info("waiting for public address discovery...")
	p2pHost.WaitForPublicAddr(ctx, 30*time.Second)

	natType := p2pHost.GetNATType()
	publicAddrs := p2pHost.GetPublicAddrs()
	logger.Info("NAT status",
		zap.String("type", string(natType)),
		zap.Int("public_addrs", len(publicAddrs)),
		zap.Strings("addrs", p2p.MultiaddrsToStrings(publicAddrs)))

	trackerClient := client.NewTrackerClient(cfg.Tracker.URL, p2pHost.GetPeerInfo())

	seeder := client.NewSeeder(p2pHost, exchange, discovery, trackerClient, protocol.PeerAnnounceInterval)

	if _, err := seeder.LoadFileFromDisk(filePath, metadata.FallbackURL); err != nil {
		return fmt.Errorf("failed to load file: %w", err)
	}

	if err := seeder.Start(); err != nil {
		return fmt.Errorf("failed to start seeder: %w", err)
	}
	defer seeder.Stop()

	logger.Info("seeding started", zap.String("hash", metadata.Hash.String()))

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sig := <-sigChan:
			logger.Info("received signal, stopping seeder", zap.String("signal", sig.String()))
			fmt.Println("\nStopping seeder...")
			return nil
		case <-ticker.C:
			stats := seeder.GetStats()
			fmt.Printf("Seeding stats - Files: %d, Bytes served: %d, Peers: %d\n",
				stats.FilesSeeding, stats.BytesServed, stats.PeersServed)
		}
	}
}

func parseBootstrapPeers(addrs []string) ([]peer.AddrInfo, error) {
	peers := make([]peer.AddrInfo, 0, len(addrs))
	for _, addr := range addrs {
		pi, err := peer.AddrInfoFromString(addr)
		if err != nil {
			logger.Warn("failed to parse bootstrap peer", zap.String("addr", addr), zap.Error(err))
			continue
		}
		peers = append(peers, *pi)
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("no valid bootstrap peers")
	}
	return peers, nil
}

func init() {
	registerCmd.Flags().StringVar(&fallbackURL, "fallback", "", "Fallback HTTP URL for the file")
	registerCmd.Flags().BoolVar(&autoSeed, "seed", false, "Automatically start seeding after registration")
	registerCmd.MarkFlagRequired("fallback")

	downloadCmd.Flags().StringVarP(&outputPath, "output", "o", "", "Output file path (required)")
	downloadCmd.Flags().BoolVar(&standaloneMode, "standalone", false, "Run in standalone mode (don't use daemon)")
	downloadCmd.Flags().BoolVar(&noAutoSeed, "no-seed", false, "Disable automatic seeding after download (default: auto-seed enabled)")
	downloadCmd.MarkFlagRequired("output")

	seedCmd.Flags().StringVar(&fallbackURL, "fallback", "", "Fallback HTTP URL for the file")
	seedCmd.Flags().BoolVar(&standaloneMode, "standalone", false, "Run in standalone mode (don't use daemon)")

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "Config file (default: ~/.p2pcdn/client.yaml)")
}

var rootCmd = &cobra.Command{
	Use:   "p2pcdn-cli",
	Short: "P2P CDN CLI client",
	Long:  "Command-line client for P2P CDN file sharing with hybrid fallback",
}
