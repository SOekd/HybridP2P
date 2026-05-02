package daemon

import (
	"P2P-CDN/internal/logger"
	"P2P-CDN/pkg/client"
	"P2P-CDN/pkg/config"
	"P2P-CDN/pkg/metrics"
	"P2P-CDN/pkg/p2p"
	"P2P-CDN/pkg/protocol"
	"P2P-CDN/pkg/storage"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"
)

type Server struct {
	config         *config.ClientConfig
	p2pHost        *p2p.P2PHost
	exchange       *p2p.BitSwapExchange
	discovery      *p2p.Discovery
	trackerClient  *client.TrackerClient
	fileManager    *FileManager
	seedingState   *SeedingStateManager
	metricsManager *metrics.MetricsManager
	seeder         *client.Seeder
	blockstore     *storage.FileBlockstore
	rlBlockstore   *storage.RateLimitedBlockstore

	seedGate     *Gate
	downloadGate *Gate

	trackerWS *TrackerWSClient

	downloads   map[string]*ActiveDownload
	downloadsMu sync.RWMutex

	wsClients   map[*websocket.Conn]string
	wsClientsMu sync.RWMutex
	wsWriteMu   sync.Mutex

	router     *gin.Engine
	httpServer *http.Server
	ctx        context.Context
	cancel     context.CancelFunc
	startTime  time.Time
}

type ActiveDownload struct {
	FileHash     string
	OutputPath   string
	Metadata     *protocol.FileMetadata
	Downloader   *client.Downloader
	Progress     *client.DownloadProgress
	Status       string
	StartedAt    time.Time
	Ctx          context.Context
	Cancel       context.CancelFunc
	ProgressChan chan client.DownloadProgress
	Error        error
	mu           sync.RWMutex
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func NewServer(cfg *config.ClientConfig) (*Server, error) {
	logLevel := cfg.Logging.Level
	if logLevel == "" {
		logLevel = "info"
	}
	logFile := cfg.Logging.File
	if logFile == "" {
		logFile = fmt.Sprintf("%s/daemon.log", cfg.Client.DataDir)
	}
	if err := logger.Init(logLevel, logFile); err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	fileManager, err := NewFileManager(cfg.Client.DataDir + "/downloads")
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create file manager: %w", err)
	}

	seedingStatePath := cfg.Client.DataDir + "/seeding.json"
	seedingState, err := NewSeedingStateManager(seedingStatePath)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create seeding state manager: %w", err)
	}

	metricsPath := filepath.Join(cfg.Client.DataDir, "metrics.json")
	metricsManager, err := metrics.NewMetricsManager(metricsPath)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create metrics manager: %w", err)
	}

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
		ExternalIP:      cfg.P2P.ExternalIP,
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create p2p host: %w", err)
	}

	blockstoreDir := fmt.Sprintf("%s/blocks", cfg.Client.DataDir)
	rawBlockstore, err := storage.NewFileBlockstore(blockstoreDir)
	if err != nil {
		p2pHost.Close()
		cancel()
		return nil, fmt.Errorf("failed to create blockstore: %w", err)
	}

	rlBlockstore := storage.NewRateLimitedBlockstore(rawBlockstore)

	exchange, err := p2p.NewBitSwapExchange(ctx, p2pHost.Host, rlBlockstore)
	if err != nil {
		rawBlockstore.Close()
		p2pHost.Close()
		cancel()
		return nil, fmt.Errorf("failed to create bitswap exchange: %w", err)
	}

	bootstrapPeers, err := parseBootstrapPeers(cfg.P2P.BootstrapPeers)
	if err != nil {
		exchange.Close()
		rawBlockstore.Close()
		p2pHost.Close()
		cancel()
		return nil, fmt.Errorf("failed to parse bootstrap peers: %w", err)
	}

	discovery, err := p2p.NewDiscovery(ctx, p2pHost.Host, bootstrapPeers)
	if err != nil {
		exchange.Close()
		rawBlockstore.Close()
		p2pHost.Close()
		cancel()
		return nil, fmt.Errorf("failed to create discovery: %w", err)
	}

	if err := p2pHost.ConnectBootstrap(ctx); err != nil {
		logger.Warn("failed to connect to bootstrap peers", zap.Error(err))
	}

	if err := discovery.Bootstrap(ctx); err != nil {
		logger.Warn("failed to bootstrap DHT", zap.Error(err))
	}

	trackerClient := client.NewTrackerClient(cfg.Tracker.URL, p2pHost.GetPeerInfo())

	seeder := client.NewSeeder(p2pHost, exchange, discovery, trackerClient, protocol.PeerAnnounceInterval)
	if err := seeder.Start(); err != nil {
		discovery.Close()
		exchange.Close()
		rawBlockstore.Close()
		p2pHost.Close()
		cancel()
		return nil, fmt.Errorf("failed to start seeder: %w", err)
	}

	rlBlockstore.SetServeCallback(func(c cid.Cid, size int) {
		seeder.RecordBlockServed(c, size)
	})

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	peerID := p2pHost.Host.ID().String()

	srv := &Server{
		config:         cfg,
		p2pHost:        p2pHost,
		exchange:       exchange,
		discovery:      discovery,
		trackerClient:  trackerClient,
		fileManager:    fileManager,
		seedingState:   seedingState,
		metricsManager: metricsManager,
		seeder:         seeder,
		blockstore:     rawBlockstore,
		rlBlockstore:   rlBlockstore,
		seedGate:       NewGate(),
		downloadGate:   NewGate(),
		downloads:      make(map[string]*ActiveDownload),
		wsClients:      make(map[*websocket.Conn]string),
		router:         router,
		ctx:            ctx,
		cancel:         cancel,
		startTime:      time.Now(),
	}

	go srv.restoreSeeds()

	if cfg.Tracker.URL != "" {
		srv.trackerWS = NewTrackerWSClient(cfg.Tracker.URL, peerID, srv)
		go srv.trackerWS.Run(ctx)
	}

	srv.setupRoutes()

	srv.startFileWatcher()

	logger.Info("daemon server initialized",
		zap.String("peer_id", peerID),
		zap.Int("listen_port", cfg.Client.ListenPort))

	return srv, nil
}

func (s *Server) setupRoutes() {
	s.router.GET("/health", s.handleHealth)

	v1 := s.router.Group("/api/v1")
	{
		v1.POST("/seed", s.handleSeed)
		v1.DELETE("/seed/:hash", s.handleUnseed)
		v1.POST("/download", s.handleDownload)
		v1.GET("/status/:hash", s.handleStatus)
		v1.GET("/info", s.handleInfo)
		v1.GET("/seeding", s.handleListSeeds)
		v1.GET("/downloads", s.handleListDownloads)
		v1.GET("/peers", s.handleListPeers)
		v1.POST("/metrics/reset", s.handleResetMetrics)
	}

	s.router.GET("/ws", s.handleWebSocket)
}

func (s *Server) Start(addr string) error {
	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: s.router,
	}

	logger.Info("starting daemon server", zap.String("addr", addr))

	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to start server: %w", err)
	}

	return nil
}

func (s *Server) Stop() error {
	logger.Info("stopping daemon server")

	s.cancel()

	s.seeder.Stop()

	s.downloadsMu.Lock()
	for _, download := range s.downloads {
		download.Cancel()
	}
	s.downloadsMu.Unlock()

	s.wsClientsMu.Lock()
	for conn := range s.wsClients {
		conn.Close()
	}
	s.wsClientsMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.httpServer.Shutdown(ctx); err != nil {
		logger.Error("failed to shutdown http server", zap.Error(err))
	}

	s.discovery.Close()
	s.exchange.Close()
	s.blockstore.Close()
	s.p2pHost.Close()

	logger.Sync()

	return nil
}

func (s *Server) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, protocol.HealthResponse{
		Status:    "ok",
		Timestamp: time.Now().Format(time.RFC3339),
		Version:   "1.0.0",
	})
}

func (s *Server) handleSeed(c *gin.Context) {
	var req protocol.DaemonSeedRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponse{
			Error:   "invalid_request",
			Message: err.Error(),
		})
		return
	}

	if req.TrackerURL != "" {
		logger.Info("custom tracker URL for seed not yet supported, using server default",
			zap.String("requested", req.TrackerURL),
			zap.String("using", s.config.Tracker.URL))
	}

	if _, err := os.Stat(req.FilePath); os.IsNotExist(err) {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponse{
			Error:   "file_not_found",
			Message: fmt.Sprintf("file not found: %s", req.FilePath),
		})
		return
	}

	logger.Info("starting seed request",
		zap.String("path", req.FilePath))

	go func(filePath, fallbackURL string) {
		if s.seedGate.IsPaused() {
			logger.Info("seeding paused by tracker, waiting…",
				zap.String("path", filePath))
		}
		if err := s.seedGate.Wait(s.ctx); err != nil {
			logger.Warn("seed cancelled while waiting for tracker gate",
				zap.String("path", filePath))
			return
		}

		logger.Info("loading file for seeding (async)",
			zap.String("path", filePath))

		metadata, err := s.seeder.LoadFileFromDisk(filePath, fallbackURL)
		if err != nil {
			logger.Error("failed to load file for seeding",
				zap.String("path", filePath),
				zap.Error(err))
			return
		}

		seedingEntry := SeedingEntry{
			FileHash:    metadata.Hash.String(),
			FilePath:    filePath,
			FallbackURL: fallbackURL,
			Size:        metadata.Size,
			ChunkCount:  metadata.ChunkCount,
			StartedAt:   time.Now(),
			BytesServed: 0,
			PeersServed: 0,
		}
		if err := s.seedingState.AddSeeding(seedingEntry); err != nil {
			logger.Warn("failed to save seeding state", zap.Error(err))
		}

		logger.Info("file loaded and seeding started",
			zap.String("hash", metadata.Hash.String()),
			zap.String("path", filePath))
	}(req.FilePath, req.FallbackURL)

	msg := "seeding started (loading file in background)"
	if s.seedGate.IsPaused() {
		msg = "seeding queued (tracker has paused seeds — will start when resumed)"
	}
	c.JSON(http.StatusOK, protocol.DaemonSeedResponse{
		Success:  true,
		FileHash: "",
		Message:  msg,
	})
}

func (s *Server) handleUnseed(c *gin.Context) {
	fileHash := c.Param("hash")

	if err := s.unseedFile(fileHash); err != nil {
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponse{
			Error:   "unseed_failed",
			Message: err.Error(),
		})
		return
	}

	logger.Info("stopped seeding file", zap.String("hash", fileHash))

	c.JSON(http.StatusOK, protocol.DaemonUnseedResponse{
		Success: true,
		Message: "seeding stopped",
	})
}

func (s *Server) handleDownload(c *gin.Context) {
	var req protocol.DaemonDownloadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponse{
			Error:   "invalid_request",
			Message: err.Error(),
		})
		return
	}

	fileHash, err := protocol.FileHashFromString(req.FileHash)
	if err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponse{
			Error:   "invalid_hash",
			Message: err.Error(),
		})
		return
	}

	s.downloadsMu.Lock()
	if existing, exists := s.downloads[req.FileHash]; exists {
		existing.mu.RLock()
		status := existing.Status
		existing.mu.RUnlock()

		existing.Cancel()
		delete(s.downloads, req.FileHash)
		logger.Info("cancelled existing download, restarting",
			zap.String("hash", req.FileHash),
			zap.String("old_status", status))
	}
	s.downloadsMu.Unlock()

	outputPath, err := s.fileManager.GetDownloadPath(req.FileHash, req.Output)
	if err != nil {
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponse{
			Error:   "path_error",
			Message: err.Error(),
		})
		return
	}

	progressChan := make(chan client.DownloadProgress, 250)

	trackerClient := s.trackerClient
	if req.TrackerURL != "" {
		trackerClient = client.NewTrackerClient(req.TrackerURL, s.p2pHost.GetPeerInfo())
		logger.Info("using custom tracker for download",
			zap.String("hash", req.FileHash),
			zap.String("tracker", req.TrackerURL))
	}

	downloader := client.NewDownloader(s.p2pHost, s.exchange, s.discovery, trackerClient, progressChan)
	downloader.SetMetricsManager(s.metricsManager)

	ctx := context.Background()
	_, trackerMetadata, err := trackerClient.GetPeers(ctx, req.FileHash)

	metadata := &protocol.FileMetadata{
		Hash: fileHash,
	}
	if trackerMetadata != nil {
		metadata.Size = trackerMetadata.Size
		metadata.ChunkSize = trackerMetadata.ChunkSize
		metadata.ChunkCount = trackerMetadata.ChunkCount
		metadata.Chunks = trackerMetadata.Chunks
		metadata.FallbackURL = trackerMetadata.FallbackURL
		logger.Info("fetched metadata from tracker",
			zap.String("hash", req.FileHash),
			zap.Uint64("size", metadata.Size),
			zap.Uint32("chunks", metadata.ChunkCount))
	} else {
		logger.Warn("tracker metadata not available, progress may be limited",
			zap.String("hash", req.FileHash),
			zap.Error(err))
	}

	dlCtx, dlCancel := context.WithCancel(s.ctx)

	activeDownload := &ActiveDownload{
		FileHash:     req.FileHash,
		OutputPath:   outputPath,
		Metadata:     metadata,
		Downloader:   downloader,
		Status:       "downloading",
		StartedAt:    time.Now(),
		Ctx:          dlCtx,
		Cancel:       dlCancel,
		ProgressChan: progressChan,
	}

	s.downloadsMu.Lock()
	s.downloads[req.FileHash] = activeDownload
	s.downloadsMu.Unlock()

	go s.runDownload(activeDownload)

	go s.broadcastProgress(activeDownload)

	logger.Info("started download",
		zap.String("hash", req.FileHash),
		zap.String("output", outputPath))

	msg := "download started"
	if s.downloadGate.IsPaused() {
		msg = "download queued (tracker has paused downloads — will start when resumed)"
	}
	c.JSON(http.StatusOK, protocol.DaemonDownloadResponse{
		Success: true,
		Message: msg,
	})
}

func (s *Server) handleStatus(c *gin.Context) {
	fileHash := c.Param("hash")

	s.downloadsMu.RLock()
	download, exists := s.downloads[fileHash]
	s.downloadsMu.RUnlock()

	if exists {
		download.mu.RLock()
		defer download.mu.RUnlock()

		status := protocol.DaemonStatusResponse{
			FileHash: fileHash,
			Status:   download.Status,
		}

		if download.Metadata != nil {
			status.FallbackURL = download.Metadata.FallbackURL
		}

		if download.Progress != nil {
			status.TotalSize = download.Progress.TotalSize
			status.Downloaded = download.Progress.Downloaded
			status.DownloadRate = float64(download.Progress.DownloadRate)
			status.UsingFallback = download.Progress.UsingFallback
		}

		if download.Error != nil {
			status.Error = download.Error.Error()
		}

		c.JSON(http.StatusOK, status)
		return
	}

	if s.seeder.HasFile(fileHash) {
		c.JSON(http.StatusOK, protocol.DaemonStatusResponse{
			FileHash: fileHash,
			Status:   "seeding",
		})
		return
	}

	c.JSON(http.StatusNotFound, protocol.ErrorResponse{
		Error:   "not_found",
		Message: "file not found in downloads or seeds",
	})
}

func (s *Server) handleInfo(c *gin.Context) {
	s.downloadsMu.RLock()
	activeDownloads := len(s.downloads)
	s.downloadsMu.RUnlock()

	stats := s.seeder.GetStats()
	peers := s.p2pHost.Host.Network().Peers()

	c.JSON(http.StatusOK, protocol.DaemonInfoResponse{
		PeerID:          s.p2pHost.Host.ID().String(),
		Version:         "1.0.0",
		Uptime:          time.Since(s.startTime).String(),
		ActiveDownloads: activeDownloads,
		ActiveSeeds:     stats.FilesSeeding,
		ConnectedPeers:  len(peers),
	})
}

func (s *Server) handleListSeeds(c *gin.Context) {
	seedingEntries := s.seedingState.GetAll()

	seeds := make([]protocol.DaemonSeedInfo, 0, len(seedingEntries))
	for _, entry := range seedingEntries {
		seeds = append(seeds, protocol.DaemonSeedInfo{
			FileHash:    entry.FileHash,
			FilePath:    entry.FilePath,
			FallbackURL: entry.FallbackURL,
			Size:        entry.Size,
			ChunkCount:  entry.ChunkCount,
			StartedAt:   entry.StartedAt.Format(time.RFC3339),
			BytesServed: entry.BytesServed,
			PeersServed: entry.PeersServed,
		})
	}

	c.JSON(http.StatusOK, protocol.DaemonListSeedsResponse{
		Seeds: seeds,
		Count: len(seeds),
	})
}

func (s *Server) handleResetMetrics(c *gin.Context) {
	if s.metricsManager == nil {
		c.JSON(http.StatusOK, gin.H{"success": true, "message": "no metrics to clear"})
		return
	}
	if err := s.metricsManager.ClearMetrics(); err != nil {
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponse{
			Error:   "clear_failed",
			Message: err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "message": "metrics cleared"})
}

func (s *Server) handleListDownloads(c *gin.Context) {
	s.downloadsMu.RLock()
	defer s.downloadsMu.RUnlock()

	downloads := []protocol.DaemonDownloadInfo{}
	for hash, dl := range s.downloads {
		dl.mu.RLock()
		info := protocol.DaemonDownloadInfo{
			FileHash:   hash,
			OutputPath: dl.OutputPath,
			Status:     dl.Status,
			StartedAt:  dl.StartedAt.Format(time.RFC3339),
		}

		if dl.Progress != nil {
			info.TotalSize = dl.Progress.TotalSize
			info.Downloaded = dl.Progress.Downloaded
			info.DownloadRate = float64(dl.Progress.DownloadRate)
			info.UsingFallback = dl.Progress.UsingFallback
		}

		downloads = append(downloads, info)
		dl.mu.RUnlock()
	}

	c.JSON(http.StatusOK, protocol.DaemonListDownloadsResponse{
		Downloads: downloads,
		Count:     len(downloads),
	})
}

func (s *Server) handleListPeers(c *gin.Context) {
	peers := s.p2pHost.Host.Network().Peers()
	peerInfos := []protocol.DaemonPeerInfo{}

	for _, peerID := range peers {
		conns := s.p2pHost.Host.Network().ConnsToPeer(peerID)
		addrs := []string{}
		for _, conn := range conns {
			addrs = append(addrs, conn.RemoteMultiaddr().String())
		}

		peerInfos = append(peerInfos, protocol.DaemonPeerInfo{
			PeerID:    peerID.String(),
			Addrs:     addrs,
			Connected: true,
		})
	}

	c.JSON(http.StatusOK, protocol.DaemonPeersResponse{
		Peers: peerInfos,
		Count: len(peerInfos),
	})
}

func (s *Server) handleWebSocket(c *gin.Context) {
	fileHashFilter := c.Query("file_hash")

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		logger.Error("failed to upgrade websocket", zap.Error(err))
		return
	}

	s.wsClientsMu.Lock()
	s.wsClients[conn] = fileHashFilter
	s.wsClientsMu.Unlock()

	logger.Debug("websocket client connected",
		zap.String("file_hash_filter", fileHashFilter))

	defer func() {
		s.wsClientsMu.Lock()
		delete(s.wsClients, conn)
		s.wsClientsMu.Unlock()
		conn.Close()
		logger.Debug("websocket client disconnected")
	}()

	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			break
		}
	}
}

func (s *Server) runDownload(download *ActiveDownload) {
	if s.downloadGate.IsPaused() {
		download.mu.Lock()
		download.Status = "waiting"
		download.mu.Unlock()
		logger.Info("download paused by tracker, waiting…",
			zap.String("hash", download.FileHash))
	}
	if err := s.downloadGate.Wait(download.Ctx); err != nil {
		download.mu.Lock()
		download.Status = "cancelled"
		download.Error = err
		download.mu.Unlock()
		close(download.ProgressChan)
		return
	}
	if download.Status == "waiting" {
		download.mu.Lock()
		download.Status = "downloading"
		download.mu.Unlock()
	}

	err := download.Downloader.DownloadFile(download.Ctx, download.Metadata, download.OutputPath)

	download.mu.Lock()
	if err != nil {
		download.Status = "error"
		download.Error = err
		logger.Error("download failed",
			zap.String("hash", download.FileHash),
			zap.Error(err))
	} else {
		download.Status = "complete"
		logger.Info("download completed",
			zap.String("hash", download.FileHash),
			zap.String("output", download.OutputPath))

		usingFallback := false
		if download.Progress != nil {
			usingFallback = download.Progress.UsingFallback
		}

		select {
		case download.ProgressChan <- client.DownloadProgress{
			FileHash:       download.Metadata.Hash,
			TotalSize:      download.Metadata.Size,
			Downloaded:     download.Metadata.Size,
			ChunksComplete: download.Metadata.ChunkCount,
			ChunksTotal:    download.Metadata.ChunkCount,
			DownloadRate:   0,
			Status:         "complete",
			UsingFallback:  usingFallback,
		}:
		case <-time.After(1 * time.Second):
			logger.Warn("timeout sending final progress update", zap.String("hash", download.FileHash))
		}

		time.Sleep(500 * time.Millisecond)
	}
	download.mu.Unlock()

	if err != nil {
		s.broadcastWSMessage(protocol.WSErrorMessage{
			Type:      "error",
			FileHash:  download.FileHash,
			Error:     "download_failed",
			Message:   err.Error(),
			Timestamp: time.Now().Format(time.RFC3339),
		})
	} else {
		s.broadcastWSMessage(protocol.WSCompleteMessage{
			Type:      "complete",
			FileHash:  download.FileHash,
			Path:      download.OutputPath,
			Size:      download.Metadata.Size,
			Timestamp: time.Now().Format(time.RFC3339),
		})
	}

	time.Sleep(500 * time.Millisecond)

	close(download.ProgressChan)

	time.Sleep(5 * time.Second)

	s.downloadsMu.Lock()
	if s.downloads[download.FileHash] == download {
		delete(s.downloads, download.FileHash)
	}
	s.downloadsMu.Unlock()

	logger.Info("download cleanup completed",
		zap.String("hash", download.FileHash),
		zap.String("final_status", download.Status))
}

func (s *Server) broadcastProgress(download *ActiveDownload) {
	for progress := range download.ProgressChan {
		download.mu.Lock()
		download.Progress = &progress
		download.mu.Unlock()

		s.broadcastWSMessage(protocol.WSProgressMessage{
			Type:           "progress",
			FileHash:       download.FileHash,
			TotalSize:      progress.TotalSize,
			Downloaded:     progress.Downloaded,
			ChunksComplete: progress.ChunksComplete,
			ChunksTotal:    progress.ChunksTotal,
			Peers:          []string{},
			DownloadRate:   float64(progress.DownloadRate),
			Status:         download.Status,
			UsingFallback:  progress.UsingFallback,
			Timestamp:      time.Now().Format(time.RFC3339),
		})
	}
}

func (s *Server) broadcastWSMessage(msg interface{}) {
	data, err := json.Marshal(msg)
	if err != nil {
		logger.Error("failed to marshal ws message", zap.Error(err))
		return
	}

	var fileHash string
	if msgMap, ok := msg.(map[string]interface{}); ok {
		if hash, ok := msgMap["file_hash"].(string); ok {
			fileHash = hash
		}
	} else {
		msgJSON, _ := json.Marshal(msg)
		var msgMap map[string]interface{}
		if json.Unmarshal(msgJSON, &msgMap) == nil {
			if hash, ok := msgMap["file_hash"].(string); ok {
				fileHash = hash
			}
		}
	}

	s.wsClientsMu.RLock()
	defer s.wsClientsMu.RUnlock()

	for conn, filter := range s.wsClients {
		if filter != "" && fileHash != "" && filter != fileHash {
			continue
		}

		s.wsWriteMu.Lock()
		err := conn.WriteMessage(websocket.TextMessage, data)
		s.wsWriteMu.Unlock()

		if err != nil {
			logger.Error("failed to send ws message", zap.Error(err))
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

func (s *Server) startFileWatcher() {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		logger.Info("file watcher started")

		for {
			select {
			case <-s.ctx.Done():
				logger.Info("file watcher stopped")
				return
			case <-ticker.C:
				s.checkSeedingFiles()
			}
		}
	}()
}

func (s *Server) checkSeedingFiles() {
	seedingEntries := s.seedingState.GetAll()

	for _, entry := range seedingEntries {
		if _, err := os.Stat(entry.FilePath); os.IsNotExist(err) {
			logger.Info("detected deleted seeded file, unseeding",
				zap.String("hash", entry.FileHash),
				zap.String("path", entry.FilePath))

			if err := s.unseedFile(entry.FileHash); err != nil {
				logger.Warn("failed to unseed deleted file",
					zap.String("hash", entry.FileHash),
					zap.Error(err))
			} else {
				logger.Info("automatically unseeded deleted file",
					zap.String("hash", entry.FileHash))
			}
		}
	}

	s.flushSeedMetrics()
}

func (s *Server) flushSeedMetrics() {
	allStats := s.seeder.GetAllStats()
	if len(allStats) == 0 {
		return
	}

	stateEntries := s.seedingState.GetAll()
	stateMap := make(map[string]SeedingEntry, len(stateEntries))
	for _, e := range stateEntries {
		stateMap[e.FileHash] = e
	}

	for _, stats := range allStats {
		hashStr := stats.Hash.String()
		entry, ok := stateMap[hashStr]
		if !ok {
			continue
		}

		sm := &metrics.SeedMetricsEntry{
			FileHash:     hashStr,
			FilePath:     entry.FilePath,
			FallbackURL:  entry.FallbackURL,
			FileSize:     entry.Size,
			ChunkCount:   entry.ChunkCount,
			StartedAt:    stats.StartTime,
			DurationSec:  time.Since(stats.StartTime).Seconds(),
			BytesServed:  stats.BytesServed,
			BlocksServed: stats.BlocksServed,
			PeersServed:  stats.PeersServed,
		}

		if err := s.metricsManager.UpsertSeedMetrics(sm); err != nil {
			logger.Warn("failed to flush seed metrics",
				zap.String("hash", hashStr),
				zap.Error(err))
		}
	}
}

func (s *Server) restoreSeeds() {
	entries := s.seedingState.GetAll()
	if len(entries) == 0 {
		return
	}

	logger.Info("restoring seeded files after restart", zap.Int("count", len(entries)))

	for _, entry := range entries {
		entry := entry
		go func() {
			if _, err := s.seeder.LoadFileFromDisk(entry.FilePath, entry.FallbackURL); err != nil {
				logger.Warn("failed to restore seed after restart — removing stale state entry",
					zap.String("hash", entry.FileHash),
					zap.String("path", entry.FilePath),
					zap.Error(err))
				_ = s.seedingState.RemoveSeeding(entry.FileHash)
				return
			}
			logger.Info("seed restored after restart",
				zap.String("hash", entry.FileHash),
				zap.String("path", entry.FilePath))
		}()
	}
}

func (s *Server) unseedFile(fileHash string) error {
	hash, err := protocol.FileHashFromString(fileHash)
	if err != nil {
		return fmt.Errorf("invalid file hash: %w", err)
	}

	if stats, ok := s.seeder.GetFileStats(hash); ok {
		if entry := s.seedingState.GetByHash(fileHash); entry != nil {
			sm := &metrics.SeedMetricsEntry{
				FileHash:     fileHash,
				FilePath:     entry.FilePath,
				FallbackURL:  entry.FallbackURL,
				FileSize:     entry.Size,
				ChunkCount:   entry.ChunkCount,
				StartedAt:    stats.StartTime,
				DurationSec:  time.Since(stats.StartTime).Seconds(),
				BytesServed:  stats.BytesServed,
				BlocksServed: stats.BlocksServed,
				PeersServed:  stats.PeersServed,
			}
			if err := s.metricsManager.UpsertSeedMetrics(sm); err != nil {
				logger.Warn("failed to save final seed metrics on unseed",
					zap.String("hash", fileHash), zap.Error(err))
			}
		}
	}

	if err := s.seeder.RemoveFile(hash); err != nil {
		logger.Warn("seeder did not have file, removing state entry anyway",
			zap.String("hash", fileHash), zap.Error(err))
	}

	if err := s.seedingState.RemoveSeeding(fileHash); err != nil {
		return fmt.Errorf("failed to remove from seeding state: %w", err)
	}

	logger.Info("file unseeded", zap.String("hash", fileHash))
	return nil
}
