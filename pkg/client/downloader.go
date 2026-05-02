package client

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"P2P-CDN/internal/logger"
	"P2P-CDN/pkg/metrics"
	"P2P-CDN/pkg/p2p"
	"P2P-CDN/pkg/protocol"
	"P2P-CDN/pkg/storage"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"
)

type Downloader struct {
	exchange  *p2p.BitSwapExchange
	discovery *p2p.Discovery
	tracker   *TrackerClient
	fallback  *HTTPFallback
	host      *p2p.P2PHost

	downloadsMu sync.RWMutex
	downloads   map[protocol.FileHash]*DownloadState

	p2pTimeout       time.Duration
	noProgressWindow time.Duration
	maxPeers         int

	progressChan chan<- DownloadProgress

	metricsManager *metrics.MetricsManager
}

type DownloadState struct {
	Metadata         *protocol.FileMetadata
	OutputPath       string
	StartTime        time.Time
	Downloaded       uint64
	ChunksComplete   uint32
	Status           protocol.DownloadStatusCode
	UsingFallback    bool
	LastProgress     time.Time
	ConnectedPeers   []peer.ID
	FirstByteTime    time.Time
	MetricsCollector *metrics.MetricsCollector
	mu               sync.Mutex
}

func NewDownloader(
	host *p2p.P2PHost,
	exchange *p2p.BitSwapExchange,
	discovery *p2p.Discovery,
	tracker *TrackerClient,
	progressChan chan<- DownloadProgress,
) *Downloader {
	return &Downloader{
		exchange:         exchange,
		discovery:        discovery,
		tracker:          tracker,
		fallback:         NewHTTPFallback(4, progressChan),
		host:             host,
		downloads:        make(map[protocol.FileHash]*DownloadState),
		p2pTimeout:       5 * time.Minute,
		noProgressWindow: 5 * time.Second,
		maxPeers:         20,
		progressChan:     progressChan,
	}
}

func (d *Downloader) SetMetricsManager(mm *metrics.MetricsManager) {
	d.metricsManager = mm
}

func (d *Downloader) DownloadFile(
	ctx context.Context,
	metadata *protocol.FileMetadata,
	outputPath string,
) error {
	logger.Info("starting download",
		zap.String("hash", metadata.Hash.String()),
		zap.Uint64("size", metadata.Size),
		zap.String("output", outputPath))

	var metricsCollector *metrics.MetricsCollector
	if d.metricsManager != nil {
		requestID := d.metricsManager.GetNextRequestID()
		metricsCollector = metrics.NewCollector(requestID, metadata.Hash.String(), metadata.Size)
	}

	state := &DownloadState{
		Metadata:         metadata,
		OutputPath:       outputPath,
		StartTime:        time.Now(),
		Status:           protocol.DownloadStatusDownloading,
		LastProgress:     time.Now(),
		MetricsCollector: metricsCollector,
	}

	d.downloadsMu.Lock()
	d.downloads[metadata.Hash] = state
	d.downloadsMu.Unlock()

	err := d.downloadP2P(ctx, state)
	if err != nil {
		logger.Warn("P2P download failed, falling back to HTTP",
			zap.String("hash", metadata.Hash.String()),
			zap.Error(err))

		state.mu.Lock()
		state.UsingFallback = true
		state.mu.Unlock()

		if err := d.downloadHTTP(ctx, state); err != nil {
			state.mu.Lock()
			state.Status = protocol.DownloadStatusFailed
			state.mu.Unlock()

			if state.MetricsCollector != nil && d.metricsManager != nil {
				entry := state.MetricsCollector.GenerateMetrics(false, err.Error())
				d.metricsManager.AddMetrics(entry)
			}

			return fmt.Errorf("both P2P and HTTP downloads failed: %w", err)
		}
	}

	if err := d.fallback.VerifyDownload(outputPath, metadata); err != nil {
		state.mu.Lock()
		state.Status = protocol.DownloadStatusFailed
		state.mu.Unlock()

		if state.MetricsCollector != nil && d.metricsManager != nil {
			entry := state.MetricsCollector.GenerateMetrics(false, err.Error())
			d.metricsManager.AddMetrics(entry)
		}

		return fmt.Errorf("download verification failed: %w", err)
	}

	state.mu.Lock()
	state.Status = protocol.DownloadStatusComplete
	state.mu.Unlock()

	if state.MetricsCollector != nil && d.metricsManager != nil {
		entry := state.MetricsCollector.GenerateMetrics(true, "")
		d.metricsManager.AddMetrics(entry)
	}

	logger.Info("download complete",
		zap.String("hash", metadata.Hash.String()),
		zap.Duration("duration", time.Since(state.StartTime)))

	return nil
}

func (d *Downloader) downloadP2P(ctx context.Context, state *DownloadState) error {
	logger.Info("attempting P2P download",
		zap.String("hash", state.Metadata.Hash.String()))

	connectStart := time.Now()

	peers, metadata, err := d.findPeers(ctx, state.Metadata.Hash)
	if err != nil {
		return fmt.Errorf("failed to find peers: %w", err)
	}

	if metadata != nil {
		state.Metadata.Size = metadata.Size
		state.Metadata.ChunkSize = metadata.ChunkSize
		state.Metadata.ChunkCount = metadata.ChunkCount
		state.Metadata.Chunks = metadata.Chunks
		if metadata.FallbackURL != "" {
			state.Metadata.FallbackURL = metadata.FallbackURL
		}
	}

	if len(peers) == 0 {
		return fmt.Errorf("no peers found")
	}

	logger.Info("found peers for download",
		zap.String("hash", state.Metadata.Hash.String()),
		zap.Int("count", len(peers)))

	connected := d.connectToPeers(ctx, peers)
	if connected == 0 {
		return fmt.Errorf("failed to connect to any peers")
	}

	if state.MetricsCollector != nil {
		state.MetricsCollector.RecordConnectionTime(time.Since(connectStart))
	}

	logger.Info("connected to peers",
		zap.String("hash", state.Metadata.Hash.String()),
		zap.Int("connected", connected))

	if state.MetricsCollector != nil {
		connectedPeers := d.host.Host.Network().Peers()
		for _, p := range connectedPeers {
			state.MetricsCollector.RecordPeer(p)
		}
	}

	cids, err := storage.GetChunkCIDs(state.Metadata)
	if err != nil {
		return fmt.Errorf("failed to get chunk CIDs: %w", err)
	}

	downloadCtx, cancel := context.WithTimeout(ctx, d.p2pTimeout)
	defer cancel()

	progressDone := make(chan struct{})
	go d.monitorProgress(downloadCtx, state, progressDone)
	defer close(progressDone)

	blockCh, err := d.exchange.GetBlocks(downloadCtx, cids)
	if err != nil {
		return fmt.Errorf("failed to request blocks: %w", err)
	}

	receivedBlocks := make(map[cid.Cid]blocks.Block)
	blockStartTime := time.Now()
	firstBlockReceived := false
	lastLogTime := time.Now()
	batchBytes := uint64(0)
	batchBlocks := 0

	noProgressTimer := time.NewTimer(d.noProgressWindow)
	defer noProgressTimer.Stop()

	for {
		var block blocks.Block
		var ok bool

		select {
		case block, ok = <-blockCh:
			if !ok {
				goto doneReceiving
			}
		case <-noProgressTimer.C:
			if state.MetricsCollector != nil {
				missingBlocks := len(cids) - len(receivedBlocks)
				for i := 0; i < missingBlocks; i++ {
					state.MetricsCollector.RecordPacketLoss()
				}
			}
			return fmt.Errorf("no progress for %v, timing out (%d/%d blocks received)", d.noProgressWindow, len(receivedBlocks), len(cids))
		case <-downloadCtx.Done():
			return fmt.Errorf("download cancelled: %w", downloadCtx.Err())
		}

		blockLatency := time.Since(blockStartTime)

		receivedBlocks[block.Cid()] = block

		noProgressTimer.Reset(d.noProgressWindow)

		if !firstBlockReceived {
			firstBlockReceived = true
			state.mu.Lock()
			state.FirstByteTime = time.Now()
			state.mu.Unlock()

			ttfb := time.Since(state.StartTime)
			if state.MetricsCollector != nil {
				state.MetricsCollector.RecordFirstByte()
				state.MetricsCollector.RecordP2PChunk()
			}
			logger.Info("first byte received", zap.Duration("ttfb", ttfb))
		} else if state.MetricsCollector != nil {
			state.MetricsCollector.RecordP2PChunk()
		}

		blockSize := uint64(len(block.RawData()))
		batchBytes += blockSize
		batchBlocks++

		if state.MetricsCollector != nil {
			state.MetricsCollector.RecordPacket(blockSize, true, blockLatency)
		}

		state.mu.Lock()
		state.ChunksComplete = uint32(len(receivedBlocks))
		state.Downloaded = uint64(state.ChunksComplete) * uint64(state.Metadata.ChunkSize)
		state.LastProgress = time.Now()
		state.mu.Unlock()

		// Log throughput every 2 seconds
		if sinceLog := time.Since(lastLogTime); sinceLog >= 2*time.Second {
			throughputKBps := float64(batchBytes) / sinceLog.Seconds() / 1024
			logger.Info("download progress",
				zap.Int("blocks_received", len(receivedBlocks)),
				zap.Int("blocks_total", len(cids)),
				zap.Float64("percent", float64(len(receivedBlocks))*100/float64(len(cids))),
				zap.Float64("throughput_kbps", throughputKBps),
				zap.Int("batch_blocks", batchBlocks),
				zap.Duration("block_latency", blockLatency),
				zap.Duration("elapsed", time.Since(state.StartTime)))
			batchBytes = 0
			batchBlocks = 0
			lastLogTime = time.Now()
		}

		if len(receivedBlocks) == len(cids) {
			break
		}

		blockStartTime = time.Now()
	}

doneReceiving:

	if len(receivedBlocks) != len(cids) {
		return fmt.Errorf("incomplete download: received %d/%d blocks", len(receivedBlocks), len(cids))
	}

	if err := d.reassembleFile(state.OutputPath, state.Metadata, receivedBlocks, cids); err != nil {
		return fmt.Errorf("failed to reassemble file: %w", err)
	}

	logger.Info("P2P download successful",
		zap.String("hash", state.Metadata.Hash.String()),
		zap.Int("blocks", len(receivedBlocks)))

	return nil
}

func (d *Downloader) downloadHTTP(ctx context.Context, state *DownloadState) error {
	logger.Info("downloading via HTTP fallback",
		zap.String("hash", state.Metadata.Hash.String()),
		zap.String("url", state.Metadata.FallbackURL))

	if state.Metadata.Size == 0 {
		logger.Info("metadata not available, fetching from tracker")
		_, metadata, err := d.tracker.GetPeers(ctx, state.Metadata.Hash.String())
		if err != nil {
			logger.Warn("failed to fetch metadata from tracker for HTTP fallback", zap.Error(err))
		} else if metadata != nil {
			state.Metadata.Size = metadata.Size
			state.Metadata.ChunkSize = metadata.ChunkSize
			state.Metadata.ChunkCount = metadata.ChunkCount
			state.Metadata.Chunks = metadata.Chunks
			if metadata.FallbackURL != "" {
				state.Metadata.FallbackURL = metadata.FallbackURL
			}
			logger.Info("metadata updated from tracker",
				zap.Uint64("size", metadata.Size),
				zap.Uint32("chunks", metadata.ChunkCount))
		}
	}

	if state.Metadata.FallbackURL == "" {
		return fmt.Errorf("no fallback URL available")
	}

	startTime := time.Now()

	err := d.fallback.DownloadFile(ctx, state.Metadata, state.OutputPath)

	if state.MetricsCollector != nil && err == nil {
		fileInfo, statErr := os.Stat(state.OutputPath)
		if statErr == nil {
			fileSize := uint64(fileInfo.Size())
			duration := time.Since(startTime)

			state.MetricsCollector.RecordFirstByte()

			state.MetricsCollector.RecordPacket(fileSize, true, duration)

			chunkCount := state.Metadata.ChunkCount
			if chunkCount == 0 && state.Metadata.Size > 0 {
				chunkCount = uint32((state.Metadata.Size + uint64(state.Metadata.ChunkSize) - 1) / uint64(state.Metadata.ChunkSize))
			}
			if chunkCount == 0 {
				chunkCount = 1
			}
			for i := uint32(0); i < chunkCount; i++ {
				state.MetricsCollector.RecordHTTPChunk()
			}
		}
	}

	return err
}

func (d *Downloader) findPeers(ctx context.Context, fileHash protocol.FileHash) ([]peer.AddrInfo, *protocol.FileMetadata, error) {
	var allPeers []peer.AddrInfo
	var metadata *protocol.FileMetadata

	trackerPeers, trackerMetadata, err := d.tracker.GetPeers(ctx, fileHash.String())
	if err != nil {
		logger.Warn("tracker unavailable, falling back to DHT", zap.Error(err))

		dhtPeers, dhtErr := d.discovery.FindFilePeers(ctx, fileHash.String(), d.maxPeers)
		if dhtErr != nil {
			logger.Warn("failed to find peers from DHT", zap.Error(dhtErr))
		} else {
			allPeers = append(allPeers, dhtPeers...)
		}
	} else {
		allPeers = append(allPeers, trackerPeers...)
		metadata = trackerMetadata

		logger.Debug("tracker query complete",
			zap.String("hash", fileHash.String()),
			zap.Int("peers", len(trackerPeers)))
	}

	uniquePeers := make(map[peer.ID]peer.AddrInfo)
	for _, p := range allPeers {
		if existing, ok := uniquePeers[p.ID]; ok {
			if len(p.Addrs) > len(existing.Addrs) {
				uniquePeers[p.ID] = p
			}
		} else {
			uniquePeers[p.ID] = p
		}
	}

	peers := make([]peer.AddrInfo, 0, len(uniquePeers))
	for _, p := range uniquePeers {
		peers = append(peers, p)
	}

	return peers, metadata, nil
}

func (d *Downloader) connectToPeers(ctx context.Context, peers []peer.AddrInfo) int {
	var connectedCount int
	var mu sync.Mutex

	logger.Info("attempting to connect to peers",
		zap.Int("total_peers", len(peers)))

	sem := make(chan struct{}, 5)
	var wg sync.WaitGroup

	for _, p := range peers {
		wg.Add(1)
		go func(peerInfo peer.AddrInfo) {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			if d.host.IsConnected(peerInfo.ID) {
				mu.Lock()
				connectedCount++
				mu.Unlock()
				logger.Debug("already connected to peer",
					zap.String("peer_id", peerInfo.ID.String()))
				return
			}

			logger.Info("connecting to peer",
				zap.String("peer_id", peerInfo.ID.String()),
				zap.Int("addr_count", len(peerInfo.Addrs)))

			connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			if err := d.host.Connect(connectCtx, peerInfo); err != nil {
				logger.Warn("failed to connect to peer",
					zap.String("peer_id", peerInfo.ID.String()),
					zap.Int("addrs", len(peerInfo.Addrs)),
					zap.Error(err))
				return
			}

			mu.Lock()
			connectedCount++
			mu.Unlock()

			logger.Info("successfully connected to peer",
				zap.String("peer_id", peerInfo.ID.String()))
		}(p)
	}

	wg.Wait()

	logger.Info("peer connection summary",
		zap.Int("attempted", len(peers)),
		zap.Int("connected", connectedCount))

	return connectedCount
}

func (d *Downloader) reassembleFile(
	outputPath string,
	metadata *protocol.FileMetadata,
	receivedBlocks map[cid.Cid]blocks.Block,
	cids []cid.Cid,
) error {
	logger.Info("reassembling file", zap.String("output", outputPath))

	dir := filepath.Dir(outputPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	for i, c := range cids {
		block, exists := receivedBlocks[c]
		if !exists {
			return fmt.Errorf("missing block %d", i)
		}

		if metadata.Chunks != nil && i < len(metadata.Chunks) {
			chunkHash := storage.HashBytes(block.RawData())
			if chunkHash != metadata.Chunks[i].Hash {
				return fmt.Errorf("chunk %d hash mismatch", i)
			}
		}

		if _, err := file.Write(block.RawData()); err != nil {
			return fmt.Errorf("failed to write block %d: %w", i, err)
		}
	}

	logger.Info("file reassembled successfully", zap.String("output", outputPath))

	return nil
}

func (d *Downloader) monitorProgress(ctx context.Context, state *DownloadState, done <-chan struct{}) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	lastDownloaded := uint64(0)
	lastUpdate := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			state.mu.Lock()
			downloaded := state.Downloaded
			chunksComplete := state.ChunksComplete
			state.mu.Unlock()

			timeSinceLastUpdate := time.Since(lastUpdate).Seconds()
			rate := uint64(0)
			if timeSinceLastUpdate > 0 {
				bytesInInterval := downloaded - lastDownloaded
				rate = uint64(float64(bytesInInterval) / timeSinceLastUpdate)
			}
			lastDownloaded = downloaded
			lastUpdate = time.Now()

			if d.progressChan != nil {
				select {
				case d.progressChan <- DownloadProgress{
					FileHash:       state.Metadata.Hash,
					TotalSize:      state.Metadata.Size,
					Downloaded:     downloaded,
					ChunksComplete: chunksComplete,
					ChunksTotal:    state.Metadata.ChunkCount,
					DownloadRate:   rate,
					Status:         "downloading",
					UsingFallback:  state.UsingFallback,
				}:
				default:
				}
			}
		}
	}
}

func (d *Downloader) GetDownloadState(fileHash protocol.FileHash) (*DownloadState, error) {
	d.downloadsMu.RLock()
	defer d.downloadsMu.RUnlock()

	state, exists := d.downloads[fileHash]
	if !exists {
		return nil, fmt.Errorf("download not found")
	}

	return state, nil
}

func (d *Downloader) CancelDownload(fileHash protocol.FileHash) error {
	d.downloadsMu.Lock()
	defer d.downloadsMu.Unlock()

	state, exists := d.downloads[fileHash]
	if !exists {
		return fmt.Errorf("download not found")
	}

	state.mu.Lock()
	state.Status = protocol.DownloadStatusCancelled
	state.mu.Unlock()

	delete(d.downloads, fileHash)

	logger.Info("download cancelled", zap.String("hash", fileHash.String()))

	return nil
}
