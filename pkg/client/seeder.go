package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"P2P-CDN/internal/logger"
	"P2P-CDN/pkg/p2p"
	"P2P-CDN/pkg/protocol"
	"P2P-CDN/pkg/storage"

	"github.com/ipfs/go-cid"
	"go.uber.org/zap"
)

type Seeder struct {
	exchange  *p2p.BitSwapExchange
	discovery *p2p.Discovery
	tracker   *TrackerClient
	host      *p2p.P2PHost

	seedingMu    sync.RWMutex
	seedingFiles map[protocol.FileHash]*SeedingFile

	cidIndexMu sync.RWMutex
	cidIndex   map[cid.Cid]protocol.FileHash

	announceInterval time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type SeedingFile struct {
	Metadata     *protocol.FileMetadata
	CIDs         []cid.Cid
	StartTime    time.Time
	BytesServed  uint64
	BlocksServed uint64
	PeersServed  int
}

type FileStats struct {
	Hash         protocol.FileHash
	BytesServed  uint64
	BlocksServed uint64
	PeersServed  int
	StartTime    time.Time
}

func NewSeeder(
	host *p2p.P2PHost,
	exchange *p2p.BitSwapExchange,
	discovery *p2p.Discovery,
	tracker *TrackerClient,
	announceInterval time.Duration,
) *Seeder {
	ctx, cancel := context.WithCancel(context.Background())

	return &Seeder{
		host:             host,
		exchange:         exchange,
		discovery:        discovery,
		tracker:          tracker,
		seedingFiles:     make(map[protocol.FileHash]*SeedingFile),
		cidIndex:         make(map[cid.Cid]protocol.FileHash),
		announceInterval: announceInterval,
		ctx:              ctx,
		cancel:           cancel,
	}
}

func (s *Seeder) Start() error {
	logger.Info("starting seeder",
		zap.Duration("announce_interval", s.announceInterval))

	s.wg.Add(1)
	go s.periodicAnnounce()

	return nil
}

func (s *Seeder) Stop() error {
	logger.Info("stopping seeder")

	s.cancel()
	s.wg.Wait()

	return nil
}

func (s *Seeder) AddFile(metadata *protocol.FileMetadata) error {
	cids, err := storage.GetChunkCIDs(metadata)
	if err != nil {
		return fmt.Errorf("failed to get chunk CIDs: %w", err)
	}

	s.seedingMu.Lock()

	if _, exists := s.seedingFiles[metadata.Hash]; exists {
		s.seedingMu.Unlock()
		logger.Debug("file already being seeded", zap.String("hash", metadata.Hash.String()))
		return nil
	}

	logger.Info("adding file to seed",
		zap.String("hash", metadata.Hash.String()),
		zap.Uint32("chunks", metadata.ChunkCount))

	s.seedingFiles[metadata.Hash] = &SeedingFile{
		Metadata:  metadata,
		CIDs:      cids,
		StartTime: time.Now(),
	}

	s.seedingMu.Unlock()

	s.cidIndexMu.Lock()
	for _, c := range cids {
		s.cidIndex[c] = metadata.Hash
	}
	s.cidIndexMu.Unlock()

	hash := metadata.Hash
	go func() {
		if err := s.announceToTracker(hash); err != nil {
			logger.Warn("failed to announce to tracker",
				zap.String("hash", hash.String()),
				zap.Error(err))
		}
		if err := s.announceToDHT(hash, cids); err != nil {
			logger.Warn("failed to announce to DHT",
				zap.String("hash", hash.String()),
				zap.Error(err))
		}
	}()

	return nil
}

func (s *Seeder) RemoveFile(fileHash protocol.FileHash) error {
	s.seedingMu.Lock()
	file, exists := s.seedingFiles[fileHash]
	if !exists {
		s.seedingMu.Unlock()
		return fmt.Errorf("file not being seeded")
	}
	cids := file.CIDs
	delete(s.seedingFiles, fileHash)
	s.seedingMu.Unlock()

	s.cidIndexMu.Lock()
	for _, c := range cids {
		delete(s.cidIndex, c)
	}
	s.cidIndexMu.Unlock()

	logger.Info("removed file from seeding", zap.String("hash", fileHash.String()))

	return nil
}

func (s *Seeder) HasFile(fileHash string) bool {
	s.seedingMu.RLock()
	defer s.seedingMu.RUnlock()

	hash, err := protocol.FileHashFromString(fileHash)
	if err != nil {
		return false
	}

	_, exists := s.seedingFiles[hash]
	return exists
}

func (s *Seeder) UnloadFile(fileHash protocol.FileHash) error {
	return s.RemoveFile(fileHash)
}

func (s *Seeder) GetSeedingFiles() []*SeedingFile {
	s.seedingMu.RLock()
	defer s.seedingMu.RUnlock()

	files := make([]*SeedingFile, 0, len(s.seedingFiles))
	for _, file := range s.seedingFiles {
		files = append(files, file)
	}

	return files
}

func (s *Seeder) GetStats() *SeederStats {
	s.seedingMu.RLock()
	defer s.seedingMu.RUnlock()

	var totalBytes uint64
	var totalPeers int

	for _, file := range s.seedingFiles {
		totalBytes += file.BytesServed
		totalPeers += file.PeersServed
	}

	return &SeederStats{
		FilesSeeding: len(s.seedingFiles),
		BytesServed:  totalBytes,
		PeersServed:  totalPeers,
	}
}

type SeederStats struct {
	FilesSeeding int
	BytesServed  uint64
	PeersServed  int
}

func (s *Seeder) periodicAnnounce() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.announceInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.announceAll()
		}
	}
}

func (s *Seeder) announceAll() {
	s.seedingMu.RLock()
	defer s.seedingMu.RUnlock()

	logger.Debug("announcing seeding files", zap.Int("count", len(s.seedingFiles)))

	for hash := range s.seedingFiles {
		if err := s.announceToTracker(hash); err != nil {
			logger.Warn("failed to announce to tracker",
				zap.String("hash", hash.String()),
				zap.Error(err))
		}
	}
}

func (s *Seeder) announceToTracker(fileHash protocol.FileHash) error {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	publicAddrs := s.host.GetPublicAddrs()
	s.tracker.UpdatePeerInfo(s.host.GetPeerInfo())

	natType := s.host.GetNATType()

	logger.Debug("announcing to tracker",
		zap.String("hash", fileHash.String()),
		zap.String("nat_type", string(natType)),
		zap.Int("public_addrs", len(publicAddrs)))

	if err := s.tracker.AnnouncePeerWithNAT(ctx, fileHash.String(), natType); err != nil {
		return fmt.Errorf("failed to announce peer: %w", err)
	}

	logger.Debug("announced to tracker", zap.String("hash", fileHash.String()))

	return nil
}

func (s *Seeder) announceToDHT(fileHash protocol.FileHash, cids []cid.Cid) error {
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()

	if err := s.discovery.AdvertiseFile(ctx, fileHash.String()); err != nil {
		return fmt.Errorf("failed to advertise file: %w", err)
	}

	logger.Debug("announced to DHT",
		zap.String("hash", fileHash.String()),
		zap.Int("cids", len(cids)))

	return nil
}

func (s *Seeder) UpdateStats(fileHash protocol.FileHash, bytesServed uint64) {
	s.seedingMu.Lock()
	defer s.seedingMu.Unlock()

	if file, exists := s.seedingFiles[fileHash]; exists {
		file.BytesServed += bytesServed
		logger.Debug("updated seeding stats",
			zap.String("hash", fileHash.String()),
			zap.Uint64("bytes", bytesServed))
	}
}

func (s *Seeder) RecordBlockServed(c cid.Cid, size int) {
	s.cidIndexMu.RLock()
	hash, ok := s.cidIndex[c]
	s.cidIndexMu.RUnlock()
	if !ok {
		return
	}

	s.seedingMu.Lock()
	if f, exists := s.seedingFiles[hash]; exists {
		f.BytesServed += uint64(size)
		f.BlocksServed++
	}
	s.seedingMu.Unlock()
}

func (s *Seeder) GetAllStats() []FileStats {
	s.seedingMu.RLock()
	defer s.seedingMu.RUnlock()

	result := make([]FileStats, 0, len(s.seedingFiles))
	for hash, f := range s.seedingFiles {
		result = append(result, FileStats{
			Hash:         hash,
			BytesServed:  f.BytesServed,
			BlocksServed: f.BlocksServed,
			PeersServed:  f.PeersServed,
			StartTime:    f.StartTime,
		})
	}
	return result
}

func (s *Seeder) GetFileStats(hash protocol.FileHash) (FileStats, bool) {
	s.seedingMu.RLock()
	defer s.seedingMu.RUnlock()

	f, ok := s.seedingFiles[hash]
	if !ok {
		return FileStats{}, false
	}
	return FileStats{
		Hash:         hash,
		BytesServed:  f.BytesServed,
		BlocksServed: f.BlocksServed,
		PeersServed:  f.PeersServed,
		StartTime:    f.StartTime,
	}, true
}

func (s *Seeder) LoadFileFromDisk(filePath string, fallbackURL string) (*protocol.FileMetadata, error) {
	logger.Info("loading file for seeding", zap.String("path", filePath))

	chunker := storage.NewChunker(protocol.DefaultChunkSize)

	metadata, err := chunker.ChunkFile(filePath, fallbackURL)
	if err != nil {
		return nil, fmt.Errorf("failed to chunk file: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	for _, chunkInfo := range metadata.Chunks {
		c, err := storage.ChunkToCID(chunkInfo.Hash)
		if err != nil {
			return nil, fmt.Errorf("failed to convert chunk to CID: %w", err)
		}

		has, err := s.exchange.HasBlock(ctx, c)
		if err != nil {
			return nil, fmt.Errorf("failed to check block: %w", err)
		}

		if has {
			continue
		}

		data, err := chunker.ReadChunk(filePath, chunkInfo.Offset, chunkInfo.Size)
		if err != nil {
			return nil, fmt.Errorf("failed to read chunk: %w", err)
		}

		block, err := storage.CreateBlock(data, c)
		if err != nil {
			return nil, fmt.Errorf("failed to create block: %w", err)
		}

		if err := s.exchange.PutBlock(ctx, block); err != nil {
			return nil, fmt.Errorf("failed to store block: %w", err)
		}

		logger.Debug("block stored in blockstore",
			zap.String("cid", c.String()),
			zap.Uint32("chunk_index", chunkInfo.Index))
	}

	logger.Info("file loaded for seeding",
		zap.String("hash", metadata.Hash.String()),
		zap.Uint32("chunks", metadata.ChunkCount))

	if err := s.AddFile(metadata); err != nil {
		return nil, fmt.Errorf("failed to add file to seeding: %w", err)
	}

	return metadata, nil
}
