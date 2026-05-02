package metrics

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type MetricsEntry struct {
	RequestID uint64    `json:"request_id"`
	FileHash  string    `json:"file_hash"`
	FileSize  uint64    `json:"file_size_bytes"`
	Timestamp time.Time `json:"timestamp"`
	Duration  float64   `json:"duration_seconds"`

	ThroughputMbps     float64 `json:"throughput_mbps"`
	LatencyMs          float64 `json:"latency_ms"`
	TimeToFirstByteSec float64 `json:"time_to_first_byte_seconds"`

	BytesReceived   uint64  `json:"bytes_received"`
	PacketsReceived uint64  `json:"packets_received"`
	PacketLoss      uint64  `json:"packet_loss"`
	DeliveryRate    float64 `json:"delivery_rate_percent"`

	CacheHit       string   `json:"cache_hit"`
	Protocol       string   `json:"protocol"`
	PeersConnected int      `json:"peers_connected"`
	PeerIDs        []string `json:"peer_ids,omitempty"`

	ChunkCount     uint32  `json:"chunk_count"`
	ChunksFromP2P  uint32  `json:"chunks_from_p2p"`
	ChunksFromHTTP uint32  `json:"chunks_from_http"`
	AvgChunkTimeMs float64 `json:"avg_chunk_time_ms"`

	ErrorCount   int    `json:"error_count"`
	ErrorMessage string `json:"error_message,omitempty"`
	Success      bool   `json:"success"`

	RetryCount       int     `json:"retry_count"`
	ConnectionTimeMs float64 `json:"connection_time_ms"`
}

type MetricsDatabase struct {
	Version        string              `json:"version"`
	LastRequestID  uint64              `json:"last_request_id"`
	LastUpdated    time.Time           `json:"last_updated"`
	TotalDownloads int                 `json:"total_downloads"`
	Metrics        []*MetricsEntry     `json:"metrics"`
	SeedMetrics    []*SeedMetricsEntry `json:"seed_metrics"`
}

type SeedMetricsEntry struct {
	FileHash     string    `json:"file_hash"`
	FilePath     string    `json:"file_path"`
	FallbackURL  string    `json:"fallback_url,omitempty"`
	FileSize     uint64    `json:"file_size_bytes"`
	ChunkCount   uint32    `json:"chunk_count"`
	StartedAt    time.Time `json:"started_at"`
	LastUpdated  time.Time `json:"last_updated"`
	DurationSec  float64   `json:"duration_seeding_seconds"`
	BytesServed  uint64    `json:"bytes_served"`
	BlocksServed uint64    `json:"blocks_served"`
	PeersServed  int       `json:"peers_served"`
}

type MetricsManager struct {
	filePath string
	db       *MetricsDatabase
	mu       sync.RWMutex
}

func NewMetricsManager(filePath string) (*MetricsManager, error) {
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create metrics directory: %w", err)
	}

	mm := &MetricsManager{
		filePath: filePath,
		db: &MetricsDatabase{
			Version:        "1.0",
			LastRequestID:  0,
			LastUpdated:    time.Now(),
			TotalDownloads: 0,
			Metrics:        make([]*MetricsEntry, 0),
			SeedMetrics:    make([]*SeedMetricsEntry, 0),
		},
	}

	if err := mm.load(); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to load metrics: %w", err)
		}
	}

	return mm, nil
}

func (mm *MetricsManager) load() error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	data, err := os.ReadFile(mm.filePath)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(data, mm.db); err != nil {
		return fmt.Errorf("failed to parse metrics: %w", err)
	}

	if mm.db.Metrics == nil {
		mm.db.Metrics = make([]*MetricsEntry, 0)
	}
	if mm.db.SeedMetrics == nil {
		mm.db.SeedMetrics = make([]*SeedMetricsEntry, 0)
	}

	return nil
}

func (mm *MetricsManager) save() error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.db.LastUpdated = time.Now()
	mm.db.TotalDownloads = len(mm.db.Metrics)

	data, err := json.MarshalIndent(mm.db, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metrics: %w", err)
	}

	tmpPath := mm.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write metrics: %w", err)
	}

	if err := os.Rename(tmpPath, mm.filePath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename metrics file: %w", err)
	}

	return nil
}

func (mm *MetricsManager) GetNextRequestID() uint64 {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.db.LastRequestID++
	return mm.db.LastRequestID
}

func (mm *MetricsManager) AddMetrics(entry *MetricsEntry) error {
	mm.mu.Lock()
	mm.db.Metrics = append(mm.db.Metrics, entry)
	mm.mu.Unlock()

	return mm.save()
}

func (mm *MetricsManager) GetMetrics() []*MetricsEntry {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	result := make([]*MetricsEntry, len(mm.db.Metrics))
	copy(result, mm.db.Metrics)
	return result
}

func (mm *MetricsManager) GetMetricsByHash(fileHash string) []*MetricsEntry {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	result := make([]*MetricsEntry, 0)
	for _, entry := range mm.db.Metrics {
		if entry.FileHash == fileHash {
			result = append(result, entry)
		}
	}
	return result
}

func (mm *MetricsManager) GetLatestMetrics(count int) []*MetricsEntry {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	start := len(mm.db.Metrics) - count
	if start < 0 {
		start = 0
	}

	result := make([]*MetricsEntry, len(mm.db.Metrics[start:]))
	copy(result, mm.db.Metrics[start:])
	return result
}

func (mm *MetricsManager) GetStats() *AggregateStats {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	stats := &AggregateStats{
		TotalDownloads: len(mm.db.Metrics),
	}

	if len(mm.db.Metrics) == 0 {
		return stats
	}

	var totalThroughput, totalLatency, totalTTFB float64
	var totalBytes, totalPackets uint64
	var p2pHits, httpFallbacks int
	var successCount int

	for _, entry := range mm.db.Metrics {
		totalThroughput += entry.ThroughputMbps
		totalLatency += entry.LatencyMs
		totalTTFB += entry.TimeToFirstByteSec
		totalBytes += entry.BytesReceived
		totalPackets += entry.PacketsReceived

		if entry.CacheHit == "yes" {
			p2pHits++
		} else {
			httpFallbacks++
		}

		if entry.Success {
			successCount++
		}
	}

	count := float64(len(mm.db.Metrics))
	stats.AvgThroughputMbps = totalThroughput / count
	stats.AvgLatencyMs = totalLatency / count
	stats.AvgTTFBSec = totalTTFB / count
	stats.TotalBytesTransferred = totalBytes
	stats.TotalPacketsReceived = totalPackets
	stats.P2PHitRate = float64(p2pHits) / count * 100
	stats.HTTPFallbackRate = float64(httpFallbacks) / count * 100
	stats.SuccessRate = float64(successCount) / count * 100

	return stats
}

type AggregateStats struct {
	TotalDownloads        int     `json:"total_downloads"`
	AvgThroughputMbps     float64 `json:"avg_throughput_mbps"`
	AvgLatencyMs          float64 `json:"avg_latency_ms"`
	AvgTTFBSec            float64 `json:"avg_ttfb_seconds"`
	TotalBytesTransferred uint64  `json:"total_bytes_transferred"`
	TotalPacketsReceived  uint64  `json:"total_packets_received"`
	P2PHitRate            float64 `json:"p2p_hit_rate_percent"`
	HTTPFallbackRate      float64 `json:"http_fallback_rate_percent"`
	SuccessRate           float64 `json:"success_rate_percent"`
}

func (mm *MetricsManager) UpsertSeedMetrics(entry *SeedMetricsEntry) error {
	entry.LastUpdated = time.Now()

	mm.mu.Lock()
	found := false
	for i, e := range mm.db.SeedMetrics {
		if e.FileHash == entry.FileHash {
			mm.db.SeedMetrics[i] = entry
			found = true
			break
		}
	}
	if !found {
		mm.db.SeedMetrics = append(mm.db.SeedMetrics, entry)
	}
	mm.mu.Unlock()

	return mm.save()
}

func (mm *MetricsManager) GetSeedMetrics() []*SeedMetricsEntry {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	result := make([]*SeedMetricsEntry, len(mm.db.SeedMetrics))
	copy(result, mm.db.SeedMetrics)
	return result
}

func (mm *MetricsManager) ClearMetrics() error {
	mm.mu.Lock()
	mm.db.Metrics = make([]*MetricsEntry, 0)
	mm.db.SeedMetrics = make([]*SeedMetricsEntry, 0)
	mm.db.TotalDownloads = 0
	mm.db.LastRequestID = 0
	mm.mu.Unlock()

	return mm.save()
}
