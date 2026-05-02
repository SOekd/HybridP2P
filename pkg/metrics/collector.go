package metrics

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

type MetricsCollector struct {
	requestID     uint64
	fileHash      string
	fileSize      uint64
	startTime     time.Time
	firstByteTime *time.Time

	bytesReceived   uint64
	packetsReceived uint64
	packetLoss      uint64
	errorCount      int
	retryCount      int

	usedFallback   bool
	chunksFromP2P  uint32
	chunksFromHTTP uint32
	peersConnected map[peer.ID]bool

	chunkTimes      []time.Duration
	connectionStart time.Time
	connectionTime  time.Duration

	mu sync.RWMutex
}

func NewCollector(requestID uint64, fileHash string, fileSize uint64) *MetricsCollector {
	return &MetricsCollector{
		requestID:      requestID,
		fileHash:       fileHash,
		fileSize:       fileSize,
		startTime:      time.Now(),
		peersConnected: make(map[peer.ID]bool),
		chunkTimes:     make([]time.Duration, 0),
	}
}

func (mc *MetricsCollector) RecordFirstByte() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.firstByteTime == nil {
		now := time.Now()
		mc.firstByteTime = &now
	}
}

func (mc *MetricsCollector) RecordPacket(size uint64, success bool, latency time.Duration) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if success {
		mc.bytesReceived += size
		mc.packetsReceived++
		mc.chunkTimes = append(mc.chunkTimes, latency)
	} else {
		mc.packetLoss++
	}
}

func (mc *MetricsCollector) RecordPacketLoss() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.packetLoss++
}

func (mc *MetricsCollector) RecordP2PChunk() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.chunksFromP2P++
}

func (mc *MetricsCollector) RecordHTTPChunk() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.chunksFromHTTP++
	mc.usedFallback = true
}

func (mc *MetricsCollector) RecordPeer(peerID peer.ID) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.peersConnected[peerID] = true
}

func (mc *MetricsCollector) RecordError() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.errorCount++
}

func (mc *MetricsCollector) RecordRetry() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.retryCount++
}

func (mc *MetricsCollector) RecordConnectionTime(duration time.Duration) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.connectionTime = duration
}

func (mc *MetricsCollector) GenerateMetrics(success bool, errorMsg string) *MetricsEntry {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	duration := time.Since(mc.startTime)

	durationSec := duration.Seconds()
	throughputMbps := 0.0
	if durationSec > 0 {
		throughputMbps = float64(mc.bytesReceived*8) / (durationSec * 1000000)
	}

	ttfb := 0.0
	if mc.firstByteTime != nil {
		ttfb = mc.firstByteTime.Sub(mc.startTime).Seconds()
	}

	avgChunkTime := 0.0
	if len(mc.chunkTimes) > 0 {
		var total time.Duration
		for _, t := range mc.chunkTimes {
			total += t
		}
		avgChunkTime = float64(total.Milliseconds()) / float64(len(mc.chunkTimes))
	}

	totalPackets := mc.packetsReceived + mc.packetLoss
	deliveryRate := 100.0
	if totalPackets > 0 {
		deliveryRate = float64(mc.packetsReceived) / float64(totalPackets) * 100
	}

	avgLatency := 0.0
	if len(mc.chunkTimes) > 0 {
		var total time.Duration
		for _, t := range mc.chunkTimes {
			total += t
		}
		avgLatency = float64(total.Milliseconds()) / float64(len(mc.chunkTimes))
	}

	cacheHit := "yes"
	protocol := "p2p"
	if mc.usedFallback {
		cacheHit = "no"
		protocol = "http"
	}

	peerIDs := make([]string, 0, len(mc.peersConnected))
	for peerID := range mc.peersConnected {
		peerIDs = append(peerIDs, peerID.String())
	}

	totalChunks := mc.chunksFromP2P + mc.chunksFromHTTP

	return &MetricsEntry{
		RequestID:          mc.requestID,
		FileHash:           mc.fileHash,
		FileSize:           mc.fileSize,
		Timestamp:          mc.startTime,
		Duration:           durationSec,
		ThroughputMbps:     throughputMbps,
		LatencyMs:          avgLatency,
		TimeToFirstByteSec: ttfb,
		BytesReceived:      mc.bytesReceived,
		PacketsReceived:    mc.packetsReceived,
		PacketLoss:         mc.packetLoss,
		DeliveryRate:       deliveryRate,
		CacheHit:           cacheHit,
		Protocol:           protocol,
		PeersConnected:     len(mc.peersConnected),
		PeerIDs:            peerIDs,
		ChunkCount:         totalChunks,
		ChunksFromP2P:      mc.chunksFromP2P,
		ChunksFromHTTP:     mc.chunksFromHTTP,
		AvgChunkTimeMs:     avgChunkTime,
		ErrorCount:         mc.errorCount,
		ErrorMessage:       errorMsg,
		Success:            success,
		RetryCount:         mc.retryCount,
		ConnectionTimeMs:   float64(mc.connectionTime.Milliseconds()),
	}
}
