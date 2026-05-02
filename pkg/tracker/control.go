package tracker

import (
	"encoding/json"
	"sync"

	"P2P-CDN/internal/logger"
	"P2P-CDN/pkg/protocol"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

type ControlState struct {
	SeedingPaused   bool  `json:"seeding_paused"`
	DownloadsPaused bool  `json:"downloads_paused"`
	MaxBandwidthBps int64 `json:"max_bandwidth_bps"`
}

type daemonConn struct {
	conn   *websocket.Conn
	peerID string
	mu     sync.Mutex
}

type ControlPlane struct {
	mu    sync.RWMutex
	state ControlState

	daemonsMu sync.RWMutex
	daemons   map[*websocket.Conn]*daemonConn
}

func NewControlPlane() *ControlPlane {
	return &ControlPlane{
		daemons: make(map[*websocket.Conn]*daemonConn),
	}
}

func (cp *ControlPlane) RegisterDaemon(conn *websocket.Conn, peerID string) {
	dc := &daemonConn{conn: conn, peerID: peerID}

	cp.daemonsMu.Lock()
	cp.daemons[conn] = dc
	cp.daemonsMu.Unlock()

	logger.Info("daemon connected to control plane", zap.String("peer_id", peerID))

	cp.mu.RLock()
	state := cp.state
	cp.mu.RUnlock()

	if state.SeedingPaused {
		cp.sendTo(dc, protocol.TrackerControlMessage{Type: "pause_seeds"})
	}
	if state.DownloadsPaused {
		cp.sendTo(dc, protocol.TrackerControlMessage{Type: "pause_downloads"})
	}
	if state.MaxBandwidthBps > 0 {
		cp.sendTo(dc, protocol.TrackerControlMessage{Type: "set_bandwidth", Value: state.MaxBandwidthBps})
	}
}

func (cp *ControlPlane) UnregisterDaemon(conn *websocket.Conn) {
	cp.daemonsMu.Lock()
	dc, ok := cp.daemons[conn]
	delete(cp.daemons, conn)
	cp.daemonsMu.Unlock()

	if ok {
		logger.Info("daemon disconnected from control plane", zap.String("peer_id", dc.peerID))
	}
}

func (cp *ControlPlane) GetState() ControlState {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	return cp.state
}

func (cp *ControlPlane) ConnectedDaemons() []string {
	cp.daemonsMu.RLock()
	defer cp.daemonsMu.RUnlock()

	ids := make([]string, 0, len(cp.daemons))
	for _, dc := range cp.daemons {
		ids = append(ids, dc.peerID)
	}
	return ids
}

func (cp *ControlPlane) PauseSeeds() {
	cp.mu.Lock()
	cp.state.SeedingPaused = true
	cp.mu.Unlock()
	cp.broadcast(protocol.TrackerControlMessage{Type: "pause_seeds"})
}

func (cp *ControlPlane) ResumeSeeds() {
	cp.mu.Lock()
	cp.state.SeedingPaused = false
	cp.mu.Unlock()
	cp.broadcast(protocol.TrackerControlMessage{Type: "resume_seeds"})
}

func (cp *ControlPlane) PauseDownloads() {
	cp.mu.Lock()
	cp.state.DownloadsPaused = true
	cp.mu.Unlock()
	cp.broadcast(protocol.TrackerControlMessage{Type: "pause_downloads"})
}

func (cp *ControlPlane) ResumeDownloads() {
	cp.mu.Lock()
	cp.state.DownloadsPaused = false
	cp.mu.Unlock()
	cp.broadcast(protocol.TrackerControlMessage{Type: "resume_downloads"})
}

func (cp *ControlPlane) ClearMetrics() {
	cp.broadcast(protocol.TrackerControlMessage{Type: "clear_metrics"})
}

func (cp *ControlPlane) SetBandwidth(bps int64) {
	cp.mu.Lock()
	cp.state.MaxBandwidthBps = bps
	cp.mu.Unlock()
	cp.broadcast(protocol.TrackerControlMessage{Type: "set_bandwidth", Value: bps})
}

func (cp *ControlPlane) broadcast(msg protocol.TrackerControlMessage) {
	data, err := json.Marshal(msg)
	if err != nil {
		logger.Error("failed to marshal control message", zap.Error(err))
		return
	}

	cp.daemonsMu.RLock()
	dcs := make([]*daemonConn, 0, len(cp.daemons))
	for _, dc := range cp.daemons {
		dcs = append(dcs, dc)
	}
	cp.daemonsMu.RUnlock()

	for _, dc := range dcs {
		dc.mu.Lock()
		if err := dc.conn.WriteMessage(websocket.TextMessage, data); err != nil {
			logger.Warn("failed to send control message to daemon",
				zap.String("peer_id", dc.peerID),
				zap.Error(err))
		}
		dc.mu.Unlock()
	}
}

func (cp *ControlPlane) sendTo(dc *daemonConn, msg protocol.TrackerControlMessage) {
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	dc.mu.Lock()
	_ = dc.conn.WriteMessage(websocket.TextMessage, data)
	dc.mu.Unlock()
}
