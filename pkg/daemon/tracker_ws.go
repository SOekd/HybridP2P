package daemon

import (
	"context"
	"encoding/json"
	"net/url"
	"time"

	"P2P-CDN/internal/logger"
	"P2P-CDN/pkg/protocol"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

type TrackerWSClient struct {
	trackerURL string
	peerID     string
	server     *Server
}

func NewTrackerWSClient(trackerURL, peerID string, server *Server) *TrackerWSClient {
	return &TrackerWSClient{
		trackerURL: trackerURL,
		peerID:     peerID,
		server:     server,
	}
}

func (tc *TrackerWSClient) Run(ctx context.Context) {
	wsURL, err := tc.buildWSURL()
	if err != nil {
		logger.Error("tracker control WS: invalid tracker URL",
			zap.String("url", tc.trackerURL),
			zap.Error(err))
		return
	}

	backoff := time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		logger.Info("connecting to tracker control WS", zap.String("url", wsURL))

		conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
		if err != nil {
			logger.Warn("tracker control WS: connection failed, retrying",
				zap.Error(err),
				zap.Duration("backoff", backoff))
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}

		backoff = time.Second
		logger.Info("tracker control WS: connected")

		hello := protocol.DaemonHelloMessage{
			Type:    "daemon_hello",
			PeerID:  tc.peerID,
			Version: "1.0.0",
		}
		if data, err := json.Marshal(hello); err == nil {
			_ = conn.WriteMessage(websocket.TextMessage, data)
		}

		tc.readLoop(ctx, conn)
		conn.Close()

		logger.Info("tracker control WS: disconnected, will reconnect")
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (tc *TrackerWSClient) readLoop(ctx context.Context, conn *websocket.Conn) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, data, err := conn.ReadMessage()
		if err != nil {
			logger.Debug("tracker control WS: read error", zap.Error(err))
			return
		}

		var msg protocol.TrackerControlMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			logger.Warn("tracker control WS: invalid message", zap.Error(err))
			continue
		}

		tc.applyCommand(msg)
	}
}

func (tc *TrackerWSClient) applyCommand(msg protocol.TrackerControlMessage) {
	switch msg.Type {
	case "pause_seeds":
		tc.server.seedGate.Pause()
		logger.Info("tracker command: seeds paused")

	case "resume_seeds":
		tc.server.seedGate.Resume()
		logger.Info("tracker command: seeds resumed")

	case "pause_downloads":
		tc.server.downloadGate.Pause()
		logger.Info("tracker command: downloads paused")

	case "resume_downloads":
		tc.server.downloadGate.Resume()
		logger.Info("tracker command: downloads resumed")

	case "set_bandwidth":
		tc.server.rlBlockstore.SetBandwidth(msg.Value)
		if msg.Value == 0 {
			logger.Info("tracker command: bandwidth limit removed (unlimited)")
		} else {
			logger.Info("tracker command: bandwidth limit set", zap.Int64("bps", msg.Value))
		}

	case "clear_metrics":
		if err := tc.server.metricsManager.ClearMetrics(); err != nil {
			logger.Warn("tracker command: failed to clear metrics", zap.Error(err))
		} else {
			logger.Info("tracker command: metrics cleared")
		}

	case "ping":

	default:
		logger.Warn("tracker control WS: unknown command type", zap.String("type", msg.Type))
	}
}

func (tc *TrackerWSClient) buildWSURL() (string, error) {
	u, err := url.Parse(tc.trackerURL)
	if err != nil {
		return "", err
	}

	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	default:
		u.Scheme = "ws"
	}

	u.Path = "/ws/daemons"
	return u.String(), nil
}
