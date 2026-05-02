package p2p

import (
	"context"

	"P2P-CDN/internal/logger"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"
)

type RelayManager struct {
	host host.Host
}

func (r *RelayManager) ConnectViaRelay(ctx context.Context, relayPeer peer.AddrInfo, targetPeer peer.ID) error {
	logger.Info("attempting relay connection",
		zap.String("relay", relayPeer.ID.String()),
		zap.String("target", targetPeer.String()))

	if err := r.host.Connect(ctx, relayPeer); err != nil {
		logger.Warn("failed to connect to relay", zap.Error(err))
		return err
	}

	logger.Info("connected to relay", zap.String("relay", relayPeer.ID.String()))

	return nil
}

func (r *RelayManager) IsUsingRelay(peerID peer.ID) bool {
	conns := r.host.Network().ConnsToPeer(peerID)
	for _, conn := range conns {
		for _, proto := range conn.RemoteMultiaddr().Protocols() {
			if proto.Name == "p2p-circuit" {
				return true
			}
		}
	}
	return false
}

func (r *RelayManager) GetRelayConnections() []peer.ID {
	var relayPeers []peer.ID

	for _, peerID := range r.host.Network().Peers() {
		if r.IsUsingRelay(peerID) {
			relayPeers = append(relayPeers, peerID)
		}
	}

	return relayPeers
}

func (r *RelayManager) GetRelayCount() int {
	return len(r.GetRelayConnections())
}
