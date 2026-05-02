package p2p

import (
	"context"
	"time"

	"P2P-CDN/internal/logger"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"
)

type HolePunchManager struct {
	host host.Host
}

func (hp *HolePunchManager) AttemptHolePunch(ctx context.Context, peerID peer.ID) error {
	logger.Info("attempting hole punch", zap.String("peer_id", peerID.String()))

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	peerInfo := hp.host.Peerstore().PeerInfo(peerID)
	if len(peerInfo.Addrs) == 0 {
		logger.Warn("no addresses known for peer", zap.String("peer_id", peerID.String()))
		return nil
	}

	if err := hp.host.Connect(ctx, peerInfo); err != nil {
		logger.Warn("hole punch failed",
			zap.String("peer_id", peerID.String()),
			zap.Error(err))
		return err
	}

	logger.Info("hole punch successful", zap.String("peer_id", peerID.String()))
	return nil
}

func (hp *HolePunchManager) IsDirectConnection(peerID peer.ID) bool {
	conns := hp.host.Network().ConnsToPeer(peerID)
	for _, conn := range conns {
		isDirect := true
		for _, proto := range conn.RemoteMultiaddr().Protocols() {
			if proto.Name == "p2p-circuit" {
				isDirect = false
				break
			}
		}
		if isDirect {
			return true
		}
	}
	return false
}

func (hp *HolePunchManager) GetDirectConnections() []peer.ID {
	var directPeers []peer.ID

	for _, peerID := range hp.host.Network().Peers() {
		if hp.IsDirectConnection(peerID) {
			directPeers = append(directPeers, peerID)
		}
	}

	return directPeers
}

func (hp *HolePunchManager) GetDirectConnectionCount() int {
	return len(hp.GetDirectConnections())
}

func (hp *HolePunchManager) UpgradeRelayToDirect(ctx context.Context, peerID peer.ID) error {
	logger.Info("attempting to upgrade relay to direct connection",
		zap.String("peer_id", peerID.String()))

	if hp.IsDirectConnection(peerID) {
		logger.Debug("connection already direct", zap.String("peer_id", peerID.String()))
		return nil
	}

	return hp.AttemptHolePunch(ctx, peerID)
}
