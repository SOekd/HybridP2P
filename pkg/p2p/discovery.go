package p2p

import (
	"context"
	"fmt"
	"time"

	"P2P-CDN/internal/logger"
	"P2P-CDN/pkg/protocol"

	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	"go.uber.org/zap"
)

type Discovery struct {
	dht              *dht.IpfsDHT
	routingDiscovery *drouting.RoutingDiscovery
	host             host.Host
}

func NewDiscovery(ctx context.Context, h host.Host, bootstrapPeers []peer.AddrInfo) (*Discovery, error) {
	kdht, err := dht.New(ctx, h,
		dht.Mode(dht.ModeAutoServer),
		dht.BootstrapPeers(bootstrapPeers...),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create DHT: %w", err)
	}

	routingDiscovery := drouting.NewRoutingDiscovery(kdht)

	logger.Info("DHT discovery created",
		zap.String("peer_id", h.ID().String()),
		zap.Int("bootstrap_peers", len(bootstrapPeers)))

	return &Discovery{
		dht:              kdht,
		routingDiscovery: routingDiscovery,
		host:             h,
	}, nil
}

func (d *Discovery) Bootstrap(ctx context.Context) error {
	logger.Info("bootstrapping DHT")

	if err := d.dht.Bootstrap(ctx); err != nil {
		return fmt.Errorf("failed to bootstrap DHT: %w", err)
	}

	logger.Info("DHT bootstrap complete")
	return nil
}

func (d *Discovery) Provide(ctx context.Context, c cid.Cid) error {
	logger.Debug("providing content", zap.String("cid", c.String()))

	if err := d.dht.Provide(ctx, c, true); err != nil {
		return fmt.Errorf("failed to provide content: %w", err)
	}

	return nil
}

func (d *Discovery) ProvideMany(ctx context.Context, cids []cid.Cid) error {
	logger.Info("providing content", zap.Int("count", len(cids)))

	for _, c := range cids {
		if err := d.Provide(ctx, c); err != nil {
			logger.Warn("failed to provide CID",
				zap.String("cid", c.String()),
				zap.Error(err))
		}
	}

	logger.Info("content provided", zap.Int("count", len(cids)))
	return nil
}

func (d *Discovery) FindProviders(ctx context.Context, c cid.Cid, maxPeers int) ([]peer.AddrInfo, error) {
	logger.Debug("finding providers", zap.String("cid", c.String()), zap.Int("max", maxPeers))

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	providersCh := d.dht.FindProvidersAsync(ctx, c, maxPeers)

	var providers []peer.AddrInfo
	for provider := range providersCh {
		if provider.ID == d.host.ID() {
			continue
		}

		providers = append(providers, provider)

		if len(providers) >= maxPeers {
			break
		}
	}

	logger.Debug("providers found",
		zap.String("cid", c.String()),
		zap.Int("count", len(providers)))

	return providers, nil
}

func (d *Discovery) FindPeer(ctx context.Context, peerID peer.ID) (peer.AddrInfo, error) {
	logger.Debug("finding peer", zap.String("peer_id", peerID.String()))

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	addrInfo, err := d.dht.FindPeer(ctx, peerID)
	if err != nil {
		return peer.AddrInfo{}, fmt.Errorf("failed to find peer: %w", err)
	}

	logger.Debug("peer found",
		zap.String("peer_id", peerID.String()),
		zap.Int("addrs", len(addrInfo.Addrs)))

	return addrInfo, nil
}

func (d *Discovery) Advertise(ctx context.Context, namespace string, ttl time.Duration) error {
	logger.Info("advertising service", zap.String("namespace", namespace))

	_, err := d.routingDiscovery.Advertise(ctx, namespace)
	if err != nil {
		return fmt.Errorf("failed to advertise: %w", err)
	}

	return nil
}

func (d *Discovery) FindPeers(ctx context.Context, namespace string, maxPeers int) ([]peer.AddrInfo, error) {
	logger.Debug("finding peers", zap.String("namespace", namespace))

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	peersCh, err := d.routingDiscovery.FindPeers(ctx, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to find peers: %w", err)
	}

	var peers []peer.AddrInfo
	for peer := range peersCh {
		if peer.ID == d.host.ID() {
			continue
		}

		peers = append(peers, peer)

		if len(peers) >= maxPeers {
			break
		}
	}

	logger.Debug("peers found",
		zap.String("namespace", namespace),
		zap.Int("count", len(peers)))

	return peers, nil
}

func (d *Discovery) AdvertiseFile(ctx context.Context, fileHash string) error {
	namespace := fmt.Sprintf("/p2pcdn/file/%s", fileHash)
	return d.Advertise(ctx, namespace, protocol.PeerAnnounceInterval)
}

func (d *Discovery) FindFilePeers(ctx context.Context, fileHash string, maxPeers int) ([]peer.AddrInfo, error) {
	namespace := fmt.Sprintf("/p2pcdn/file/%s", fileHash)
	return d.FindPeers(ctx, namespace, maxPeers)
}

func (d *Discovery) StartPeriodicAdvertise(ctx context.Context, namespace string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	if err := d.Advertise(ctx, namespace, interval*2); err != nil {
		logger.Error("failed to advertise", zap.Error(err))
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := d.Advertise(ctx, namespace, interval*2); err != nil {
				logger.Error("failed to advertise", zap.Error(err))
			}
		}
	}
}

func (d *Discovery) GetRoutingTable() *RoutingTableInfo {
	return &RoutingTableInfo{
		Size: d.dht.RoutingTable().Size(),
	}
}

type RoutingTableInfo struct {
	Size int
}

func (d *Discovery) Close() error {
	logger.Info("closing DHT discovery")
	return d.dht.Close()
}

func (d *Discovery) ConnectToPeers(ctx context.Context, peers []peer.AddrInfo) int {
	var connectedCount int

	for _, pi := range peers {
		if d.host.Network().Connectedness(pi.ID) == 1 {
			continue
		}

		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if err := d.host.Connect(ctx, pi); err != nil {
			logger.Debug("failed to connect to peer",
				zap.String("peer_id", pi.ID.String()),
				zap.Error(err))
			cancel()
			continue
		}
		cancel()

		connectedCount++
		logger.Debug("connected to peer", zap.String("peer_id", pi.ID.String()))
	}

	return connectedCount
}

func (d *Discovery) DiscoverAndConnect(ctx context.Context, namespace string, maxPeers int) (int, error) {
	logger.Info("discovering and connecting to peers", zap.String("namespace", namespace))

	dutil.Advertise(ctx, d.routingDiscovery, namespace)

	peers, err := d.FindPeers(ctx, namespace, maxPeers)
	if err != nil {
		return 0, fmt.Errorf("failed to find peers: %w", err)
	}

	connected := d.ConnectToPeers(ctx, peers)

	logger.Info("discovery complete",
		zap.String("namespace", namespace),
		zap.Int("found", len(peers)),
		zap.Int("connected", connected))

	return connected, nil
}
