package p2p

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"P2P-CDN/internal/logger"
	"P2P-CDN/pkg/protocol"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	libp2ptls "github.com/libp2p/go-libp2p/p2p/security/tls"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"
)

type HostConfig struct {
	ListenPort      int
	DataDir         string
	EnableRelay     bool
	EnableHolePunch bool
	BootstrapPeers  []string
	ExternalIP      string
}

type P2PHost struct {
	Host           host.Host
	Config         *HostConfig
	bootstrapPeers []peer.AddrInfo
}

func NewHost(cfg *HostConfig) (*P2PHost, error) {
	priv, err := loadOrGenerateKey(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to load/generate key: %w", err)
	}

	bootstrapPeers, err := parseBootstrapPeers(cfg.BootstrapPeers)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bootstrap peers: %w", err)
	}

	logger.Info("bootstrap configuration",
		zap.Int("bootstrap_peer_count", len(bootstrapPeers)),
		zap.Bool("enable_relay", cfg.EnableRelay),
		zap.Bool("enable_holepunch", cfg.EnableHolePunch))

	connMgr, err := connmgr.NewConnManager(
		protocol.LowWaterConnections,
		protocol.MaxConnections,
		connmgr.WithGracePeriod(time.Minute),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection manager: %w", err)
	}

	resourceManager, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(rcmgr.InfiniteLimits))
	if err != nil {
		return nil, fmt.Errorf("failed to create resource manager: %w", err)
	}

	opts := []libp2p.Option{
		libp2p.Identity(priv),

		libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", cfg.ListenPort),
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", cfg.ListenPort),
			fmt.Sprintf("/ip6/::/tcp/%d", cfg.ListenPort),
			fmt.Sprintf("/ip6/::/udp/%d/quic-v1", cfg.ListenPort),
		),

		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Transport(quic.NewTransport),

		libp2p.Security(noise.ID, noise.New),
		libp2p.Security(libp2ptls.ID, libp2ptls.New),

		libp2p.ConnectionManager(connMgr),
		libp2p.ResourceManager(resourceManager),

		libp2p.NATPortMap(),
		libp2p.EnableNATService(),
	}

	if cfg.ExternalIP != "" {
		externalAddrs := []multiaddr.Multiaddr{
			mustMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", cfg.ExternalIP, cfg.ListenPort)),
			mustMultiaddr(fmt.Sprintf("/ip4/%s/udp/%d/quic-v1", cfg.ExternalIP, cfg.ListenPort)),
		}
		logger.Info("announcing external address",
			zap.String("external_ip", cfg.ExternalIP),
			zap.Int("port", cfg.ListenPort))
		opts = append(opts, libp2p.AddrsFactory(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
			return append(addrs, externalAddrs...)
		}))
	}

	if cfg.EnableRelay {
		opts = append(opts,
			libp2p.EnableAutoRelayWithStaticRelays(bootstrapPeers),
			libp2p.EnableRelayService(),
		)
	}

	if cfg.EnableHolePunch {
		opts = append(opts, libp2p.EnableHolePunching())
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	logger.Info("libp2p host created",
		zap.String("peer_id", h.ID().String()),
		zap.Strings("addrs", multiaddrsToStrings(h.Addrs())),
		zap.Int("listen_port", cfg.ListenPort))

	return &P2PHost{
		Host:           h,
		Config:         cfg,
		bootstrapPeers: bootstrapPeers,
	}, nil
}

func (p *P2PHost) Close() error {
	logger.Info("closing libp2p host")
	return p.Host.Close()
}

func (p *P2PHost) Connect(ctx context.Context, pi peer.AddrInfo) error {
	logger.Debug("connecting to peer",
		zap.String("peer_id", pi.ID.String()),
		zap.Int("addr_count", len(pi.Addrs)))

	if err := p.Host.Connect(ctx, pi); err != nil {
		return fmt.Errorf("failed to connect to peer: %w", err)
	}

	logger.Info("connected to peer", zap.String("peer_id", pi.ID.String()))
	return nil
}

func (p *P2PHost) ConnectBootstrap(ctx context.Context) error {
	logger.Info("connecting to bootstrap peers", zap.Int("count", len(p.bootstrapPeers)))

	var connectedCount int
	for _, pi := range p.bootstrapPeers {
		if err := p.Connect(ctx, pi); err != nil {
			logger.Warn("failed to connect to bootstrap peer",
				zap.String("peer_id", pi.ID.String()),
				zap.Error(err))
			continue
		}
		connectedCount++
	}

	if connectedCount == 0 {
		return fmt.Errorf("failed to connect to any bootstrap peers")
	}

	logger.Info("connected to bootstrap peers",
		zap.Int("connected", connectedCount),
		zap.Int("total", len(p.bootstrapPeers)))

	return nil
}

func (p *P2PHost) GetPeerInfo() peer.AddrInfo {
	return peer.AddrInfo{
		ID:    p.Host.ID(),
		Addrs: p.Host.Addrs(),
	}
}

func loadOrGenerateKey(dataDir string) (crypto.PrivKey, error) {
	keyPath := filepath.Join(dataDir, "peer.key")

	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	if _, err := os.Stat(keyPath); err == nil {
		keyData, err := os.ReadFile(keyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read key file: %w", err)
		}

		priv, err := crypto.UnmarshalPrivateKey(keyData)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal private key: %w", err)
		}

		logger.Info("loaded existing peer identity", zap.String("key_path", keyPath))
		return priv, nil
	}

	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key: %w", err)
	}

	keyData, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal private key: %w", err)
	}

	if err := os.WriteFile(keyPath, keyData, 0600); err != nil {
		return nil, fmt.Errorf("failed to write key file: %w", err)
	}

	logger.Info("generated new peer identity", zap.String("key_path", keyPath))
	return priv, nil
}

func mustMultiaddr(s string) multiaddr.Multiaddr {
	ma, err := multiaddr.NewMultiaddr(s)
	if err != nil {
		panic(fmt.Sprintf("invalid multiaddr %q: %v", s, err))
	}
	return ma
}

func parseBootstrapPeers(addrs []string) ([]peer.AddrInfo, error) {
	var peers []peer.AddrInfo

	for _, addr := range addrs {
		ma, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			logger.Warn("invalid bootstrap peer address", zap.String("addr", addr), zap.Error(err))
			continue
		}

		pi, err := peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			logger.Warn("failed to parse peer info", zap.String("addr", addr), zap.Error(err))
			continue
		}

		peers = append(peers, *pi)
	}

	if len(peers) == 0 {
		return nil, fmt.Errorf("no valid bootstrap peers")
	}

	return peers, nil
}

func MultiaddrsToStrings(addrs []multiaddr.Multiaddr) []string {
	result := make([]string, len(addrs))
	for i, addr := range addrs {
		result[i] = addr.String()
	}
	return result
}

func multiaddrsToStrings(addrs []multiaddr.Multiaddr) []string {
	return MultiaddrsToStrings(addrs)
}

func (p *P2PHost) GetConnectedPeers() []peer.ID {
	return p.Host.Network().Peers()
}

func (p *P2PHost) GetPeerCount() int {
	return len(p.GetConnectedPeers())
}

func (p *P2PHost) IsConnected(peerID peer.ID) bool {
	conns := p.Host.Network().ConnsToPeer(peerID)
	return len(conns) > 0
}

func (p *P2PHost) GetNATType() protocol.NATType {
	addrs := p.Host.Addrs()

	hasRelay := false
	hasPublic := false

	for _, addr := range addrs {
		addrStr := addr.String()
		if containsString(addrStr, "/p2p-circuit") {
			hasRelay = true
		}
		if isPublicAddr(addr) {
			hasPublic = true
		}
	}

	if hasPublic {
		return protocol.NATTypeOpen
	} else if hasRelay {
		return protocol.NATTypeStrict
	} else {
		return protocol.NATTypeModerate
	}
}

func (p *P2PHost) GetPublicAddrs() []multiaddr.Multiaddr {
	var publicAddrs []multiaddr.Multiaddr

	for _, addr := range p.Host.Addrs() {
		if isPublicAddr(addr) || isRelayAddr(addr) {
			publicAddrs = append(publicAddrs, addr)
		}
	}

	return publicAddrs
}

func (p *P2PHost) WaitForPublicAddr(ctx context.Context, timeout time.Duration) bool {
	logger.Info("waiting for public address discovery",
		zap.Duration("timeout", timeout))

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			publicAddrs := p.GetPublicAddrs()
			if len(publicAddrs) > 0 {
				logger.Info("public address discovered",
					zap.Strings("addrs", multiaddrsToStrings(publicAddrs)))
				return true
			}

			if time.Now().After(deadline) {
				logger.Warn("timeout waiting for public address discovery")
				return false
			}
		}
	}
}

func isPublicAddr(addr multiaddr.Multiaddr) bool {
	ipStr, err := addr.ValueForProtocol(multiaddr.P_IP4)
	if err != nil {
		ipStr, err = addr.ValueForProtocol(multiaddr.P_IP6)
		if err != nil {
			return false
		}
	}

	return !isPrivateIP(ipStr) && !isLoopbackIP(ipStr)
}

func isRelayAddr(addr multiaddr.Multiaddr) bool {
	return containsString(addr.String(), "/p2p-circuit")
}

func isPrivateIP(ipStr string) bool {
	return containsString(ipStr, "192.168.") ||
		containsString(ipStr, "10.") ||
		containsString(ipStr, "172.16.") ||
		containsString(ipStr, "172.17.") ||
		containsString(ipStr, "172.18.") ||
		containsString(ipStr, "172.19.") ||
		containsString(ipStr, "172.20.") ||
		containsString(ipStr, "172.21.") ||
		containsString(ipStr, "172.22.") ||
		containsString(ipStr, "172.23.") ||
		containsString(ipStr, "172.24.") ||
		containsString(ipStr, "172.25.") ||
		containsString(ipStr, "172.26.") ||
		containsString(ipStr, "172.27.") ||
		containsString(ipStr, "172.28.") ||
		containsString(ipStr, "172.29.") ||
		containsString(ipStr, "172.30.") ||
		containsString(ipStr, "172.31.") ||
		containsString(ipStr, "fc00:") ||
		containsString(ipStr, "fd00:")
}

func isLoopbackIP(ipStr string) bool {
	return ipStr == "127.0.0.1" ||
		ipStr == "::1" ||
		containsString(ipStr, "127.")
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr || len(s) > len(substr) &&
			(s[:len(substr)] == substr ||
				findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
