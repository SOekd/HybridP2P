package tracker

import (
	"context"

	"P2P-CDN/pkg/protocol"
)

type NATCoordinator struct {
	store *Store
}

func NewNATCoordinator(store *Store) *NATCoordinator {
	return &NATCoordinator{
		store: store,
	}
}

func (n *NATCoordinator) GetTraversalStrategy(peer1Type, peer2Type protocol.NATType) TraversalStrategy {
	if peer1Type == protocol.NATTypeOpen || peer2Type == protocol.NATTypeOpen {
		return StrategyDirect
	}

	if peer1Type != protocol.NATTypeStrict && peer2Type != protocol.NATTypeStrict {
		return StrategyHolePunch
	}

	return StrategyRelay
}

func (n *NATCoordinator) GetRelayServers(ctx context.Context) ([]protocol.RelayServer, error) {
	relays, err := n.store.GetRelayServers(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]protocol.RelayServer, len(relays))
	for i, relay := range relays {
		result[i] = protocol.RelayServer{
			PeerID: relay.PeerID,
			Addrs:  relay.Addrs,
		}
	}

	return result, nil
}

func (n *NATCoordinator) GetNATInfo(ctx context.Context) (*protocol.GetNATInfoResponse, error) {
	relays, err := n.GetRelayServers(ctx)
	if err != nil {
		return nil, err
	}

	return &protocol.GetNATInfoResponse{
		RelayServers: relays,
		STUNServers: []string{
			"stun:stun.l.google.com:19302",
			"stun:stun1.l.google.com:19302",
			"stun:stun2.l.google.com:19302",
		},
	}, nil
}

type TraversalStrategy int

const (
	StrategyDirect TraversalStrategy = iota

	StrategyHolePunch

	StrategyRelay
)

func (s TraversalStrategy) String() string {
	switch s {
	case StrategyDirect:
		return "direct"
	case StrategyHolePunch:
		return "holepunch"
	case StrategyRelay:
		return "relay"
	default:
		return "unknown"
	}
}

func (s TraversalStrategy) EstimateSuccessRate() float64 {
	switch s {
	case StrategyDirect:
		return 0.95
	case StrategyHolePunch:
		return 0.80
	case StrategyRelay:
		return 0.99
	default:
		return 0.0
	}
}
