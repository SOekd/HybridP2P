package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"P2P-CDN/internal/logger"
	"P2P-CDN/pkg/protocol"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"
)

type TrackerClient struct {
	baseURL  string
	client   *http.Client
	peerInfo peer.AddrInfo
}

func NewTrackerClient(trackerURL string, peerInfo peer.AddrInfo) *TrackerClient {
	return &TrackerClient{
		baseURL: trackerURL,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		peerInfo: peerInfo,
	}
}

func (t *TrackerClient) UpdatePeerInfo(peerInfo peer.AddrInfo) {
	t.peerInfo = peerInfo
}

func (t *TrackerClient) RegisterFile(ctx context.Context, req *protocol.RegisterFileRequest) error {
	logger.Debug("registering file with tracker",
		zap.String("hash", req.Hash))

	url := fmt.Sprintf("%s/api/v1/files/register", t.baseURL)

	respData, err := t.doRequest(ctx, "POST", url, req)
	if err != nil {
		return fmt.Errorf("failed to register file: %w", err)
	}

	var resp protocol.RegisterFileResponse
	if err := json.Unmarshal(respData, &resp); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("registration failed: %s", resp.Message)
	}

	logger.Info("file registered with tracker", zap.String("hash", req.Hash))

	return nil
}

func (t *TrackerClient) AnnouncePeer(ctx context.Context, fileHash string) error {
	return t.AnnouncePeerWithNAT(ctx, fileHash, protocol.NATTypeUnknown)
}

func (t *TrackerClient) AnnouncePeerWithNAT(ctx context.Context, fileHash string, natType protocol.NATType) error {
	logger.Debug("announcing peer to tracker",
		zap.String("hash", fileHash),
		zap.String("nat_type", string(natType)))

	var addrs []string
	for _, addr := range t.peerInfo.Addrs {
		addrStr := addr.String()
		if !isLoopbackAddr(addrStr) {
			addrs = append(addrs, addrStr)
		}
	}

	if len(addrs) < len(t.peerInfo.Addrs) {
		logger.Debug("filtered loopback addresses",
			zap.Int("original", len(t.peerInfo.Addrs)),
			zap.Int("usable", len(addrs)))
	}

	req := &protocol.AnnouncePeerRequest{
		PeerID:     t.peerInfo.ID.String(),
		Addrs:      addrs,
		FileHashes: []string{fileHash},
		NATType:    string(natType),
	}

	url := fmt.Sprintf("%s/api/v1/peers/announce", t.baseURL)

	respData, err := t.doRequest(ctx, "POST", url, req)
	if err != nil {
		return fmt.Errorf("failed to announce peer: %w", err)
	}

	var resp protocol.AnnouncePeerResponse
	if err := json.Unmarshal(respData, &resp); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("announce failed: %s", resp.Message)
	}

	return nil
}

func isLoopbackAddr(addrStr string) bool {
	return containsStr(addrStr, "127.0.0.1") ||
		containsStr(addrStr, "/127.") ||
		containsStr(addrStr, "::1/")
}

func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func (t *TrackerClient) GetPeers(ctx context.Context, fileHash string) ([]peer.AddrInfo, *protocol.FileMetadata, error) {
	logger.Debug("getting peers from tracker", zap.String("hash", fileHash))

	url := fmt.Sprintf("%s/api/v1/peers/%s", t.baseURL, fileHash)

	respData, err := t.doRequest(ctx, "GET", url, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get peers: %w", err)
	}

	var resp protocol.GetPeersResponse
	if err := json.Unmarshal(respData, &resp); err != nil {
		return nil, nil, fmt.Errorf("failed to parse response: %w", err)
	}

	peers := make([]peer.AddrInfo, 0, len(resp.Peers))
	for _, p := range resp.Peers {
		peerID, err := peer.Decode(p.PeerID)
		if err != nil {
			logger.Warn("failed to decode peer ID",
				zap.String("peer_id", p.PeerID),
				zap.Error(err))
			continue
		}

		if peerID == t.peerInfo.ID {
			logger.Debug("skipping self peer from tracker",
				zap.String("peer_id", peerID.String()))
			continue
		}

		addrs := make([]multiaddr.Multiaddr, 0, len(p.Addrs))
		for _, addrStr := range p.Addrs {
			maddr, err := multiaddr.NewMultiaddr(addrStr)
			if err != nil {
				logger.Warn("failed to parse multiaddr",
					zap.String("peer_id", p.PeerID),
					zap.String("addr", addrStr),
					zap.Error(err))
				continue
			}
			addrs = append(addrs, maddr)
		}

		peers = append(peers, peer.AddrInfo{
			ID:    peerID,
			Addrs: addrs,
		})
	}

	hashParsed, err := protocol.FileHashFromString(fileHash)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid file hash: %w", err)
	}

	metadata := &protocol.FileMetadata{
		Hash:        hashParsed,
		Size:        resp.Size,
		ChunkSize:   resp.ChunkSize,
		ChunkCount:  resp.ChunkCount,
		Chunks:      resp.Chunks,
		FallbackURL: resp.FallbackURL,
	}

	logger.Debug("retrieved peers from tracker",
		zap.String("hash", fileHash),
		zap.Int("count", len(peers)))

	return peers, metadata, nil
}

func (t *TrackerClient) UpdateStatus(ctx context.Context, fileHashes []string) error {
	req := &protocol.UpdateStatusRequest{
		PeerID:     t.peerInfo.ID.String(),
		FileHashes: fileHashes,
	}

	url := fmt.Sprintf("%s/api/v1/peers/status", t.baseURL)

	respData, err := t.doRequest(ctx, "POST", url, req)
	if err != nil {
		return fmt.Errorf("failed to update status: %w", err)
	}

	var resp protocol.UpdateStatusResponse
	if err := json.Unmarshal(respData, &resp); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("update status failed: %s", resp.Message)
	}

	return nil
}

func (t *TrackerClient) GetNATInfo(ctx context.Context) (*protocol.NATInfoResponse, error) {
	url := fmt.Sprintf("%s/api/v1/nat/info", t.baseURL)

	respData, err := t.doRequest(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get NAT info: %w", err)
	}

	var resp protocol.NATInfoResponse
	if err := json.Unmarshal(respData, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &resp, nil
}

func (t *TrackerClient) HealthCheck(ctx context.Context) error {
	url := fmt.Sprintf("%s/health", t.baseURL)

	respData, err := t.doRequest(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	var resp protocol.HealthResponse
	if err := json.Unmarshal(respData, &resp); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if resp.Status != "healthy" {
		return fmt.Errorf("tracker unhealthy: %s", resp.Status)
	}

	return nil
}

func (t *TrackerClient) doRequest(ctx context.Context, method, url string, body interface{}) ([]byte, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(respData))
	}

	return respData, nil
}
