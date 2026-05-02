package tracker

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"P2P-CDN/internal/logger"
	"P2P-CDN/pkg/protocol"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type Handler struct {
	store          *Store
	natCoordinator *NATCoordinator
}

func NewHandler(store *Store) *Handler {
	return &Handler{
		store:          store,
		natCoordinator: NewNATCoordinator(store),
	}
}

func (h *Handler) RegisterFile(c *gin.Context) {
	var req protocol.RegisterFileRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		logger.Warn("invalid register file request", zap.Error(err))
		c.JSON(http.StatusBadRequest, protocol.ErrorResponse{
			Error:   "invalid_request",
			Message: err.Error(),
			Code:    http.StatusBadRequest,
		})
		return
	}

	if req.Hash == "" || req.FallbackURL == "" {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponse{
			Error:   "invalid_request",
			Message: "hash and fallback_url are required",
			Code:    http.StatusBadRequest,
		})
		return
	}

	if err := h.store.RegisterFile(c.Request.Context(), &req); err != nil {
		logger.Error("failed to register file", zap.Error(err), zap.String("hash", req.Hash))
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponse{
			Error:   "internal_error",
			Message: "failed to register file",
			Code:    http.StatusInternalServerError,
		})
		return
	}

	logger.Info("file registered",
		zap.String("hash", req.Hash),
		zap.Uint64("size", req.Size),
		zap.String("fallback_url", req.FallbackURL))

	c.JSON(http.StatusOK, protocol.RegisterFileResponse{
		Success: true,
		Message: "file registered successfully",
	})
}

func (h *Handler) AnnouncePeer(c *gin.Context) {
	var req protocol.AnnouncePeerRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		logger.Warn("invalid announce peer request", zap.Error(err))
		c.JSON(http.StatusBadRequest, protocol.ErrorResponse{
			Error:   "invalid_request",
			Message: err.Error(),
			Code:    http.StatusBadRequest,
		})
		return
	}

	if req.PeerID == "" {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponse{
			Error:   "invalid_request",
			Message: "peer_id is required",
			Code:    http.StatusBadRequest,
		})
		return
	}

	clientIP := c.ClientIP()
	if clientIP != "" && !isPrivateIP(clientIP) {
		publicAddrs := buildPublicAddrs(clientIP, req.Addrs)
		req.Addrs = append(req.Addrs, publicAddrs...)

		logger.Debug("added public IP to peer addresses",
			zap.String("peer_id", req.PeerID),
			zap.String("public_ip", clientIP),
			zap.Int("public_addrs_added", len(publicAddrs)))
	}

	if err := h.store.AnnouncePeer(c.Request.Context(), &req); err != nil {
		logger.Error("failed to announce peer", zap.Error(err), zap.String("peer_id", req.PeerID))
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponse{
			Error:   "internal_error",
			Message: "failed to announce peer",
			Code:    http.StatusInternalServerError,
		})
		return
	}

	logger.Debug("peer announced",
		zap.String("peer_id", req.PeerID),
		zap.Int("file_count", len(req.FileHashes)),
		zap.String("nat_type", req.NATType))

	c.JSON(http.StatusOK, protocol.AnnouncePeerResponse{
		Success: true,
		Message: "peer announced successfully",
	})
}

func (h *Handler) GetPeers(c *gin.Context) {
	fileHash := c.Param("hash")
	if fileHash == "" {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponse{
			Error:   "invalid_request",
			Message: "file hash is required",
			Code:    http.StatusBadRequest,
		})
		return
	}

	file, err := h.store.GetFile(c.Request.Context(), fileHash)
	if err != nil {
		logger.Warn("file not found", zap.String("hash", fileHash))
		c.JSON(http.StatusNotFound, protocol.ErrorResponse{
			Error:   "file_not_found",
			Message: "file not found",
			Code:    http.StatusNotFound,
		})
		return
	}

	peers, err := h.store.GetPeers(c.Request.Context(), fileHash)
	if err != nil {
		logger.Error("failed to get peers", zap.Error(err), zap.String("hash", fileHash))
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponse{
			Error:   "internal_error",
			Message: "failed to get peers",
			Code:    http.StatusInternalServerError,
		})
		return
	}

	peerResponses := make([]protocol.PeerResponse, len(peers))
	for i, peer := range peers {
		peerResponses[i] = protocol.PeerResponse{
			PeerID:   peer.PeerID,
			Addrs:    peer.Addrs,
			NATType:  peer.NATType,
			LastSeen: peer.LastSeen.Format(time.RFC3339),
		}
	}

	logger.Debug("peers retrieved",
		zap.String("hash", fileHash),
		zap.Int("peer_count", len(peers)))

	var chunks []protocol.ChunkInfo
	if file.ChunksJSON != nil && *file.ChunksJSON != "" {
		if err := json.Unmarshal([]byte(*file.ChunksJSON), &chunks); err != nil {
			logger.Warn("failed to unmarshal chunks", zap.Error(err))
		}
	}

	c.JSON(http.StatusOK, protocol.GetPeersResponse{
		Peers:       peerResponses,
		FallbackURL: file.FallbackURL,
		Size:        uint64(file.Size),
		ChunkSize:   uint32(file.ChunkSize),
		ChunkCount:  uint32(file.ChunkCount),
		Chunks:      chunks,
	})
}

func (h *Handler) UpdateStatus(c *gin.Context) {
	var req protocol.UpdateStatusRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponse{
			Error:   "invalid_request",
			Message: err.Error(),
			Code:    http.StatusBadRequest,
		})
		return
	}

	announceReq := &protocol.AnnouncePeerRequest{
		PeerID:     req.PeerID,
		FileHashes: req.FileHashes,
	}

	if err := h.store.AnnouncePeer(c.Request.Context(), announceReq); err != nil {
		logger.Error("failed to update peer status", zap.Error(err), zap.String("peer_id", req.PeerID))
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponse{
			Error:   "internal_error",
			Message: "failed to update status",
			Code:    http.StatusInternalServerError,
		})
		return
	}

	c.JSON(http.StatusOK, protocol.UpdateStatusResponse{
		Success: true,
		Message: "status updated successfully",
	})
}

func (h *Handler) GetNATInfo(c *gin.Context) {
	info, err := h.natCoordinator.GetNATInfo(c.Request.Context())
	if err != nil {
		logger.Error("failed to get NAT info", zap.Error(err))
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponse{
			Error:   "internal_error",
			Message: "failed to get NAT info",
			Code:    http.StatusInternalServerError,
		})
		return
	}

	c.JSON(http.StatusOK, info)
}

func (h *Handler) Health(c *gin.Context) {
	if err := h.store.db.Ping(); err != nil {
		logger.Error("database health check failed", zap.Error(err))
		c.JSON(http.StatusServiceUnavailable, protocol.HealthResponse{
			Status:    "unhealthy",
			Timestamp: time.Now().Format(time.RFC3339),
			Version:   protocol.Version,
		})
		return
	}

	if err := h.store.redis.Ping(c.Request.Context()).Err(); err != nil {
		logger.Error("redis health check failed", zap.Error(err))
		c.JSON(http.StatusServiceUnavailable, protocol.HealthResponse{
			Status:    "unhealthy",
			Timestamp: time.Now().Format(time.RFC3339),
			Version:   protocol.Version,
		})
		return
	}

	c.JSON(http.StatusOK, protocol.HealthResponse{
		Status:    "healthy",
		Timestamp: time.Now().Format(time.RFC3339),
		Version:   protocol.Version,
	})
}

func (h *Handler) GetStats(c *gin.Context) {
	stats, err := h.store.GetStats(c.Request.Context())
	if err != nil {
		logger.Error("failed to get stats", zap.Error(err))
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponse{
			Error:   "internal_error",
			Message: "failed to get stats",
			Code:    http.StatusInternalServerError,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"total_files":  stats.TotalFiles,
		"active_peers": stats.ActivePeers,
		"total_peers":  stats.TotalPeers,
		"timestamp":    time.Now().Format(time.RFC3339),
	})
}

func isPrivateIP(ip string) bool {
	if ip == "127.0.0.1" || ip == "::1" || ip == "localhost" {
		return true
	}

	if len(ip) > 0 {
		if ip[:3] == "10." {
			return true
		}
		if len(ip) > 6 && ip[:4] == "172." {
			if ip[4] == '1' || ip[4] == '2' || ip[4] == '3' {
				return true
			}
		}
		if len(ip) > 11 && ip[:8] == "192.168." {
			return true
		}
	}

	return false
}

func buildPublicAddrs(publicIP string, existingAddrs []string) []string {
	var publicAddrs []string
	portsUsed := make(map[string]bool)

	for _, addr := range existingAddrs {
		if tcpIdx := findProtocolPort(addr, "/tcp/"); tcpIdx != -1 {
			port := extractPort(addr, tcpIdx+5)
			if port != "" && !portsUsed["tcp:"+port] {
				publicAddrs = append(publicAddrs, fmt.Sprintf("/ip4/%s/tcp/%s", publicIP, port))
				portsUsed["tcp:"+port] = true
			}
		}

		if udpIdx := findProtocolPort(addr, "/udp/"); udpIdx != -1 {
			port := extractPort(addr, udpIdx+5)
			if port != "" && !portsUsed["udp:"+port] {
				if hasQuic(addr) {
					publicAddrs = append(publicAddrs, fmt.Sprintf("/ip4/%s/udp/%s/quic-v1", publicIP, port))
				} else {
					publicAddrs = append(publicAddrs, fmt.Sprintf("/ip4/%s/udp/%s", publicIP, port))
				}
				portsUsed["udp:"+port] = true
			}
		}
	}

	return publicAddrs
}

func findProtocolPort(addr, protocol string) int {
	for i := 0; i <= len(addr)-len(protocol); i++ {
		if addr[i:i+len(protocol)] == protocol {
			return i
		}
	}
	return -1
}

func extractPort(addr string, startIdx int) string {
	var port string
	for i := startIdx; i < len(addr); i++ {
		if addr[i] >= '0' && addr[i] <= '9' {
			port += string(addr[i])
		} else {
			break
		}
	}
	return port
}

func hasQuic(addr string) bool {
	return findProtocolPort(addr, "/quic-v1") != -1
}
