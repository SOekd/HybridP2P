package tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"P2P-CDN/internal/logger"
	"P2P-CDN/pkg/config"
	"P2P-CDN/pkg/protocol"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

type Server struct {
	config       *config.TrackerConfig
	store        *Store
	handler      *Handler
	controlPlane *ControlPlane
	engine       *gin.Engine
	server       *http.Server
}

var daemonUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func NewServer(cfg *config.TrackerConfig) (*Server, error) {
	mysqlDSN := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4&collation=utf8mb4_unicode_ci",
		cfg.Database.User,
		cfg.Database.Password,
		cfg.Database.Host,
		cfg.Database.Port,
		cfg.Database.Database,
	)

	store, err := NewStore(mysqlDSN, cfg.Redis.URL, cfg.Redis.Password, cfg.Redis.DB)
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %w", err)
	}

	controlPlane := NewControlPlane()

	handler := NewHandler(store)

	if cfg.Logging.Level == "debug" {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	engine := gin.New()

	engine.Use(gin.Recovery())
	engine.Use(LoggerMiddleware())
	engine.Use(CORSMiddleware())
	engine.Use(RateLimitMiddleware(store, cfg.Tracker.RateLimit.RequestsPerMinute))

	setupRoutes(engine, handler, controlPlane)

	return &Server{
		config:       cfg,
		store:        store,
		handler:      handler,
		controlPlane: controlPlane,
		engine:       engine,
	}, nil
}

func (s *Server) GetControlPlane() *ControlPlane {
	return s.controlPlane
}

func (s *Server) Start(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.config.Tracker.Port)

	s.server = &http.Server{
		Addr:           addr,
		Handler:        s.engine,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	logger.Info("starting tracker server", zap.String("addr", addr))

	go s.cleanupLoop(ctx)

	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("server error: %w", err)
	}

	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	logger.Info("shutting down tracker server")

	if s.server != nil {
		if err := s.server.Shutdown(ctx); err != nil {
			logger.Error("failed to shutdown server", zap.Error(err))
			return err
		}
	}

	if err := s.store.Close(); err != nil {
		logger.Error("failed to close store", zap.Error(err))
		return err
	}

	logger.Info("tracker server shutdown complete")
	return nil
}

func (s *Server) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count, err := s.store.CleanupStalePeers(ctx)
			if err != nil {
				logger.Error("failed to cleanup stale peers", zap.Error(err))
			} else if count > 0 {
				logger.Info("cleaned up stale peers", zap.Int("count", count))
			}
		}
	}
}

func setupRoutes(engine *gin.Engine, handler *Handler, cp *ControlPlane) {
	v1 := engine.Group("/api/v1")
	{
		files := v1.Group("/files")
		{
			files.POST("/register", handler.RegisterFile)
		}

		peers := v1.Group("/peers")
		{
			peers.POST("/announce", handler.AnnouncePeer)
			peers.GET("/:hash", handler.GetPeers)
			peers.POST("/status", handler.UpdateStatus)
		}

		nat := v1.Group("/nat")
		{
			nat.GET("/info", handler.GetNATInfo)
		}

		v1.GET("/stats", handler.GetStats)
	}

	engine.GET("/ws/daemons", func(c *gin.Context) {
		handleDaemonWS(c, cp)
	})

	engine.GET("/health", handler.Health)

	engine.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"service": "P2P CDN Tracker",
			"version": "0.1.0",
			"status":  "running",
		})
	})
}

func handleDaemonWS(c *gin.Context, cp *ControlPlane) {
	conn, err := daemonUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		logger.Error("failed to upgrade daemon websocket", zap.Error(err))
		return
	}
	defer conn.Close()

	peerID := "unknown"

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, data, err := conn.ReadMessage()
	if err == nil {
		var hello protocol.DaemonHelloMessage
		if json.Unmarshal(data, &hello) == nil && hello.Type == "daemon_hello" && hello.PeerID != "" {
			peerID = hello.PeerID
		}
	}
	conn.SetReadDeadline(time.Time{})

	cp.RegisterDaemon(conn, peerID)
	defer cp.UnregisterDaemon(conn)

	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			break
		}
	}
}

func LoggerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		c.Next()

		latency := time.Since(start)
		status := c.Writer.Status()
		method := c.Request.Method
		clientIP := c.ClientIP()

		if status >= 400 {
			logger.Warn("request completed",
				zap.String("method", method),
				zap.String("path", path),
				zap.String("query", query),
				zap.Int("status", status),
				zap.Duration("latency", latency),
				zap.String("client_ip", clientIP))
		} else {
			logger.Debug("request completed",
				zap.String("method", method),
				zap.String("path", path),
				zap.Int("status", status),
				zap.Duration("latency", latency))
		}
	}
}

func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT, DELETE")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

func RateLimitMiddleware(store *Store, requestsPerMinute int) gin.HandlerFunc {
	if requestsPerMinute <= 0 {
		return func(c *gin.Context) {
			c.Next()
		}
	}

	return func(c *gin.Context) {
		clientIP := c.ClientIP()

		now := time.Now()
		window := now.Truncate(time.Minute)
		key := fmt.Sprintf("ratelimit:%s:%d", clientIP, window.Unix())

		ctx := c.Request.Context()

		count, err := store.redis.Incr(ctx, key).Result()
		if err != nil {
			logger.Error("rate limit redis error", zap.Error(err))
			c.Next()
			return
		}

		if count == 1 {
			store.redis.Expire(ctx, key, 2*time.Minute)
		}

		if count > int64(requestsPerMinute) {
			nextWindow := window.Add(time.Minute)
			retryAfter := int(time.Until(nextWindow).Seconds())
			if retryAfter < 0 {
				retryAfter = 0
			}

			logger.Warn("rate limit exceeded",
				zap.String("ip", clientIP),
				zap.Int64("count", count),
				zap.Int("limit", requestsPerMinute),
				zap.Int("retry_after", retryAfter))

			c.Header("X-RateLimit-Limit", fmt.Sprintf("%d", requestsPerMinute))
			c.Header("X-RateLimit-Remaining", "0")
			c.Header("X-RateLimit-Reset", fmt.Sprintf("%d", nextWindow.Unix()))
			c.Header("Retry-After", fmt.Sprintf("%d", retryAfter))

			c.JSON(http.StatusTooManyRequests, gin.H{
				"error":               "rate_limit_exceeded",
				"message":             fmt.Sprintf("Rate limit exceeded. Maximum %d requests per minute allowed.", requestsPerMinute),
				"retry_after_seconds": retryAfter,
			})
			c.Abort()
			return
		}

		remaining := requestsPerMinute - int(count)
		if remaining < 0 {
			remaining = 0
		}

		c.Header("X-RateLimit-Limit", fmt.Sprintf("%d", requestsPerMinute))
		c.Header("X-RateLimit-Remaining", fmt.Sprintf("%d", remaining))
		c.Header("X-RateLimit-Reset", fmt.Sprintf("%d", window.Add(time.Minute).Unix()))

		c.Next()
	}
}
