package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"P2P-CDN/internal/logger"
	"P2P-CDN/pkg/config"
	"P2P-CDN/pkg/tracker"

	"go.uber.org/zap"
)

func main() {
	configPath := flag.String("config", "", "Path to config file")
	port := flag.Int("port", 0, "HTTP port (overrides config)")
	logLevel := flag.String("log", "", "Log level (debug, info, warn, error)")
	flag.Parse()

	cfg, err := config.LoadTrackerConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	if *port > 0 {
		cfg.Tracker.Port = *port
	}
	if *logLevel != "" {
		cfg.Logging.Level = *logLevel
	}

	if err := logger.Init(cfg.Logging.Level); err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("starting P2P CDN tracker",
		zap.String("version", "0.1.0"),
		zap.Int("port", cfg.Tracker.Port),
		zap.String("log_level", cfg.Logging.Level))

	server, err := tracker.NewServer(cfg)
	if err != nil {
		logger.Fatal("failed to create server", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		if err := server.Start(ctx); err != nil {
			errCh <- err
		}
	}()

	go tracker.RunCLI(server.GetControlPlane())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		logger.Fatal("server error", zap.Error(err))
	case sig := <-sigCh:
		logger.Info("received signal, shutting down", zap.String("signal", sig.String()))
	}

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown error", zap.Error(err))
		os.Exit(1)
	}

	logger.Info("tracker shutdown complete")
}
