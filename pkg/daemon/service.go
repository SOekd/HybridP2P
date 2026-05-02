package daemon

import (
	"P2P-CDN/internal/logger"
	"P2P-CDN/pkg/config"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kardianos/service"
	"go.uber.org/zap"
)

type DaemonService struct {
	server *Server
	config *config.ClientConfig
	addr   string
}

func NewDaemonService(cfg *config.ClientConfig, addr string) *DaemonService {
	return &DaemonService{
		config: cfg,
		addr:   addr,
	}
}

func (ds *DaemonService) Start(s service.Service) error {
	logger.Info("daemon service starting")

	server, err := NewServer(ds.config)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	ds.server = server

	go func() {
		if err := ds.server.Start(ds.addr); err != nil {
			logger.Error("server failed", zap.Error(err))
		}
	}()

	go RunCLI(cliAPIURL(ds.addr), ds.config.Client.DataDir)

	logger.Info("daemon service started")
	return nil
}

func (ds *DaemonService) Stop(s service.Service) error {
	logger.Info("daemon service stopping")

	if ds.server != nil {
		if err := ds.server.Stop(); err != nil {
			logger.Error("failed to stop server", zap.Error(err))
			return err
		}
	}

	logger.Info("daemon service stopped")
	return nil
}

func ServiceConfig(name, displayName, description string) *service.Config {
	execPath, err := os.Executable()
	if err != nil {
		execPath = os.Args[0]
	}

	execDir := filepath.Dir(execPath)
	configPath := filepath.Join(execDir, "configs", "client.yaml")

	return &service.Config{
		Name:        name,
		DisplayName: displayName,
		Description: description,
		Arguments:   []string{"--config", configPath},
		Option: service.KeyValue{
			"Restart":           "on-failure",
			"SuccessExitStatus": "1 2 8 SIGKILL",
		},
	}
}

func InstallService(name, displayName, description string) error {
	cfg := ServiceConfig(name, displayName, description)

	prg := &DaemonService{}

	s, err := service.New(prg, cfg)
	if err != nil {
		return fmt.Errorf("failed to create service: %w", err)
	}

	return s.Install()
}

func UninstallService(name, displayName, description string) error {
	cfg := ServiceConfig(name, displayName, description)

	prg := &DaemonService{}

	s, err := service.New(prg, cfg)
	if err != nil {
		return fmt.Errorf("failed to create service: %w", err)
	}

	return s.Uninstall()
}

func StartService(name, displayName, description string) error {
	cfg := ServiceConfig(name, displayName, description)

	prg := &DaemonService{}

	s, err := service.New(prg, cfg)
	if err != nil {
		return fmt.Errorf("failed to create service: %w", err)
	}

	return s.Start()
}

func StopService(name, displayName, description string) error {
	cfg := ServiceConfig(name, displayName, description)

	prg := &DaemonService{}

	s, err := service.New(prg, cfg)
	if err != nil {
		return fmt.Errorf("failed to create service: %w", err)
	}

	return s.Stop()
}

func RunService(cfg *config.ClientConfig, addr, name, displayName, description string) error {
	svcConfig := ServiceConfig(name, displayName, description)

	prg := NewDaemonService(cfg, addr)

	s, err := service.New(prg, svcConfig)
	if err != nil {
		return fmt.Errorf("failed to create service: %w", err)
	}

	errs := make(chan error, 5)
	logger.Info("running daemon service")

	go func() {
		errs <- s.Run()
	}()

	err = <-errs
	if err != nil {
		return err
	}

	return nil
}
