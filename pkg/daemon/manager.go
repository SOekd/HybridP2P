package daemon

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
)

type DaemonManager struct {
	pidFile    string
	apiURL     string
	executable string
	logFile    string
	healthPath string
}

func NewDaemonManager(pidFile, apiURL, executable, logFile string) *DaemonManager {
	return &DaemonManager{
		pidFile:    pidFile,
		apiURL:     apiURL,
		executable: executable,
		logFile:    logFile,
		healthPath: "/health",
	}
}

func (dm *DaemonManager) IsRunning() bool {
	if err := CleanupStale(dm.pidFile); err != nil {
		return false
	}

	pid, err := ReadPID(dm.pidFile)
	if err != nil {
		return false
	}

	if !IsProcessRunning(pid) {
		RemovePID(dm.pidFile)
		return false
	}

	if err := dm.HealthCheck(); err != nil {
		return false
	}

	return true
}

func (dm *DaemonManager) Start() error {
	if dm.IsRunning() {
		return fmt.Errorf("daemon is already running")
	}

	execPath := dm.executable
	if !filepath.IsAbs(execPath) {
		currentExe, err := os.Executable()
		if err == nil {
			execDir := filepath.Dir(currentExe)
			execPath = filepath.Join(execDir, dm.executable)
		}
	}

	if _, err := os.Stat(execPath); os.IsNotExist(err) {
		return fmt.Errorf("daemon executable not found: %s", execPath)
	}

	if err := os.MkdirAll(filepath.Dir(dm.logFile), 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	logFile, err := os.OpenFile(dm.logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}
	defer logFile.Close()

	cmd := exec.Command(execPath)
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	configureDaemonProcess(cmd)

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start daemon: %w", err)
	}

	pidPath := dm.pidFile
	if err := os.MkdirAll(filepath.Dir(pidPath), 0755); err != nil {
		return fmt.Errorf("failed to create PID directory: %w", err)
	}

	pidFile, err := os.Create(pidPath)
	if err != nil {
		return fmt.Errorf("failed to create PID file: %w", err)
	}
	defer pidFile.Close()

	if _, err := fmt.Fprintf(pidFile, "%d", cmd.Process.Pid); err != nil {
		return fmt.Errorf("failed to write PID: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	backoff := 100 * time.Millisecond
	maxBackoff := 2 * time.Second

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("daemon failed to start within timeout")
		case <-time.After(backoff):
			if err := dm.HealthCheck(); err == nil {
				return nil
			}

			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func (dm *DaemonManager) Stop() error {
	pid, err := ReadPID(dm.pidFile)
	if err != nil {
		return fmt.Errorf("daemon is not running")
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("failed to find daemon process: %w", err)
	}

	if runtime.GOOS == "windows" {
		if err := process.Kill(); err != nil {
			return fmt.Errorf("failed to stop daemon: %w", err)
		}
	} else {
		if err := process.Signal(syscall.SIGTERM); err != nil {
			return fmt.Errorf("failed to stop daemon: %w", err)
		}
	}

	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			process.Kill()
			RemovePID(dm.pidFile)
			return fmt.Errorf("daemon did not stop gracefully, forced kill")
		case <-ticker.C:
			if !IsProcessRunning(pid) {
				RemovePID(dm.pidFile)
				return nil
			}
		}
	}
}

func (dm *DaemonManager) Restart() error {
	if dm.IsRunning() {
		if err := dm.Stop(); err != nil {
			return fmt.Errorf("failed to stop daemon: %w", err)
		}
	}

	return dm.Start()
}

func (dm *DaemonManager) HealthCheck() error {
	client := &http.Client{
		Timeout: 2 * time.Second,
	}

	url := dm.apiURL + dm.healthPath
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check returned status %d", resp.StatusCode)
	}

	return nil
}

func (dm *DaemonManager) EnsureRunning() error {
	if dm.IsRunning() {
		return nil
	}

	return dm.Start()
}

func (dm *DaemonManager) GetPID() (int, error) {
	return ReadPID(dm.pidFile)
}

func (dm *DaemonManager) GetStatus() (map[string]interface{}, error) {
	status := make(map[string]interface{})

	pid, err := dm.GetPID()
	if err != nil {
		status["running"] = false
		status["error"] = err.Error()
		return status, nil
	}

	status["running"] = IsProcessRunning(pid)
	status["pid"] = pid

	if status["running"].(bool) {
		if uptime, err := getProcessUptime(pid); err == nil {
			status["uptime"] = uptime.String()
		}
	}

	return status, nil
}

func getProcessUptime(pid int) (time.Duration, error) {
	if runtime.GOOS == "windows" {
		return 0, fmt.Errorf("uptime not implemented for Windows")
	}

	return 0, fmt.Errorf("uptime not implemented")
}

func ExpandPath(path string) (string, error) {
	if !strings.HasPrefix(path, "~") {
		return path, nil
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get home directory: %w", err)
	}

	return filepath.Join(home, path[1:]), nil
}
