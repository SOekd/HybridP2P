package daemon

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

func WritePID(path string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create PID directory: %w", err)
	}

	pid := os.Getpid()
	pidStr := strconv.Itoa(pid)

	if err := os.WriteFile(path, []byte(pidStr), 0644); err != nil {
		return fmt.Errorf("failed to write PID file: %w", err)
	}

	return nil
}

func ReadPID(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, fmt.Errorf("PID file does not exist")
		}
		return 0, fmt.Errorf("failed to read PID file: %w", err)
	}

	pidStr := strings.TrimSpace(string(data))
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		return 0, fmt.Errorf("invalid PID in file: %w", err)
	}

	return pid, nil
}

func RemovePID(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove PID file: %w", err)
	}
	return nil
}

func IsProcessRunning(pid int) bool {
	if pid <= 0 {
		return false
	}

	if runtime.GOOS == "windows" {
		return isProcessRunningWindows(pid)
	}
	return isProcessRunningUnix(pid)
}

func isProcessRunningWindows(pid int) bool {
	cmd := exec.Command("tasklist", "/FI", fmt.Sprintf("PID eq %d", pid), "/NH", "/FO", "CSV")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return false
	}

	outputStr := string(output)
	return strings.Contains(outputStr, strconv.Itoa(pid))
}

func isProcessRunningUnix(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	err = process.Signal(syscall.Signal(0))
	return err == nil
}

func IsStale(path string) (bool, error) {
	pid, err := ReadPID(path)
	if err != nil {
		if os.IsNotExist(err) || strings.Contains(err.Error(), "does not exist") {
			return false, nil
		}
		return false, err
	}

	return !IsProcessRunning(pid), nil
}

func CleanupStale(path string) error {
	stale, err := IsStale(path)
	if err != nil {
		return err
	}

	if stale {
		return RemovePID(path)
	}

	return nil
}
