package main

import (
	"P2P-CDN/pkg/daemon"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

const (
	daemonServiceName = "p2pcdn-daemon"
	daemonDisplayName = "P2P CDN Daemon"
	daemonDescription = "P2P Content Distribution Network daemon service"
	defaultDaemonAddr = "http://127.0.0.1:9090"
)

var (
	daemonAddr   string
	forceRestart bool
)

func getDaemonManager() *daemon.DaemonManager {
	execPath, err := os.Executable()
	if err != nil {
		execPath = os.Args[0]
	}
	execDir := filepath.Dir(execPath)

	daemonExec := filepath.Join(execDir, "p2pcdn-daemon")
	if _, err := os.Stat(daemonExec); os.IsNotExist(err) {
		daemonExec = filepath.Join(execDir, "p2pcdn-daemon.exe")
	}

	pidFile := filepath.Join(execDir, ".daemon.pid")
	logFile := filepath.Join(execDir, "daemon.log")

	return daemon.NewDaemonManager(pidFile, defaultDaemonAddr, daemonExec, logFile)
}

var daemonCmd = &cobra.Command{
	Use:   "daemon",
	Short: "Manage the P2P CDN daemon",
	Long:  "Commands to start, stop, and manage the P2P CDN daemon service",
}

var daemonStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the daemon",
	Long:  "Starts the P2P CDN daemon in the background",
	RunE: func(cmd *cobra.Command, args []string) error {
		dm := getDaemonManager()

		if dm.IsRunning() {
			fmt.Println("Daemon is already running")
			if !forceRestart {
				return nil
			}
			fmt.Println("Restarting daemon...")
			if err := dm.Restart(); err != nil {
				return fmt.Errorf("failed to restart daemon: %w", err)
			}
			fmt.Println("Daemon restarted successfully")
			return nil
		}

		fmt.Println("Starting daemon...")
		if err := dm.Start(); err != nil {
			return fmt.Errorf("failed to start daemon: %w", err)
		}

		pid, _ := dm.GetPID()
		fmt.Printf("Daemon started successfully (PID: %d)\n", pid)
		fmt.Printf("Daemon API: %s\n", defaultDaemonAddr)
		return nil
	},
}

var daemonStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop the daemon",
	Long:  "Stops the P2P CDN daemon",
	RunE: func(cmd *cobra.Command, args []string) error {
		dm := getDaemonManager()

		if !dm.IsRunning() {
			fmt.Println("Daemon is not running")
			return nil
		}

		fmt.Println("Stopping daemon...")
		if err := dm.Stop(); err != nil {
			return fmt.Errorf("failed to stop daemon: %w", err)
		}

		fmt.Println("Daemon stopped successfully")
		return nil
	},
}

var daemonRestartCmd = &cobra.Command{
	Use:   "restart",
	Short: "Restart the daemon",
	Long:  "Restarts the P2P CDN daemon",
	RunE: func(cmd *cobra.Command, args []string) error {
		dm := getDaemonManager()

		fmt.Println("Restarting daemon...")
		if err := dm.Restart(); err != nil {
			return fmt.Errorf("failed to restart daemon: %w", err)
		}

		pid, _ := dm.GetPID()
		fmt.Printf("Daemon restarted successfully (PID: %d)\n", pid)
		return nil
	},
}

var daemonStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show daemon status",
	Long:  "Shows the current status of the P2P CDN daemon",
	RunE: func(cmd *cobra.Command, args []string) error {
		dm := getDaemonManager()

		status, err := dm.GetStatus()
		if err != nil {
			return fmt.Errorf("failed to get status: %w", err)
		}

		if running, ok := status["running"].(bool); ok && running {
			fmt.Println("Daemon status: RUNNING")
			if pid, ok := status["pid"].(int); ok {
				fmt.Printf("PID: %d\n", pid)
			}
			if uptime, ok := status["uptime"].(string); ok && uptime != "" {
				fmt.Printf("Uptime: %s\n", uptime)
			}

			client := daemon.NewClient(defaultDaemonAddr)
			if info, err := client.GetInfo(); err == nil {
				fmt.Printf("Peer ID: %s\n", info.PeerID)
				fmt.Printf("Active Downloads: %d\n", info.ActiveDownloads)
				fmt.Printf("Active Seeds: %d\n", info.ActiveSeeds)
				fmt.Printf("Connected Peers: %d\n", info.ConnectedPeers)
			}
		} else {
			fmt.Println("Daemon status: STOPPED")
			if errMsg, ok := status["error"].(string); ok {
				fmt.Printf("Error: %s\n", errMsg)
			}
		}

		return nil
	},
}

var daemonInstallCmd = &cobra.Command{
	Use:   "install",
	Short: "Install daemon as system service",
	Long:  "Installs the P2P CDN daemon as a system service (requires admin/root)",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("Installing daemon as system service...")

		if err := daemon.InstallService(daemonServiceName, daemonDisplayName, daemonDescription); err != nil {
			return fmt.Errorf("failed to install service: %w", err)
		}

		fmt.Println("Daemon installed successfully as system service")
		fmt.Println("Use 'systemctl start p2pcdn-daemon' (Linux) or 'sc start p2pcdn-daemon' (Windows) to start")
		return nil
	},
}

var daemonUninstallCmd = &cobra.Command{
	Use:   "uninstall",
	Short: "Uninstall daemon system service",
	Long:  "Uninstalls the P2P CDN daemon system service (requires admin/root)",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("Uninstalling daemon system service...")

		if err := daemon.UninstallService(daemonServiceName, daemonDisplayName, daemonDescription); err != nil {
			return fmt.Errorf("failed to uninstall service: %w", err)
		}

		fmt.Println("Daemon uninstalled successfully")
		return nil
	},
}

var daemonLogsCmd = &cobra.Command{
	Use:   "logs",
	Short: "Show daemon logs",
	Long:  "Displays the daemon log file",
	RunE: func(cmd *cobra.Command, args []string) error {
		execPath, err := os.Executable()
		if err != nil {
			execPath = os.Args[0]
		}
		execDir := filepath.Dir(execPath)
		logFile := filepath.Join(execDir, "daemon.log")

		data, err := os.ReadFile(logFile)
		if err != nil {
			return fmt.Errorf("failed to read log file: %w", err)
		}

		fmt.Print(string(data))
		return nil
	},
}

var metricsCmd = &cobra.Command{
	Use:   "metrics",
	Short: "Manage metrics",
}

var metricsResetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Clear all collected metrics",
	RunE: func(cmd *cobra.Command, args []string) error {
		client := daemon.NewClient(defaultDaemonAddr)
		if err := client.ResetMetrics(); err != nil {
			return fmt.Errorf("failed to reset metrics: %w", err)
		}
		fmt.Println("Metrics cleared successfully")
		return nil
	},
}

func init() {
	metricsCmd.AddCommand(metricsResetCmd)
	rootCmd.AddCommand(metricsCmd)

	daemonCmd.AddCommand(daemonStartCmd)
	daemonCmd.AddCommand(daemonStopCmd)
	daemonCmd.AddCommand(daemonRestartCmd)
	daemonCmd.AddCommand(daemonStatusCmd)
	daemonCmd.AddCommand(daemonInstallCmd)
	daemonCmd.AddCommand(daemonUninstallCmd)
	daemonCmd.AddCommand(daemonLogsCmd)

	daemonStartCmd.Flags().BoolVarP(&forceRestart, "force", "f", false, "Force restart if already running")
	daemonCmd.PersistentFlags().StringVar(&daemonAddr, "addr", defaultDaemonAddr, "Daemon API address")

	rootCmd.AddCommand(daemonCmd)
}
