package main

import (
	"P2P-CDN/pkg/daemon"
	"context"
	"fmt"
	"path/filepath"
	"time"
)

var (
	standaloneMode bool
)

func getDaemonClient() (*daemon.Client, error) {
	dm := getDaemonManager()

	if !dm.IsRunning() {
		return nil, fmt.Errorf("daemon is not running")
	}

	return daemon.NewClient(defaultDaemonAddr), nil
}

func tryUseDaemon() (*daemon.Client, bool) {
	if standaloneMode {
		return nil, false
	}

	client, err := getDaemonClient()
	if err != nil {
		return nil, false
	}

	if err := client.Health(); err != nil {
		return nil, false
	}

	return client, true
}

func seedViaDaemon(client *daemon.Client, filePath, fallbackURL, trackerURL string) error {
	fmt.Println("Using daemon for seeding...")

	if abs, err := filepath.Abs(filePath); err == nil {
		filePath = abs
	}

	_, err := client.SeedWithTracker(filePath, fallbackURL, trackerURL)
	if err != nil {
		return fmt.Errorf("daemon seed failed: %w", err)
	}

	fmt.Printf("Seeding started in background\n")
	fmt.Printf("  File: %s\n", filePath)
	fmt.Printf("  Daemon is processing and will seed when ready\n")
	fmt.Printf("\nTip: Use 'p2pcdn list-seeds' to see all seeding files\n")

	return nil
}

func downloadViaDaemon(client *daemon.Client, fileHash, outputPath, trackerURL string, autoSeed bool, fallbackURL string) error {
	fmt.Println("Using daemon for download...")

	if abs, err := filepath.Abs(outputPath); err == nil {
		outputPath = abs
	}

	if err := client.DownloadWithTracker(fileHash, outputPath, trackerURL); err != nil {
		return fmt.Errorf("daemon download failed: %w", err)
	}

	fmt.Printf("Download started via daemon\n")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	progressChan, errChan, err := client.StreamProgress(ctx, fileHash)
	if err != nil {
		fmt.Printf("Could not connect to progress stream: %v\n", err)
		fmt.Println("Download is running in background. Check status with: p2pcdn daemon status")
		return nil
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	lastProgress := time.Now()

	for {
		select {
		case progress, ok := <-progressChan:
			if !ok {
				return nil
			}

			if progress.Status == "complete" {
				fmt.Printf("\nDownload completed!\n")
				fmt.Printf("  File saved to: %s\n", outputPath)

				if autoSeed {
					fmt.Println("\nAuto-seeding downloaded file...")

					seedFallbackURL := fallbackURL
					if seedFallbackURL == "" {
						if status, err := client.GetStatus(fileHash); err == nil {
							seedFallbackURL = status.FallbackURL
						}
					}

					if _, err := client.SeedWithTracker(outputPath, seedFallbackURL, trackerURL); err != nil {
						fmt.Printf("Failed to start auto-seed: %v\n", err)
					} else {
						fmt.Printf("Seeding started in background\n")
						fmt.Printf("  File: %s\n", outputPath)
						fmt.Printf("  Hash: %s (already known from download)\n", fileHash)
						fmt.Printf("\nThe daemon is processing the file and will seed when ready.\n")
					}
				}

				return nil
			}

			lastProgress = time.Now()

			percentage := float64(0)
			if progress.TotalSize > 0 {
				percentage = float64(progress.Downloaded) / float64(progress.TotalSize) * 100
			}

			status := "Downloading"
			if progress.UsingFallback {
				status = "Fallback"
			}

			fmt.Printf("\r[%s] %.2f%% (%d/%d bytes) - %.2f KB/s   ",
				status,
				percentage,
				progress.Downloaded,
				progress.TotalSize,
				progress.DownloadRate/1024)

		case err, ok := <-errChan:
			if !ok {
				errChan = nil
				continue
			}
			if err != nil {
				fmt.Printf("\nDownload error: %v\n", err)
				return err
			}

		case <-ticker.C:
			if time.Since(lastProgress) > 60*time.Second {
				fmt.Printf("\nProgress stream timeout\n")
				fmt.Println("Download may still be running. Check with: p2pcdn daemon status")
				return nil
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func ensureDaemonRunning() bool {
	if standaloneMode {
		return false
	}

	dm := getDaemonManager()
	if dm.IsRunning() {
		return true
	}

	fmt.Println("Daemon is not running")
	fmt.Println()
	fmt.Println("The daemon provides better performance by maintaining active P2P connections.")
	fmt.Print("Would you like to start it? [Y/n]: ")

	var response string
	fmt.Scanln(&response)

	if response != "" && response != "y" && response != "Y" {
		fmt.Println("Continuing in standalone mode...")
		return false
	}

	fmt.Println("Starting daemon...")
	if err := dm.Start(); err != nil {
		fmt.Printf("Failed to start daemon: %v\n", err)
		fmt.Println("Continuing in standalone mode...")
		return false
	}

	pid, _ := dm.GetPID()
	fmt.Printf("Daemon started (PID: %d)\n", pid)
	fmt.Println()

	return true
}
