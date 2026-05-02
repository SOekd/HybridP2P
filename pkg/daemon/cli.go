package daemon

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

func RunCLI(apiURL, dataDir string) {
	// Wait for daemon HTTP server to be ready
	httpClient := &http.Client{Timeout: 2 * time.Second}
	for {
		resp, err := httpClient.Get(apiURL + "/health")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	c := NewClient(apiURL)

	printDaemonBanner()
	printDaemonHelp()

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("\n> ")

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			handleDaemonCommand(c, line, dataDir)
		}
		fmt.Print("> ")
	}
}

func printDaemonBanner() {
	fmt.Println("P2P CDN Daemon")
}

func printDaemonHelp() {
	fmt.Println("\nCommands:")
	fmt.Println("  status                           Show daemon info")
	fmt.Println("  seeds                            List seeded files")
	fmt.Println("  seed <path> [fallback <url>]     Start seeding a file")
	fmt.Println("  unseed <hash>                    Stop seeding a file")
	fmt.Println("  download <hash> [path]           Download a file by hash")
	fmt.Println("  downloads                        List active downloads")
	fmt.Println("  peers                            List connected P2P peers")
	fmt.Println("  metrics reset                    Clear collected metrics")
	fmt.Println("  help                             Show this help")
	fmt.Println("  quit / exit                      Stop the daemon")
}

func handleDaemonCommand(c *Client, line, dataDir string) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return
	}

	cmd := strings.ToLower(parts[0])

	switch cmd {
	case "status":
		cliStatus(c)

	case "seeds", "list-seeds":
		cliListSeeds(c)

	case "seed":
		if len(parts) < 2 {
			fmt.Println("Usage: seed <path> [fallback <url>]")
			return
		}
		path := parts[1]
		fallbackURL := ""
		for i := 2; i < len(parts)-1; i++ {
			if strings.ToLower(parts[i]) == "fallback" {
				fallbackURL = parts[i+1]
				break
			}
		}
		cliSeed(c, path, fallbackURL)

	case "unseed":
		if len(parts) < 2 {
			fmt.Println("Usage: unseed <hash>")
			return
		}
		cliUnseed(c, parts[1])

	case "download":
		if len(parts) < 2 {
			fmt.Println("Usage: download <hash> [path]")
			return
		}
		hash := parts[1]
		outputPath := ""
		if len(parts) >= 3 {
			outputPath = parts[2]
		}
		cliDownload(c, hash, outputPath, dataDir)

	case "downloads":
		cliListDownloads(c)

	case "peers":
		cliListPeers(c)

	case "metrics":
		if len(parts) >= 2 && strings.ToLower(parts[1]) == "reset" {
			if err := c.ResetMetrics(); err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("Metrics cleared")
			}
		} else {
			fmt.Println("Usage: metrics reset")
		}

	case "help":
		printDaemonHelp()

	case "quit", "exit":
		fmt.Println("Stopping daemon...")
		os.Exit(0)

	default:
		fmt.Printf("Unknown command: %q  (type 'help' for list)\n", line)
	}
}

func cliStatus(c *Client) {
	info, err := c.GetInfo()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Println("\nDaemon Status")
	fmt.Printf("  Peer ID:          %s\n", info.PeerID)
	fmt.Printf("  Active Seeds:     %d\n", info.ActiveSeeds)
	fmt.Printf("  Active Downloads: %d\n", info.ActiveDownloads)
	fmt.Printf("  Connected Peers:  %d\n", info.ConnectedPeers)
}

func cliListSeeds(c *Client) {
	resp, err := c.ListSeeds()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	if resp.Count == 0 {
		fmt.Println("No files being seeded")
		return
	}
	fmt.Printf("\nSeeding %d file(s):\n", resp.Count)
	for i, s := range resp.Seeds {
		fmt.Printf("\n  [%d] Hash:     %s\n", i+1, s.FileHash)
		fmt.Printf("      Path:     %s\n", s.FilePath)
		fmt.Printf("      Fallback: %s\n", s.FallbackURL)
		fmt.Printf("      Size:     %d bytes (%d chunks)\n", s.Size, s.ChunkCount)
		fmt.Printf("      Started:  %s\n", s.StartedAt)
	}
}

func cliSeed(c *Client, path, fallbackURL string) {
	fmt.Printf("Seeding %s...\n", path)
	hash, err := c.SeedWithTracker(path, fallbackURL, "")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Seeding started\n")
	if hash != "" {
		fmt.Printf("  Hash: %s\n", hash)
	}
	fmt.Println("  Use 'seeds' to check status")
}

func cliUnseed(c *Client, hash string) {
	if err := c.Unseed(hash); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Stopped seeding %s\n", hash)
}

func cliDownload(c *Client, hash, outputPath, dataDir string) {
	if outputPath == "" {
		if dataDir != "" {
			outputPath = dataDir + "/" + hash[:8] + ".bin"
		} else {
			outputPath = hash[:8] + ".bin"
		}
	}
	fmt.Printf("Downloading %s → %s...\n", hash, outputPath)
	if err := c.Download(hash, outputPath); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Download complete: %s\n", outputPath)
}

func cliListDownloads(c *Client) {
	resp, err := c.ListDownloads()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	if len(resp.Downloads) == 0 {
		fmt.Println("No active downloads")
		return
	}
	fmt.Printf("\nActive downloads (%d):\n", len(resp.Downloads))
	for i, d := range resp.Downloads {
		fmt.Printf("\n  [%d] Hash:   %s\n", i+1, d.FileHash)
		fmt.Printf("      Status: %s\n", d.Status)
		fmt.Printf("      Output: %s\n", d.OutputPath)
	}
}

func cliListPeers(c *Client) {
	resp, err := c.ListPeers()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	if len(resp.Peers) == 0 {
		fmt.Println("No connected peers")
		return
	}
	fmt.Printf("\nConnected peers (%d):\n", len(resp.Peers))
	for i, p := range resp.Peers {
		fmt.Printf("  [%d] %s\n", i+1, p)
	}
}

func cliAPIURL(addr string) string {
	if strings.HasPrefix(addr, "0.0.0.0:") {
		return "http://127.0.0.1:" + addr[len("0.0.0.0:"):]
	}
	return "http://" + addr
}
