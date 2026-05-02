package tracker

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"P2P-CDN/pkg/bandwidth"
)

func RunCLI(cp *ControlPlane) {
	printBanner()
	printHelp()

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("\n> ")

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			handleCLICommand(cp, line)
		}
		fmt.Print("> ")
	}
}

func printBanner() {
	fmt.Println("P2P CDN Tracker")
}

func printHelp() {
	fmt.Println("\nCommands:")
	fmt.Println("  pause seeds          Pause seeding on all daemons")
	fmt.Println("  resume seeds         Resume seeding on all daemons")
	fmt.Println("  pause downloads      Pause downloads on all daemons")
	fmt.Println("  resume downloads     Resume downloads on all daemons")
	fmt.Println("  bandwidth <value>    Set seeder upload bandwidth limit (megabits)")
	fmt.Println("                         e.g. 100mb  50mbps  1gb  unlimited")
	fmt.Println("  status               Show current global state")
	fmt.Println("  daemons              List connected daemons")
	fmt.Println("  clear metrics        Wipe metrics.json on all connected daemons")
	fmt.Println("  help                 Show this help")
	fmt.Println("  quit / exit          Stop the tracker")
}

func handleCLICommand(cp *ControlPlane, line string) {
	parts := strings.Fields(strings.ToLower(line))
	if len(parts) == 0 {
		return
	}

	switch {
	case matchCmd(parts, "pause", "seeds"):
		cp.PauseSeeds()
		fmt.Println("Seeding paused — all daemons will block new seeds")

	case matchCmd(parts, "resume", "seeds"):
		cp.ResumeSeeds()
		fmt.Println("Seeding resumed")

	case matchCmd(parts, "pause", "downloads"):
		cp.PauseDownloads()
		fmt.Println("Downloads paused — all daemons will block new downloads")

	case matchCmd(parts, "resume", "downloads"):
		cp.ResumeDownloads()
		fmt.Println("Downloads resumed")

	case len(parts) >= 2 && parts[0] == "bandwidth":
		bps, err := bandwidth.Parse(parts[1])
		if err != nil {
			fmt.Printf("Invalid value: %v\n", err)
			fmt.Println("  Examples: bandwidth 10mb  bandwidth 50mbps  bandwidth unlimited")
			return
		}
		cp.SetBandwidth(bps)
		if bps == 0 {
			fmt.Println("Bandwidth limit removed (unlimited)")
		} else {
			fmt.Printf("Bandwidth limit set to %s\n", bandwidth.Format(bps))
		}

	case matchCmd(parts, "clear", "metrics"):
		cp.ClearMetrics()
		fmt.Println("clear_metrics broadcast — all connected daemons will wipe their metrics")

	case parts[0] == "status":
		printCLIStatus(cp)

	case parts[0] == "daemons":
		printCLIDaemons(cp)

	case parts[0] == "help":
		printHelp()

	case parts[0] == "quit" || parts[0] == "exit":
		fmt.Println("Stopping tracker…")
		os.Exit(0)

	default:
		fmt.Printf("Unknown command: %q  (type 'help' for list)\n", line)
	}
}

func matchCmd(parts []string, expected ...string) bool {
	if len(parts) < len(expected) {
		return false
	}
	for i, e := range expected {
		if parts[i] != e {
			return false
		}
	}
	return true
}

func printCLIStatus(cp *ControlPlane) {
	state := cp.GetState()
	daemons := cp.ConnectedDaemons()

	seedLabel := "running"
	if state.SeedingPaused {
		seedLabel = "PAUSED"
	}
	dlLabel := "running"
	if state.DownloadsPaused {
		dlLabel = "PAUSED"
	}
	bwLabel := "unlimited"
	if state.MaxBandwidthBps > 0 {
		bwLabel = bandwidth.Format(state.MaxBandwidthBps)
	}

	fmt.Println("\nStatus")
	fmt.Printf("  Seeds:      %s\n", seedLabel)
	fmt.Printf("  Downloads:  %s\n", dlLabel)
	fmt.Printf("  Bandwidth:  %s\n", bwLabel)
	fmt.Printf("  Daemons:    %d connected\n", len(daemons))
}

func printCLIDaemons(cp *ControlPlane) {
	daemons := cp.ConnectedDaemons()
	if len(daemons) == 0 {
		fmt.Println("No daemons connected")
		return
	}
	fmt.Printf("\nConnected Daemons (%d)\n", len(daemons))
	for i, id := range daemons {
		fmt.Printf("  %d. %s\n", i+1, id)
	}
}
