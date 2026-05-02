package config

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

func PromptForTracker(cfg *ClientConfig, configPath string) error {
	if cfg.Tracker.URL != "" {
		return nil
	}

	fmt.Println()
	fmt.Println("Tracker Configuration")
	fmt.Println()
	fmt.Println("REQUIRED: Tracker URL is necessary for peer discovery.")
	fmt.Println("Without a tracker, downloads and seeding will not work properly.")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  http://localhost:8080")
	fmt.Println("  http://tracker.example.com:8080")
	fmt.Println("  http://190.115.198.31:50000")
	fmt.Println()

	reader := bufio.NewReader(os.Stdin)

	var trackerURL string
	for {
		fmt.Print("Enter tracker URL: ")

		input, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}

		trackerURL = strings.TrimSpace(input)

		if trackerURL == "" {
			fmt.Println("Tracker URL cannot be empty. Please enter a valid URL.")
			fmt.Println()
			continue
		}

		if !strings.Contains(trackerURL, "://") && !strings.Contains(trackerURL, ":") {
			fmt.Println("Invalid URL format. Include protocol (http://) or at least host:port")
			fmt.Println()
			continue
		}

		break
	}

	if !strings.HasPrefix(trackerURL, "http://") && !strings.HasPrefix(trackerURL, "https://") {
		trackerURL = "http://" + trackerURL
	}

	cfg.Tracker.URL = trackerURL

	if err := SaveTrackerURL(configPath, trackerURL); err != nil {
		fmt.Printf("Could not save tracker to config: %v\n", err)
		fmt.Println("You can manually edit:", configPath)
		return nil
	}

	fmt.Println()
	fmt.Println("Tracker URL saved:", trackerURL)
	fmt.Println()

	return nil
}

func SaveTrackerURL(configPath string, trackerURL string) error {
	v := viper.New()
	v.SetConfigFile(configPath)

	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}

	v.Set("tracker.url", trackerURL)

	if err := v.WriteConfig(); err != nil {
		return fmt.Errorf("failed to write config: %w", err)
	}

	return nil
}
