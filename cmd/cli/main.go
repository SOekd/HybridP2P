package main

import (
	"fmt"
	"os"
)

func main() {
	rootCmd.AddCommand(registerCmd)
	rootCmd.AddCommand(downloadCmd)
	rootCmd.AddCommand(seedCmd)
	rootCmd.AddCommand(unseedCmd)
	rootCmd.AddCommand(listSeedsCmd)
	rootCmd.AddCommand(statusCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
