package main

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/EmpoweredVote/EV-Backend/internal/essentials/ballotready"
)

func main() {
	// Load environment variables
	if err := godotenv.Load(".env.local"); err != nil {
		log.Printf("Warning: .env.local not found, using system environment variables")
	}

	apiKey := os.Getenv("BALLOTREADY_KEY")
	if apiKey == "" {
		log.Fatal("BALLOTREADY_KEY environment variable not set")
	}

	fmt.Println("Testing BallotReady API - Fetching federal officials...")
	fmt.Println("This will save JSON responses to: /Users/chrisandrews/Documents/GitHub/EV-Backend/ballotready_samples/")
	fmt.Println()

	if err := ballotready.TestFederalOfficials(); err != nil {
		log.Fatalf("Error: %v", err)
	}

	fmt.Println("\n✓ Complete! Review the JSON files in ballotready_samples/ directory")
}
