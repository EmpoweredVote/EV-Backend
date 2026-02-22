// Package main — bulk-import CLI tool.
// This tool previously used the Cicero API warmer to seed ZIP-based politician data.
// The BallotReady warmer and live API calls have been removed in Phase 27.
// This tool is no longer functional — data import now requires a new pipeline.
package main

import (
	"fmt"
	"log"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime)
	fmt.Println("=========================================")
	fmt.Println("bulk-import: DEPRECATED")
	fmt.Println("=========================================")
	fmt.Println("The live BallotReady warmer has been removed (Phase 27).")
	fmt.Println("Data import requires a new pipeline implementation.")
	fmt.Println("This CLI tool is no longer functional.")
}
