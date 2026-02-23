// Package main — bulk-import CLI tool.
// This tool previously used the Cicero API warmer to seed ZIP-based politician data.
// Live API calls have been removed. This tool is no longer functional.
// Data import now requires a new pipeline.
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
	fmt.Println("This tool has been deprecated. Data import pipeline is no longer available.")
	fmt.Println("Data import requires a new pipeline implementation.")
	fmt.Println("This CLI tool is no longer functional.")
}
