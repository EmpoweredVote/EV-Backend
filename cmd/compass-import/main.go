package main

import (
	"flag"
	"log"
	"os"

	"github.com/EmpoweredVote/EV-Backend/internal/compassimport"
)

func main() {
	var (
		csvPath   = flag.String("csv", "", "path to CSV export")
		dbURL     = flag.String("db", "", "DATABASE_URL")
		namespace = flag.String("namespace", "", "UUID Namespace (required, stable forever)")
		wipe      = flag.Bool("wipe", false, "DANGER: truncates compass tables before importing")
	)
	flag.Parse()

	if *csvPath == "" || *dbURL == "" || *namespace == "" {
		flag.Usage()
		os.Exit(2)
	}

	cfg := compassimport.Config{
		CSVPath:     *csvPath,
		DatabaseURL: *dbURL,
		Namespace:   *namespace,
		Wipe:        *wipe,
	}

	if err := compassimport.Run(cfg); err != nil {
		log.Fatal(err)
	}

}
