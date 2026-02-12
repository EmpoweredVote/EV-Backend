package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/EmpoweredVote/EV-Backend/internal/essentials"
	"github.com/EmpoweredVote/EV-Backend/internal/essentials/cicero"
	"github.com/joho/godotenv"
)

// States to backfill with a representative ZIP code for each
var targets = []struct {
	State string
	Zip   string
}{
	{State: "IN", Zip: "47401"}, // Bloomington, IN
	{State: "CA", Zip: "90001"}, // Los Angeles, CA
}

func main() {
	_ = godotenv.Load(".env.local")

	apiKey := os.Getenv("CICERO_KEY")
	if apiKey == "" {
		log.Fatal("CICERO_KEY environment variable is required")
	}

	db.Connect()
	essentials.Init()

	client := cicero.NewClient(apiKey)

	// Verify Cicero API is reachable
	ctx := context.Background()
	if err := client.HealthCheck(ctx); err != nil {
		log.Fatalf("Cicero API health check failed: %v", err)
	}
	fmt.Println("Cicero API: OK")

	dryRun := false
	for _, arg := range os.Args[1:] {
		if arg == "--dry-run" {
			dryRun = true
			fmt.Println("Mode: DRY RUN (no database writes)")
		}
	}
	if !dryRun {
		fmt.Println("Mode: LIVE (will write to database)")
	}
	fmt.Println()

	totalNew := 0
	totalSkipped := 0

	for _, target := range targets {
		fmt.Printf("========================================\n")
		fmt.Printf("Backfilling STATE_EXEC for %s (ZIP: %s)\n", target.State, target.Zip)
		fmt.Printf("========================================\n")

		// Fetch STATE_EXEC officials from Cicero
		officials, err := client.FetchOfficialsByZip(ctx, target.Zip, []string{"STATE_EXEC"})
		if err != nil {
			log.Printf("  ERROR fetching %s: %v", target.State, err)
			continue
		}
		fmt.Printf("  Cicero returned %d STATE_EXEC officials\n\n", len(officials))

		// Transform to normalized format
		normalized := cicero.TransformBatch(officials)

		// Check each against existing DB records
		now := time.Now()
		for _, off := range normalized {
			title := off.Office.Title
			name := fmt.Sprintf("%s %s", off.FirstName, off.LastName)
			appointed := off.IsAppointed
			status := "ELECTED"
			if appointed {
				status = "APPOINTED"
			}

			// Check if this official already exists by name + title + state
			exists, existingName := officialExistsInDB(target.State, title, off.FirstName, off.LastName)
			if exists {
				fmt.Printf("  SKIP  %-10s %-25s %s (already in DB as %s)\n", status, name, title, existingName)
				totalSkipped++
				continue
			}

			fmt.Printf("  NEW   %-10s %-25s %s\n", status, name, title)

			if !dryRun {
				id, err := essentials.UpsertNormalizedOfficial(ctx, off, now)
				if err != nil {
					log.Printf("    ERROR upserting %s: %v", name, err)
					continue
				}
				fmt.Printf("    -> Inserted with ID: %s\n", id)
			}
			totalNew++
		}
		fmt.Println()
	}

	fmt.Printf("========================================\n")
	fmt.Printf("Done! New: %d, Skipped: %d\n", totalNew, totalSkipped)
	if dryRun {
		fmt.Println("(dry run — no changes written)")
	}
	fmt.Printf("========================================\n")
}

// officialExistsInDB checks if a politician with a matching title already exists
// for the given state. Matches on last name + title to handle name variations.
func officialExistsInDB(state, title, firstName, lastName string) (bool, string) {
	type result struct {
		FullName string
	}
	var r result

	// Match by last name + office title + state (handles name variations like "Bob" vs "Robert")
	err := db.DB.Raw(`
		SELECT p.full_name
		FROM essentials.politicians p
		JOIN essentials.offices o ON o.politician_id = p.id
		JOIN essentials.districts d ON d.id = o.district_id
		WHERE d.state = ?
		  AND d.district_type = 'STATE_EXEC'
		  AND LOWER(o.title) = LOWER(?)
		  AND LOWER(p.last_name) = LOWER(?)
		LIMIT 1
	`, state, title, lastName).Scan(&r).Error

	if err != nil || r.FullName == "" {
		// No match by last name + title; also try first+last name in case title differs
		err = db.DB.Raw(`
			SELECT p.full_name
			FROM essentials.politicians p
			JOIN essentials.offices o ON o.politician_id = p.id
			JOIN essentials.districts d ON d.id = o.district_id
			WHERE d.state = ?
			  AND d.district_type = 'STATE_EXEC'
			  AND LOWER(p.first_name) = LOWER(?)
			  AND LOWER(p.last_name) = LOWER(?)
			LIMIT 1
		`, state, firstName, strings.ToLower(lastName)).Scan(&r).Error
		if err != nil || r.FullName == "" {
			return false, ""
		}
	}

	return true, r.FullName
}
