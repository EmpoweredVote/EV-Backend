// cmd/seed-sources reads data/seed/source_ids.json and upserts rows into
// transparent_motivations.politician_sources. It also ensures every politician
// in prototype_politicians.json has at least one row (with research_status
// "needs_research" if no source IDs have been researched yet).
//
// Usage:
//
//	go run cmd/seed-sources/main.go
//
// Env:
//
//	DATABASE_URL  Postgres DSN (required)
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/EmpoweredVote/EV-Backend/internal/campaign_finance"
	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

// SourceIDEntry mirrors the shape of data/seed/source_ids.json.
type SourceIDEntry struct {
	EssentialsPoliticianID string `json:"essentials_politician_id"`
	PoliticianName         string `json:"politician_name"`
	SourceSystem           string `json:"source_system"`
	ExternalID             string `json:"external_id"`
	ResearchStatus         string `json:"research_status"`
	Notes                  string `json:"notes"`
}

// PrototypePolitician mirrors data/seed/prototype_politicians.json.
type PrototypePolitician struct {
	EssentialsPoliticianID string `json:"essentials_politician_id"`
	Name                   string `json:"name"`
}

func dataDir() string {
	if override := os.Getenv("SEED_SOURCES_DATA_DIR"); override != "" {
		return override
	}
	return filepath.Join("data", "seed")
}

func readJSON(filename string, v any) error {
	path := filepath.Join(dataDir(), filename)
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(v); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	return nil
}

func main() {
	_ = godotenv.Load(".env.local")
	_ = godotenv.Load(".env")

	if os.Getenv("DATABASE_URL") == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	db.Connect()

	// Ensure schema + tables exist (idempotent).
	campaign_finance.Init()

	// Load seed data.
	var entries []SourceIDEntry
	if err := readJSON("source_ids.json", &entries); err != nil {
		log.Fatalf("read source_ids.json: %v", err)
	}

	var politicians []PrototypePolitician
	if err := readJSON("prototype_politicians.json", &politicians); err != nil {
		log.Fatalf("read prototype_politicians.json: %v", err)
	}

	// Track which politician IDs already have at least one source entry.
	seededIDs := map[string]bool{}
	for _, e := range entries {
		seededIDs[e.EssentialsPoliticianID] = true
	}

	// For any politician with no source entries at all, add a placeholder.
	for _, p := range politicians {
		if !seededIDs[p.EssentialsPoliticianID] {
			entries = append(entries, SourceIDEntry{
				EssentialsPoliticianID: p.EssentialsPoliticianID,
				PoliticianName:         p.Name,
				SourceSystem:           "unknown",
				ExternalID:             "",
				ResearchStatus:         "needs_research",
				Notes:                  "No source IDs researched yet",
			})
		}
	}

	// Upsert each entry.
	created, updated, skipped := 0, 0, 0

	for _, e := range entries {
		polID, err := uuid.Parse(e.EssentialsPoliticianID)
		if err != nil {
			log.Printf("SKIP %s (%s): invalid UUID: %v", e.PoliticianName, e.EssentialsPoliticianID, err)
			skipped++
			continue
		}

		var existing campaign_finance.PoliticianSource
		result := db.DB.Where(
			"essentials_politician_id = ? AND source_system = ?",
			polID, e.SourceSystem,
		).First(&existing)

		if result.Error != nil {
			// Row not found — create it.
			row := campaign_finance.PoliticianSource{
				ID:                     uuid.New(),
				EssentialsPoliticianID: polID,
				SourceSystem:           e.SourceSystem,
				ExternalID:             e.ExternalID,
				ResearchStatus:         e.ResearchStatus,
				Notes:                  e.Notes,
			}
			if err := db.DB.Create(&row).Error; err != nil {
				log.Fatalf("create politician_source for %s/%s: %v", e.PoliticianName, e.SourceSystem, err)
			}
			fmt.Printf("  CREATED  %-30s  %-14s  %-14s  %s\n", e.PoliticianName, e.SourceSystem, e.ExternalID, e.ResearchStatus)
			created++
		} else {
			// Row exists — update it (unless no change needed).
			existing.ExternalID = e.ExternalID
			existing.ResearchStatus = e.ResearchStatus
			existing.Notes = e.Notes
			if err := db.DB.Save(&existing).Error; err != nil {
				log.Fatalf("update politician_source for %s/%s: %v", e.PoliticianName, e.SourceSystem, err)
			}
			fmt.Printf("  UPDATED  %-30s  %-14s  %-14s  %s\n", e.PoliticianName, e.SourceSystem, e.ExternalID, e.ResearchStatus)
			updated++
		}
	}

	fmt.Printf("\n=== Seed complete: %d created, %d updated, %d skipped ===\n", created, updated, skipped)

	// Print summary query results.
	type StatusCount struct {
		ResearchStatus string
		Count          int64
	}
	var counts []StatusCount
	db.DB.Raw(`SELECT research_status, count(*) as count FROM transparent_motivations.politician_sources GROUP BY research_status ORDER BY research_status`).Scan(&counts)
	fmt.Println("\nDB summary:")
	for _, c := range counts {
		fmt.Printf("  %-20s %d rows\n", c.ResearchStatus, c.Count)
	}
}
