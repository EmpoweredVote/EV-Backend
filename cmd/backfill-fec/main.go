// cmd/backfill-fec loads all confirmed FEC politician sources from the DB and
// runs the FEC ingestion pipeline for every available election cycle, iterating
// backward from the current cycle until a cycle returns 0 results.
//
// The binary is idempotent: re-running it creates new IngestionRun rows but
// inserts 0 new contribution rows for already-ingested data (ON CONFLICT DO NOTHING).
//
// Usage:
//
//	DATABASE_URL=<dsn> FEC_API_KEY=<key> go run cmd/backfill-fec/main.go
package main

import (
	"log"
	"os"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/campaign_finance"
	"github.com/EmpoweredVote/EV-Backend/internal/campaign_finance/adapter"
	"github.com/EmpoweredVote/EV-Backend/internal/campaign_finance/adapter/fec"
	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/joho/godotenv"
)

func main() {
	// Load .env.local if present (no-op in production where env is set directly).
	_ = godotenv.Load(".env.local")

	if os.Getenv("DATABASE_URL") == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}

	// Connect to the database using the same pattern as the main app.
	db.Connect()

	// Auto-migrate campaign_finance tables so the binary works against a fresh DB.
	campaign_finance.Init()

	// Load all confirmed FEC politician sources.
	var sources []campaign_finance.PoliticianSource
	if err := db.DB.Where("source_system IN ? AND research_status = ?", []string{"fec_house", "fec_senate"}, "confirmed").
		Find(&sources).Error; err != nil {
		log.Fatalf("backfill-fec: failed to load confirmed FEC sources: %v", err)
	}

	if len(sources) == 0 {
		log.Println("backfill-fec: no confirmed FEC sources found — nothing to do")
		return
	}
	log.Printf("backfill-fec: found %d confirmed FEC source(s)", len(sources))

	// Determine current election cycle (FEC cycles are even years).
	currentYear := time.Now().Year()
	if currentYear%2 != 0 {
		currentYear++ // round up to next even year
	}
	startCycle := currentYear

	totalRuns := 0
	totalInserted := 0
	totalSkipped := 0

	for _, ps := range sources {
		log.Printf("backfill-fec: processing source %s (external_id=%s)", ps.ID, ps.ExternalID)

		for cycle := startCycle; ; cycle -= 2 {
			cycleStr := itoa(cycle)

			log.Printf("  cycle %s: starting ingestion", cycleStr)

			runStarted := time.Now()

			fecAdapter := fec.New(cycleStr)
			if err := adapter.RunIngestion(fecAdapter, ps, cycleStr); err != nil {
				log.Printf("  cycle %s: ERROR — %v (continuing to next politician)", cycleStr, err)
				break
			}

			// Retrieve the IngestionRun created by this call to check RecordsFetched.
			var run campaign_finance.IngestionRun
			if err := db.DB.
				Where("politician_source_id = ? AND election_cycle = ? AND started_at >= ?",
					ps.ID, cycleStr, runStarted).
				Order("started_at DESC").
				First(&run).Error; err != nil {
				log.Printf("  cycle %s: could not retrieve run record: %v", cycleStr, err)
				break
			}

			totalRuns++
			totalInserted += run.RecordsInserted
			totalSkipped += run.RecordsSkipped

			log.Printf("  cycle %s: status=%s fetched=%d inserted=%d skipped=%d",
				cycleStr, run.Status, run.RecordsFetched, run.RecordsInserted, run.RecordsSkipped)

			// Stop iterating cycles for this politician when the API returns no records.
			// This avoids looping all the way back to 1979 for politicians with limited history.
			if run.RecordsFetched == 0 {
				log.Printf("  cycle %s: 0 records fetched — stopping cycle iteration for this source", cycleStr)
				break
			}
		}
	}

	log.Printf("backfill-fec: complete — runs=%d contributions_inserted=%d contributions_skipped=%d",
		totalRuns, totalInserted, totalSkipped)
}

// itoa converts an int to its decimal string representation without importing strconv.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := make([]byte, 0, 8)
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	if neg {
		buf = append([]byte{'-'}, buf...)
	}
	return string(buf)
}
