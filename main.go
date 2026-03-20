package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/auth"
	"github.com/EmpoweredVote/EV-Backend/internal/campaign_finance"
	"github.com/EmpoweredVote/EV-Backend/internal/campaign_finance/adapter"
	"github.com/EmpoweredVote/EV-Backend/internal/campaign_finance/adapter/calaccess"
	"github.com/EmpoweredVote/EV-Backend/internal/campaign_finance/adapter/fec"
	"github.com/EmpoweredVote/EV-Backend/internal/campaign_finance/adapter/indiana"
	"github.com/EmpoweredVote/EV-Backend/internal/campaign_finance/adapter/socrata"
	"github.com/EmpoweredVote/EV-Backend/internal/campaign_finance/scheduler"
	"github.com/EmpoweredVote/EV-Backend/internal/compass"
	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/EmpoweredVote/EV-Backend/internal/essentials"
	"github.com/EmpoweredVote/EV-Backend/internal/meetings"
	"github.com/EmpoweredVote/EV-Backend/internal/middleware"
	"github.com/EmpoweredVote/EV-Backend/internal/quoteimport"
	"github.com/EmpoweredVote/EV-Backend/internal/stanceimport"
	"github.com/EmpoweredVote/EV-Backend/internal/staging"
	"github.com/EmpoweredVote/EV-Backend/internal/treasury"
	"github.com/EmpoweredVote/EV-Backend/internal/webhooks"
	"github.com/go-chi/chi/v5"
	"github.com/joho/godotenv"
)

func RootHandler(w http.ResponseWriter, r *http.Request) {
	response := "Server is up!"
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintln(w, response)
}

func main() {
	_ = godotenv.Load(".env.local")
	db.Connect()

	port := os.Getenv("PORT")
	if port == "" {
		port = "5050"
	}

	auth.Init()
	compass.Init()
	essentials.Init()
	treasury.Init()
	staging.Init()
	meetings.Init()
	campaign_finance.Init()

	// Wire FEC ingestion function — avoids import cycle between campaign_finance
	// and campaign_finance/adapter by injecting the implementation at startup.
	campaign_finance.SetFECIngestFunc(func(ps campaign_finance.PoliticianSource, cycle string) error {
		return adapter.RunIngestion(fec.New(cycle), ps, cycle)
	})

	// Wire Cal-Access ingestion — downloads ZIP once, runs per-politician ingestion.
	campaign_finance.SetCalAccessIngestAllFunc(func() ([]campaign_finance.CalAccessResult, error) {
		// Query all confirmed cal_access politician sources
		var sources []campaign_finance.PoliticianSource
		if err := db.DB.Where("source_system = ? AND research_status = ?", "cal_access", "confirmed").Find(&sources).Error; err != nil {
			return nil, fmt.Errorf("query cal_access sources: %w", err)
		}
		if len(sources) == 0 {
			return nil, fmt.Errorf("no confirmed cal_access politician sources found")
		}

		// Create adapter (does NOT download — PreDownload does that)
		ca := calaccess.New(db.DB)
		defer ca.Cleanup()

		// Download ZIP once (check ETag)
		if err := ca.PreDownload(context.Background()); err != nil {
			return nil, fmt.Errorf("cal-access download: %w", err)
		}

		var results []campaign_finance.CalAccessResult

		if ca.ZIPSkipped() {
			// ZIP unchanged (HTTP 304) — write skipped_no_change run per politician
			now := time.Now()
			dlAt := ca.ZIPDownloadedAt()
			for _, ps := range sources {
				skipRun := campaign_finance.IngestionRun{
					AdapterName:        "cal_access",
					PoliticianSourceID: &ps.ID,
					StartedAt:          now,
					CompletedAt:        &now,
					Status:             "skipped_no_change",
					Notes:              "ZIP unchanged (HTTP 304)",
					SourceETag:         ca.ZIPETag(),
					ZIPDownloadedAt:    &dlAt,
				}
				db.DB.Create(&skipRun)
				results = append(results, campaign_finance.CalAccessResult{
					PoliticianSourceID: ps.ID.String(),
					Status:             "skipped_no_change",
				})
			}
			if err := ca.SaveETag(); err != nil {
				log.Printf("warning: failed to save cal-access ETag: %v", err)
			}
			return results, nil
		}

		// ZIP has new data — run per-politician ingestion (non-aborting)
		for _, ps := range sources {
			result := campaign_finance.CalAccessResult{PoliticianSourceID: ps.ID.String()}
			if err := adapter.RunIngestion(ca, ps, ""); err != nil {
				result.Status = "failed"
				result.Error = err.Error()
				log.Printf("cal-access ingestion failed for %s: %v", ps.ID, err)
			} else {
				var run campaign_finance.IngestionRun
				db.DB.Where("politician_source_id = ? AND adapter_name = ?", ps.ID, "cal_access").
					Order("started_at DESC").First(&run)
				result.Status = run.Status
				result.RecordsInserted = run.RecordsInserted
			}
			results = append(results, result)
		}

		if err := ca.SaveETag(); err != nil {
			log.Printf("warning: failed to save cal-access ETag: %v", err)
		}
		return results, nil
	})

	// Wire Socrata ingestion — REST API, no ZIP lifecycle.
	socrataToken := os.Getenv("SOCRATA_APP_TOKEN")
	if socrataToken == "" {
		log.Fatal("SOCRATA_APP_TOKEN environment variable is not set")
	}
	campaign_finance.SetSocrataIngestAllFunc(func() ([]campaign_finance.SocrataResult, error) {
		// Query all confirmed la_socrata politician sources.
		var sources []campaign_finance.PoliticianSource
		if err := db.DB.Where("source_system = ? AND research_status = ?", "la_socrata", "confirmed").Find(&sources).Error; err != nil {
			return nil, fmt.Errorf("query la_socrata sources: %w", err)
		}
		if len(sources) == 0 {
			return nil, fmt.Errorf("no confirmed la_socrata politician sources found")
		}

		sa := socrata.New(db.DB, socrataToken)
		var results []campaign_finance.SocrataResult

		// Non-aborting loop — per-politician failure logged, continues to next.
		for _, ps := range sources {
			result := campaign_finance.SocrataResult{PoliticianSourceID: ps.ID.String()}

			// Warn if external_id is empty (no cmt_id seeded).
			if ps.ExternalID == "" {
				result.Status = "completed_with_warning"
				result.Error = "no external_id (cmt_id) configured"
				log.Printf("socrata: skipping %s — no external_id", ps.ID)
				now := time.Now()
				warnRun := campaign_finance.IngestionRun{
					AdapterName:        "la_socrata",
					PoliticianSourceID: &ps.ID,
					StartedAt:          now,
					CompletedAt:        &now,
					Status:             "completed_with_warning",
					Notes:              "no external_id (cmt_id) configured — skipped",
				}
				db.DB.Create(&warnRun)
				results = append(results, result)
				continue
			}

			if err := adapter.RunIngestion(sa, ps, ""); err != nil {
				result.Status = "failed"
				result.Error = err.Error()
				log.Printf("socrata ingestion failed for %s: %v", ps.ID, err)
			} else {
				// Retrieve the IngestionRun that RunIngestion just created.
				var run campaign_finance.IngestionRun
				db.DB.Where("politician_source_id = ? AND adapter_name = ?", ps.ID, "la_socrata").
					Order("started_at DESC").First(&run)

				// Zero-record check: if RunIngestion succeeded but fetched zero records,
				// override to completed_with_warning.
				if run.RecordsFetched == 0 {
					run.Status = "completed_with_warning"
					run.Notes = "zero contribution rows returned for seeded politician"
					db.DB.Save(&run)
					log.Printf("socrata: zero records for %s (cmt_id=%s) — marked completed_with_warning", ps.ID, ps.ExternalID)
				}

				result.Status = run.Status
				result.RecordsInserted = run.RecordsInserted
			}
			results = append(results, result)
		}
		return results, nil
	})

	// Wire Indiana ingestion — annual ZIP, OrgId exact-match, unresolved queue.
	campaign_finance.SetIndianaIngestAllFunc(func() ([]campaign_finance.IndianaResult, int, error) {
		// Query all confirmed indiana politician sources.
		var sources []campaign_finance.PoliticianSource
		if err := db.DB.Where("source_system = ? AND research_status = ?", "indiana", "confirmed").Find(&sources).Error; err != nil {
			return nil, 0, fmt.Errorf("query indiana sources: %w", err)
		}
		if len(sources) == 0 {
			// No confirmed sources — log warning, return completed_with_warning (do NOT hard-fail).
			log.Printf("indiana: no confirmed politician sources found — returning completed_with_warning")
			return []campaign_finance.IndianaResult{{
				Status: "completed_with_warning",
				Error:  "no confirmed indiana politician sources found",
			}}, 0, nil
		}

		// Use previous complete year (most recent available data).
		year := time.Now().Year() - 1

		// Create adapter
		ia := indiana.New(db.DB, year)
		defer ia.Cleanup()

		// Download ZIP (check ETag)
		if err := ia.PreDownload(context.Background()); err != nil {
			return nil, 0, fmt.Errorf("indiana download: %w", err)
		}

		var results []campaign_finance.IndianaResult

		if ia.ZIPSkipped() {
			// ZIP unchanged (HTTP 304) — write skipped_no_change run per politician.
			now := time.Now()
			dlAt := ia.ZIPDownloadedAt()
			for _, ps := range sources {
				skipRun := campaign_finance.IngestionRun{
					AdapterName:        "indiana",
					PoliticianSourceID: &ps.ID,
					StartedAt:          now,
					CompletedAt:        &now,
					Status:             "skipped_no_change",
					Notes:              "ZIP unchanged (HTTP 304)",
					SourceETag:         ia.ZIPETag(),
					ZIPDownloadedAt:    &dlAt,
				}
				db.DB.Create(&skipRun)
				results = append(results, campaign_finance.IndianaResult{
					PoliticianSourceID: ps.ID.String(),
					Status:             "skipped_no_change",
				})
			}
			if err := ia.SaveETag(); err != nil {
				log.Printf("warning: failed to save indiana ETag: %v", err)
			}
			return results, 0, nil
		}

		// ZIP has new data — run per-politician ingestion (non-aborting).
		for _, ps := range sources {
			result := campaign_finance.IndianaResult{PoliticianSourceID: ps.ID.String()}
			if err := adapter.RunIngestion(ia, ps, ""); err != nil {
				result.Status = "failed"
				result.Error = err.Error()
				log.Printf("indiana ingestion failed for %s: %v", ps.ID, err)
			} else {
				// Look up the run to report stats.
				var run campaign_finance.IngestionRun
				db.DB.Where("politician_source_id = ? AND adapter_name = ?", ps.ID, "indiana").
					Order("started_at DESC").First(&run)
				result.Status = run.Status
				result.RecordsInserted = run.RecordsInserted
				result.RecordsFetched = run.RecordsFetched

				// Zero records for a seeded politician -> completed_with_warning.
				if run.RecordsFetched == 0 && run.Status == "completed" {
					run.Status = "completed_with_warning"
					run.Notes = "zero records fetched for seeded politician"
					db.DB.Save(&run)
					result.Status = "completed_with_warning"
				}
			}
			results = append(results, result)
		}

		// Write unresolved rows to queue (after all politicians processed).
		unresolvedCount := 0
		if ia.UnresolvedCount() > 0 {
			var lastRun campaign_finance.IngestionRun
			db.DB.Where("adapter_name = ?", "indiana").Order("started_at DESC").First(&lastRun)
			written, err := ia.WriteUnresolved(lastRun.ID)
			if err != nil {
				log.Printf("indiana: failed to write unresolved rows: %v", err)
			} else {
				log.Printf("indiana: wrote %d unresolved rows", written)
				unresolvedCount = written
			}

			// Warning threshold: if unresolved > 50% of total fetched, mark runs as warning.
			// Use RecordsFetched (not RecordsInserted) as denominator.
			totalFetched := 0
			for _, r := range results {
				totalFetched += r.RecordsFetched
			}
			if totalFetched > 0 && float64(unresolvedCount)/float64(totalFetched+unresolvedCount) > 0.50 {
				log.Printf("indiana: unresolved ratio %.0f%% exceeds 50%% threshold",
					float64(unresolvedCount)/float64(totalFetched+unresolvedCount)*100)
				for _, ps := range sources {
					db.DB.Model(&campaign_finance.IngestionRun{}).
						Where("politician_source_id = ? AND adapter_name = ? AND status = ?", ps.ID, "indiana", "completed").
						Order("started_at DESC").Limit(1).
						Update("status", "completed_with_warning")
				}
			}

			// Update RecordsUnresolved on each run.
			for _, ps := range sources {
				db.DB.Model(&campaign_finance.IngestionRun{}).
					Where("politician_source_id = ? AND adapter_name = ? AND records_unresolved = 0", ps.ID, "indiana").
					Order("started_at DESC").Limit(1).
					Update("records_unresolved", unresolvedCount)
			}
		}

		if err := ia.SaveETag(); err != nil {
			log.Printf("warning: failed to save indiana ETag: %v", err)
		}
		return results, unresolvedCount, nil
	})

	// --- Scheduler setup ---
	// Start after all Init() calls (AutoMigrate must run first) and before
	// CLI subcommand dispatch. Non-fatal: missing Redis / config just logs a warning.
	fecIngestAllFn := func() error {
		var sources []campaign_finance.PoliticianSource
		if err := db.DB.Where("source_system = ? AND research_status = ?", "fec", "confirmed").Find(&sources).Error; err != nil {
			return fmt.Errorf("query fec sources: %w", err)
		}
		if len(sources) == 0 {
			log.Printf("scheduler: no confirmed FEC sources, skipping")
			return nil
		}
		// Current election cycle (even year).
		year := time.Now().Year()
		if year%2 != 0 {
			year++
		}
		cycle := fmt.Sprintf("%d", year)
		for _, ps := range sources {
			if err := adapter.RunIngestion(fec.New(cycle), ps, cycle); err != nil {
				log.Printf("scheduler: FEC ingestion failed for %s: %v", ps.ID, err)
				// Non-aborting: continue to next source.
			}
		}
		return nil
	}

	sched, schedErr := scheduler.New(scheduler.Config{
		RedisURL: os.Getenv("UPSTASH_REDIS_URL"),
		HealthcheckURLs: map[string]string{
			"fec":        os.Getenv("HC_FEC_URL"),
			"cal-access": os.Getenv("HC_CAL_ACCESS_URL"),
			"indiana":    os.Getenv("HC_INDIANA_URL"),
			"socrata":    os.Getenv("HC_SOCRATA_URL"),
		},
		FECIngestFn: fecIngestAllFn,
		DB:          db.DB,
	})
	if schedErr != nil {
		log.Printf("warning: scheduler init failed, FEC auto-polling disabled: %v", schedErr)
	} else {
		sched.Start()
		defer sched.Stop()
	}

	// Register FEC ingest-all closure with the admin handler so it is reachable
	// via POST /admin/ingest/fec (token-authenticated, no session required).
	campaign_finance.SetFECIngestAllFunc(fecIngestAllFn)

	// SQS worker: long-polls for EventBridge-triggered cal-access and indiana runs.
	// Dispatchers are thin closures that delegate to the package-level ingest fns.
	sqsDispatchers := map[string]func() error{
		"cal-access": func() error { return campaign_finance.DispatchAdapter("cal-access") },
		"indiana":    func() error { return campaign_finance.DispatchAdapter("indiana") },
	}

	sqsCtx, sqsCancel := context.WithCancel(context.Background())
	defer sqsCancel()
	if err := scheduler.StartSQSWorker(sqsCtx, scheduler.SQSWorkerConfig{
		QueueURL:    os.Getenv("SQS_INGEST_QUEUE_URL"),
		Dispatchers: sqsDispatchers,
		HealthcheckURLs: map[string]string{
			"cal-access": os.Getenv("HC_CAL_ACCESS_URL"),
			"indiana":    os.Getenv("HC_INDIANA_URL"),
		},
	}); err != nil {
		log.Printf("warning: SQS worker init failed: %v", err)
	}

	// CLI subcommand dispatch — must come after all Init() calls so tables
	// are migrated and the global db.DB connection is ready.
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "import-stances":
			csvPath := "data/stance_research.csv"
			dryRun := false
			if len(os.Args) > 2 {
				// First positional arg after the subcommand is the CSV path
				// unless it starts with "--"
				if os.Args[2][0] != '-' {
					csvPath = os.Args[2]
				}
			}
			for _, arg := range os.Args[2:] {
				if arg == "--dry-run" {
					dryRun = true
				}
			}
			result, err := stanceimport.Run(stanceimport.Config{
				CSVPath: csvPath,
				DryRun:  dryRun,
			})
			if err != nil {
				log.Fatal("import-stances failed: ", err)
			}
			fmt.Printf("Import complete: %d processed, %d inserted, %d updated, %d skipped\n",
				result.Processed, result.Inserted, result.Updated, result.Skipped)
			os.Exit(0)
		case "search-politician":
			if len(os.Args) < 3 {
				log.Fatal("usage: ./server search-politician <name>")
			}
			searchName := os.Args[2]
			type polResult struct {
				ID         string `gorm:"column:id"`
				FullName   string `gorm:"column:full_name"`
				FirstName  string `gorm:"column:first_name"`
				LastName   string `gorm:"column:last_name"`
				ExternalID int    `gorm:"column:external_id"`
				Party      string `gorm:"column:party"`
			}
			var results []polResult
			db.DB.Raw(`SELECT id, full_name, first_name, last_name, external_id, party
				FROM essentials.politicians
				WHERE LOWER(full_name) LIKE LOWER(?) OR LOWER(last_name) LIKE LOWER(?)`,
				"%"+searchName+"%", "%"+searchName+"%").Scan(&results)
			if len(results) == 0 {
				fmt.Printf("No politicians found matching '%s'\n", searchName)
			} else {
				fmt.Printf("Found %d matches for '%s':\n", len(results), searchName)
				for _, r := range results {
					fmt.Printf("  ID: %s  external_id: %d  name: %s  party: %s\n", r.ID, r.ExternalID, r.FullName, r.Party)
				}
			}
			os.Exit(0)
		case "add-politician":
			if len(os.Args) < 6 {
				log.Fatal("usage: ./server add-politician <full_name> <first_name> <last_name> <party>")
			}
			fullName, firstName, lastName, party := os.Args[2], os.Args[3], os.Args[4], os.Args[5]
			var count int64
			db.DB.Table("essentials.politicians").Where("LOWER(full_name) = LOWER(?)", fullName).Count(&count)
			if count > 0 {
				fmt.Printf("Politician '%s' already exists (%d records)\n", fullName, count)
				os.Exit(1)
			}
			err := db.DB.Exec(`INSERT INTO essentials.politicians (id, full_name, first_name, last_name, party)
				VALUES (uuid_generate_v4(), ?, ?, ?, ?)`, fullName, firstName, lastName, party).Error
			if err != nil {
				log.Fatal("insert failed: ", err)
			}
			fmt.Printf("Added politician: %s (%s)\n", fullName, party)
			os.Exit(0)
		case "run-sql":
			if len(os.Args) < 3 {
				log.Fatal("usage: ./server run-sql <sql>")
			}
			sql := os.Args[2]
			result := db.DB.Exec(sql)
			if result.Error != nil {
				log.Fatal("SQL error: ", result.Error)
			}
			fmt.Printf("OK — %d rows affected\n", result.RowsAffected)
			os.Exit(0)
		case "import-quotes":
			csvPath := "data/quote_collection.csv"
			dryRun := false
			if len(os.Args) > 2 {
				csvPath = os.Args[2]
			}
			for _, arg := range os.Args[2:] {
				if arg == "--dry-run" {
					dryRun = true
				}
			}
			result, err := quoteimport.Run(quoteimport.Config{
				CSVPath: csvPath,
				DryRun:  dryRun,
			})
			if err != nil {
				log.Fatal("import-quotes failed: ", err)
			}
			fmt.Printf("Import complete: %d processed, %d inserted, %d updated, %d skipped\n",
				result.Processed, result.Inserted, result.Updated, result.Skipped)
			os.Exit(0)
		}
	}

	r := chi.NewRouter()
	r.Use(middleware.CORSMiddleware)
	r.Get("/", RootHandler)

	r.Mount("/auth", auth.SetupRoutes())
	r.Mount("/compass", compass.SetupRoutes())
	r.Mount("/essentials", essentials.SetupRoutes())
	r.Mount("/treasury", treasury.SetupRoutes())
	r.Mount("/staging", staging.SetupRoutes())
	r.Mount("/meetings", meetings.SetupRoutes())
	r.Mount("/webhooks", webhooks.SetupRoutes())
	campaign_finance.Routes(r)

	fmt.Printf("Server listening on :%s...\n", port)

	if err := http.ListenAndServe("0.0.0.0:"+port, r); err != nil {
		log.Fatal("Server failed: ", err)
	}
}
