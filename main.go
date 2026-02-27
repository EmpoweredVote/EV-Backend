package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/EmpoweredVote/EV-Backend/internal/auth"
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

	fmt.Printf("Server listening on :%s...\n", port)

	if err := http.ListenAndServe("0.0.0.0:"+port, r); err != nil {
		log.Fatal("Server failed: ", err)
	}
}
