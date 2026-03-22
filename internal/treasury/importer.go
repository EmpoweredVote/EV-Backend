package treasury

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/lib/pq"
)

// ImportBudgetsConfig holds configuration for the import-budgets CLI subcommand.
type ImportBudgetsConfig struct {
	DataDir string // path to directory containing JSON files (default: "treasury-tracker/public/data")
	DryRun  bool
}

// ImportBudgetsResult holds the result of an import-budgets run.
type ImportBudgetsResult struct {
	FilesProcessed int
	Inserted       int
	Skipped        int
	Errors         []string
}

// budgetJSON mirrors the JSON structure of the Bloomington budget files.
type budgetJSON struct {
	Metadata struct {
		CityName          string   `json:"cityName"`
		FiscalYear        int      `json:"fiscalYear"`
		Population        int      `json:"population"`
		TotalBudget       float64  `json:"totalBudget"`
		TotalCompensation float64  `json:"totalCompensation"` // salary files use this
		TotalRevenue      float64  `json:"totalRevenue"`      // revenue files use this
		GeneratedAt       string   `json:"generatedAt"`
		Hierarchy         []string `json:"hierarchy"`
		DataSource        string   `json:"dataSource"`
		DatasetType       string   `json:"datasetType"` // may be absent in older files
	} `json:"metadata"`
	Categories []CategoryImport `json:"categories"`
}

// ImportBudgets reads all Bloomington JSON budget files from config.DataDir and
// upserts them into the database using GORM. It reuses the importCategories
// function from handlers.go for recursive category tree insertion.
//
// Files processed: budget-YYYY.json, revenue-YYYY.json, salaries-YYYY.json
// for years 2021-2025 (15 files total).
// Linked files (budget-YYYY-linked.json) are intentionally skipped.
func ImportBudgets(config ImportBudgetsConfig) (ImportBudgetsResult, error) {
	if config.DataDir == "" {
		config.DataDir = "treasury-tracker/public/data"
	}

	var result ImportBudgetsResult

	datasets := []struct {
		prefix      string
		datasetType string
	}{
		{"budget", "operating"},
		{"revenue", "revenue"},
		{"salaries", "salaries"},
	}
	years := []int{2021, 2022, 2023, 2024, 2025}

	for _, ds := range datasets {
		for _, year := range years {
			filename := fmt.Sprintf("%s-%d.json", ds.prefix, year)
			filePath := filepath.Join(config.DataDir, filename)

			result.FilesProcessed++

			data, err := os.ReadFile(filePath)
			if err != nil {
				msg := fmt.Sprintf("failed to read %s: %v", filename, err)
				result.Errors = append(result.Errors, msg)
				log.Printf("SKIP: %s", msg)
				continue
			}

			var parsed budgetJSON
			if err := json.Unmarshal(data, &parsed); err != nil {
				msg := fmt.Sprintf("failed to parse %s: %v", filename, err)
				result.Errors = append(result.Errors, msg)
				log.Printf("SKIP: %s", msg)
				continue
			}

			// Resolve total budget amount: prefer totalBudget, fall back to
			// totalCompensation (salary files), then totalRevenue (revenue files).
			totalBudget := parsed.Metadata.TotalBudget
			if totalBudget == 0 {
				totalBudget = parsed.Metadata.TotalCompensation
			}
			if totalBudget == 0 {
				totalBudget = parsed.Metadata.TotalRevenue
			}

			fiscalYear := parsed.Metadata.FiscalYear
			if fiscalYear == 0 {
				fiscalYear = year
			}

			cityName := parsed.Metadata.CityName
			if cityName == "" {
				cityName = "Bloomington"
			}

			if config.DryRun {
				log.Printf("[DRY-RUN] Would import %s: %s %d (%s), totalBudget=%.2f, %d root categories",
					filename, cityName, fiscalYear, ds.datasetType, totalBudget, len(parsed.Categories))
				continue
			}

			tx := db.DB.Begin()
			if tx.Error != nil {
				msg := fmt.Sprintf("failed to begin transaction for %s: %v", filename, tx.Error)
				result.Errors = append(result.Errors, msg)
				continue
			}

			// Find or create municipality.
			var municipality Municipality
			if err := tx.Where("name = ? AND state = ? AND entity_type = ?", cityName, "IN", "city").
				First(&municipality).Error; err != nil {
				municipality = Municipality{
					Name:       cityName,
					State:      "IN",
					EntityType: "city",
					Population: parsed.Metadata.Population,
				}
				if err := tx.Create(&municipality).Error; err != nil {
					tx.Rollback()
					msg := fmt.Sprintf("failed to create municipality for %s: %v", filename, err)
					result.Errors = append(result.Errors, msg)
					continue
				}
			}

			// Check if budget already exists — skip (idempotent).
			var existingBudget Budget
			checkErr := tx.Where("municipality_id = ? AND fiscal_year = ? AND dataset_type = ?",
				municipality.ID, fiscalYear, ds.datasetType).First(&existingBudget).Error
			if checkErr == nil {
				tx.Rollback()
				log.Printf("SKIP: budget already exists for %s %d (%s)", cityName, fiscalYear, ds.datasetType)
				result.Skipped++
				continue
			}

			// Create budget record.
			budget := Budget{
				MunicipalityID:       municipality.ID,
				FiscalYear:           fiscalYear,
				DatasetType:          ds.datasetType,
				TotalBudget:          totalBudget,
				FiscalYearStartMonth: 1, // Indiana = January
				DataSource:           parsed.Metadata.DataSource,
				Hierarchy:            pq.StringArray(parsed.Metadata.Hierarchy),
			}
			if err := tx.Create(&budget).Error; err != nil {
				tx.Rollback()
				msg := fmt.Sprintf("failed to create budget for %s: %v", filename, err)
				result.Errors = append(result.Errors, msg)
				continue
			}

			// Recursively import categories.
			if err := importCategories(tx, budget.ID, nil, parsed.Categories, 0); err != nil {
				tx.Rollback()
				msg := fmt.Sprintf("failed to import categories for %s: %v", filename, err)
				result.Errors = append(result.Errors, msg)
				continue
			}

			if err := tx.Commit().Error; err != nil {
				msg := fmt.Sprintf("failed to commit transaction for %s: %v", filename, err)
				result.Errors = append(result.Errors, msg)
				continue
			}

			log.Printf("Inserted: %s %d (%s), totalBudget=%.2f", cityName, fiscalYear, ds.datasetType, totalBudget)
			result.Inserted++
		}
	}

	return result, nil
}
