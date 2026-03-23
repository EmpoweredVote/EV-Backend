package treasury

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/lib/pq"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
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

// ─── Gateway Import ───────────────────────────────────────────────────────────

// filterEntityRows filters bulk Gateway CSV rows for a specific entity.
// For counties, matches by county_code + unit_code. For cities, matches by unit_code only.
func filterEntityRows(records [][]string, headerIdx map[string]int, entity GatewayEntityConfig, unitCodeCol, countyCodeCol string) [][]string {
	var filtered [][]string
	ucIdx := headerIdx[unitCodeCol]
	ccIdx := headerIdx[countyCodeCol]

	for _, row := range records {
		if len(row) <= ucIdx {
			continue
		}
		rowUnitCode := strings.TrimSpace(row[ucIdx])
		if rowUnitCode != entity.UnitCode {
			continue
		}
		// For counties, also match county_code to disambiguate unit_code=0000
		if entity.CountyCode != "" && len(row) > ccIdx {
			rowCountyCode := strings.TrimSpace(row[ccIdx])
			if rowCountyCode != entity.CountyCode {
				continue
			}
		}
		filtered = append(filtered, row)
	}
	return filtered
}

// ImportGatewayBudgets fetches, parses, and inserts Indiana Gateway budget data
// for all entities defined in the config file. It is the Gateway equivalent of
// ImportBudgets (which handles Bloomington JSON files).
//
// Usage: ./server import-budgets --source=gateway [--config=treasury-import-config.json] [--dry-run]
func ImportGatewayBudgets(configFile string, dryRun bool) (ImportBudgetsResult, error) {
	if configFile == "" {
		configFile = "treasury-import-config.json"
	}

	data, err := os.ReadFile(configFile)
	if err != nil {
		return ImportBudgetsResult{}, fmt.Errorf("failed to read config %s: %w", configFile, err)
	}

	var config GatewayConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return ImportBudgetsResult{}, fmt.Errorf("failed to parse config %s: %w", configFile, err)
	}

	client := &http.Client{Timeout: 60 * time.Second}
	var result ImportBudgetsResult

	for _, entity := range config.GatewayEntities {
		delimiter := '|'
		if entity.Delimiter != "" {
			delimiter = rune(entity.Delimiter[0])
		}

		for _, year := range entity.FiscalYears {
			result.FilesProcessed++

			body, err := fetchGatewayCSV(client, entity, year)
			if err != nil {
				msg := fmt.Sprintf("gateway fetch failed for %s %d: %v", entity.DisplayName, year, err)
				result.Errors = append(result.Errors, msg)
				log.Printf("SKIP: %s", msg)
				continue
			}

			r := newGatewayCSVReader(body, delimiter)

			headers, err := r.Read()
			if err != nil {
				body.Close()
				msg := fmt.Sprintf("failed to read headers for %s %d: %v", entity.DisplayName, year, err)
				result.Errors = append(result.Errors, msg)
				log.Printf("SKIP: %s", msg)
				continue
			}

			// Validate required columns exist
			unitCodeCol := entity.UnitCodeColumn
			if unitCodeCol == "" {
				unitCodeCol = "unit_code"
			}
			countyCodeCol := entity.CountyCodeColumn
			if countyCodeCol == "" {
				countyCodeCol = "cnty_cd"
			}
			required := append(entity.HierarchyColumns, entity.AmountColumn, unitCodeCol)
			if entity.CountyCode != "" {
				required = append(required, countyCodeCol)
			}
			headerIdx, err := validateHeaders(headers, required)
			if err != nil {
				body.Close()
				msg := fmt.Sprintf("header validation failed for %s %d: %v", entity.DisplayName, year, err)
				result.Errors = append(result.Errors, msg)
				log.Printf("SKIP: %s", msg)
				continue
			}

			allRecords, err := r.ReadAll()
			body.Close()
			if err != nil {
				msg := fmt.Sprintf("failed to read CSV rows for %s %d: %v", entity.DisplayName, year, err)
				result.Errors = append(result.Errors, msg)
				log.Printf("SKIP: %s", msg)
				continue
			}

			// Filter rows for this specific entity by unit_code (and county_code if set)
			records := filterEntityRows(allRecords, headerIdx, entity, unitCodeCol, countyCodeCol)
			log.Printf("Filtered %d/%d rows for %s (unit_code=%s) year %d",
				len(records), len(allRecords), entity.DisplayName, entity.UnitCode, year)

			if len(records) == 0 {
				msg := fmt.Sprintf("no rows found for %s (unit_code=%s) in year %d", entity.DisplayName, entity.UnitCode, year)
				result.Errors = append(result.Errors, msg)
				log.Printf("SKIP: %s", msg)
				continue
			}

			// For each dataset type, build tree from filtered rows
			for _, datasetType := range entity.DatasetTypes {
				rootCategories, totalBudget, err := buildGatewayCategoryTree(records, headerIdx, entity)
				if err != nil {
					msg := fmt.Sprintf("failed to build category tree for %s %d %s: %v", entity.DisplayName, year, datasetType, err)
					result.Errors = append(result.Errors, msg)
					log.Printf("SKIP: %s", msg)
					continue
				}

				if dryRun {
					log.Printf("[DRY-RUN] Would import %s %d (%s): totalBudget=%.2f, %d fund categories, %d rows",
						entity.DisplayName, year, datasetType, totalBudget, len(rootCategories), len(records))
					continue
				}

				tx := db.DB.Begin()
				if tx.Error != nil {
					msg := fmt.Sprintf("failed to begin transaction for %s %d %s: %v", entity.DisplayName, year, datasetType, tx.Error)
					result.Errors = append(result.Errors, msg)
					continue
				}

				// Find or create municipality.
				var municipality Municipality
				if err := tx.Where("name = ? AND state = ? AND entity_type = ?",
					entity.DisplayName, entity.State, entity.EntityType).
					First(&municipality).Error; err != nil {
					municipality = Municipality{
						Name:       entity.DisplayName,
						State:      entity.State,
						EntityType: entity.EntityType,
					}
					if err := tx.Create(&municipality).Error; err != nil {
						tx.Rollback()
						msg := fmt.Sprintf("failed to create municipality for %s %d %s: %v", entity.DisplayName, year, datasetType, err)
						result.Errors = append(result.Errors, msg)
						continue
					}
				}

				// Idempotent check: skip if budget already exists.
				var existingBudget Budget
				checkErr := tx.Where("municipality_id = ? AND fiscal_year = ? AND dataset_type = ?",
					municipality.ID, year, datasetType).First(&existingBudget).Error
				if checkErr == nil {
					tx.Rollback()
					log.Printf("SKIP: budget already exists for %s %d (%s)", entity.DisplayName, year, datasetType)
					result.Skipped++
					continue
				}

				// Create budget record.
				budget := Budget{
					MunicipalityID:       municipality.ID,
					FiscalYear:           year,
					DatasetType:          datasetType,
					TotalBudget:          totalBudget,
					FiscalYearStartMonth: entity.FiscalYearStartMonth,
					DataSource:           "Indiana Gateway",
					Hierarchy:            pq.StringArray(entity.HierarchyColumns),
				}
				if err := tx.Create(&budget).Error; err != nil {
					tx.Rollback()
					msg := fmt.Sprintf("failed to create budget for %s %d %s: %v", entity.DisplayName, year, datasetType, err)
					result.Errors = append(result.Errors, msg)
					continue
				}

				// Recursively import categories using existing importCategories helper.
				if err := importCategories(tx, budget.ID, nil, rootCategories, 0); err != nil {
					tx.Rollback()
					msg := fmt.Sprintf("failed to import categories for %s %d %s: %v", entity.DisplayName, year, datasetType, err)
					result.Errors = append(result.Errors, msg)
					continue
				}

				if err := tx.Commit().Error; err != nil {
					msg := fmt.Sprintf("failed to commit transaction for %s %d %s: %v", entity.DisplayName, year, datasetType, err)
					result.Errors = append(result.Errors, msg)
					continue
				}

				log.Printf("Inserted: %s %d (%s), totalBudget=%.2f, %d funds",
					entity.DisplayName, year, datasetType, totalBudget, len(rootCategories))
				result.Inserted++
			}
		}
	}

	return result, nil
}

// fetchGatewayCSV downloads budget CSV data from the Indiana Gateway.
//
// The Indiana Gateway uses ASP.NET WebForms with ViewState tokens. The download
// is a two-step process:
//   1. GET the download page to extract __VIEWSTATE and __EVENTVALIDATION tokens
//   2. POST with those tokens plus the correct form field values
//
// The download returns a bulk CSV containing ALL entities for the selected year.
// Filtering by specific entity (unit_code) is done after download by the caller.
func fetchGatewayCSV(client *http.Client, entity GatewayEntityConfig, year int) (io.ReadCloser, error) {
	// Step 1: GET the page to extract ASP.NET tokens
	getResp, err := client.Get(entity.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to GET download page: %w", err)
	}
	pageBytes, err := io.ReadAll(getResp.Body)
	getResp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to read download page: %w", err)
	}
	pageHTML := string(pageBytes)

	viewState := extractFormValue(pageHTML, "__VIEWSTATE")
	eventValidation := extractFormValue(pageHTML, "__EVENTVALIDATION")
	viewStateGenerator := extractFormValue(pageHTML, "__VIEWSTATEGENERATOR")

	if viewState == "" || eventValidation == "" {
		return nil, fmt.Errorf("could not extract ASP.NET ViewState tokens from download page")
	}

	// Step 2: POST with ASP.NET form fields
	unitType := entity.UnitType
	if unitType == "" {
		unitType = "All"
	}

	formValues := url.Values{}
	formValues.Set("__VIEWSTATE", viewState)
	formValues.Set("__EVENTVALIDATION", eventValidation)
	formValues.Set("__VIEWSTATEGENERATOR", viewStateGenerator)
	formValues.Set("ctl00$ContentPlaceHolder1$RadComboBox1", "Budget Data")
	formValues.Set("ctl00$ContentPlaceHolder1$RadComboBox2", "Disbursements by Fund and Department")
	formValues.Set("ctl00$ContentPlaceHolder1$DropDownListUnitType", unitType)
	formValues.Set("ctl00$ContentPlaceHolder1$DropDownListYear", strconv.Itoa(year))
	formValues.Set("ctl00$ContentPlaceHolder1$button_download1", "Download")

	req, err := http.NewRequest("POST", entity.BaseURL, strings.NewReader(formValues.Encode()))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "EmpoweredVote-Treasury-Importer/1.0")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("gateway returned HTTP %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if strings.Contains(contentType, "text/html") {
		resp.Body.Close()
		return nil, fmt.Errorf("gateway returned HTML page instead of CSV (content-type: %s) — POST parameters may need updating", contentType)
	}

	return resp.Body, nil
}

// extractFormValue extracts a hidden form field value from HTML by field name.
func extractFormValue(html string, fieldName string) string {
	// Look for: id="fieldName" ... value="..."
	needle := `id="` + fieldName + `"`
	idx := strings.Index(html, needle)
	if idx == -1 {
		return ""
	}
	// Find value="..." after the id
	rest := html[idx:]
	valStart := strings.Index(rest, `value="`)
	if valStart == -1 {
		return ""
	}
	rest = rest[valStart+7:]
	valEnd := strings.Index(rest, `"`)
	if valEnd == -1 {
		return ""
	}
	return rest[:valEnd]
}

// ─── Gateway Types ────────────────────────────────────────────────────────────

// GatewayEntityConfig holds metadata for a single Indiana Gateway entity.
type GatewayEntityConfig struct {
	DisplayName          string   `json:"display_name"`
	State                string   `json:"state"`
	EntityType           string   `json:"entity_type"`
	UnitCode             string   `json:"unit_code"`
	UnitType             string   `json:"unit_type"`
	CountyCode           string   `json:"county_code,omitempty"`
	BaseURL              string   `json:"base_url"`
	Delimiter            string   `json:"delimiter"`
	Encoding             string   `json:"encoding"`
	FiscalYearStartMonth int      `json:"fiscal_year_start_month"`
	FiscalYears          []int    `json:"fiscal_years"`
	DatasetTypes         []string `json:"dataset_types"`
	HierarchyColumns     []string `json:"hierarchy_columns"`
	AmountColumn         string   `json:"amount_column"`
	UnitCodeColumn       string   `json:"unit_code_column,omitempty"`
	CountyCodeColumn     string   `json:"county_code_column,omitempty"`
	YearColumn           string   `json:"year_column,omitempty"`
}

// GatewayConfig holds the full treasury import configuration.
type GatewayConfig struct {
	GatewayEntities []GatewayEntityConfig `json:"gateway_entities"`
	SocrataEntities []SocrataEntityConfig `json:"socrata_entities"`
	ArcGISEntities  []ArcGISEntityConfig  `json:"arcgis_entities"`
}

// ─── Socrata Types ────────────────────────────────────────────────────────────

// SocrataDatasetConfig holds config for one Socrata dataset (one dataset_type).
type SocrataDatasetConfig struct {
	DatasetType      string   `json:"dataset_type"`        // "operating", "revenue", "salaries"
	DatasetID        string   `json:"dataset_id"`          // Socrata 4x4 ID e.g. "5242-pnmt"
	HierarchyColumns []string `json:"hierarchy_columns"`   // e.g. ["Department_Name", "SubDepartment_Name", "Program_Name"]
	AmountColumn     string   `json:"amount_column"`       // e.g. "Appropriation"
	FiscalYearColumn string   `json:"fiscal_year_column"`  // e.g. "Fiscal_Year"
}

// SocrataEntityConfig holds metadata for a single Socrata entity (e.g. LA City).
type SocrataEntityConfig struct {
	DisplayName          string                 `json:"display_name"`
	State                string                 `json:"state"`
	EntityType           string                 `json:"entity_type"`
	BaseURL              string                 `json:"base_url"`              // e.g. "https://data.lacity.org"
	FiscalYearStartMonth int                    `json:"fiscal_year_start_month"`
	FiscalYears          []int                  `json:"fiscal_years"`
	Datasets             []SocrataDatasetConfig `json:"datasets"`
}

// ─── ArcGIS Types ─────────────────────────────────────────────────────────────

// ArcGISDatasetConfig holds config for one ArcGIS FeatureServer dataset.
type ArcGISDatasetConfig struct {
	DatasetType      string   `json:"dataset_type"`
	FeatureServerURL string   `json:"feature_server_url"`
	FiscalYearField  string   `json:"fiscal_year_field"`   // e.g. "Budget_Fiscal_Year"
	HierarchyColumns []string `json:"hierarchy_columns"`
	AmountColumn     string   `json:"amount_column"`
}

// ArcGISEntityConfig holds metadata for a single ArcGIS Hub entity (e.g. LA County).
type ArcGISEntityConfig struct {
	DisplayName          string                `json:"display_name"`
	State                string                `json:"state"`
	EntityType           string                `json:"entity_type"`
	FiscalYearStartMonth int                   `json:"fiscal_year_start_month"`
	FiscalYears          []int                 `json:"fiscal_years"`
	Datasets             []ArcGISDatasetConfig `json:"datasets"`
}

// ArcGISField describes a field returned by an ArcGIS FeatureServer query.
type ArcGISField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// ArcGISFeature is a single feature from an ArcGIS FeatureServer query.
type ArcGISFeature struct {
	Attributes map[string]interface{} `json:"attributes"`
}

// ArcGISQueryResponse is the JSON response from an ArcGIS FeatureServer /query endpoint.
type ArcGISQueryResponse struct {
	Fields                []ArcGISField   `json:"fields"`
	Features              []ArcGISFeature `json:"features"`
	ExceededTransferLimit bool            `json:"exceededTransferLimit"`
}

// ─── Gateway Helper Functions ─────────────────────────────────────────────────

// newGatewayCSVReader wraps body with Windows-1252 → UTF-8 decoding and
// configures csv.Reader with the given delimiter, LazyQuotes, and TrimLeadingSpace.
func newGatewayCSVReader(body io.Reader, delimiter rune) *csv.Reader {
	utf8Reader := transform.NewReader(body, charmap.Windows1252.NewDecoder())
	r := csv.NewReader(utf8Reader)
	r.Comma = delimiter
	r.LazyQuotes = true
	r.TrimLeadingSpace = true
	return r
}

// parseAmount parses government-formatted amount strings into float64.
// Handles: comma-formatted numbers, parenthesized negatives, blank strings.
func parseAmount(s string) (float64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0.0, nil
	}

	// Handle parenthesized negatives: "(12,345.00)" → "-12345.00"
	negative := false
	if strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		negative = true
		s = s[1 : len(s)-1]
	}

	// Strip commas: "1,234,567.00" → "1234567.00"
	s = strings.ReplaceAll(s, ",", "")

	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0.0, fmt.Errorf("parseAmount: cannot parse %q as float: %w", strings.TrimSpace(s), err)
	}

	if negative {
		val = -val
	}
	return val, nil
}

// validateHeaders builds a header-to-index map and returns an error listing
// any required columns that are missing.
func validateHeaders(headers []string, required []string) (map[string]int, error) {
	idx := make(map[string]int, len(headers))
	for i, h := range headers {
		idx[strings.TrimSpace(h)] = i
	}

	var missing []string
	for _, col := range required {
		if _, ok := idx[col]; !ok {
			missing = append(missing, col)
		}
	}

	if len(missing) > 0 {
		return nil, fmt.Errorf("gateway CSV missing required columns: %v (got: %v)", missing, headers)
	}
	return idx, nil
}

// ─── Tree accumulator types for buildGatewayCategoryTree ─────────────────────

type lineAccum struct {
	description string
	amount      float64
}

type deptAccum struct {
	name   string
	amount float64
	items  []lineAccum
}

type fundAccum struct {
	name   string
	amount float64
	depts  map[string]*deptAccum
	order  int // insertion order for deterministic output
}

// buildGatewayCategoryTree converts flat CSV records into a []CategoryImport tree.
// records is the slice of data rows (headers already consumed).
// headerIdx maps trimmed header name → column index.
// Returns the root categories, the total budget amount, and any error.
func buildGatewayCategoryTree(records [][]string, headerIdx map[string]int, entity GatewayEntityConfig) ([]CategoryImport, float64, error) {
	funds := make(map[string]*fundAccum)
	fundOrder := []string{} // preserve insertion order for deterministic sorting

	var totalBudget float64
	totalRows := 0
	zeroRows := 0

	for _, row := range records {
		if len(row) == 0 {
			continue
		}

		// Extract hierarchy values safely
		getField := func(col string) string {
			i, ok := headerIdx[col]
			if !ok || i >= len(row) {
				return ""
			}
			return strings.TrimSpace(row[i])
		}

		fundName := getField(entity.HierarchyColumns[0])
		deptName := ""
		if len(entity.HierarchyColumns) > 1 {
			deptName = getField(entity.HierarchyColumns[1])
		}
		lineDesc := ""
		if len(entity.HierarchyColumns) > 2 {
			lineDesc = getField(entity.HierarchyColumns[2])
		}

		amountStr := getField(entity.AmountColumn)
		amount, err := parseAmount(amountStr)
		if err != nil {
			log.Printf("WARNING: could not parse amount %q in row %v: %v", amountStr, row, err)
			continue
		}

		totalRows++
		if amount == 0 {
			zeroRows++
		}

		// Accumulate into fund → dept tree
		if _, ok := funds[fundName]; !ok {
			funds[fundName] = &fundAccum{
				name:  fundName,
				depts: make(map[string]*deptAccum),
				order: len(fundOrder),
			}
			fundOrder = append(fundOrder, fundName)
		}
		fund := funds[fundName]
		fund.amount += amount

		if deptName != "" {
			if _, ok := fund.depts[deptName]; !ok {
				fund.depts[deptName] = &deptAccum{name: deptName}
			}
			dept := fund.depts[deptName]
			dept.amount += amount
			if lineDesc != "" {
				dept.items = append(dept.items, lineAccum{description: lineDesc, amount: amount})
			}
		}

		totalBudget += amount
	}

	// Warn if too many zero-amount rows (possible encoding misparse indicator)
	if totalRows > 0 && float64(zeroRows)/float64(totalRows) > 0.05 {
		log.Printf("WARNING: %.0f%% of rows have zero amounts (%d/%d) — possible encoding misparse",
			float64(zeroRows)/float64(totalRows)*100, zeroRows, totalRows)
	}

	// Convert accumulated tree to []CategoryImport (funds in insertion order)
	var rootCategories []CategoryImport
	for _, fundName := range fundOrder {
		fund := funds[fundName]

		var pct float64
		if totalBudget != 0 {
			pct = (fund.amount / totalBudget) * 100
		}

		fundCat := CategoryImport{
			Name:       fund.name,
			Amount:     fund.amount,
			Percentage: pct,
		}

		// Add department subcategories in insertion order
		deptOrder := []string{}
		for deptName := range fund.depts {
			deptOrder = append(deptOrder, deptName)
		}
		// Sort deterministically
		sortStrings(deptOrder)

		for _, deptName := range deptOrder {
			dept := fund.depts[deptName]

			var deptPct float64
			if totalBudget != 0 {
				deptPct = (dept.amount / totalBudget) * 100
			}

			deptCat := CategoryImport{
				Name:       dept.name,
				Amount:     dept.amount,
				Percentage: deptPct,
			}

			// Line items go into the dept's LineItems (not further subcategories)
			for _, item := range dept.items {
				deptCat.LineItems = append(deptCat.LineItems, LineItemImport{
					Description:    item.description,
					ApprovedAmount: item.amount,
				})
			}
			deptCat.Items = len(deptCat.LineItems)

			fundCat.Subcategories = append(fundCat.Subcategories, deptCat)
		}
		fundCat.Items = len(fundCat.Subcategories)

		rootCategories = append(rootCategories, fundCat)
	}

	return rootCategories, totalBudget, nil
}

// sortStrings sorts a slice of strings in place (simple insertion sort to avoid
// importing sort package just for this helper).
func sortStrings(ss []string) {
	for i := 1; i < len(ss); i++ {
		for j := i; j > 0 && ss[j] < ss[j-1]; j-- {
			ss[j], ss[j-1] = ss[j-1], ss[j]
		}
	}
}

// ─── Socrata Fetch + Import ───────────────────────────────────────────────────

// fetchSocrataCSV downloads a full CSV dataset from the Socrata platform.
// It performs a simple HTTP GET and returns the response body for the caller to close.
// Large CSVs (like LA City's 58K-row appropriations file) require a 120-second timeout.
func fetchSocrataCSV(client *http.Client, baseURL, datasetID string) (io.ReadCloser, error) {
	csvURL := fmt.Sprintf("%s/api/views/%s/rows.csv?accessType=DOWNLOAD", baseURL, datasetID)
	resp, err := client.Get(csvURL)
	if err != nil {
		return nil, fmt.Errorf("socrata fetch failed for %s: %w", datasetID, err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("socrata returned HTTP %d for dataset %s", resp.StatusCode, datasetID)
	}
	contentType := resp.Header.Get("Content-Type")
	if strings.Contains(contentType, "text/html") || strings.Contains(contentType, "application/json") {
		resp.Body.Close()
		return nil, fmt.Errorf("socrata returned %s instead of CSV for dataset %s — dataset ID may be wrong", contentType, datasetID)
	}
	return resp.Body, nil
}

// parseFiscalYearRange parses a fiscal year string that may be a range ("2020-2021")
// or a plain integer ("2024"). Returns the end year as int.
// For range strings, the last segment after "-" is used (the end year).
func parseFiscalYearRange(s string) (int, error) {
	s = strings.TrimSpace(s)
	if strings.Contains(s, "-") {
		parts := strings.Split(s, "-")
		last := strings.TrimSpace(parts[len(parts)-1])
		year, err := strconv.Atoi(last)
		if err != nil {
			return 0, fmt.Errorf("parseFiscalYearRange: cannot parse end year from %q: %w", s, err)
		}
		return year, nil
	}
	year, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("parseFiscalYearRange: cannot parse %q as integer: %w", s, err)
	}
	return year, nil
}

// categoryNode is an N-level tree accumulator used by both Socrata and ArcGIS tree builders.
type categoryNode struct {
	name     string
	amount   float64
	children map[string]*categoryNode
	order    []string // insertion order for deterministic output
}

// addToTree walks hierarchy columns and accumulates amount at each node.
// getLevel returns the string value for a given hierarchy level index.
// If a level value is empty, the row is accumulated at the nearest non-empty parent.
func addToTree(root map[string]*categoryNode, rootOrder *[]string, levels []string, amount float64) {
	if len(levels) == 0 {
		return
	}
	// Walk down the hierarchy, creating nodes as needed
	current := root
	currentOrder := rootOrder
	for _, level := range levels {
		if level == "" {
			break
		}
		if _, ok := current[level]; !ok {
			current[level] = &categoryNode{
				name:     level,
				children: make(map[string]*categoryNode),
			}
			*currentOrder = append(*currentOrder, level)
		}
		node := current[level]
		node.amount += amount
		current = node.children
		currentOrder = &node.order
	}
}

// convertTreeToCategories recursively converts categoryNode map to []CategoryImport.
func convertTreeToCategories(nodeMap map[string]*categoryNode, order []string, totalBudget float64) []CategoryImport {
	var cats []CategoryImport
	for _, name := range order {
		node := nodeMap[name]
		var pct float64
		if totalBudget != 0 {
			pct = (node.amount / totalBudget) * 100
		}
		cat := CategoryImport{
			Name:       node.name,
			Amount:     node.amount,
			Percentage: pct,
		}
		if len(node.children) > 0 {
			cat.Subcategories = convertTreeToCategories(node.children, node.order, totalBudget)
			cat.Items = len(cat.Subcategories)
		}
		cats = append(cats, cat)
	}
	return cats
}

// buildSocrataCategoryTree converts flat CSV records into a []CategoryImport hierarchy.
// records contains data rows (header row already consumed and passed as headerIdx).
// Rows are filtered to only those matching targetYear in dataset.FiscalYearColumn.
// For revenue datasets where FY is a range like "2020-2021", parseFiscalYearRange
// extracts the end year for comparison.
func buildSocrataCategoryTree(records [][]string, headerIdx map[string]int, dataset SocrataDatasetConfig, targetYear int) ([]CategoryImport, float64, error) {
	root := make(map[string]*categoryNode)
	var rootOrder []string
	var totalBudget float64

	fyIdx, ok := headerIdx[dataset.FiscalYearColumn]
	if !ok {
		return nil, 0, fmt.Errorf("fiscal year column %q not found in headers", dataset.FiscalYearColumn)
	}
	amtIdx, ok := headerIdx[dataset.AmountColumn]
	if !ok {
		return nil, 0, fmt.Errorf("amount column %q not found in headers", dataset.AmountColumn)
	}

	for _, row := range records {
		if len(row) == 0 {
			continue
		}

		// Determine fiscal year for this row
		fyStr := ""
		if fyIdx < len(row) {
			fyStr = strings.TrimSpace(row[fyIdx])
		}
		rowYear, err := parseFiscalYearRange(fyStr)
		if err != nil {
			// Skip rows with unparseable fiscal year
			log.Printf("WARNING: skipping row with unparseable fiscal year %q: %v", fyStr, err)
			continue
		}
		if rowYear != targetYear {
			continue
		}

		// Parse amount
		amtStr := ""
		if amtIdx < len(row) {
			amtStr = strings.TrimSpace(row[amtIdx])
		}
		amount, err := parseAmount(amtStr)
		if err != nil {
			log.Printf("WARNING: could not parse Socrata amount %q: %v", amtStr, err)
			continue
		}

		// Extract hierarchy levels
		levels := make([]string, 0, len(dataset.HierarchyColumns))
		for _, col := range dataset.HierarchyColumns {
			val := ""
			if idx, ok := headerIdx[col]; ok && idx < len(row) {
				val = strings.TrimSpace(row[idx])
			}
			levels = append(levels, val)
		}

		addToTree(root, &rootOrder, levels, amount)
		totalBudget += amount
	}

	categories := convertTreeToCategories(root, rootOrder, totalBudget)
	return categories, totalBudget, nil
}

// ImportSocrataBudgets fetches, parses, and inserts Socrata CSV budget data
// for all socrata_entities defined in the config file.
//
// Usage: ./server import-budgets --source=socrata [--config=treasury-import-config.json] [--dry-run]
func ImportSocrataBudgets(configFile string, dryRun bool) (ImportBudgetsResult, error) {
	if configFile == "" {
		configFile = "treasury-import-config.json"
	}

	data, err := os.ReadFile(configFile)
	if err != nil {
		return ImportBudgetsResult{}, fmt.Errorf("failed to read config %s: %w", configFile, err)
	}

	var config GatewayConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return ImportBudgetsResult{}, fmt.Errorf("failed to parse config %s: %w", configFile, err)
	}

	// 120-second timeout for large Socrata CSV files (LA City is ~58K rows)
	client := &http.Client{Timeout: 120 * time.Second}
	var result ImportBudgetsResult

	for _, entity := range config.SocrataEntities {
		for _, year := range entity.FiscalYears {
			for _, dataset := range entity.Datasets {
				result.FilesProcessed++

				body, err := fetchSocrataCSV(client, entity.BaseURL, dataset.DatasetID)
				if err != nil {
					msg := fmt.Sprintf("socrata fetch failed for %s %d %s: %v", entity.DisplayName, year, dataset.DatasetType, err)
					result.Errors = append(result.Errors, msg)
					log.Printf("SKIP: %s", msg)
					continue
				}

				// Socrata serves UTF-8 natively — no charset conversion needed
				r := csv.NewReader(body)
				r.LazyQuotes = true
				r.TrimLeadingSpace = true

				headers, err := r.Read()
				if err != nil {
					body.Close()
					msg := fmt.Sprintf("failed to read Socrata headers for %s %d %s: %v", entity.DisplayName, year, dataset.DatasetType, err)
					result.Errors = append(result.Errors, msg)
					log.Printf("SKIP: %s", msg)
					continue
				}

				required := append(dataset.HierarchyColumns, dataset.AmountColumn, dataset.FiscalYearColumn)
				headerIdx, err := validateHeaders(headers, required)
				if err != nil {
					body.Close()
					msg := fmt.Sprintf("header validation failed for %s %d %s: %v", entity.DisplayName, year, dataset.DatasetType, err)
					result.Errors = append(result.Errors, msg)
					log.Printf("SKIP: %s", msg)
					continue
				}

				allRecords, err := r.ReadAll()
				body.Close()
				if err != nil {
					msg := fmt.Sprintf("failed to read Socrata CSV rows for %s %d %s: %v", entity.DisplayName, year, dataset.DatasetType, err)
					result.Errors = append(result.Errors, msg)
					log.Printf("SKIP: %s", msg)
					continue
				}

				rootCategories, totalBudget, err := buildSocrataCategoryTree(allRecords, headerIdx, dataset, year)
				if err != nil {
					msg := fmt.Sprintf("failed to build Socrata category tree for %s %d %s: %v", entity.DisplayName, year, dataset.DatasetType, err)
					result.Errors = append(result.Errors, msg)
					log.Printf("SKIP: %s", msg)
					continue
				}

				if len(rootCategories) == 0 {
					msg := fmt.Sprintf("no rows found for %s year %d in %s dataset", entity.DisplayName, year, dataset.DatasetType)
					result.Errors = append(result.Errors, msg)
					log.Printf("SKIP: %s", msg)
					continue
				}

				if dryRun {
					log.Printf("[DRY-RUN] Would import Socrata %s %d (%s): totalBudget=%.2f, %d root categories",
						entity.DisplayName, year, dataset.DatasetType, totalBudget, len(rootCategories))
					continue
				}

				tx := db.DB.Begin()
				if tx.Error != nil {
					msg := fmt.Sprintf("failed to begin transaction for %s %d %s: %v", entity.DisplayName, year, dataset.DatasetType, tx.Error)
					result.Errors = append(result.Errors, msg)
					continue
				}

				// Find or create municipality.
				var municipality Municipality
				if err := tx.Where("name = ? AND state = ? AND entity_type = ?",
					entity.DisplayName, entity.State, entity.EntityType).
					First(&municipality).Error; err != nil {
					municipality = Municipality{
						Name:       entity.DisplayName,
						State:      entity.State,
						EntityType: entity.EntityType,
					}
					if err := tx.Create(&municipality).Error; err != nil {
						tx.Rollback()
						msg := fmt.Sprintf("failed to create municipality for %s %d %s: %v", entity.DisplayName, year, dataset.DatasetType, err)
						result.Errors = append(result.Errors, msg)
						continue
					}
				}

				// Idempotent check: skip if budget already exists.
				var existingBudget Budget
				checkErr := tx.Where("municipality_id = ? AND fiscal_year = ? AND dataset_type = ?",
					municipality.ID, year, dataset.DatasetType).First(&existingBudget).Error
				if checkErr == nil {
					tx.Rollback()
					log.Printf("SKIP: budget already exists for %s %d (%s)", entity.DisplayName, year, dataset.DatasetType)
					result.Skipped++
					continue
				}

				// Create budget record.
				budget := Budget{
					MunicipalityID:       municipality.ID,
					FiscalYear:           year,
					DatasetType:          dataset.DatasetType,
					TotalBudget:          totalBudget,
					FiscalYearStartMonth: entity.FiscalYearStartMonth,
					DataSource:           "Socrata: " + entity.BaseURL,
					Hierarchy:            pq.StringArray(dataset.HierarchyColumns),
				}
				if err := tx.Create(&budget).Error; err != nil {
					tx.Rollback()
					msg := fmt.Sprintf("failed to create budget for %s %d %s: %v", entity.DisplayName, year, dataset.DatasetType, err)
					result.Errors = append(result.Errors, msg)
					continue
				}

				// Recursively import categories using existing importCategories helper.
				if err := importCategories(tx, budget.ID, nil, rootCategories, 0); err != nil {
					tx.Rollback()
					msg := fmt.Sprintf("failed to import categories for %s %d %s: %v", entity.DisplayName, year, dataset.DatasetType, err)
					result.Errors = append(result.Errors, msg)
					continue
				}

				if err := tx.Commit().Error; err != nil {
					msg := fmt.Sprintf("failed to commit transaction for %s %d %s: %v", entity.DisplayName, year, dataset.DatasetType, err)
					result.Errors = append(result.Errors, msg)
					continue
				}

				log.Printf("Inserted Socrata: %s %d (%s), totalBudget=%.2f, %d root categories",
					entity.DisplayName, year, dataset.DatasetType, totalBudget, len(rootCategories))
				result.Inserted++
			}
		}
	}

	return result, nil
}

// ─── ArcGIS Fetch + Import ────────────────────────────────────────────────────

// fetchArcGISFeatures fetches all features from an ArcGIS FeatureServer endpoint
// for a specific fiscal year using paginated queries (resultOffset pagination).
// ArcGIS caps each response at maxRecordCount=2000; this function loops until
// ExceededTransferLimit is false or no features are returned.
// A 100ms sleep between pages avoids rate-limiting on the public service.
func fetchArcGISFeatures(client *http.Client, featureServerURL string, fiscalYearField string, fiscalYear int, outFields []string) ([]ArcGISFeature, error) {
	const pageSize = 2000
	var all []ArcGISFeature
	offset := 0

	outFieldStr := "*"
	if len(outFields) > 0 {
		outFieldStr = strings.Join(outFields, ",")
	}

	for {
		params := url.Values{}
		params.Set("where", fmt.Sprintf("%s='%d'", fiscalYearField, fiscalYear))
		params.Set("outFields", outFieldStr)
		params.Set("resultRecordCount", strconv.Itoa(pageSize))
		params.Set("resultOffset", strconv.Itoa(offset))
		params.Set("f", "json")

		queryURL := fmt.Sprintf("%s/query?%s", featureServerURL, params.Encode())
		resp, err := client.Get(queryURL)
		if err != nil {
			return nil, fmt.Errorf("arcgis page fetch (offset=%d) failed: %w", offset, err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("arcgis read body (offset=%d) failed: %w", offset, err)
		}

		var page ArcGISQueryResponse
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, fmt.Errorf("arcgis JSON parse (offset=%d) failed: %w", offset, err)
		}

		all = append(all, page.Features...)
		log.Printf("ArcGIS page offset=%d: got %d features (total so far: %d, exceededTransferLimit=%v)",
			offset, len(page.Features), len(all), page.ExceededTransferLimit)

		if !page.ExceededTransferLimit || len(page.Features) == 0 {
			break
		}
		offset += pageSize
		time.Sleep(100 * time.Millisecond)
	}

	return all, nil
}

// buildArcGISCategoryTree converts ArcGIS FeatureServer features into a []CategoryImport hierarchy.
// Hierarchy levels are extracted from feature attributes using dataset.HierarchyColumns.
// Negative amounts pass through as-is (credits/adjustments).
func buildArcGISCategoryTree(features []ArcGISFeature, dataset ArcGISDatasetConfig) ([]CategoryImport, float64, error) {
	root := make(map[string]*categoryNode)
	var rootOrder []string
	var totalBudget float64

	for _, feature := range features {
		// Extract amount — may be float64 or nil from JSON
		amtRaw := feature.Attributes[dataset.AmountColumn]
		var amount float64
		switch v := amtRaw.(type) {
		case float64:
			amount = v
		case nil:
			amount = 0
		default:
			// Try string parsing as fallback
			amtStr := fmt.Sprintf("%v", v)
			var err error
			amount, err = parseAmount(amtStr)
			if err != nil {
				log.Printf("WARNING: could not parse ArcGIS amount %q: %v", amtStr, err)
				continue
			}
		}

		// Extract hierarchy levels from attributes
		levels := make([]string, 0, len(dataset.HierarchyColumns))
		for _, col := range dataset.HierarchyColumns {
			val := ""
			if v, ok := feature.Attributes[col]; ok && v != nil {
				val = strings.TrimSpace(fmt.Sprintf("%v", v))
			}
			levels = append(levels, val)
		}

		addToTree(root, &rootOrder, levels, amount)
		totalBudget += amount
	}

	categories := convertTreeToCategories(root, rootOrder, totalBudget)
	return categories, totalBudget, nil
}

// ImportArcGISBudgets fetches, parses, and inserts ArcGIS FeatureServer budget data
// for all arcgis_entities defined in the config file.
//
// Usage: ./server import-budgets --source=arcgis [--config=treasury-import-config.json] [--dry-run]
func ImportArcGISBudgets(configFile string, dryRun bool) (ImportBudgetsResult, error) {
	if configFile == "" {
		configFile = "treasury-import-config.json"
	}

	data, err := os.ReadFile(configFile)
	if err != nil {
		return ImportBudgetsResult{}, fmt.Errorf("failed to read config %s: %w", configFile, err)
	}

	var config GatewayConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return ImportBudgetsResult{}, fmt.Errorf("failed to parse config %s: %w", configFile, err)
	}

	client := &http.Client{Timeout: 60 * time.Second}
	var result ImportBudgetsResult

	for _, entity := range config.ArcGISEntities {
		for _, year := range entity.FiscalYears {
			for _, dataset := range entity.Datasets {
				result.FilesProcessed++

				// Build outFields list from hierarchy columns + amount column
				outFields := append(dataset.HierarchyColumns, dataset.AmountColumn)
				if dataset.FiscalYearField != "" {
					outFields = append(outFields, dataset.FiscalYearField)
				}

				features, err := fetchArcGISFeatures(client, dataset.FeatureServerURL, dataset.FiscalYearField, year, outFields)
				if err != nil {
					msg := fmt.Sprintf("arcgis fetch failed for %s %d %s: %v", entity.DisplayName, year, dataset.DatasetType, err)
					result.Errors = append(result.Errors, msg)
					log.Printf("SKIP: %s", msg)
					continue
				}

				if len(features) == 0 {
					msg := fmt.Sprintf("no features found for %s year %d in %s dataset", entity.DisplayName, year, dataset.DatasetType)
					result.Errors = append(result.Errors, msg)
					log.Printf("SKIP: %s", msg)
					continue
				}

				rootCategories, totalBudget, err := buildArcGISCategoryTree(features, dataset)
				if err != nil {
					msg := fmt.Sprintf("failed to build ArcGIS category tree for %s %d %s: %v", entity.DisplayName, year, dataset.DatasetType, err)
					result.Errors = append(result.Errors, msg)
					log.Printf("SKIP: %s", msg)
					continue
				}

				if dryRun {
					log.Printf("[DRY-RUN] Would import ArcGIS %s %d (%s): totalBudget=%.2f, %d root categories, %d features",
						entity.DisplayName, year, dataset.DatasetType, totalBudget, len(rootCategories), len(features))
					continue
				}

				tx := db.DB.Begin()
				if tx.Error != nil {
					msg := fmt.Sprintf("failed to begin transaction for %s %d %s: %v", entity.DisplayName, year, dataset.DatasetType, tx.Error)
					result.Errors = append(result.Errors, msg)
					continue
				}

				// Find or create municipality.
				var municipality Municipality
				if err := tx.Where("name = ? AND state = ? AND entity_type = ?",
					entity.DisplayName, entity.State, entity.EntityType).
					First(&municipality).Error; err != nil {
					municipality = Municipality{
						Name:       entity.DisplayName,
						State:      entity.State,
						EntityType: entity.EntityType,
					}
					if err := tx.Create(&municipality).Error; err != nil {
						tx.Rollback()
						msg := fmt.Sprintf("failed to create municipality for %s %d %s: %v", entity.DisplayName, year, dataset.DatasetType, err)
						result.Errors = append(result.Errors, msg)
						continue
					}
				}

				// Idempotent check: skip if budget already exists.
				var existingBudget Budget
				checkErr := tx.Where("municipality_id = ? AND fiscal_year = ? AND dataset_type = ?",
					municipality.ID, year, dataset.DatasetType).First(&existingBudget).Error
				if checkErr == nil {
					tx.Rollback()
					log.Printf("SKIP: budget already exists for %s %d (%s)", entity.DisplayName, year, dataset.DatasetType)
					result.Skipped++
					continue
				}

				// Create budget record.
				budget := Budget{
					MunicipalityID:       municipality.ID,
					FiscalYear:           year,
					DatasetType:          dataset.DatasetType,
					TotalBudget:          totalBudget,
					FiscalYearStartMonth: entity.FiscalYearStartMonth,
					DataSource:           "ArcGIS: " + dataset.FeatureServerURL,
					Hierarchy:            pq.StringArray(dataset.HierarchyColumns),
				}
				if err := tx.Create(&budget).Error; err != nil {
					tx.Rollback()
					msg := fmt.Sprintf("failed to create budget for %s %d %s: %v", entity.DisplayName, year, dataset.DatasetType, err)
					result.Errors = append(result.Errors, msg)
					continue
				}

				// Recursively import categories using existing importCategories helper.
				if err := importCategories(tx, budget.ID, nil, rootCategories, 0); err != nil {
					tx.Rollback()
					msg := fmt.Sprintf("failed to import categories for %s %d %s: %v", entity.DisplayName, year, dataset.DatasetType, err)
					result.Errors = append(result.Errors, msg)
					continue
				}

				if err := tx.Commit().Error; err != nil {
					msg := fmt.Sprintf("failed to commit transaction for %s %d %s: %v", entity.DisplayName, year, dataset.DatasetType, err)
					result.Errors = append(result.Errors, msg)
					continue
				}

				log.Printf("Inserted ArcGIS: %s %d (%s), totalBudget=%.2f, %d root categories, %d features",
					entity.DisplayName, year, dataset.DatasetType, totalBudget, len(rootCategories), len(features))
				result.Inserted++
			}
		}
	}

	return result, nil
}
