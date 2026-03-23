package treasury

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestBuildSocrataCategoryTree tests tree building from LA City appropriations CSV.
// Uses FY2024 data — expects 3 root categories (Police, Fire, Public Works).
func TestBuildSocrataCategoryTree(t *testing.T) {
	fixturePath := filepath.Join("testdata", "la_city_appropriations_sample.csv")
	f, err := os.Open(fixturePath)
	if err != nil {
		t.Fatalf("failed to open fixture: %v", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.LazyQuotes = true
	r.TrimLeadingSpace = true

	headers, err := r.Read()
	if err != nil {
		t.Fatalf("failed to read headers: %v", err)
	}

	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("failed to read records: %v", err)
	}

	dataset := SocrataDatasetConfig{
		DatasetType:      "operating",
		DatasetID:        "5242-pnmt",
		HierarchyColumns: []string{"Department_Name", "SubDepartment_Name", "Program_Name"},
		AmountColumn:     "Appropriation",
		FiscalYearColumn: "Fiscal_Year",
	}

	required := append(dataset.HierarchyColumns, dataset.AmountColumn, dataset.FiscalYearColumn)
	headerIdx, err := validateHeaders(headers, required)
	if err != nil {
		t.Fatalf("header validation failed: %v", err)
	}

	categories, totalBudget, err := buildSocrataCategoryTree(records, headerIdx, dataset, 2024)
	if err != nil {
		t.Fatalf("buildSocrataCategoryTree failed: %v", err)
	}

	// Expect 3 root categories for FY2024: Police, Fire, Public Works
	if len(categories) != 3 {
		t.Errorf("expected 3 root categories, got %d: %v", len(categories), categoryNames(categories))
	}

	// Police root amount = 1166229399 + 234567890 = 1400797289
	for _, cat := range categories {
		if cat.Name == "Police" {
			const wantPolice = 1166229399 + 234567890
			if cat.Amount != wantPolice {
				t.Errorf("Police amount = %.0f, want %.0f", cat.Amount, float64(wantPolice))
			}
		}
		if cat.Name == "Fire" {
			const wantFire = 543210000
			if cat.Amount != wantFire {
				t.Errorf("Fire amount = %.0f, want %.0f", cat.Amount, float64(wantFire))
			}
		}
	}

	// totalBudget = 1166229399 + 543210000 + 234567890 + 87654321 = 2031661610
	const wantTotal = 1166229399 + 543210000 + 234567890 + 87654321
	if totalBudget != wantTotal {
		t.Errorf("totalBudget = %.0f, want %.0f", totalBudget, float64(wantTotal))
	}
}

// TestSocrataFiscalYearFilter tests that only rows matching the target year are included.
// FY2023 data in fixture: Police (1100000000) + Fire (500000000) = 1600000000.
func TestSocrataFiscalYearFilter(t *testing.T) {
	fixturePath := filepath.Join("testdata", "la_city_appropriations_sample.csv")
	f, err := os.Open(fixturePath)
	if err != nil {
		t.Fatalf("failed to open fixture: %v", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.LazyQuotes = true
	r.TrimLeadingSpace = true

	headers, err := r.Read()
	if err != nil {
		t.Fatalf("failed to read headers: %v", err)
	}

	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("failed to read records: %v", err)
	}

	dataset := SocrataDatasetConfig{
		DatasetType:      "operating",
		DatasetID:        "5242-pnmt",
		HierarchyColumns: []string{"Department_Name", "SubDepartment_Name", "Program_Name"},
		AmountColumn:     "Appropriation",
		FiscalYearColumn: "Fiscal_Year",
	}

	required := append(dataset.HierarchyColumns, dataset.AmountColumn, dataset.FiscalYearColumn)
	headerIdx, err := validateHeaders(headers, required)
	if err != nil {
		t.Fatalf("header validation failed: %v", err)
	}

	categories, totalBudget, err := buildSocrataCategoryTree(records, headerIdx, dataset, 2023)
	if err != nil {
		t.Fatalf("buildSocrataCategoryTree failed: %v", err)
	}

	// Expect 2 root categories for FY2023: Police, Fire
	if len(categories) != 2 {
		t.Errorf("expected 2 root categories for FY2023, got %d: %v", len(categories), categoryNames(categories))
	}

	// totalBudget = 1100000000 + 500000000 = 1600000000
	const wantTotal = float64(1100000000 + 500000000)
	if totalBudget != wantTotal {
		t.Errorf("totalBudget for FY2023 = %.0f, want %.0f", totalBudget, wantTotal)
	}
}

// TestBuildArcGISCategoryTree tests tree building from LA County expenditures JSON fixture.
// Expects 2 root categories: General Fund and Special Fund.
func TestBuildArcGISCategoryTree(t *testing.T) {
	fixturePath := filepath.Join("testdata", "la_county_expenditures_sample.json")
	data, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("failed to read fixture: %v", err)
	}

	var response ArcGISQueryResponse
	if err := json.Unmarshal(data, &response); err != nil {
		t.Fatalf("failed to unmarshal fixture: %v", err)
	}

	dataset := ArcGISDatasetConfig{
		DatasetType:      "operating",
		FeatureServerURL: "https://services.arcgis.com/test/FeatureServer/0",
		FiscalYearField:  "Budget_Fiscal_Year",
		HierarchyColumns: []string{"Fund_Group", "Department", "Expenditure_Category", "Expenditure_Class"},
		AmountColumn:     "Amount",
	}

	categories, totalBudget, err := buildArcGISCategoryTree(response.Features, dataset)
	if err != nil {
		t.Fatalf("buildArcGISCategoryTree failed: %v", err)
	}

	// Expect 2 root categories: General Fund, Special Fund
	if len(categories) != 2 {
		t.Errorf("expected 2 root categories, got %d: %v", len(categories), categoryNames(categories))
	}

	// General Fund amount = 2741537.29 + 150000.00 + 890000.50 = 3781537.79
	const wantGeneralFund = 2741537.29 + 150000.00 + 890000.50
	// Special Fund amount = -25000.00 + 1200000.00 = 1175000.00
	const wantSpecialFund = -25000.00 + 1200000.00

	for _, cat := range categories {
		switch cat.Name {
		case "General Fund":
			if !almostEqual(cat.Amount, wantGeneralFund) {
				t.Errorf("General Fund amount = %.2f, want %.2f", cat.Amount, wantGeneralFund)
			}
		case "Special Fund":
			if !almostEqual(cat.Amount, wantSpecialFund) {
				t.Errorf("Special Fund amount = %.2f, want %.2f", cat.Amount, wantSpecialFund)
			}
		default:
			t.Errorf("unexpected root category: %q", cat.Name)
		}
	}

	// totalBudget = 3781537.79 + 1175000.00 = 4956537.79
	const wantTotal = wantGeneralFund + wantSpecialFund
	if !almostEqual(totalBudget, wantTotal) {
		t.Errorf("totalBudget = %.2f, want %.2f", totalBudget, wantTotal)
	}
}

// TestArcGISNegativeAmounts verifies negative amounts pass through the tree builder correctly.
// The Special Fund > Health Services > Services & Supplies > Medical Supplies path has -25000.00.
func TestArcGISNegativeAmounts(t *testing.T) {
	fixturePath := filepath.Join("testdata", "la_county_expenditures_sample.json")
	data, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("failed to read fixture: %v", err)
	}

	var response ArcGISQueryResponse
	if err := json.Unmarshal(data, &response); err != nil {
		t.Fatalf("failed to unmarshal fixture: %v", err)
	}

	dataset := ArcGISDatasetConfig{
		DatasetType:      "operating",
		FeatureServerURL: "https://services.arcgis.com/test/FeatureServer/0",
		FiscalYearField:  "Budget_Fiscal_Year",
		HierarchyColumns: []string{"Fund_Group", "Department", "Expenditure_Category", "Expenditure_Class"},
		AmountColumn:     "Amount",
	}

	categories, _, err := buildArcGISCategoryTree(response.Features, dataset)
	if err != nil {
		t.Fatalf("buildArcGISCategoryTree failed: %v", err)
	}

	// Navigate Special Fund > Health Services > Services & Supplies > Medical Supplies
	var specialFund *CategoryImport
	for i := range categories {
		if categories[i].Name == "Special Fund" {
			specialFund = &categories[i]
			break
		}
	}
	if specialFund == nil {
		t.Fatal("Special Fund not found in root categories")
	}

	var healthServices *CategoryImport
	for i := range specialFund.Subcategories {
		if specialFund.Subcategories[i].Name == "Health Services" {
			healthServices = &specialFund.Subcategories[i]
			break
		}
	}
	if healthServices == nil {
		t.Fatal("Health Services not found under Special Fund")
	}

	var servicesSupplies *CategoryImport
	for i := range healthServices.Subcategories {
		if healthServices.Subcategories[i].Name == "Services & Supplies" {
			servicesSupplies = &healthServices.Subcategories[i]
			break
		}
	}
	if servicesSupplies == nil {
		t.Fatal("Services & Supplies not found under Health Services")
	}

	var medicalSupplies *CategoryImport
	for i := range servicesSupplies.Subcategories {
		if servicesSupplies.Subcategories[i].Name == "Medical Supplies" {
			medicalSupplies = &servicesSupplies.Subcategories[i]
			break
		}
	}
	if medicalSupplies == nil {
		t.Fatal("Medical Supplies not found under Services & Supplies")
	}

	if medicalSupplies.Amount != -25000.00 {
		t.Errorf("Medical Supplies amount = %.2f, want -25000.00", medicalSupplies.Amount)
	}
}

// TestSocrataHeaderTrimSpace verifies that a header with leading/trailing spaces
// like " Appropriation " is trimmed and matched correctly by validateHeaders.
func TestSocrataHeaderTrimSpace(t *testing.T) {
	// Simulate the LA City CSV header with leading space on Appropriation
	headers := []string{
		"Dept_Code", "Department_Name", "SubDept_Code", "SubDepartment_Name",
		"Prog_Code", "Program_Name", "Program_Priority", "Source_Fund_Code",
		"Source_Fund_Name", "Account_Code", "Account_Name", " Appropriation ", "Fiscal_Year", "Expense_Type",
	}
	required := []string{"Department_Name", "Appropriation", "Fiscal_Year"}

	idx, err := validateHeaders(headers, required)
	if err != nil {
		t.Fatalf("validateHeaders failed with leading-space header: %v", err)
	}

	// Appropriation should be at index 11 (0-based)
	if idx["Appropriation"] != 11 {
		t.Errorf("Appropriation index = %d, want 11", idx["Appropriation"])
	}
}

// TestParseFiscalYearRange tests parsing of fiscal year range strings.
// "2020-2021" → 2021 (end year), "2024-2025" → 2025, "2024" → 2024 (plain integer).
func TestParseFiscalYearRange(t *testing.T) {
	tests := []struct {
		input   string
		want    int
		wantErr bool
	}{
		{"2020-2021", 2021, false},
		{"2024-2025", 2025, false},
		{"2024", 2024, false},
		{"  2023  ", 2023, false},
		{"  2022-2023  ", 2023, false},
		{"invalid", 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := parseFiscalYearRange(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("parseFiscalYearRange(%q) expected error, got nil (result=%d)", tc.input, got)
				}
				return
			}
			if err != nil {
				t.Errorf("parseFiscalYearRange(%q) unexpected error: %v", tc.input, err)
				return
			}
			if got != tc.want {
				t.Errorf("parseFiscalYearRange(%q) = %d, want %d", tc.input, got, tc.want)
			}
		})
	}
}

// categoryNames returns the names of root categories for error messages.
func categoryNames(cats []CategoryImport) []string {
	names := make([]string, len(cats))
	for i, c := range cats {
		names[i] = c.Name
	}
	return names
}

// almostEqual compares floats with a small epsilon tolerance for floating-point arithmetic.
func almostEqual(a, b float64) bool {
	const epsilon = 0.01
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff < epsilon
}
