package treasury

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/text/encoding/charmap"
)

// TestGatewayCSVParse reads the fixture file with newGatewayCSVReader using pipe delimiter.
func TestGatewayCSVParse(t *testing.T) {
	fixturePath := filepath.Join("testdata", "ellettsville_sample.csv")
	f, err := os.Open(fixturePath)
	if err != nil {
		t.Fatalf("failed to open fixture: %v", err)
	}
	defer f.Close()

	r := newGatewayCSVReader(f, '|')

	// Read header row
	headers, err := r.Read()
	if err != nil {
		t.Fatalf("failed to read header: %v", err)
	}
	if len(headers) != 12 {
		t.Fatalf("expected 12 header columns, got %d: %v", len(headers), headers)
	}

	// Read all data rows
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatalf("failed to read rows: %v", err)
	}
	// 5 Ellettsville + 2 Monroe County + 1 Adams County = 8 data rows
	if len(rows) != 8 {
		t.Errorf("expected 8 data rows, got %d", len(rows))
	}

	// Check first data row fields are accessible by index
	firstRow := rows[0]
	if len(firstRow) < 12 {
		t.Fatalf("first row has fewer than 12 fields: %v", firstRow)
	}
	// unit_name should be ELLETTSVILLE CIVIL TOWN
	if !strings.Contains(firstRow[5], "ELLETTSVILLE") {
		t.Errorf("first row unit_name should contain ELLETTSVILLE, got: %q", firstRow[5])
	}
}

// TestParseAmount covers various amount format cases.
func TestParseAmount(t *testing.T) {
	tests := []struct {
		input     string
		want      float64
		wantError bool
	}{
		{"125000.00", 125000.00, false},
		{"1,234,567.00", 1234567.00, false},
		{"(500.00)", -500.00, false},
		{"", 0.0, false},
		{"  ", 0.0, false},
		{"not_a_number", 0.0, true},
		{"(1,234.56)", -1234.56, false},
		{"0.00", 0.0, false},
		{"3656551.0000", 3656551.0, false},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := parseAmount(tc.input)
			if tc.wantError && err == nil {
				t.Errorf("parseAmount(%q) expected error but got nil, result=%v", tc.input, got)
			}
			if !tc.wantError && err != nil {
				t.Errorf("parseAmount(%q) unexpected error: %v", tc.input, err)
			}
			if !tc.wantError && got != tc.want {
				t.Errorf("parseAmount(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

// TestValidateHeaders tests header validation logic.
func TestValidateHeaders(t *testing.T) {
	t.Run("all required columns present", func(t *testing.T) {
		headers := []string{"year", "cnty_cd", "unit_code", "fund_description", "Total budget estimate_adopted", "extra_col"}
		required := []string{"fund_description", "Total budget estimate_adopted", "unit_code"}

		idx, err := validateHeaders(headers, required)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if idx["fund_description"] != 3 {
			t.Errorf("fund_description index = %d, want 3", idx["fund_description"])
		}
		if idx["Total budget estimate_adopted"] != 4 {
			t.Errorf("Total budget estimate_adopted index = %d, want 4", idx["Total budget estimate_adopted"])
		}
	})

	t.Run("missing one column", func(t *testing.T) {
		headers := []string{"year", "cnty_cd", "fund_description"}
		required := []string{"fund_description", "Total budget estimate_adopted", "unit_code"}

		_, err := validateHeaders(headers, required)
		if err == nil {
			t.Fatal("expected error for missing column, got nil")
		}
		errStr := err.Error()
		if !strings.Contains(errStr, "missing required columns") {
			t.Errorf("error should mention 'missing required columns', got: %s", errStr)
		}
		if !strings.Contains(errStr, "Total budget estimate_adopted") {
			t.Errorf("error should mention missing column, got: %s", errStr)
		}
	})

	t.Run("extra columns in CSV", func(t *testing.T) {
		headers := []string{"year", "cnty_cd", "unit_code", "fund_description", "Total budget estimate_adopted", "col_extra1", "col_extra2"}
		required := []string{"fund_description", "Total budget estimate_adopted", "unit_code"}

		_, err := validateHeaders(headers, required)
		if err != nil {
			t.Errorf("extra columns should not cause error, got: %v", err)
		}
	})
}

// TestPipeDelimiter confirms pipe-delimited fields are split correctly.
func TestPipeDelimiter(t *testing.T) {
	fixturePath := filepath.Join("testdata", "ellettsville_sample.csv")
	f, err := os.Open(fixturePath)
	if err != nil {
		t.Fatalf("failed to open fixture: %v", err)
	}
	defer f.Close()

	r := newGatewayCSVReader(f, '|')

	headers, err := r.Read()
	if err != nil {
		t.Fatalf("failed to read header: %v", err)
	}

	// If pipe delimiter was not set, all columns would appear as one field
	if len(headers) < 2 {
		t.Fatalf("pipe delimiter not working: got %d field(s) instead of multiple: %v", len(headers), headers)
	}

	// Verify expected header names from actual Gateway format
	expectedHeaders := []string{"year", "cnty_cd", "cnty_description", "unit_type", "unit_code", "unit_name"}
	for i, h := range expectedHeaders {
		if i >= len(headers) {
			t.Fatalf("missing expected header at index %d: %s", i, h)
		}
		if headers[i] != h {
			t.Errorf("header[%d] = %q, want %q", i, headers[i], h)
		}
	}
}

// TestUTF8Reencode verifies Windows-1252 bytes are converted to correct UTF-8.
func TestUTF8Reencode(t *testing.T) {
	// Create a pipe-delimited row with Windows-1252 encoded characters
	input := []byte("year|fund_description|amount\n2024|Général Fund|50000.00\n")

	encoder := charmap.Windows1252.NewEncoder()
	win1252Bytes, err := encoder.Bytes(input)
	if err != nil {
		t.Fatalf("failed to encode to Windows-1252: %v", err)
	}

	r := newGatewayCSVReader(bytes.NewReader(win1252Bytes), '|')

	_, err = r.Read()
	if err != nil {
		t.Fatalf("failed to read header: %v", err)
	}

	row, err := r.Read()
	if err != nil {
		t.Fatalf("failed to read data row: %v", err)
	}

	if len(row) < 2 {
		t.Fatalf("expected at least 2 fields, got %d", len(row))
	}

	fundName := row[1]
	if !strings.Contains(fundName, "é") {
		t.Errorf("fund name should contain UTF-8 é after re-encoding, got: %q", fundName)
	}
}

// TestImportEllettsville tests CSV parse -> filter -> tree build for Ellettsville.
func TestImportEllettsville(t *testing.T) {
	fixturePath := filepath.Join("testdata", "ellettsville_sample.csv")
	f, err := os.Open(fixturePath)
	if err != nil {
		t.Fatalf("failed to open fixture: %v", err)
	}
	defer f.Close()

	entity := GatewayEntityConfig{
		DisplayName:          "Ellettsville",
		State:                "IN",
		EntityType:           "city",
		UnitCode:             "0788",
		UnitType:             "City/Town",
		HierarchyColumns:     []string{"fund_description"},
		AmountColumn:         "Total budget estimate_adopted",
		UnitCodeColumn:       "unit_code",
		FiscalYearStartMonth: 1,
	}

	r := newGatewayCSVReader(f, '|')

	headers, err := r.Read()
	if err != nil {
		t.Fatalf("failed to read headers: %v", err)
	}

	required := append(entity.HierarchyColumns, entity.AmountColumn, entity.UnitCodeColumn)
	headerIdx, err := validateHeaders(headers, required)
	if err != nil {
		t.Fatalf("header validation failed: %v", err)
	}

	allRecords, err := r.ReadAll()
	if err != nil {
		t.Fatalf("failed to read rows: %v", err)
	}

	// Filter for Ellettsville only (unit_code=0788)
	records := filterEntityRows(allRecords, headerIdx, entity, "unit_code", "cnty_cd")
	if len(records) != 5 {
		t.Errorf("expected 5 Ellettsville rows, got %d", len(records))
	}

	categories, totalBudget, err := buildGatewayCategoryTree(records, headerIdx, entity)
	if err != nil {
		t.Fatalf("buildGatewayCategoryTree failed: %v", err)
	}

	// Verify expected fund names from Gateway data
	fundNames := make(map[string]bool)
	for _, cat := range categories {
		fundNames[cat.Name] = true
	}

	expectedFunds := []string{"GENERAL", "DEBT SERVICE", "LOCAL INCOME TAX", "LOCAL ROAD & STREET", "LEASE RENTAL PAYMENT"}
	for _, fund := range expectedFunds {
		if !fundNames[fund] {
			t.Errorf("expected fund %q not found in categories, got: %v", fund, fundNames)
		}
	}

	if totalBudget <= 0 {
		t.Errorf("totalBudget should be > 0, got %v", totalBudget)
	}

	// GENERAL fund should have adopted budget of 3656551.0
	for _, cat := range categories {
		if cat.Name == "GENERAL" {
			if cat.Amount != 3656551.0 {
				t.Errorf("GENERAL fund amount = %v, want 3656551.0", cat.Amount)
			}
			break
		}
	}
}

// TestImportMonroeCounty tests that county entity filtering works correctly.
func TestImportMonroeCounty(t *testing.T) {
	fixturePath := filepath.Join("testdata", "ellettsville_sample.csv")
	f, err := os.Open(fixturePath)
	if err != nil {
		t.Fatalf("failed to open fixture: %v", err)
	}
	defer f.Close()

	entity := GatewayEntityConfig{
		DisplayName:          "Monroe County",
		State:                "IN",
		EntityType:           "county",
		UnitCode:             "0000",
		UnitType:             "County",
		CountyCode:           "53",
		HierarchyColumns:     []string{"fund_description"},
		AmountColumn:         "Total budget estimate_adopted",
		UnitCodeColumn:       "unit_code",
		CountyCodeColumn:     "cnty_cd",
		FiscalYearStartMonth: 1,
	}

	r := newGatewayCSVReader(f, '|')

	headers, err := r.Read()
	if err != nil {
		t.Fatalf("failed to read headers: %v", err)
	}

	required := append(entity.HierarchyColumns, entity.AmountColumn, entity.UnitCodeColumn, entity.CountyCodeColumn)
	headerIdx, err := validateHeaders(headers, required)
	if err != nil {
		t.Fatalf("header validation failed: %v", err)
	}

	allRecords, err := r.ReadAll()
	if err != nil {
		t.Fatalf("failed to read rows: %v", err)
	}

	// Filter for Monroe County (unit_code=0000, cnty_cd=53)
	// Should NOT include Adams County (cnty_cd=01, unit_code=0000)
	records := filterEntityRows(allRecords, headerIdx, entity, "unit_code", "cnty_cd")
	if len(records) != 2 {
		t.Errorf("expected 2 Monroe County rows, got %d", len(records))
	}

	categories, totalBudget, err := buildGatewayCategoryTree(records, headerIdx, entity)
	if err != nil {
		t.Fatalf("buildGatewayCategoryTree failed: %v", err)
	}

	if len(categories) == 0 {
		t.Error("expected at least one root category")
	}

	// totalBudget should be positive (48,777,223 - 5,000,000 for the two rows)
	if totalBudget <= 0 {
		t.Errorf("totalBudget should be > 0, got %v", totalBudget)
	}

	// Monroe County should have GENERAL and DEBT SERVICE funds
	fundNames := make(map[string]bool)
	for _, cat := range categories {
		fundNames[cat.Name] = true
	}
	if !fundNames["GENERAL"] {
		t.Error("expected GENERAL fund for Monroe County")
	}
	if !fundNames["DEBT SERVICE"] {
		t.Error("expected DEBT SERVICE fund for Monroe County")
	}
}

// TestFilterEntityRows tests the entity row filtering logic.
func TestFilterEntityRows(t *testing.T) {
	headerIdx := map[string]int{
		"unit_code": 4,
		"cnty_cd":   1,
	}

	records := [][]string{
		{"2024", "53", "Monroe", "3", "0788", "ELLETTSVILLE"},
		{"2024", "53", "Monroe", "1", "0000", "MONROE COUNTY"},
		{"2024", "01", "Adams", "1", "0000", "ADAMS COUNTY"},
	}

	t.Run("filter by unit_code only", func(t *testing.T) {
		entity := GatewayEntityConfig{UnitCode: "0788"}
		filtered := filterEntityRows(records, headerIdx, entity, "unit_code", "cnty_cd")
		if len(filtered) != 1 {
			t.Errorf("expected 1 row, got %d", len(filtered))
		}
	})

	t.Run("filter by unit_code and county_code", func(t *testing.T) {
		entity := GatewayEntityConfig{UnitCode: "0000", CountyCode: "53"}
		filtered := filterEntityRows(records, headerIdx, entity, "unit_code", "cnty_cd")
		if len(filtered) != 1 {
			t.Errorf("expected 1 row (Monroe County only), got %d", len(filtered))
		}
	})
}
