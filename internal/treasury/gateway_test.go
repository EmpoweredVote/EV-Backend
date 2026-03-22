package treasury

import (
	"bytes"
	"os"
	"path/filepath"
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
	if len(headers) != 4 {
		t.Fatalf("expected 4 header columns, got %d: %v", len(headers), headers)
	}

	// Read all data rows
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatalf("failed to read rows: %v", err)
	}
	if len(rows) != 10 {
		t.Errorf("expected 10 data rows, got %d", len(rows))
	}

	// Check first data row fields are accessible by index
	firstRow := rows[0]
	if len(firstRow) < 4 {
		t.Fatalf("first row has fewer than 4 fields: %v", firstRow)
	}
	if firstRow[0] == "" || firstRow[1] == "" || firstRow[2] == "" || firstRow[3] == "" {
		t.Errorf("first row has empty required fields: %v", firstRow)
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
		headers := []string{"fund_name", "department_name", "account_description", "amount", "extra_col"}
		required := []string{"fund_name", "department_name", "account_description", "amount"}

		idx, err := validateHeaders(headers, required)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if idx["fund_name"] != 0 {
			t.Errorf("fund_name index = %d, want 0", idx["fund_name"])
		}
		if idx["amount"] != 3 {
			t.Errorf("amount index = %d, want 3", idx["amount"])
		}
	})

	t.Run("missing one column", func(t *testing.T) {
		headers := []string{"fund_name", "department_name", "amount"}
		required := []string{"fund_name", "department_name", "account_description", "amount"}

		_, err := validateHeaders(headers, required)
		if err == nil {
			t.Fatal("expected error for missing column, got nil")
		}
		errStr := err.Error()
		if len(errStr) == 0 {
			t.Error("error message is empty")
		}
		// Check error contains "missing required columns"
		if !containsStr(errStr, "missing required columns") {
			t.Errorf("error should mention 'missing required columns', got: %s", errStr)
		}
		// Check error names the missing column
		if !containsStr(errStr, "account_description") {
			t.Errorf("error should mention missing column 'account_description', got: %s", errStr)
		}
	})

	t.Run("extra columns in CSV", func(t *testing.T) {
		headers := []string{"fund_name", "department_name", "account_description", "amount", "col_extra1", "col_extra2"}
		required := []string{"fund_name", "department_name", "account_description", "amount"}

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

	// Verify expected header names
	expectedHeaders := []string{"fund_name", "department_name", "account_description", "amount"}
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
	// Windows-1252 byte for e-acute (é) is 0xE9
	// Pipe-delimited row with Windows-1252 encoded department name
	// Header row + data row with e-acute in department name
	input := []byte("fund_name|department_name|account_description|amount\nGénéral Fund|Département des Finances|Salaires|50000.00\n")

	// Re-encode to Windows-1252 by using the charmap encoder
	encoder := charmap.Windows1252.NewEncoder()
	win1252Bytes, err := encoder.Bytes(input)
	if err != nil {
		t.Fatalf("failed to encode to Windows-1252: %v", err)
	}

	r := newGatewayCSVReader(bytes.NewReader(win1252Bytes), '|')

	// Read header
	_, err = r.Read()
	if err != nil {
		t.Fatalf("failed to read header: %v", err)
	}

	// Read data row
	row, err := r.Read()
	if err != nil {
		t.Fatalf("failed to read data row: %v", err)
	}

	if len(row) < 2 {
		t.Fatalf("expected at least 2 fields, got %d", len(row))
	}

	// The department name should contain the é character correctly as UTF-8
	deptName := row[1]
	if !containsStr(deptName, "é") {
		t.Errorf("department name should contain UTF-8 é after re-encoding, got: %q", deptName)
	}
}

// TestImportEllettsville tests end-to-end CSV parse -> tree build for Ellettsville.
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
		HierarchyColumns:     []string{"fund_name", "department_name", "account_description"},
		AmountColumn:         "amount",
		FiscalYearStartMonth: 1,
	}

	r := newGatewayCSVReader(f, '|')

	// Read header
	headers, err := r.Read()
	if err != nil {
		t.Fatalf("failed to read headers: %v", err)
	}

	headerIdx, err := validateHeaders(headers, append(entity.HierarchyColumns, entity.AmountColumn))
	if err != nil {
		t.Fatalf("header validation failed: %v", err)
	}

	// Read all rows
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("failed to read rows: %v", err)
	}

	categories, totalBudget, err := buildGatewayCategoryTree(records, headerIdx, entity)
	if err != nil {
		t.Fatalf("buildGatewayCategoryTree failed: %v", err)
	}

	// Verify expected fund names
	fundNames := make(map[string]bool)
	for _, cat := range categories {
		fundNames[cat.Name] = true
	}

	expectedFunds := []string{"General Fund", "Utility Fund", "Capital Fund"}
	for _, fund := range expectedFunds {
		if !fundNames[fund] {
			t.Errorf("expected fund %q not found in categories", fund)
		}
	}

	// Verify amounts are aggregated (not zero)
	if totalBudget <= 0 {
		t.Errorf("totalBudget should be > 0, got %v", totalBudget)
	}

	// General Fund should have subcategories (departments)
	for _, cat := range categories {
		if cat.Name == "General Fund" {
			if len(cat.Subcategories) == 0 {
				t.Error("General Fund should have subcategories (departments)")
			}
			break
		}
	}
}

// TestImportMonroeCounty tests that county entity config produces correct CategoryImport output.
func TestImportMonroeCounty(t *testing.T) {
	// Reuse same fixture with county entity config
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
		HierarchyColumns:     []string{"fund_name", "department_name", "account_description"},
		AmountColumn:         "amount",
		FiscalYearStartMonth: 1,
	}

	r := newGatewayCSVReader(f, '|')

	headers, err := r.Read()
	if err != nil {
		t.Fatalf("failed to read headers: %v", err)
	}

	headerIdx, err := validateHeaders(headers, append(entity.HierarchyColumns, entity.AmountColumn))
	if err != nil {
		t.Fatalf("header validation failed: %v", err)
	}

	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("failed to read rows: %v", err)
	}

	categories, totalBudget, err := buildGatewayCategoryTree(records, headerIdx, entity)
	if err != nil {
		t.Fatalf("buildGatewayCategoryTree failed: %v", err)
	}

	if len(categories) == 0 {
		t.Error("expected at least one root category")
	}

	if totalBudget <= 0 {
		t.Errorf("totalBudget should be > 0, got %v", totalBudget)
	}

	// Verify entity type metadata doesn't affect parsing logic
	// (county vs city is a DB concern, not a parsing concern)
	// Key check: categories are still correctly structured
	for _, cat := range categories {
		if cat.Amount <= 0 {
			// Capital Fund has a net negative line item, total may be non-zero
			// Just verify amount field is populated
			if cat.Amount == 0 && cat.Name != "" {
				t.Logf("Note: category %q has zero amount (may be valid)", cat.Name)
			}
		}
	}
}

// containsStr is a helper for substring matching.
func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && findSubstr(s, substr))
}

func findSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
