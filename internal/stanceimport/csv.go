package stanceimport

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// StanceRow holds one parsed row from the stance research CSV.
type StanceRow struct {
	FullName   string
	ExternalID string // may be empty
	TopicKey   string
	Value      int
	SourceURLs []string // up to 3 non-empty URLs
}

// ParseCSV opens path and returns all parsed stance rows.
// Validation (range checks, DB lookups) happens in Run; this function
// only parses the file structure.
func ParseCSV(path string) ([]StanceRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.LazyQuotes = true
	r.FieldsPerRecord = -1 // variable number of fields allowed

	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read csv: %w", err)
	}
	if len(records) < 2 {
		return nil, fmt.Errorf("csv has no data rows")
	}

	// Build column index from header row
	header := records[0]
	// Strip BOM from first header cell if present
	if len(header) > 0 {
		header[0] = strings.TrimPrefix(header[0], "\ufeff")
	}

	col := make(map[string]int)
	for i, h := range header {
		col[strings.TrimSpace(h)] = i
	}

	// Validate required columns
	required := []string{"full_name", "topic_key", "value"}
	for _, req := range required {
		if _, ok := col[req]; !ok {
			return nil, fmt.Errorf("missing required column: %s", req)
		}
	}

	get := func(rec []string, name string) string {
		idx, ok := col[name]
		if !ok || idx >= len(rec) {
			return ""
		}
		return strings.TrimSpace(rec[idx])
	}

	var rows []StanceRow
	for i, rec := range records[1:] {
		rowNum := i + 2 // 1-based, skipping header

		valueStr := get(rec, "value")
		if valueStr == "" {
			return nil, fmt.Errorf("row %d: value is empty", rowNum)
		}
		val, err := strconv.Atoi(valueStr)
		if err != nil {
			return nil, fmt.Errorf("row %d: value %q is not an integer: %w", rowNum, valueStr, err)
		}

		// Collect non-empty source URLs
		var sources []string
		for _, col := range []string{"source_url_1", "source_url_2", "source_url_3"} {
			u := get(rec, col)
			if u != "" {
				sources = append(sources, u)
			}
		}

		rows = append(rows, StanceRow{
			FullName:   get(rec, "full_name"),
			ExternalID: get(rec, "external_id"),
			TopicKey:   get(rec, "topic_key"),
			Value:      val,
			SourceURLs: sources,
		})
	}

	return rows, nil
}
