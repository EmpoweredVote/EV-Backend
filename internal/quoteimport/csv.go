package quoteimport

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
)

// QuoteRow holds one parsed row from the quote collection CSV.
type QuoteRow struct {
	FullName   string
	TopicKey   string
	QuoteText  string
	SourceURL  string
	SourceName string
}

// ParseCSV opens path and returns all parsed quote rows.
// Validation (DB lookups, empty checks) happens in Run; this function
// only parses the file structure.
func ParseCSV(path string) ([]QuoteRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.LazyQuotes = true // quote_text contains commas and embedded quotes

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
	required := []string{"full_name", "topic_key", "quote_text", "source_url", "source_name"}
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

	var rows []QuoteRow
	for _, rec := range records[1:] {
		rows = append(rows, QuoteRow{
			FullName:   get(rec, "full_name"),
			TopicKey:   get(rec, "topic_key"),
			QuoteText:  get(rec, "quote_text"),
			SourceURL:  get(rec, "source_url"),
			SourceName: get(rec, "source_name"),
		})
	}

	return rows, nil
}
