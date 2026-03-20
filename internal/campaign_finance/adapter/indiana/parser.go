package indiana

import (
	"archive/zip"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"
)

// ParsedRow holds the typed fields from a single Indiana contribution CSV row.
// All fields are derived from the header-driven column index, making the parser
// resilient to column reordering across annual exports.
type ParsedRow struct {
	FileNumber       string
	CommitteeType    string
	Committee        string
	CandidateName    string
	ContributorType  string
	ContributorName  string
	Address          string
	City             string
	State            string
	ZIP              string
	Occupation       string
	ContributionType string
	Description      string
	Amount           float64
	ContributionDate time.Time
	ReceivedBy       string
	Amended          string
	RowNumber        int
}

// stripBOM removes a leading UTF-8 BOM from a string, which is common in
// Windows-generated CSV files exported from the Indiana Campaign Finance portal.
func stripBOM(s string) string {
	return strings.TrimPrefix(s, "\xEF\xBB\xBF")
}

// colGet returns the value at the given column name from a record, using the
// header-to-index map. Returns empty string if the column name is not found or
// the index is out of range.
func colGet(record []string, colIdx map[string]int, name string) string {
	i, ok := colIdx[name]
	if !ok || i >= len(record) {
		return ""
	}
	return strings.TrimSpace(record[i])
}

// ParseCSV opens a ZIP archive at zipPath, locates the inner CSV file by suffix,
// and parses it into matched and unmatched slices based on knownFileNumbers.
//
// A row is "matched" if its FileNumber is present in knownFileNumbers.
// Unmatched rows are returned separately for the unresolved queue.
//
// Column positions are determined by reading the header row first — NOT by
// hardcoded indexes — making the parser resilient to column reordering.
//
// Returns:
//   - matched: rows whose FileNumber is in knownFileNumbers
//   - unmatched: rows whose FileNumber is NOT in knownFileNumbers
//   - totalParsed: total data rows examined (matched + unmatched + skipped)
//   - err: non-nil only on fatal parse errors (missing CSV, bad header, etc.)
func ParseCSV(zipPath string, knownFileNumbers map[string]bool) (matched []ParsedRow, unmatched []ParsedRow, totalParsed int, err error) {
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("indiana: ParseCSV: open zip %s: %w", zipPath, err)
	}
	defer zr.Close()

	// Locate the inner CSV by suffix — do NOT hardcode the filename because
	// Indiana prepends the year (e.g. "2024_ContributionData.csv").
	var csvFile *zip.File
	for _, f := range zr.File {
		if strings.HasSuffix(f.Name, "ContributionData.csv") {
			csvFile = f
			break
		}
	}
	if csvFile == nil {
		return nil, nil, 0, fmt.Errorf("indiana: ParseCSV: no file matching *ContributionData.csv in %s", zipPath)
	}

	rc, err := csvFile.Open()
	if err != nil {
		return nil, nil, 0, fmt.Errorf("indiana: ParseCSV: open inner csv: %w", err)
	}
	defer rc.Close()

	csvReader := csv.NewReader(rc)
	csvReader.LazyQuotes = true
	csvReader.FieldsPerRecord = -1

	// Read header row and build column-name-to-index map.
	header, err := csvReader.Read()
	if err != nil {
		return nil, nil, 0, fmt.Errorf("indiana: ParseCSV: read header: %w", err)
	}
	colIdx := make(map[string]int, len(header))
	for i, h := range header {
		// Strip BOM from the first field; trim whitespace from all.
		h = strings.TrimSpace(stripBOM(h))
		colIdx[h] = i
	}

	rowNum := 1 // header is row 0; data rows start at 1
	for {
		record, readErr := csvReader.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			log.Printf("indiana: ParseCSV: row %d read error: %v (skipping)", rowNum, readErr)
			rowNum++
			totalParsed++
			continue
		}
		totalParsed++
		rowNum++

		fileNumber := colGet(record, colIdx, "FileNumber")
		amountStr := colGet(record, colIdx, "Amount")
		amount, parseErr := strconv.ParseFloat(amountStr, 64)
		if parseErr != nil {
			log.Printf("indiana: ParseCSV: row %d: cannot parse Amount %q: %v (skipping row)", rowNum, amountStr, parseErr)
			continue
		}

		dateStr := colGet(record, colIdx, "ContributionDate")
		contribDate, dateErr := time.Parse("01/02/2006", dateStr)
		if dateErr != nil {
			// Do NOT skip — amount matters more than date. Zero time signals unknown.
			log.Printf("indiana: ParseCSV: row %d: cannot parse ContributionDate %q: %v (using zero time)", rowNum, dateStr, dateErr)
		}

		row := ParsedRow{
			FileNumber:       fileNumber,
			CommitteeType:    colGet(record, colIdx, "CommitteeType"),
			Committee:        colGet(record, colIdx, "Committee"),
			CandidateName:    colGet(record, colIdx, "CandidateName"),
			ContributorType:  colGet(record, colIdx, "ContributorType"),
			ContributorName:  colGet(record, colIdx, "ContributorName"),
			Address:          colGet(record, colIdx, "Address"),
			City:             colGet(record, colIdx, "City"),
			State:            colGet(record, colIdx, "State"),
			ZIP:              colGet(record, colIdx, "Zip"),
			Occupation:       colGet(record, colIdx, "Occupation"),
			ContributionType: colGet(record, colIdx, "ContributionType"),
			Description:      colGet(record, colIdx, "Description"),
			Amount:           amount,
			ContributionDate: contribDate,
			ReceivedBy:       colGet(record, colIdx, "ReceivedBy"),
			Amended:          colGet(record, colIdx, "Amended"),
			RowNumber:        rowNum - 1,
		}

		if knownFileNumbers[fileNumber] {
			matched = append(matched, row)
		} else {
			unmatched = append(unmatched, row)
		}
	}

	return matched, unmatched, totalParsed, nil
}
