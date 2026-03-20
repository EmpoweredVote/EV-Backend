package calaccess

import (
	"archive/zip"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"strconv"
	"time"

	"golang.org/x/text/encoding/charmap"
)

// Error type taxonomy — used in SkippedRow.ErrorType.
// Detail field contains field names or counts only — never raw values or PII.
const (
	ErrFieldCountMismatch   = "field_count_mismatch"
	ErrInvalidDate          = "invalid_date"
	ErrMissingRequiredField = "missing_required_field"
	ErrAmountParseError     = "amount_parse_error"
	ErrEncodingError        = "encoding_error"
)

// SkippedRow records a row that was excluded during parsing.
// Detail contains field names or structural info — never raw values or PII.
type SkippedRow struct {
	RowNum    int
	ErrorType string
	Detail    string
}

// ParsedRow holds a single contribution record extracted from RCPT_CD.TSV.
type ParsedRow struct {
	CMTE_ID     string
	FilingID    string
	RecType     string
	FormType    string
	CtribNameL  string
	CtribNameF  string
	CtribEmp    string
	CtribOcc    string
	Amount      float64
	TranDate    time.Time
	AmendID     int
	LineItem    int
}

// requiredColumns lists the columns that must be present in the TSV header.
var requiredColumns = []string{
	"CMTE_ID", "AMOUNT", "TRAN_DATE", "FILING_ID",
	"AMEND_ID", "LINE_ITEM", "REC_TYPE", "FORM_TYPE",
}

// ParseRCPT opens the Cal-Access bulk ZIP, finds RCPT_CD.TSV, and streams all
// rows that match targetFilerIDs. Rows for other filers are silently skipped and
// NOT counted in the skipped slice (they are not errors).
//
// totalParsed is the count of ALL data rows in the file (excluding header), across
// all filer IDs — used for the global CCDC cross-check.
func ParseRCPT(zipPath string, targetFilerIDs map[string]bool) (rows []ParsedRow, skipped []SkippedRow, totalParsed int, err error) {
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("calaccess: ParseRCPT: open zip: %w", err)
	}
	defer zr.Close()

	// Find RCPT_CD.TSV in the archive.
	var rcptFile *zip.File
	for _, f := range zr.File {
		if f.Name == "RCPT_CD.TSV" {
			rcptFile = f
			break
		}
	}
	if rcptFile == nil {
		return nil, nil, 0, fmt.Errorf("calaccess: ParseRCPT: RCPT_CD.TSV not found in zip")
	}

	rc, err := rcptFile.Open()
	if err != nil {
		return nil, nil, 0, fmt.Errorf("calaccess: ParseRCPT: open RCPT_CD.TSV: %w", err)
	}
	defer rc.Close()

	// Wrap with Windows-1252 decoder.
	decoded := charmap.Windows1252.NewDecoder().Reader(rc)

	csvReader := csv.NewReader(decoded)
	csvReader.Comma = '\t'
	csvReader.LazyQuotes = true
	csvReader.FieldsPerRecord = -1 // variable field count allowed
	csvReader.ReuseRecord = true   // performance: caller must copy fields immediately

	// Read and validate header row.
	header, err := csvReader.Read()
	if err != nil {
		return nil, nil, 0, fmt.Errorf("calaccess: ParseRCPT: read header: %w", err)
	}

	// Build column index map.
	colIdx := make(map[string]int, len(header))
	for i, h := range header {
		colIdx[h] = i
	}

	// Validate all required columns are present.
	for _, col := range requiredColumns {
		if _, ok := colIdx[col]; !ok {
			return nil, nil, 0, fmt.Errorf("calaccess: ParseRCPT: required column %q not found in header", col)
		}
	}

	// Optional columns — zero-value used if absent.
	optIdx := func(name string) int {
		if i, ok := colIdx[name]; ok {
			return i
		}
		return -1
	}

	ctribNameLIdx := optIdx("CTRIB_NAML")
	ctribNameFIdx := optIdx("CTRIB_NAMF")
	ctribEmpIdx   := optIdx("CTRIB_EMP")
	ctribOccIdx   := optIdx("CTRIB_OCC")

	// Helper: safely get a field by index from a record (ReuseRecord slice).
	getField := func(record []string, idx int) string {
		if idx < 0 || idx >= len(record) {
			return ""
		}
		return record[idx]
	}

	rowNum := 0
	for {
		record, readErr := csvReader.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			log.Printf("calaccess: ParseRCPT: row %d read error: %v", rowNum+1, readErr)
			rowNum++
			totalParsed++
			skipped = append(skipped, SkippedRow{
				RowNum:    rowNum,
				ErrorType: ErrEncodingError,
				Detail:    "csv_read_error",
			})
			continue
		}

		rowNum++
		totalParsed++

		// Minimum field count check.
		minFields := 0
		for _, req := range requiredColumns {
			if colIdx[req]+1 > minFields {
				minFields = colIdx[req] + 1
			}
		}
		if len(record) < minFields {
			skipped = append(skipped, SkippedRow{
				RowNum:    rowNum,
				ErrorType: ErrFieldCountMismatch,
				Detail:    fmt.Sprintf("got %d fields, need at least %d", len(record), minFields),
			})
			continue
		}

		// IMMEDIATELY copy required fields from ReuseRecord slice.
		cmteID   := record[colIdx["CMTE_ID"]]
		amountStr := record[colIdx["AMOUNT"]]
		tranDateStr := record[colIdx["TRAN_DATE"]]
		filingID := record[colIdx["FILING_ID"]]
		amendIDStr := record[colIdx["AMEND_ID"]]
		lineItemStr := record[colIdx["LINE_ITEM"]]
		recType  := record[colIdx["REC_TYPE"]]
		formType := record[colIdx["FORM_TYPE"]]

		// Copy optional fields.
		ctribNameL := getField(record, ctribNameLIdx)
		ctribNameF := getField(record, ctribNameFIdx)
		ctribEmp   := getField(record, ctribEmpIdx)
		ctribOcc   := getField(record, ctribOccIdx)

		// Validate CMTE_ID.
		if cmteID == "" {
			skipped = append(skipped, SkippedRow{
				RowNum:    rowNum,
				ErrorType: ErrMissingRequiredField,
				Detail:    "CMTE_ID",
			})
			continue
		}

		// Filter: silently skip rows not belonging to our target filers.
		if !targetFilerIDs[cmteID] {
			continue
		}

		// Parse amount.
		amount, parseErr := strconv.ParseFloat(amountStr, 64)
		if parseErr != nil {
			skipped = append(skipped, SkippedRow{
				RowNum:    rowNum,
				ErrorType: ErrAmountParseError,
				Detail:    "AMOUNT",
			})
			continue
		}

		// Parse TRAN_DATE.
		if tranDateStr == "" {
			skipped = append(skipped, SkippedRow{
				RowNum:    rowNum,
				ErrorType: ErrMissingRequiredField,
				Detail:    "TRAN_DATE",
			})
			continue
		}
		tranDate, parseErr := time.Parse("01/02/2006", tranDateStr)
		if parseErr != nil {
			skipped = append(skipped, SkippedRow{
				RowNum:    rowNum,
				ErrorType: ErrInvalidDate,
				Detail:    "TRAN_DATE",
			})
			continue
		}

		// Parse AMEND_ID (non-fatal if missing — default 0).
		amendID, _ := strconv.Atoi(amendIDStr)

		// Parse LINE_ITEM (non-fatal if missing — default 0).
		lineItem, _ := strconv.Atoi(lineItemStr)

		rows = append(rows, ParsedRow{
			CMTE_ID:    cmteID,
			FilingID:   filingID,
			RecType:    recType,
			FormType:   formType,
			CtribNameL: ctribNameL,
			CtribNameF: ctribNameF,
			CtribEmp:   ctribEmp,
			CtribOcc:   ctribOcc,
			Amount:     amount,
			TranDate:   tranDate,
			AmendID:    amendID,
			LineItem:   lineItem,
		})
	}

	return rows, skipped, totalParsed, nil
}
