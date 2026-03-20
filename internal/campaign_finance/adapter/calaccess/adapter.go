package calaccess

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	campaign_finance "github.com/EmpoweredVote/EV-Backend/internal/campaign_finance"
	"github.com/EmpoweredVote/EV-Backend/internal/campaign_finance/adapter"
)

// CalAccessAdapter implements adapter.SourceAdapter for the Cal-Access bulk ZIP.
//
// Usage pattern:
//  1. Call PreDownload once per ingestion run (downloads the ZIP or detects 304).
//  2. Call Fetch/Normalize/Upsert for each politician.
//  3. Call SaveETag after all politicians are processed to persist the new ETag.
//  4. Call Cleanup to remove the temp ZIP file.
type CalAccessAdapter struct {
	DB *gorm.DB

	zipPath    string
	zipETag    string
	zipDownloadedAt time.Time
	zipSkipped bool
	zipDownloaded bool

	// globalTotalParsed is the total row count seen by the first ParseRCPT call.
	// Subsequent calls may differ slightly (file is the same, count should match),
	// but we record it once from the first call.
	globalTotalParsed int
	parsedOnce        bool

	// Per-politician state set during Fetch, consumed in Normalize.
	lastPoliticianSkipped       int
	lastPoliticianTotalExamined int
}

// New creates a CalAccessAdapter backed by the provided database connection.
func New(db *gorm.DB) *CalAccessAdapter {
	return &CalAccessAdapter{DB: db}
}

// Name returns the data_source identifier used in contributions.data_source.
func (a *CalAccessAdapter) Name() string { return "cal_access" }

// PreDownload downloads the Cal-Access bulk ZIP (or detects 304 Not Modified).
// Must be called before any Fetch call.
func (a *CalAccessAdapter) PreDownload(ctx context.Context) error {
	localPath, etag, downloadedAt, skipped, err := DownloadZIP(ctx, a.DB)
	if err != nil {
		return fmt.Errorf("CalAccessAdapter.PreDownload: %w", err)
	}

	a.zipPath = localPath
	a.zipETag = etag
	a.zipDownloadedAt = downloadedAt
	a.zipSkipped = skipped
	a.zipDownloaded = true

	return nil
}

// SaveETag persists the ZIP ETag and global row count to DataSourceMetadata.
// Call this after all politicians have been processed for the run.
func (a *CalAccessAdapter) SaveETag() error {
	if a.zipETag != "" {
		saveETag(a.DB, a.zipETag)
	}
	if a.globalTotalParsed > 0 {
		saveTotalRows(a.DB, a.globalTotalParsed)
	}
	return nil
}

// ZIPSkipped reports whether the server returned 304 Not Modified.
func (a *CalAccessAdapter) ZIPSkipped() bool { return a.zipSkipped }

// ZIPETag returns the ETag received from the Cal-Access CDN for this run.
func (a *CalAccessAdapter) ZIPETag() string { return a.zipETag }

// ZIPDownloadedAt returns the time the download request was initiated.
func (a *CalAccessAdapter) ZIPDownloadedAt() time.Time { return a.zipDownloadedAt }

// Fetch parses RCPT_CD.TSV and returns all contribution rows for ps.ExternalID.
// PreDownload must have been called successfully before Fetch.
func (a *CalAccessAdapter) Fetch(ps campaign_finance.PoliticianSource) (adapter.FetchResult, error) {
	if !a.zipDownloaded {
		return adapter.FetchResult{}, fmt.Errorf("CalAccessAdapter.Fetch: PreDownload must be called before Fetch")
	}

	if a.zipSkipped {
		// Server indicated no new data — return empty result.
		return adapter.FetchResult{}, nil
	}

	targetIDs := map[string]bool{ps.ExternalID: true}
	parsedRows, skippedRows, totalParsed, err := ParseRCPT(a.zipPath, targetIDs)
	if err != nil {
		return adapter.FetchResult{}, fmt.Errorf("CalAccessAdapter.Fetch: %w", err)
	}

	// Record global total from first parse (same ZIP, same count every time).
	if !a.parsedOnce {
		a.globalTotalParsed = totalParsed
		a.parsedOnce = true
	}

	// Per-politician tracking for NormalizeResult.TotalParsed.
	a.lastPoliticianSkipped = len(skippedRows)
	a.lastPoliticianTotalExamined = len(parsedRows) + len(skippedRows)

	// Convert ParsedRow slice to the generic []map[string]interface{} expected by FetchResult.
	records := make([]map[string]interface{}, 0, len(parsedRows))
	for _, row := range parsedRows {
		records = append(records, map[string]interface{}{
			"CMTE_ID":     row.CMTE_ID,
			"FILING_ID":   row.FilingID,
			"REC_TYPE":    row.RecType,
			"FORM_TYPE":   row.FormType,
			"CTRIB_NAML":  row.CtribNameL,
			"CTRIB_NAMF":  row.CtribNameF,
			"CTRIB_EMP":   row.CtribEmp,
			"CTRIB_OCC":   row.CtribOcc,
			"AMOUNT":      row.Amount,
			"TRAN_DATE":   row.TranDate,
			"AMEND_ID":    row.AmendID,
			"LINE_ITEM":   row.LineItem,
		})
	}

	return adapter.FetchResult{
		Records:       records,
		TotalExpected: len(records),
		TotalFetched:  len(records),
	}, nil
}

// Normalize converts raw FetchResult records into Contribution structs.
func (a *CalAccessAdapter) Normalize(raw adapter.FetchResult, ps campaign_finance.PoliticianSource) (adapter.NormalizeResult, error) {
	contributions := make([]campaign_finance.Contribution, 0, len(raw.Records))

	for _, rec := range raw.Records {
		filingID, _ := rec["FILING_ID"].(string)
		amendID, _  := rec["AMEND_ID"].(int)
		lineItem, _ := rec["LINE_ITEM"].(int)
		amount, _   := rec["AMOUNT"].(float64)
		tranDate, _ := rec["TRAN_DATE"].(time.Time)

		// Build election cycle: round contribution year up to next even year.
		year := tranDate.Year()
		if year%2 != 0 {
			year++
		}
		electionCycle := fmt.Sprintf("%d", year)

		sourceTransactionID := fmt.Sprintf("%s_%d_%d", filingID, amendID, lineItem)

		rawBytes, _ := json.Marshal(rec)

		tranDateCopy := tranDate
		contributions = append(contributions, campaign_finance.Contribution{
			PoliticianSourceID:  ps.ID,
			DataSource:          "cal_access",
			SourceTransactionID: sourceTransactionID,
			Amount:              amount,
			ContributionDate:    &tranDateCopy,
			ElectionCycle:       electionCycle,
			ConfidenceLevel:     "HIGH",
			RawRecord:           rawBytes,
			DonorID:             nil,
			CommitteeID:         nil,
		})
	}

	return adapter.NormalizeResult{
		Contributions: contributions,
		Skipped:       a.lastPoliticianSkipped,
		TotalParsed:   a.lastPoliticianTotalExamined,
	}, nil
}

// Upsert writes normalized contributions to the DB idempotently using ON CONFLICT DO NOTHING.
func (a *CalAccessAdapter) Upsert(normalized adapter.NormalizeResult) (adapter.UpsertResult, error) {
	if len(normalized.Contributions) == 0 {
		return adapter.UpsertResult{}, nil
	}

	result := a.DB.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "data_source"}, {Name: "source_transaction_id"}},
		DoNothing: true,
	}).CreateInBatches(&normalized.Contributions, 100)

	if result.Error != nil {
		return adapter.UpsertResult{Errors: 1}, result.Error
	}

	inserted := int(result.RowsAffected)
	skipped := len(normalized.Contributions) - inserted

	return adapter.UpsertResult{
		Inserted:   inserted,
		Skipped:    skipped,
		Unresolved: 0,
		Errors:     0,
	}, nil
}

// Cleanup removes the temp ZIP file downloaded by PreDownload.
func (a *CalAccessAdapter) Cleanup() {
	cleanupZIP(a.zipPath)
	a.zipPath = ""
}

// Compile-time assertion that CalAccessAdapter satisfies the SourceAdapter interface.
var _ adapter.SourceAdapter = (*CalAccessAdapter)(nil)
