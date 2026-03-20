package indiana

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	campaign_finance "github.com/EmpoweredVote/EV-Backend/internal/campaign_finance"
	"github.com/EmpoweredVote/EV-Backend/internal/campaign_finance/adapter"
	"github.com/google/uuid"
)

// IndianaAdapter implements adapter.SourceAdapter for Indiana Campaign Finance
// bulk CSV data. It downloads the annual contribution ZIP, parses the CSV once
// (caching results by FileNumber), and routes rows to either confirmed contributions
// or the unresolved queue based on politician_sources research_status.
type IndianaAdapter struct {
	DB *gorm.DB

	year int

	// Download state — populated by PreDownload.
	zipPath         string
	zipETag         string
	zipDownloadedAt time.Time
	zipSkipped      bool
	zipDownloaded   bool

	// Entity resolution maps — populated by PreDownload from politician_sources.
	// confirmedFileNumbers: FileNumber -> PoliticianSource.ID for research_status="confirmed"
	confirmedFileNumbers map[string]uuid.UUID
	// allKnownFileNumbers: all indiana sources regardless of research_status
	allKnownFileNumbers map[string]bool

	// CSV parse cache — populated on first Fetch call.
	parsedOnce  bool
	parsedCache map[string][]ParsedRow // FileNumber -> confirmed rows

	// unmatchedRows accumulates rows from known-but-unconfirmed FileNumbers
	// across all Fetch calls, for WriteUnresolved.
	unmatchedRows []ParsedRow
}

// New creates an IndianaAdapter for the given year.
func New(db *gorm.DB, year int) *IndianaAdapter {
	return &IndianaAdapter{
		DB:                   db,
		year:                 year,
		confirmedFileNumbers: make(map[string]uuid.UUID),
		allKnownFileNumbers:  make(map[string]bool),
		parsedCache:          make(map[string][]ParsedRow),
	}
}

// Name returns the data_source value for contributions written by this adapter.
func (a *IndianaAdapter) Name() string { return "indiana" }

// PreDownload downloads the annual ZIP (with ETag caching) and queries
// politician_sources to build entity resolution maps.
// Must be called before Fetch.
func (a *IndianaAdapter) PreDownload(ctx context.Context) error {
	localPath, etag, downloadedAt, skipped, err := DownloadZIP(ctx, a.DB, a.year)
	if err != nil {
		return fmt.Errorf("IndianaAdapter.PreDownload: %w", err)
	}
	a.zipPath = localPath
	a.zipETag = etag
	a.zipDownloadedAt = downloadedAt
	a.zipSkipped = skipped
	a.zipDownloaded = true

	// Build entity resolution maps from politician_sources.
	var sources []campaign_finance.PoliticianSource
	if err := a.DB.Where("source_system = ?", "indiana").Find(&sources).Error; err != nil {
		return fmt.Errorf("IndianaAdapter.PreDownload: query politician_sources: %w", err)
	}
	if len(sources) == 0 {
		log.Printf("indiana: PreDownload: no politician_sources with source_system='indiana' found — all rows will be dropped")
	}
	for _, s := range sources {
		a.allKnownFileNumbers[s.ExternalID] = true
		if s.ResearchStatus == "confirmed" {
			a.confirmedFileNumbers[s.ExternalID] = s.ID
		}
	}
	return nil
}

// ZIPSkipped reports whether the server returned 304 Not Modified.
func (a *IndianaAdapter) ZIPSkipped() bool { return a.zipSkipped }

// ZIPETag returns the ETag received from the most recent download response.
func (a *IndianaAdapter) ZIPETag() string { return a.zipETag }

// ZIPDownloadedAt returns the timestamp of the download request.
func (a *IndianaAdapter) ZIPDownloadedAt() time.Time { return a.zipDownloadedAt }

// SaveETag persists the current ETag to DataSourceMetadata.
func (a *IndianaAdapter) SaveETag() error {
	if a.zipETag != "" {
		saveETag(a.DB, a.year, a.zipETag)
	}
	return nil
}

// Cleanup removes the temp ZIP file.
func (a *IndianaAdapter) Cleanup() {
	cleanupZIP(a.zipPath)
	a.zipPath = ""
}

// ensureParsed parses the CSV exactly once across all Fetch calls, caching
// confirmed rows by FileNumber and accumulating unresolved rows.
func (a *IndianaAdapter) ensureParsed() error {
	if a.parsedOnce {
		return nil
	}
	a.parsedOnce = true

	matched, unmatched, total, err := ParseCSV(a.zipPath, a.allKnownFileNumbers)
	if err != nil {
		return fmt.Errorf("indiana: ensureParsed: %w", err)
	}
	log.Printf("indiana: ParseCSV year=%d: %d total rows, %d matched known FileNumbers, %d unmatched (dropped)",
		a.year, total, len(matched), total-len(matched)-len(unmatched))

	// Split matched rows into confirmed (-> parsedCache) and unresolved (-> unmatchedRows).
	for _, row := range matched {
		if _, isConfirmed := a.confirmedFileNumbers[row.FileNumber]; isConfirmed {
			a.parsedCache[row.FileNumber] = append(a.parsedCache[row.FileNumber], row)
		} else {
			// In allKnownFileNumbers but NOT confirmed — goes to unresolved queue.
			a.unmatchedRows = append(a.unmatchedRows, row)
		}
	}

	// unmatched from ParseCSV = completely unknown FileNumbers — silently drop.
	_ = unmatched

	log.Printf("indiana: year=%d: %d rows to confirmed contributions, %d rows to unresolved queue",
		a.year, func() int {
			n := 0
			for _, rows := range a.parsedCache {
				n += len(rows)
			}
			return n
		}(), len(a.unmatchedRows))

	return nil
}

// Fetch returns all confirmed contribution rows for the given PoliticianSource.
// On the first call it parses the full CSV and caches results; subsequent calls
// use the cache.
func (a *IndianaAdapter) Fetch(ps campaign_finance.PoliticianSource) (adapter.FetchResult, error) {
	if !a.zipDownloaded {
		return adapter.FetchResult{}, fmt.Errorf("indiana: Fetch called before PreDownload")
	}
	if a.zipSkipped {
		return adapter.FetchResult{}, nil
	}

	if err := a.ensureParsed(); err != nil {
		return adapter.FetchResult{}, err
	}

	rows := a.parsedCache[ps.ExternalID]
	records := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		records = append(records, map[string]interface{}{
			"FileNumber":       row.FileNumber,
			"CommitteeType":    row.CommitteeType,
			"Committee":        row.Committee,
			"CandidateName":    row.CandidateName,
			"ContributorType":  row.ContributorType,
			"ContributorName":  row.ContributorName,
			"Address":          row.Address,
			"City":             row.City,
			"State":            row.State,
			"ZIP":              row.ZIP,
			"Occupation":       row.Occupation,
			"ContributionType": row.ContributionType,
			"Description":      row.Description,
			"Amount":           row.Amount,
			"ContributionDate": row.ContributionDate,
			"ReceivedBy":       row.ReceivedBy,
			"Amended":          row.Amended,
			"RowNumber":        row.RowNumber,
		})
	}
	return adapter.FetchResult{
		Records:       records,
		TotalExpected: len(records),
		TotalFetched:  len(records),
	}, nil
}

// Normalize converts FetchResult records into Contribution structs.
// source_transaction_id is a composite of FileNumber|ContributionDate|ContributorName|Amount,
// truncated to 128 characters to fit the DB column.
// Delegates to NormalizeRow to keep normalization logic in one place (no drift with backfill).
func (a *IndianaAdapter) Normalize(raw adapter.FetchResult, ps campaign_finance.PoliticianSource) (adapter.NormalizeResult, error) {
	contributions := make([]campaign_finance.Contribution, 0, len(raw.Records))

	for _, rec := range raw.Records {
		contrib, err := NormalizeRow(rec, ps)
		if err != nil {
			// Log and skip rows that cannot be normalized (e.g., unparseable amount).
			log.Printf("indiana: Normalize: skip row: %v", err)
			continue
		}
		contributions = append(contributions, contrib)
	}

	return adapter.NormalizeResult{
		Contributions: contributions,
		Skipped:       0,
		TotalParsed:   len(raw.Records),
	}, nil
}

// Upsert writes normalized contributions to the DB using ON CONFLICT DO NOTHING
// on (data_source, source_transaction_id).
func (a *IndianaAdapter) Upsert(normalized adapter.NormalizeResult) (adapter.UpsertResult, error) {
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
	return adapter.UpsertResult{Inserted: inserted, Skipped: skipped, Unresolved: 0, Errors: 0}, nil
}

// WriteUnresolved writes rows from known-but-unconfirmed FileNumbers to the
// unresolved_contributions table. This enables Phase 8 backfill: once OrgIds
// are confirmed, these rows can be promoted to contributions without re-parsing.
//
// runID is the IngestionRun.ID to associate these rows with.
// Returns the count of rows written.
func (a *IndianaAdapter) WriteUnresolved(runID uint) (int, error) {
	if len(a.unmatchedRows) == 0 {
		return 0, nil
	}

	batch := make([]campaign_finance.UnresolvedContribution, 0, len(a.unmatchedRows))
	for _, row := range a.unmatchedRows {
		rawBytes, err := json.Marshal(map[string]interface{}{
			"FileNumber":       row.FileNumber,
			"CommitteeType":    row.CommitteeType,
			"Committee":        row.Committee,
			"CandidateName":    row.CandidateName,
			"ContributorType":  row.ContributorType,
			"ContributorName":  row.ContributorName,
			"Address":          row.Address,
			"City":             row.City,
			"State":            row.State,
			"ZIP":              row.ZIP,
			"Occupation":       row.Occupation,
			"ContributionType": row.ContributionType,
			"Description":      row.Description,
			"Amount":           row.Amount,
			"ContributionDate": row.ContributionDate,
			"ReceivedBy":       row.ReceivedBy,
			"Amended":          row.Amended,
			"RowNumber":        row.RowNumber,
		})
		if err != nil {
			log.Printf("indiana: WriteUnresolved: marshal row %d: %v (skipping)", row.RowNumber, err)
			continue
		}
		batch = append(batch, campaign_finance.UnresolvedContribution{
			AdapterName:    "indiana",
			IngestionRunID: runID,
			RawRow:         rawBytes,
			RowNumber:      row.RowNumber,
			ExternalID:     row.FileNumber,
		})
	}

	if len(batch) == 0 {
		return 0, nil
	}

	result := a.DB.CreateInBatches(&batch, 100)
	if result.Error != nil {
		return 0, fmt.Errorf("indiana: WriteUnresolved: %w", result.Error)
	}
	return len(batch), nil
}

// UnresolvedCount returns the number of rows queued for the unresolved contributions table.
// Valid after the first Fetch call.
func (a *IndianaAdapter) UnresolvedCount() int {
	return len(a.unmatchedRows)
}

// truncateSourceTransactionID is an internal helper used in Normalize.
// Exported for testing; otherwise treated as internal.
func truncateSourceTransactionID(parts ...string) string {
	s := strings.Join(parts, "|")
	if len(s) > 128 {
		return s[:128]
	}
	return s
}

// Compile-time assertion: IndianaAdapter must implement SourceAdapter.
var _ adapter.SourceAdapter = (*IndianaAdapter)(nil)
