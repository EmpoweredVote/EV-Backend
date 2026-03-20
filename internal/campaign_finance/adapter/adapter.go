package adapter

import "github.com/EmpoweredVote/EV-Backend/internal/campaign_finance"

// FetchResult is returned by the Fetch phase of the adapter pipeline.
// TotalExpected comes from pagination.count on the first FEC API page.
// TotalFetched is the actual number of records received across all pages.
type FetchResult struct {
	Records       []map[string]interface{}
	TotalExpected int
	TotalFetched  int
}

// NormalizeResult is returned by the Normalize phase.
// Skipped counts memo items (memo_code="X") and superseded amendments (is_amended=true).
// TotalParsed is the number of rows the parser examined for this politician (used by callers
// to compute >1% skip threshold).
type NormalizeResult struct {
	Contributions []campaign_finance.Contribution
	Skipped       int
	TotalParsed   int
}

// UpsertResult is returned by the Upsert phase.
// Inserted: rows newly written to contributions table.
// Skipped: rows already present (ON CONFLICT DO NOTHING).
// Unresolved: records with no matching PoliticianSource (logged but not inserted).
// Errors: count of records that failed for unexpected reasons.
type UpsertResult struct {
	Inserted   int
	Skipped    int
	Unresolved int
	Errors     int
}

// SourceAdapter is the contract all ingestion adapters must implement.
// Adding a new data source means implementing this interface — no existing adapter code changes.
type SourceAdapter interface {
	// Name returns the data_source value used in contributions.data_source.
	// Must match the CHECK constraint values: fec | indiana | cal_access | la_socrata | community_verified
	Name() string

	// Fetch retrieves all raw records for the given politician source.
	// For FEC: fetches all Schedule A pages via keyset pagination.
	Fetch(ps campaign_finance.PoliticianSource) (FetchResult, error)

	// Normalize converts raw records into Contribution structs ready for upsert.
	// Filtering of memo items and amended filings happens here.
	Normalize(raw FetchResult, ps campaign_finance.PoliticianSource) (NormalizeResult, error)

	// Upsert writes normalized contributions to the DB idempotently.
	// Uses ON CONFLICT (data_source, source_transaction_id) DO NOTHING.
	Upsert(normalized NormalizeResult) (UpsertResult, error)
}
