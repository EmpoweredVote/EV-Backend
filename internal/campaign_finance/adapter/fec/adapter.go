package fec

import (
	"github.com/EmpoweredVote/EV-Backend/internal/campaign_finance"
	"github.com/EmpoweredVote/EV-Backend/internal/campaign_finance/adapter"
	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"gorm.io/gorm/clause"
)

// FECAdapter implements adapter.SourceAdapter for the FEC Schedule A API.
// Cycle must be set before calling Fetch — use New(cycle) to construct.
type FECAdapter struct {
	Cycle string
}

// New creates a FECAdapter configured for the given election cycle (e.g. "2024").
func New(cycle string) *FECAdapter {
	return &FECAdapter{Cycle: cycle}
}

// Name returns the data_source identifier for FEC contributions.
func (a *FECAdapter) Name() string { return "fec" }

// Fetch retrieves all Schedule A contribution records for the given politician source.
func (a *FECAdapter) Fetch(ps campaign_finance.PoliticianSource) (adapter.FetchResult, error) {
	records, totalExpected, err := FetchAllPages(ps.ExternalID, a.Cycle)
	if err != nil {
		return adapter.FetchResult{}, err
	}

	return adapter.FetchResult{
		Records:       records,
		TotalExpected: totalExpected,
		TotalFetched:  len(records),
	}, nil
}

// Normalize delegates to the normalizer.go Normalize function.
func (a *FECAdapter) Normalize(raw adapter.FetchResult, ps campaign_finance.PoliticianSource) (adapter.NormalizeResult, error) {
	return Normalize(raw.Records, ps)
}

// Upsert writes contributions to the DB idempotently using ON CONFLICT DO NOTHING.
// Conflicts on (data_source, source_transaction_id) are silently skipped — re-running
// produces 0 new rows for already-ingested records.
func (a *FECAdapter) Upsert(normalized adapter.NormalizeResult) (adapter.UpsertResult, error) {
	if len(normalized.Contributions) == 0 {
		return adapter.UpsertResult{}, nil
	}

	result := db.DB.Clauses(clause.OnConflict{
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

// Compile-time verification that FECAdapter satisfies the SourceAdapter interface.
var _ adapter.SourceAdapter = (*FECAdapter)(nil)
