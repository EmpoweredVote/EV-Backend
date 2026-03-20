package socrata

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	campaign_finance "github.com/EmpoweredVote/EV-Backend/internal/campaign_finance"
	"github.com/EmpoweredVote/EV-Backend/internal/campaign_finance/adapter"
)

// Compile-time assertion: SocrataAdapter must satisfy the SourceAdapter interface.
var _ adapter.SourceAdapter = (*SocrataAdapter)(nil)

// SocrataAdapter implements adapter.SourceAdapter for the LA City Socrata
// campaign contributions dataset (data.lacity.org, dataset m6g2-gc6c).
type SocrataAdapter struct {
	DB     *gorm.DB
	client *SocrataClient
}

// New creates a SocrataAdapter backed by the provided database and Socrata app token.
func New(db *gorm.DB, appToken string) *SocrataAdapter {
	return &SocrataAdapter{
		DB:     db,
		client: NewClient(appToken),
	}
}

// Name returns the data_source identifier stored in contributions.data_source.
// Must match the CHECK constraint value in models.go.
func (a *SocrataAdapter) Name() string { return "la_socrata" }

// Fetch retrieves all contribution records for the given PoliticianSource.
// Performs a delta-fetch (since last completed run) when a prior run exists;
// otherwise performs a full fetch.
func (a *SocrataAdapter) Fetch(ps campaign_finance.PoliticianSource) (adapter.FetchResult, error) {
	var since *time.Time

	var lastRun campaign_finance.IngestionRun
	err := a.DB.Where(
		"politician_source_id = ? AND adapter_name = ? AND status = ?",
		ps.ID, "la_socrata", "completed",
	).Order("completed_at DESC").First(&lastRun).Error

	if err == nil && lastRun.CompletedAt != nil {
		// Delta-fetch: only records newer than the last successful run.
		since = lastRun.CompletedAt
	}
	// If err != nil (record not found or other DB error), fall back to full fetch.

	records, err := a.client.FetchContributions(context.Background(), ps.ExternalID, since)
	if err != nil {
		return adapter.FetchResult{}, fmt.Errorf("SocrataAdapter.Fetch: %w", err)
	}

	return adapter.FetchResult{
		Records:       records,
		TotalExpected: len(records),
		TotalFetched:  len(records),
	}, nil
}

// Normalize converts raw Socrata JSON records into Contribution structs.
// Skips records with missing or unparseable con_amount or con_date.
func (a *SocrataAdapter) Normalize(raw adapter.FetchResult, ps campaign_finance.PoliticianSource) (adapter.NormalizeResult, error) {
	contributions := make([]campaign_finance.Contribution, 0, len(raw.Records))
	skipped := 0

	for _, rec := range raw.Records {
		// Extract required string fields using ok-check pattern.
		cmtID, _ := rec["cmt_id"].(string)
		conName, _ := rec["con_name"].(string)

		// con_amount is a string in Socrata JSON — must parse via strconv.
		amountStr, _ := rec["con_amount"].(string)
		if amountStr == "" {
			skipped++
			continue
		}
		amount, err := strconv.ParseFloat(amountStr, 64)
		if err != nil {
			log.Printf("SocrataAdapter.Normalize: skip record — con_amount parse error: %v (value=%q)", err, amountStr)
			skipped++
			continue
		}

		// con_date parsing.
		dateStr, _ := rec["con_date"].(string)
		if dateStr == "" {
			skipped++
			continue
		}
		conDate, err := time.Parse("2006-01-02T15:04:05.000", dateStr)
		if err != nil {
			log.Printf("SocrataAdapter.Normalize: skip record — con_date parse error: %v (value=%q)", err, dateStr)
			skipped++
			continue
		}

		// Build election cycle: round contribution year UP to next even year.
		year := conDate.Year()
		if year%2 != 0 {
			year++
		}
		electionCycle := fmt.Sprintf("%d", year)

		// Build composite source_transaction_id, capped at 128 chars total.
		// Format: cmt_id|con_date|con_name|con_amount
		// con_name is truncated if necessary to keep total within 128 chars.
		maxNameLen := 128 - len(cmtID) - len(dateStr) - len(amountStr) - 3 // 3 pipe separators
		if maxNameLen < 0 {
			maxNameLen = 0
		}
		conNameForID := conName
		if len(conNameForID) > maxNameLen {
			conNameForID = conNameForID[:maxNameLen]
		}
		sourceTransactionID := fmt.Sprintf("%s|%s|%s|%s", cmtID, dateStr, conNameForID, amountStr)

		// Extract optional fields — absent fields default to empty string.
		conCityNm, _ := rec["con_city_nm"].(string)
		conStateNm, _ := rec["con_state_nm"].(string)
		conOccp, _ := rec["con_occp"].(string)
		conEmpr, _ := rec["con_empr"].(string)

		// Enrich rec with optional fields for raw record storage (they may be absent from source).
		enrichedRec := make(map[string]interface{}, len(rec)+4)
		for k, v := range rec {
			enrichedRec[k] = v
		}
		enrichedRec["con_city_nm"] = conCityNm
		enrichedRec["con_state_nm"] = conStateNm
		enrichedRec["con_occp"] = conOccp
		enrichedRec["con_empr"] = conEmpr

		rawBytes, _ := json.Marshal(enrichedRec)

		conDateCopy := conDate
		contributions = append(contributions, campaign_finance.Contribution{
			PoliticianSourceID:  ps.ID,
			DataSource:          "la_socrata",
			SourceTransactionID: sourceTransactionID,
			Amount:              amount,
			ContributionDate:    &conDateCopy,
			ElectionCycle:       electionCycle,
			ConfidenceLevel:     "HIGH",
			RawRecord:           rawBytes,
			DonorID:             nil,
			CommitteeID:         nil,
		})
	}

	return adapter.NormalizeResult{
		Contributions: contributions,
		Skipped:       skipped,
		TotalParsed:   len(raw.Records),
	}, nil
}

// Upsert writes normalized contributions to the DB idempotently.
// Uses ON CONFLICT (data_source, source_transaction_id) DO NOTHING.
func (a *SocrataAdapter) Upsert(normalized adapter.NormalizeResult) (adapter.UpsertResult, error) {
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
