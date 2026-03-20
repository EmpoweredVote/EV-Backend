package fec

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/campaign_finance"
	"github.com/EmpoweredVote/EV-Backend/internal/campaign_finance/adapter"
)

// shouldSkipRecord returns true for records that must not be written to the contributions table:
//   - memo_code="X": memo item, not incorporated into FEC totals per FEC documentation
//   - is_amended=true: superseded filing, replaced by a later amendment
func shouldSkipRecord(record map[string]interface{}) bool {
	if memoCode, ok := record["memo_code"].(string); ok && memoCode == "X" {
		return true
	}
	if isAmended, ok := record["is_amended"].(bool); ok && isAmended {
		return true
	}
	return false
}

// Normalize converts raw FEC Schedule A records into Contribution structs.
// Memo items and superseded amendments are counted in NormalizeResult.Skipped
// and excluded from the Contributions slice.
func Normalize(records []map[string]interface{}, ps campaign_finance.PoliticianSource) (adapter.NormalizeResult, error) {
	result := adapter.NormalizeResult{}

	for _, record := range records {
		if shouldSkipRecord(record) {
			result.Skipped++
			continue
		}

		contribution, err := normalizeRecord(record, ps)
		if err != nil {
			return result, fmt.Errorf("normalizeRecord failed: %w", err)
		}

		result.Contributions = append(result.Contributions, contribution)
	}

	return result, nil
}

func normalizeRecord(record map[string]interface{}, ps campaign_finance.PoliticianSource) (campaign_finance.Contribution, error) {
	c := campaign_finance.Contribution{
		PoliticianSourceID: ps.ID,
		ConfidenceLevel:    "HIGH",
		DataSource:         "fec",
		DonorID:            nil,
		CommitteeID:        nil,
	}

	// Amount
	if v, ok := record["contribution_receipt_amount"].(float64); ok {
		c.Amount = v
	}

	// Contribution date: take first 10 chars of the date string (YYYY-MM-DD)
	if v, ok := record["contribution_receipt_date"].(string); ok && len(v) >= 10 {
		t, err := time.Parse("2006-01-02", v[:10])
		if err == nil {
			c.ContributionDate = &t
		}
	}

	// Election cycle: stored as float64 in JSON, format as 4-digit string
	if v, ok := record["two_year_transaction_period"].(float64); ok {
		c.ElectionCycle = fmt.Sprintf("%.0f", v)
	}

	// Source transaction ID
	if v, ok := record["sub_id"].(string); ok {
		c.SourceTransactionID = v
	}

	// Preserve full raw record as JSON for future entity resolution
	raw, err := json.Marshal(record)
	if err != nil {
		return c, fmt.Errorf("marshal raw record: %w", err)
	}
	c.RawRecord = raw

	return c, nil
}
