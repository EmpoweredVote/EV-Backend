package indiana

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	campaign_finance "github.com/EmpoweredVote/EV-Backend/internal/campaign_finance"
)

// NormalizeRow converts a single deserialized RawRow map and a PoliticianSource
// into a Contribution struct. It handles both the direct CSV parse path (where
// Amount is float64 and ContributionDate is time.Time) and the jsonb backfill
// path (where Amount may be float64 and ContributionDate may be a string).
//
// The source_transaction_id produced here is identical to the one produced by
// IndianaAdapter.Normalize(), ensuring no normalization drift between the live
// ingestion path and the Phase 8 backfill path.
func NormalizeRow(rec map[string]interface{}, ps campaign_finance.PoliticianSource) (campaign_finance.Contribution, error) {
	// --- Amount: float64 first, then string fallback ---
	var amount float64
	switch v := rec["Amount"].(type) {
	case float64:
		amount = v
	case string:
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return campaign_finance.Contribution{}, fmt.Errorf("NormalizeRow: parse Amount %q: %w", v, err)
		}
		amount = parsed
	}

	// --- ContributionDate: time.Time first, then RFC3339 string fallback ---
	var tranDate time.Time
	switch v := rec["ContributionDate"].(type) {
	case time.Time:
		tranDate = v
	case string:
		if v != "" {
			parsed, err := time.Parse(time.RFC3339, v)
			if err != nil {
				// Zero time — matches amount-primary-fact convention from 06-01
				tranDate = time.Time{}
			} else {
				tranDate = parsed
			}
		}
	}

	contributorName, _ := rec["ContributorName"].(string)
	fileNumber, _ := rec["FileNumber"].(string)

	// Election cycle: round up to next even year.
	year := tranDate.Year()
	if year == 0 {
		year = time.Now().Year()
	}
	if year%2 != 0 {
		year++
	}
	electionCycle := fmt.Sprintf("%d", year)

	// source_transaction_id: composite, truncated to 128 chars.
	rawTxID := fmt.Sprintf("%s|%s|%s|%.2f",
		fileNumber,
		tranDate.Format("2006-01-02"),
		contributorName,
		amount,
	)
	if len(rawTxID) > 128 {
		rawTxID = rawTxID[:128]
	}

	rawBytes, _ := json.Marshal(rec)

	var contribDate *time.Time
	if !tranDate.IsZero() {
		t := tranDate
		contribDate = &t
	}

	return campaign_finance.Contribution{
		PoliticianSourceID:  ps.ID,
		DataSource:          "indiana",
		SourceTransactionID: rawTxID,
		Amount:              amount,
		ContributionDate:    contribDate,
		ElectionCycle:       electionCycle,
		ConfidenceLevel:     "HIGH",
		RawRecord:           rawBytes,
	}, nil
}
