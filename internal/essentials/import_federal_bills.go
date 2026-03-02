package essentials

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/google/uuid"
	"gorm.io/gorm/clause"
)

// ImportFederalBillsConfig controls the behavior of the ImportFederalBills function.
type ImportFederalBillsConfig struct {
	// DryRun logs what would be done without writing to the database.
	DryRun bool

	// CongressNumbers lists which congress numbers to import (default: [119, 118]).
	CongressNumbers []int

	// SkipSummaries skips the CRS summary fetch phase for faster test runs.
	SkipSummaries bool

	// CongressAPIKey is the Congress.gov API key (required).
	CongressAPIKey string

	// MaxErrors aborts the import after this many consecutive per-politician
	// failures (default: 50). Prevents infinite loops during Congress.gov outages.
	MaxErrors int
}

// ImportFederalBillsResult contains counts from a completed import run.
type ImportFederalBillsResult struct {
	BillsUpserted      int
	CosponsorsUpserted int
	SummariesFetched   int
	Skipped            int
	Errors             []string
}

// ImportFederalBills fetches sponsored and cosponsored legislation from the
// Congress.gov API v3 for all politicians with bioguide bridge rows and
// upserts them into the legislative_bills and legislative_bill_cosponsors tables.
//
// Flow:
//  1. Validate config and apply defaults.
//  2. Create a CongressClient with token-bucket rate limiting.
//  3. Load all bioguide bridge rows into an in-memory map.
//  4. For each congress number, for each politician with a bioguide ID:
//     a. Fetch and upsert sponsored bills (sets SponsorID).
//     b. Fetch and upsert cosponsored bills (does NOT overwrite SponsorID).
//  5. Fetch CRS plain-language summaries for bills that still have an empty summary.
func ImportFederalBills(cfg ImportFederalBillsConfig) (ImportFederalBillsResult, error) {
	result := ImportFederalBillsResult{}

	// ── Step 1: Validate config and apply defaults ─────────────────────────
	if cfg.CongressAPIKey == "" {
		return result, fmt.Errorf("CongressAPIKey is required")
	}
	if len(cfg.CongressNumbers) == 0 {
		cfg.CongressNumbers = []int{119, 118}
	}
	if cfg.MaxErrors == 0 {
		cfg.MaxErrors = 50
	}

	log.Printf("[import-federal-bills] Starting import for congress numbers: %v", cfg.CongressNumbers)
	if cfg.DryRun {
		log.Printf("[import-federal-bills] DRY RUN — no database writes will be made")
	}

	// ── Step 2: Create CongressClient ─────────────────────────────────────
	client := NewCongressClient(cfg.CongressAPIKey)

	// ── Step 3: Load bioguide bridge table ────────────────────────────────
	// Load all bioguide bridge rows up front in a single query.
	type bridgeRow struct {
		IDValue      string    `gorm:"column:id_value"`
		PoliticianID uuid.UUID `gorm:"column:politician_id"`
	}
	var bridges []bridgeRow
	if err := db.DB.Table("essentials.legislative_politician_id_map").
		Select("id_value, politician_id").
		Where("id_type = ?", "bioguide").
		Find(&bridges).Error; err != nil {
		return result, fmt.Errorf("loading bioguide bridge table: %w", err)
	}
	bioguideMap := make(map[string]uuid.UUID, len(bridges))
	for _, b := range bridges {
		bioguideMap[b.IDValue] = b.PoliticianID
	}
	log.Printf("[import-federal-bills] Loaded %d bioguide bridge entries", len(bioguideMap))

	if len(bioguideMap) == 0 {
		log.Printf("[import-federal-bills] WARNING: no bioguide bridge entries found — run backfill-legislative-ids first")
		return result, nil
	}

	// ── Step 4: Import bills per congress ────────────────────────────────
	for _, congressNum := range cfg.CongressNumbers {
		log.Printf("[import-federal-bills] Processing %dth Congress", congressNum)

		// Get or create the legislative session for this congress
		sessionID, _, err := getOrCreateSession(congressNum)
		if err != nil {
			return result, fmt.Errorf("getting session for congress %d: %w", congressNum, err)
		}

		consecutiveErrors := 0
		processed := 0
		total := len(bioguideMap)

		for bioguideID, politicianID := range bioguideMap {
			ctx := context.Background()

			// ── Sponsored bills ──────────────────────────────────────────
			sponsored, err := client.GetSponsoredLegislation(ctx, bioguideID, congressNum)
			if err != nil {
				log.Printf("[import-federal-bills] ERROR fetching sponsored bills for bioguide=%s congress=%d: %v",
					bioguideID, congressNum, err)
				result.Errors = append(result.Errors,
					fmt.Sprintf("sponsored bills bioguide=%s congress=%d: %v", bioguideID, congressNum, err))
				consecutiveErrors++
				if consecutiveErrors >= cfg.MaxErrors {
					return result, fmt.Errorf("too many consecutive failures (%d) — Congress.gov may be unavailable", cfg.MaxErrors)
				}
				continue
			}
			consecutiveErrors = 0 // reset on success

			for _, bill := range sponsored {
				billDBID, upserted, err := upsertSponsoredBill(bill, congressNum, sessionID, politicianID, cfg.DryRun)
				if err != nil {
					log.Printf("[import-federal-bills] ERROR upserting sponsored bill %s-%s-%s: %v",
						strconv.Itoa(congressNum), bill.Type, bill.Number, err)
					result.Errors = append(result.Errors,
						fmt.Sprintf("sponsored bill upsert %d-%s-%s: %v", congressNum, bill.Type, bill.Number, err))
					continue
				}
				if upserted {
					result.BillsUpserted++
				}
				_ = billDBID // bill DB ID tracked for potential future use
			}

			// ── Cosponsored bills ────────────────────────────────────────
			cosponsored, err := client.GetCosponsoredLegislation(ctx, bioguideID, congressNum)
			if err != nil {
				log.Printf("[import-federal-bills] ERROR fetching cosponsored bills for bioguide=%s congress=%d: %v",
					bioguideID, congressNum, err)
				result.Errors = append(result.Errors,
					fmt.Sprintf("cosponsored bills bioguide=%s congress=%d: %v", bioguideID, congressNum, err))
				consecutiveErrors++
				if consecutiveErrors >= cfg.MaxErrors {
					return result, fmt.Errorf("too many consecutive failures (%d) — Congress.gov may be unavailable", cfg.MaxErrors)
				}
				continue
			}
			consecutiveErrors = 0

			for _, bill := range cosponsored {
				billDBID, _, err := upsertCosponsoredBill(bill, congressNum, sessionID, cfg.DryRun)
				if err != nil {
					log.Printf("[import-federal-bills] ERROR upserting cosponsored bill %d-%s-%s: %v",
						congressNum, bill.Type, bill.Number, err)
					result.Errors = append(result.Errors,
						fmt.Sprintf("cosponsored bill upsert %d-%s-%s: %v", congressNum, bill.Type, bill.Number, err))
					continue
				}

				if billDBID != uuid.Nil {
					// Upsert the cosponsorship link (DoNothing on conflict)
					if !cfg.DryRun {
						cosponsor := LegislativeBillCosponsor{
							BillID:       billDBID,
							PoliticianID: politicianID,
						}
						if dbErr := db.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&cosponsor).Error; dbErr != nil {
							log.Printf("[import-federal-bills] ERROR upserting cosponsor link bill=%s politician=%s: %v",
								billDBID, politicianID, dbErr)
						} else {
							result.CosponsorsUpserted++
						}
					} else {
						log.Printf("[import-federal-bills] DRY RUN — would upsert cosponsor link bill=%s politician=%s",
							billDBID, politicianID)
						result.CosponsorsUpserted++
					}
				}
			}

			processed++
			if processed%25 == 0 {
				log.Printf("[import-federal-bills] Processed %d/%d politicians for %dth Congress",
					processed, total, congressNum)
			}
		}

		log.Printf("[import-federal-bills] Finished %dth Congress: %d politicians processed",
			congressNum, processed)
	}

	// ── Step 5: CRS Summary Fetch ─────────────────────────────────────────
	if cfg.SkipSummaries {
		log.Printf("[import-federal-bills] Skipping CRS summary fetch (--skip-summaries)")
	} else {
		summariesFetched, err := fetchMissingSummaries(client, cfg.DryRun)
		if err != nil {
			// Non-fatal: log and continue
			log.Printf("[import-federal-bills] WARNING: summary fetch encountered error: %v", err)
		}
		result.SummariesFetched = summariesFetched
		log.Printf("[import-federal-bills] Fetched %d CRS summaries", summariesFetched)
	}

	log.Printf("[import-federal-bills] Import complete: %d bills, %d cosponsors, %d summaries, %d skipped, %d errors",
		result.BillsUpserted, result.CosponsorsUpserted, result.SummariesFetched, result.Skipped, len(result.Errors))

	return result, nil
}

// upsertSponsoredBill upserts a bill into legislative_bills with the SponsorID set.
// Returns the DB UUID of the bill and a boolean indicating if the upsert modified a row.
func upsertSponsoredBill(bill congressBillItem, congressNum int, sessionID uuid.UUID, sponsorID uuid.UUID, dryRun bool) (uuid.UUID, bool, error) {
	externalID := fmt.Sprintf("%d-%s-%s", congressNum, bill.Type, bill.Number)
	number := fmt.Sprintf("%s %s", bill.Type, bill.Number)
	statusLabel := normalizeBillStatus(bill.LatestAction.Text)

	var introducedAt *time.Time
	if bill.IntroducedDate != "" {
		t, err := time.Parse("2006-01-02", bill.IntroducedDate)
		if err == nil {
			introducedAt = &t
		}
	}

	if dryRun {
		log.Printf("[import-federal-bills] DRY RUN — would upsert sponsored bill: %s (%s)", externalID, bill.LatestTitle)
		return uuid.New(), true, nil
	}

	rec := LegislativeBill{
		SessionID:    sessionID,
		ExternalID:   externalID,
		Jurisdiction: "federal",
		Number:       number,
		Title:        bill.LatestTitle,
		RawStatus:    bill.LatestAction.Text,
		StatusLabel:  statusLabel,
		SponsorID:    &sponsorID,
		IntroducedAt: introducedAt,
		URL:          bill.URL,
		Source:       "congress",
	}

	// Upsert on (external_id, jurisdiction) — update all mutable fields including sponsor_id.
	if err := db.DB.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "external_id"},
			{Name: "jurisdiction"},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			"title", "raw_status", "status_label", "summary", "sponsor_id", "url",
		}),
	}).Create(&rec).Error; err != nil {
		return uuid.Nil, false, err
	}

	// After upsert, query back the actual DB ID (OnConflict may not return ID for existing rows).
	var existing LegislativeBill
	if err := db.DB.
		Where("external_id = ? AND jurisdiction = ?", externalID, "federal").
		Select("id").
		First(&existing).Error; err != nil {
		return uuid.Nil, false, fmt.Errorf("querying bill ID after upsert: %w", err)
	}

	return existing.ID, true, nil
}

// upsertCosponsoredBill upserts a cosponsored bill into legislative_bills WITHOUT
// overwriting the SponsorID (the primary sponsor may already be set correctly).
// Returns the DB UUID of the bill.
func upsertCosponsoredBill(bill congressBillItem, congressNum int, sessionID uuid.UUID, dryRun bool) (uuid.UUID, bool, error) {
	externalID := fmt.Sprintf("%d-%s-%s", congressNum, bill.Type, bill.Number)
	number := fmt.Sprintf("%s %s", bill.Type, bill.Number)
	statusLabel := normalizeBillStatus(bill.LatestAction.Text)

	var introducedAt *time.Time
	if bill.IntroducedDate != "" {
		t, err := time.Parse("2006-01-02", bill.IntroducedDate)
		if err == nil {
			introducedAt = &t
		}
	}

	if dryRun {
		log.Printf("[import-federal-bills] DRY RUN — would upsert cosponsored bill: %s (%s)", externalID, bill.LatestTitle)
		return uuid.New(), true, nil
	}

	rec := LegislativeBill{
		SessionID:    sessionID,
		ExternalID:   externalID,
		Jurisdiction: "federal",
		Number:       number,
		Title:        bill.LatestTitle,
		RawStatus:    bill.LatestAction.Text,
		StatusLabel:  statusLabel,
		SponsorID:    nil, // CRITICAL: do not set — preserve existing sponsor
		IntroducedAt: introducedAt,
		URL:          bill.URL,
		Source:       "congress",
	}

	// Upsert on (external_id, jurisdiction) — do NOT update sponsor_id.
	if err := db.DB.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "external_id"},
			{Name: "jurisdiction"},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			"title", "raw_status", "status_label", "url",
		}),
	}).Create(&rec).Error; err != nil {
		return uuid.Nil, false, err
	}

	// Query back the actual DB ID.
	var existing LegislativeBill
	if err := db.DB.
		Where("external_id = ? AND jurisdiction = ?", externalID, "federal").
		Select("id").
		First(&existing).Error; err != nil {
		return uuid.Nil, false, fmt.Errorf("querying cosponsored bill ID after upsert: %w", err)
	}

	return existing.ID, true, nil
}

// fetchMissingSummaries queries all federal bills with empty summary, then fetches
// CRS plain-language summaries from Congress.gov and updates each bill.
// Returns the number of summaries fetched and stored.
func fetchMissingSummaries(client *CongressClient, dryRun bool) (int, error) {
	type billRow struct {
		ID         uuid.UUID `gorm:"column:id"`
		ExternalID string    `gorm:"column:external_id"`
	}
	var bills []billRow
	if err := db.DB.Table("essentials.legislative_bills").
		Select("id, external_id").
		Where("jurisdiction = ? AND (summary = '' OR summary IS NULL)", "federal").
		Find(&bills).Error; err != nil {
		return 0, fmt.Errorf("querying bills without summaries: %w", err)
	}

	log.Printf("[import-federal-bills] Found %d federal bills without summaries", len(bills))

	fetched := 0
	ctx := context.Background()

	for _, b := range bills {
		congressNum, billType, billNumber, err := parseBillExternalID(b.ExternalID)
		if err != nil {
			log.Printf("[import-federal-bills] SKIP unparseable external_id=%s: %v", b.ExternalID, err)
			continue
		}

		summary, err := client.GetBillSummary(ctx, congressNum, billType, billNumber)
		if err != nil {
			log.Printf("[import-federal-bills] WARN summary fetch failed for %s: %v", b.ExternalID, err)
			continue // non-fatal
		}
		if summary == "" {
			continue // no CRS summary available for this bill
		}

		if dryRun {
			log.Printf("[import-federal-bills] DRY RUN — would update summary for bill %s (%d chars)",
				b.ExternalID, len(summary))
			fetched++
			continue
		}

		if err := db.DB.Table("essentials.legislative_bills").
			Where("id = ?", b.ID).
			Update("summary", summary).Error; err != nil {
			log.Printf("[import-federal-bills] WARN failed to update summary for bill %s: %v", b.ExternalID, err)
			continue
		}

		fetched++
	}

	return fetched, nil
}

// parseBillExternalID parses a bill external_id in the format "{congress}-{type}-{number}"
// (e.g., "119-HR-1044") into its components.
func parseBillExternalID(externalID string) (congress int, billType, billNumber string, err error) {
	parts := strings.SplitN(externalID, "-", 3)
	if len(parts) != 3 {
		return 0, "", "", fmt.Errorf("expected format 'congress-type-number', got %q", externalID)
	}
	congress, err = strconv.Atoi(parts[0])
	if err != nil {
		return 0, "", "", fmt.Errorf("invalid congress number %q in external_id %q: %w", parts[0], externalID, err)
	}
	return congress, parts[1], parts[2], nil
}
