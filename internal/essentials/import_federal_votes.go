package essentials

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/google/uuid"
	"gorm.io/gorm/clause"
)

// ImportFederalVotesConfig controls the import behavior for federal voting records.
//
// House votes are imported from Congress.gov using CongressAPIKey.
// Senate votes are imported from LegiScan using LegiScanAPIKey.
type ImportFederalVotesConfig struct {
	DryRun          bool
	CongressNumbers []int  // default [119, 118]
	CongressAPIKey  string // for House votes via Congress.gov
	LegiScanAPIKey  string // for Senate votes via LegiScan
	HouseOnly       bool   // skip Senate import
	SenateOnly      bool   // skip House import
	MaxErrors       int    // abort after N consecutive errors (default 50)
}

// ImportFederalVotesResult contains counts from a federal votes import run.
type ImportFederalVotesResult struct {
	HouseVotesUpserted  int
	SenateVotesUpserted int
	LegiScanBridgeRows  int // new id_type="legiscan" bridge rows created
	Skipped             int
	Errors              []string
}

// sessionBillRecord holds a BallotReady bill_id and bill_number for LegiScan master list.
type masterListBill struct {
	BillID int    `json:"bill_id"`
	Number string `json:"number"`
	Title  string `json:"title"`
}

// legiScanSessionEntry represents an entry from the getSessionList response.
type legiScanSessionEntry struct {
	SessionID  int    `json:"session_id"`
	StateID    int    `json:"state_id"`
	YearStart  int    `json:"year_start"`
	YearEnd    int    `json:"year_end"`
	Special    int    `json:"special"`
	SessionName string `json:"session_name"`
}

// ImportFederalVotes imports House roll call votes from Congress.gov and Senate
// roll call votes from LegiScan into the legislative_votes table.
//
// CRITICAL design decisions:
//   - House = Congress.gov ONLY (Senate roll calls are NOT available from Congress.gov API v3)
//   - Senate = LegiScan ONLY (not Congress.gov)
//   - Individual vote failures are logged and skipped; import continues
//   - LegiScan budget is checked before Senate import begins
func ImportFederalVotes(cfg ImportFederalVotesConfig) (ImportFederalVotesResult, error) {
	result := ImportFederalVotesResult{}

	// Step 1: Validate and apply defaults
	if len(cfg.CongressNumbers) == 0 {
		cfg.CongressNumbers = []int{119, 118}
	}
	if cfg.MaxErrors == 0 {
		cfg.MaxErrors = 50
	}

	ctx := context.Background()

	// Step 2: Load bioguide bridge table into map[bioguideID]politicianID
	bioguideMap, err := loadBioguideMap()
	if err != nil {
		return result, fmt.Errorf("loading bioguide bridge table: %w", err)
	}
	log.Printf("[import-federal-votes] Loaded %d bioguide bridge entries", len(bioguideMap))

	// =========================================================================
	// Step 3: House Votes Import (Congress.gov)
	// =========================================================================
	if !cfg.SenateOnly {
		if cfg.CongressAPIKey == "" {
			return result, fmt.Errorf("CONGRESS_API_KEY is required for House votes import")
		}
		congressClient := NewCongressClient(cfg.CongressAPIKey)

		for _, congress := range cfg.CongressNumbers {
			sessionID, _, err := getOrCreateSession(congress)
			if err != nil {
				return result, fmt.Errorf("getting session for congress %d: %w", congress, err)
			}

			// House sessions: 1 and 2
			for sessionNum := 1; sessionNum <= 2; sessionNum++ {
				rollCalls, err := congressClient.GetHouseVoteList(ctx, congress, sessionNum)
				if err != nil {
					// 404 is expected for session 2 of an ongoing congress
					if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "returned 404") {
						log.Printf("[import-federal-votes] No House votes for %dth Congress session %d (404 — session may not exist yet)", congress, sessionNum)
						break
					}
					log.Printf("[import-federal-votes] ERROR fetching House vote list for congress %d session %d: %v", congress, sessionNum, err)
					result.Errors = append(result.Errors, fmt.Sprintf("GetHouseVoteList(%d,%d): %v", congress, sessionNum, err))
					continue
				}

				if len(rollCalls) == 0 {
					log.Printf("[import-federal-votes] No House votes for %dth Congress session %d", congress, sessionNum)
					continue
				}

				log.Printf("[import-federal-votes] %dth Congress session %d: fetching member votes for %d roll calls", congress, sessionNum, len(rollCalls))

				for _, rollCall := range rollCalls {
					// Parse vote date
					voteDate, dateErr := time.Parse("2006-01-02", rollCall.VoteDate)
					if dateErr != nil {
						log.Printf("[import-federal-votes] WARN: cannot parse voteDate %q for roll call %d — using zero time", rollCall.VoteDate, rollCall.RollCallNumber)
						voteDate = time.Time{}
					}

					// Attempt to find matching bill in DB by congress+bill number
					var billID *uuid.UUID
					if rollCall.BillNumber != "" {
						// Normalize: e.g., "H.R. 1044" -> "119-H.R.-1044" or similar
						// Our external_id format from import_federal_bills: "{congress}-{type}-{number}"
						// BillNumber from Congress.gov vote list uses format like "H.R. 1044"
						// Try both formats: direct and by scanning
						var bill LegislativeBill
						externalID := fmt.Sprintf("%d-%s", congress, strings.ReplaceAll(rollCall.BillNumber, " ", "-"))
						if dbErr := db.DB.Where("external_id = ? AND jurisdiction = ?", externalID, "federal").First(&bill).Error; dbErr == nil {
							billID = &bill.ID
						}
					}

					// Fetch individual member votes
					memberVotes, mvErr := congressClient.GetHouseVoteMemberVotes(ctx, congress, sessionNum, rollCall.RollCallNumber)
					if mvErr != nil {
						log.Printf("[import-federal-votes] ERROR fetching member votes for roll call %d/%d/%d: %v",
							congress, sessionNum, rollCall.RollCallNumber, mvErr)
						result.Errors = append(result.Errors, fmt.Sprintf("GetHouseVoteMemberVotes(%d,%d,%d): %v",
							congress, sessionNum, rollCall.RollCallNumber, mvErr))
						if len(result.Errors) >= cfg.MaxErrors {
							log.Printf("[import-federal-votes] Reached max errors (%d) — aborting House import", cfg.MaxErrors)
							goto houseDone
						}
						continue
					}

					externalVoteID := fmt.Sprintf("house-%d-%d-%d", congress, sessionNum, rollCall.RollCallNumber)
					result := houseVoteUpsert(cfg, &result, sessionID, billID, externalVoteID, rollCall, voteDate, bioguideMap, memberVotes)
					_ = result
				}

				log.Printf("[import-federal-votes] House votes for %dth Congress session %d: %d roll calls processed", congress, sessionNum, len(rollCalls))
			}
		}
	houseDone:
		log.Printf("[import-federal-votes] House import complete: %d votes upserted", result.HouseVotesUpserted)
	}

	// =========================================================================
	// Step 4: Senate Votes Import (LegiScan)
	// =========================================================================
	if !cfg.HouseOnly {
		if cfg.LegiScanAPIKey == "" {
			return result, fmt.Errorf("LEGISCAN_API_KEY is required for Senate votes import")
		}

		lsClient, lsErr := NewLegiScanClient(cfg.LegiScanAPIKey, "")
		if lsErr != nil {
			return result, fmt.Errorf("creating LegiScan client: %w", lsErr)
		}

		// Budget check before Senate import
		used, limit := lsClient.GetBudgetStatus()
		remaining := lsClient.RemainingBudget()
		log.Printf("[import-federal-votes] LegiScan budget remaining: %d/%d queries", remaining, limit)
		if remaining < 1000 {
			log.Printf("[import-federal-votes] WARNING: LegiScan budget low (used %d/%d) — Senate import may exhaust monthly budget", used, limit)
		}

		// Step 4a: Build LegiScan people_id bridge and legiscanMap per congress
		for _, congress := range cfg.CongressNumbers {
			sessionID, _, sessionErr := getOrCreateSession(congress)
			if sessionErr != nil {
				log.Printf("[import-federal-votes] ERROR getting session for congress %d: %v", congress, sessionErr)
				continue
			}

			// Get LegiScan session ID for this congress
			legiScanSessionID, lsSessionErr := findLegiScanSessionID(ctx, lsClient, congress)
			if lsSessionErr != nil {
				log.Printf("[import-federal-votes] ERROR finding LegiScan session for congress %d: %v", congress, lsSessionErr)
				result.Errors = append(result.Errors, fmt.Sprintf("findLegiScanSession(congress=%d): %v", congress, lsSessionErr))
				continue
			}
			if legiScanSessionID == 0 {
				log.Printf("[import-federal-votes] No LegiScan session found for congress %d — skipping", congress)
				continue
			}

			log.Printf("[import-federal-votes] Found LegiScan session_id=%d for congress %d", legiScanSessionID, congress)

			// Build people_id → politician UUID bridge map for senators
			legiscanMap, bridgeCount, bridgeErr := buildLegiScanSenatorBridge(ctx, lsClient, legiScanSessionID, cfg)
			if bridgeErr != nil {
				log.Printf("[import-federal-votes] ERROR building LegiScan senator bridge: %v", bridgeErr)
				result.Errors = append(result.Errors, fmt.Sprintf("buildLegiScanSenatorBridge: %v", bridgeErr))
				continue
			}
			result.LegiScanBridgeRows += bridgeCount
			log.Printf("[import-federal-votes] Built LegiScan bridge: %d senators matched, %d new bridge rows", len(legiscanMap), bridgeCount)

			// Step 4b: Import Senate votes from bill master list
			senateBillErr := importSenateVotes(ctx, lsClient, legiScanSessionID, sessionID, legiscanMap, cfg, &result)
			if senateBillErr != nil {
				log.Printf("[import-federal-votes] ERROR in Senate votes import for congress %d: %v", congress, senateBillErr)
				result.Errors = append(result.Errors, fmt.Sprintf("importSenateVotes(congress=%d): %v", congress, senateBillErr))
			}
		}

		log.Printf("[import-federal-votes] Senate import complete: %d votes upserted", result.SenateVotesUpserted)
	}

	return result, nil
}

// houseVoteUpsert handles upserting LegislativeVote records for House member votes.
// Returns the updated result pointer (mutates HouseVotesUpserted and Skipped).
func houseVoteUpsert(
	cfg ImportFederalVotesConfig,
	res *ImportFederalVotesResult,
	sessionID uuid.UUID,
	billID *uuid.UUID,
	externalVoteID string,
	rollCall houseVoteItem,
	voteDate time.Time,
	bioguideMap map[string]uuid.UUID,
	memberVotes []houseVoteMemberItem,
) *ImportFederalVotesResult {
	for _, mv := range memberVotes {
		polID, ok := bioguideMap[mv.BioguideID]
		if !ok {
			res.Skipped++
			continue
		}

		vote := LegislativeVote{
			PoliticianID:   polID,
			BillID:         billID,
			SessionID:      sessionID,
			ExternalVoteID: externalVoteID,
			VoteQuestion:   rollCall.VoteQuestion,
			Position:       normalizeVoteCast(mv.VoteCast),
			VoteDate:       voteDate,
			Result:         normalizeVoteResult(rollCall.Result),
			YeaCount:       rollCall.YeaTotal,
			NayCount:       rollCall.NayTotal,
			Source:         "congress",
		}

		if cfg.DryRun {
			log.Printf("[import-federal-votes] DRY RUN — would upsert House vote: bioguide=%s roll_call=%s position=%s",
				mv.BioguideID, externalVoteID, vote.Position)
			res.HouseVotesUpserted++
			continue
		}

		if dbErr := db.DB.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "politician_id"},
				{Name: "bill_id"},
				{Name: "session_id"},
				{Name: "external_vote_id"},
			},
			DoUpdates: clause.AssignmentColumns([]string{"position", "vote_question", "result"}),
		}).Create(&vote).Error; dbErr != nil {
			log.Printf("[import-federal-votes] ERROR upserting House vote %s for bioguide=%s: %v",
				externalVoteID, mv.BioguideID, dbErr)
			continue
		}

		res.HouseVotesUpserted++
	}
	return res
}

// loadBioguideMap loads all bioguide bridge entries from the ID map table.
// Returns map[bioguideID]politicianUUID.
func loadBioguideMap() (map[string]uuid.UUID, error) {
	type bridgeRow struct {
		IDValue      string    `gorm:"column:id_value"`
		PoliticianID uuid.UUID `gorm:"column:politician_id"`
	}
	var bridges []bridgeRow
	if err := db.DB.Table("essentials.legislative_politician_id_map").
		Select("id_value, politician_id").
		Where("id_type = ?", "bioguide").
		Find(&bridges).Error; err != nil {
		return nil, err
	}
	m := make(map[string]uuid.UUID, len(bridges))
	for _, b := range bridges {
		m[b.IDValue] = b.PoliticianID
	}
	return m, nil
}

// findLegiScanSessionID queries LegiScan's getSessionList for US Congress sessions
// and returns the session_id that matches the given congress number.
//
// Congress-to-year mapping:
//   - 119th Congress = 2025
//   - 118th Congress = 2023
//   - n-th Congress = 1789 + 2*(n-1) but simplified to year_start matching
func findLegiScanSessionID(ctx context.Context, client *LegiScanClient, congress int) (int, error) {
	body, err := client.Query(ctx, "getSessionList", map[string]string{"state": "US"})
	if err != nil {
		return 0, fmt.Errorf("getSessionList: %w", err)
	}

	var wrapper struct {
		Status   string                 `json:"status"`
		Sessions []legiScanSessionEntry `json:"sessions"`
	}
	if err := json.Unmarshal(body, &wrapper); err != nil {
		return 0, fmt.Errorf("parsing getSessionList response: %w", err)
	}

	// 119th Congress started in 2025, 118th in 2023, etc.
	// Formula: year_start = 2025 - 2*(119 - congress)
	expectedYearStart := 2025 - 2*(119-congress)

	for _, s := range wrapper.Sessions {
		if s.Special == 0 && s.YearStart == expectedYearStart {
			return s.SessionID, nil
		}
	}

	return 0, nil // not found
}

// buildLegiScanSenatorBridge fetches getSessionPeople for a LegiScan session,
// matches senators to our politicians table by name, and inserts bridge rows
// for unmatched senators. Returns (legiscanMap, newBridgeRowCount, error).
func buildLegiScanSenatorBridge(
	ctx context.Context,
	client *LegiScanClient,
	legiScanSessionID int,
	cfg ImportFederalVotesConfig,
) (map[int]uuid.UUID, int, error) {
	legiscanMap := make(map[int]uuid.UUID)

	body, err := client.Query(ctx, "getSessionPeople", map[string]string{
		"id": strconv.Itoa(legiScanSessionID),
	})
	if err != nil {
		return nil, 0, fmt.Errorf("getSessionPeople(session=%d): %w", legiScanSessionID, err)
	}

	// getSessionPeople response: {"status":"OK","sessionpeople":{"session":{...},"people":[...]}}
	var wrapper struct {
		Status      string `json:"status"`
		SessionPeople struct {
			People []LegiScanPerson `json:"people"`
		} `json:"sessionpeople"`
	}
	if err := json.Unmarshal(body, &wrapper); err != nil {
		return nil, 0, fmt.Errorf("parsing getSessionPeople response: %w", err)
	}

	log.Printf("[import-federal-votes] getSessionPeople returned %d people for session %d",
		len(wrapper.SessionPeople.People), legiScanSessionID)

	// Load existing legiscan bridge rows to avoid duplicates
	type bridgeRow struct {
		IDValue      string    `gorm:"column:id_value"`
		PoliticianID uuid.UUID `gorm:"column:politician_id"`
	}
	var existingBridges []bridgeRow
	if dbErr := db.DB.Table("essentials.legislative_politician_id_map").
		Select("id_value, politician_id").
		Where("id_type = ?", "legiscan").
		Find(&existingBridges).Error; dbErr != nil {
		return nil, 0, fmt.Errorf("loading existing legiscan bridge entries: %w", dbErr)
	}
	existingMap := make(map[string]uuid.UUID, len(existingBridges))
	for _, b := range existingBridges {
		existingMap[b.IDValue] = b.PoliticianID
		peopleIDInt, _ := strconv.Atoi(b.IDValue)
		legiscanMap[peopleIDInt] = b.PoliticianID
	}

	newBridgeCount := 0

	for _, person := range wrapper.SessionPeople.People {
		// Only process senators — House votes come from Congress.gov
		if strings.ToLower(person.Role) != "sen" && !strings.Contains(strings.ToLower(person.Role), "senator") {
			continue
		}

		peopleIDStr := strconv.Itoa(person.PeopleID)

		// If already in bridge map, populate legiscanMap and continue
		if existingPolID, exists := existingMap[peopleIDStr]; exists {
			legiscanMap[person.PeopleID] = existingPolID
			continue
		}

		// Try name-based matching: exact first + last name match (single result only)
		type polMatch struct {
			ID uuid.UUID `gorm:"column:id"`
		}
		var matches []polMatch
		if dbErr := db.DB.Raw(
			`SELECT id FROM essentials.politicians WHERE LOWER(last_name) = LOWER(?) AND LOWER(first_name) = LOWER(?)`,
			person.LastName, person.FirstName,
		).Scan(&matches).Error; dbErr != nil {
			log.Printf("[import-federal-votes] WARN: DB error matching senator %s %s: %v",
				person.FirstName, person.LastName, dbErr)
			continue
		}

		if len(matches) != 1 {
			// Skip ambiguous matches (0 = not found, 2+ = multiple politicians with same name)
			if len(matches) > 1 {
				log.Printf("[import-federal-votes] SKIP ambiguous senator match for %s %s (%d matches)",
					person.FirstName, person.LastName, len(matches))
			}
			continue
		}

		matchedPolID := matches[0].ID
		legiscanMap[person.PeopleID] = matchedPolID

		if cfg.DryRun {
			log.Printf("[import-federal-votes] DRY RUN — would insert bridge: people_id=%d -> politician_id=%s (%s %s)",
				person.PeopleID, matchedPolID, person.FirstName, person.LastName)
			newBridgeCount++
			continue
		}

		// Insert new bridge row (DoNothing to handle races)
		bridge := LegislativePoliticianIDMap{
			PoliticianID: matchedPolID,
			IDType:       "legiscan",
			IDValue:      peopleIDStr,
			VerifiedAt:   time.Now(),
			Source:       "legiscan-session-people",
		}
		if dbErr := db.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&bridge).Error; dbErr != nil {
			log.Printf("[import-federal-votes] ERROR inserting legiscan bridge row for people_id=%d: %v",
				person.PeopleID, dbErr)
			continue
		}
		newBridgeCount++
		log.Printf("[import-federal-votes] Inserted bridge: people_id=%d -> %s (%s %s)",
			person.PeopleID, matchedPolID, person.FirstName, person.LastName)
	}

	return legiscanMap, newBridgeCount, nil
}

// importSenateVotes fetches the master bill list for a LegiScan session, then for each
// bill fetches roll calls (Senate chamber only) and upserts vote records.
func importSenateVotes(
	ctx context.Context,
	client *LegiScanClient,
	legiScanSessionID int,
	sessionID uuid.UUID,
	legiscanMap map[int]uuid.UUID,
	cfg ImportFederalVotesConfig,
	res *ImportFederalVotesResult,
) error {
	// Fetch master bill list
	masterBody, err := client.Query(ctx, "getMasterList", map[string]string{
		"id": strconv.Itoa(legiScanSessionID),
	})
	if err != nil {
		return fmt.Errorf("getMasterList(session=%d): %w", legiScanSessionID, err)
	}

	// CRITICAL: getMasterList returns a MAP, not an array.
	// Key "0" is session metadata — skip it.
	var masterWrapper struct {
		Status     string                     `json:"status"`
		MasterList map[string]json.RawMessage `json:"masterlist"`
	}
	if err := json.Unmarshal(masterBody, &masterWrapper); err != nil {
		return fmt.Errorf("parsing getMasterList response: %w", err)
	}

	// Parse each bill entry (skip key "0" which is session metadata)
	var bills []masterListBill
	for key, rawVal := range masterWrapper.MasterList {
		if key == "0" {
			continue
		}
		var bill masterListBill
		if err := json.Unmarshal(rawVal, &bill); err != nil {
			log.Printf("[import-federal-votes] WARN: skipping unparseable masterlist entry key=%s: %v", key, err)
			continue
		}
		if bill.BillID > 0 {
			bills = append(bills, bill)
		}
	}

	// Sort by bill_id for deterministic processing order
	sort.Slice(bills, func(i, j int) bool {
		return bills[i].BillID < bills[j].BillID
	})

	log.Printf("[import-federal-votes] Senate master list: %d bills to process for session %d",
		len(bills), legiScanSessionID)

	billsProcessed := 0
	rollCallsImported := 0

	for idx, bill := range bills {
		// Periodic budget check every 100 bills
		if idx > 0 && idx%100 == 0 {
			remaining := client.RemainingBudget()
			if remaining < 100 {
				log.Printf("[import-federal-votes] WARNING: LegiScan budget critically low (%d remaining) — stopping Senate import early", remaining)
				break
			}
			log.Printf("[import-federal-votes] Progress: %d/%d bills processed, LegiScan budget remaining: %d", idx, len(bills), remaining)
		}

		// Fetch bill details (includes vote summary list)
		billBody, billErr := client.Query(ctx, "getBill", map[string]string{
			"id": strconv.Itoa(bill.BillID),
		})
		if billErr != nil {
			log.Printf("[import-federal-votes] ERROR getBill(%d): %v", bill.BillID, billErr)
			res.Errors = append(res.Errors, fmt.Sprintf("getBill(%d): %v", bill.BillID, billErr))
			if len(res.Errors) >= cfg.MaxErrors {
				log.Printf("[import-federal-votes] Reached max errors (%d) — aborting Senate import", cfg.MaxErrors)
				break
			}
			continue
		}

		// Parse bill response to get votes list
		var billResp struct {
			Bill struct {
				BillID int `json:"bill_id"`
				Votes  []struct {
					RollCallID int    `json:"roll_call_id"`
					Date       string `json:"date"`
					Desc       string `json:"desc"`
					Yea        int    `json:"yea"`
					Nay        int    `json:"nay"`
					Passed     int    `json:"passed"`
					Chamber    string `json:"chamber"` // "S" for Senate, "H" for House
				} `json:"votes"`
			} `json:"bill"`
		}
		if err := json.Unmarshal(billBody, &billResp); err != nil {
			log.Printf("[import-federal-votes] WARN: cannot parse getBill response for bill_id=%d: %v", bill.BillID, err)
			continue
		}

		billsProcessed++

		// Try to find matching bill in our DB for cross-linking
		var dbBillID *uuid.UUID
		if bill.Number != "" {
			var dbBill LegislativeBill
			// LegiScan bill numbers vary: "SB 123", "S 456", etc.
			// Do a LIKE search to find the best match
			if dbErr := db.DB.Where("session_id = ? AND number LIKE ?",
				sessionID, "%"+strings.TrimSpace(bill.Number)+"%",
			).First(&dbBill).Error; dbErr == nil {
				dbBillID = &dbBill.ID
			}
		}

		// Process only Senate roll calls (Chamber == "S")
		for _, voteRef := range billResp.Bill.Votes {
			if voteRef.Chamber != "S" {
				continue // Skip House roll calls — those come from Congress.gov
			}

			// Fetch full roll call with individual member votes
			rollCallBody, rcErr := client.Query(ctx, "getRollCall", map[string]string{
				"id": strconv.Itoa(voteRef.RollCallID),
			})
			if rcErr != nil {
				log.Printf("[import-federal-votes] ERROR getRollCall(%d) for bill %d: %v",
					voteRef.RollCallID, bill.BillID, rcErr)
				res.Errors = append(res.Errors, fmt.Sprintf("getRollCall(%d): %v", voteRef.RollCallID, rcErr))
				if len(res.Errors) >= cfg.MaxErrors {
					log.Printf("[import-federal-votes] Reached max errors (%d) — aborting Senate import", cfg.MaxErrors)
					return nil
				}
				continue
			}

			var rcWrapper struct {
				RollCall LegiScanRollCall `json:"roll_call"`
			}
			if err := json.Unmarshal(rollCallBody, &rcWrapper); err != nil {
				log.Printf("[import-federal-votes] WARN: cannot parse getRollCall(%d) response: %v", voteRef.RollCallID, err)
				continue
			}
			rollCall := rcWrapper.RollCall

			// Parse vote date
			voteDate, dateErr := time.Parse("2006-01-02", rollCall.Date)
			if dateErr != nil {
				log.Printf("[import-federal-votes] WARN: cannot parse vote date %q for roll call %d", rollCall.Date, rollCall.RollCallID)
				voteDate = time.Time{}
			}

			// Determine result
			voteResult := "failed"
			if rollCall.Passed == 1 {
				voteResult = "passed"
			}

			externalVoteID := fmt.Sprintf("legiscan-%d", rollCall.RollCallID)

			// Upsert individual member votes
			for _, memberVote := range rollCall.Votes {
				polID, ok := legiscanMap[memberVote.PeopleID]
				if !ok {
					res.Skipped++
					continue
				}

				vote := LegislativeVote{
					PoliticianID:   polID,
					BillID:         dbBillID,
					SessionID:      sessionID,
					ExternalVoteID: externalVoteID,
					VoteQuestion:   rollCall.Desc,
					Position:       normalizeVoteCast(memberVote.VoteText),
					VoteDate:       voteDate,
					Result:         voteResult,
					YeaCount:       rollCall.Yea,
					NayCount:       rollCall.Nay,
					Source:         "legiscan",
				}

				if cfg.DryRun {
					log.Printf("[import-federal-votes] DRY RUN — would upsert Senate vote: people_id=%d roll_call=%s position=%s",
						memberVote.PeopleID, externalVoteID, vote.Position)
					res.SenateVotesUpserted++
					continue
				}

				if dbErr := db.DB.Clauses(clause.OnConflict{
					Columns: []clause.Column{
						{Name: "politician_id"},
						{Name: "bill_id"},
						{Name: "session_id"},
						{Name: "external_vote_id"},
					},
					DoUpdates: clause.AssignmentColumns([]string{"position", "vote_question", "result"}),
				}).Create(&vote).Error; dbErr != nil {
					log.Printf("[import-federal-votes] ERROR upserting Senate vote %s for people_id=%d: %v",
						externalVoteID, memberVote.PeopleID, dbErr)
					continue
				}

				res.SenateVotesUpserted++
			}

			rollCallsImported++
		}
	}

	log.Printf("[import-federal-votes] Senate votes for session %d: %d bills processed, %d roll calls",
		legiScanSessionID, billsProcessed, rollCallsImported)

	return nil
}
