package essentials

import (
	"encoding/xml"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/google/uuid"
	"gorm.io/gorm/clause"
)

// ImportFederalVotesBulkConfig controls the bulk clerk vote import.
type ImportFederalVotesBulkConfig struct {
	DryRun       bool
	DownloadOnly bool   // download XML files but don't parse/import
	ParseOnly    bool   // parse from local cache, don't download
	DataDir      string // cache directory (default: ~/.ev-backend/bulk-data/)
	Concurrency  int    // download concurrency (default: 5)
	CongressNumbers []int // default [119, 118]
	HouseOnly    bool   // skip Senate
	SenateOnly   bool   // skip House
}

// ImportFederalVotesBulkResult contains counts from a bulk vote import run.
type ImportFederalVotesBulkResult struct {
	HouseVotesUpserted  int
	SenateVotesUpserted int
	HouseRollCalls      int
	SenateRollCalls     int
	FilesProcessed      int
	Skipped             int
	Errors              []string
}

// ImportFederalVotesBulk imports federal votes from House and Senate clerk XML files.
// No API keys required — uses free, public, no-rate-limit bulk downloads.
func ImportFederalVotesBulk(cfg ImportFederalVotesBulkConfig) (ImportFederalVotesBulkResult, error) {
	result := ImportFederalVotesBulkResult{}

	// Apply defaults
	if cfg.DataDir == "" {
		cfg.DataDir = defaultDataDir()
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 5
	}
	if len(cfg.CongressNumbers) == 0 {
		cfg.CongressNumbers = []int{119, 118}
	}

	log.Printf("[import-votes-bulk] Starting bulk import for congress numbers: %v", cfg.CongressNumbers)
	log.Printf("[import-votes-bulk] Data directory: %s", cfg.DataDir)
	if cfg.DryRun {
		log.Printf("[import-votes-bulk] DRY RUN — no database writes")
	}

	// Load bioguide map for House votes
	var bioguideMap map[string]uuid.UUID
	if !cfg.DownloadOnly && !cfg.SenateOnly {
		var err error
		bioguideMap, err = loadBioguideMap()
		if err != nil {
			return result, fmt.Errorf("loading bioguide map: %w", err)
		}
		log.Printf("[import-votes-bulk] Loaded %d bioguide bridge entries", len(bioguideMap))
	}

	// Load LIS map for Senate votes
	var lisMap map[string]uuid.UUID
	if !cfg.DownloadOnly && !cfg.HouseOnly {
		var err error
		lisMap, err = loadLISMap()
		if err != nil {
			return result, fmt.Errorf("loading LIS map: %w", err)
		}
		log.Printf("[import-votes-bulk] Loaded %d LIS bridge entries", len(lisMap))
		if len(lisMap) == 0 {
			log.Printf("[import-votes-bulk] WARNING: no LIS bridge entries found — run backfill-legislative-ids first for Senate votes")
		}
	}

	for _, congress := range cfg.CongressNumbers {
		var sessionID uuid.UUID
		if !cfg.DownloadOnly {
			var err error
			sessionID, _, err = getOrCreateSession(congress)
			if err != nil {
				return result, fmt.Errorf("getting session for congress %d: %w", congress, err)
			}
		}

		// ============================================================
		// House Votes
		// ============================================================
		if !cfg.SenateOnly {
			houseErr := importHouseVotesBulk(congress, sessionID, bioguideMap, cfg, &result)
			if houseErr != nil {
				log.Printf("[import-votes-bulk] ERROR in House votes for congress %d: %v", congress, houseErr)
				result.Errors = append(result.Errors, fmt.Sprintf("house votes congress %d: %v", congress, houseErr))
			}
		}

		// ============================================================
		// Senate Votes
		// ============================================================
		if !cfg.HouseOnly {
			senateErr := importSenateVotesBulk(congress, sessionID, lisMap, cfg, &result)
			if senateErr != nil {
				log.Printf("[import-votes-bulk] ERROR in Senate votes for congress %d: %v", congress, senateErr)
				result.Errors = append(result.Errors, fmt.Sprintf("senate votes congress %d: %v", congress, senateErr))
			}
		}
	}

	log.Printf("[import-votes-bulk] Complete: %d files, %d House votes (%d roll calls), %d Senate votes (%d roll calls), %d skipped, %d errors",
		result.FilesProcessed, result.HouseVotesUpserted, result.HouseRollCalls,
		result.SenateVotesUpserted, result.SenateRollCalls, result.Skipped, len(result.Errors))

	return result, nil
}

// importHouseVotesBulk handles downloading and processing House clerk XML files.
func importHouseVotesBulk(
	congress int,
	sessionID uuid.UUID,
	bioguideMap map[string]uuid.UUID,
	cfg ImportFederalVotesBulkConfig,
	result *ImportFederalVotesBulkResult,
) error {
	year1, year2 := congressYears(congress)

	for _, year := range []int{year1, year2} {
		session := 1
		if year == year2 {
			session = 2
		}

		var xmlPaths []string

		if !cfg.ParseOnly {
			// Download House vote files
			destDir := filepath.Join(cfg.DataDir, "votes", "house", fmt.Sprintf("%d", year))

			log.Printf("[import-votes-bulk] Discovering House votes for %d...", year)
			urls, err := discoverHouseVotes(year)
			if err != nil {
				log.Printf("[import-votes-bulk] WARN: House vote discovery failed for %d: %v", year, err)
				continue
			}
			if len(urls) == 0 {
				log.Printf("[import-votes-bulk] No House votes found for %d", year)
				continue
			}
			log.Printf("[import-votes-bulk] Found %d House roll calls for %d", len(urls), year)

			results := downloadConcurrent(urls, destDir, cfg.Concurrency)
			for _, r := range results {
				if r.Err != nil {
					log.Printf("[import-votes-bulk] ERROR downloading %s: %v", r.URL, r.Err)
					continue
				}
				xmlPaths = append(xmlPaths, r.LocalPath)
			}
		} else {
			// ParseOnly: read from local cache
			dir := filepath.Join(cfg.DataDir, "votes", "house", fmt.Sprintf("%d", year))
			entries, err := os.ReadDir(dir)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return fmt.Errorf("reading house cache dir %s: %w", dir, err)
			}
			for _, e := range entries {
				if !e.IsDir() && strings.HasSuffix(e.Name(), ".xml") {
					xmlPaths = append(xmlPaths, filepath.Join(dir, e.Name()))
				}
			}
		}

		if cfg.DownloadOnly {
			log.Printf("[import-votes-bulk] Download-only: %d House files cached for %d", len(xmlPaths), year)
			continue
		}

		if len(xmlPaths) == 0 {
			continue
		}

		log.Printf("[import-votes-bulk] Processing %d House vote files for %d", len(xmlPaths), year)

		for _, xmlPath := range xmlPaths {
			err := processHouseVoteXML(xmlPath, congress, session, sessionID, bioguideMap, cfg.DryRun, result)
			if err != nil {
				log.Printf("[import-votes-bulk] ERROR processing %s: %v", filepath.Base(xmlPath), err)
				result.Errors = append(result.Errors, fmt.Sprintf("parse house %s: %v", filepath.Base(xmlPath), err))
				continue
			}
			result.FilesProcessed++
		}
	}

	log.Printf("[import-votes-bulk] House votes complete: %d roll calls, %d votes upserted",
		result.HouseRollCalls, result.HouseVotesUpserted)
	return nil
}

// importSenateVotesBulk handles downloading and processing Senate clerk XML files.
func importSenateVotesBulk(
	congress int,
	sessionID uuid.UUID,
	lisMap map[string]uuid.UUID,
	cfg ImportFederalVotesBulkConfig,
	result *ImportFederalVotesBulkResult,
) error {
	for session := 1; session <= 2; session++ {
		var xmlPaths []string

		if !cfg.ParseOnly {
			destDir := filepath.Join(cfg.DataDir, "votes", "senate", fmt.Sprintf("%d-%d", congress, session))

			log.Printf("[import-votes-bulk] Discovering Senate votes for %dth Congress session %d...", congress, session)
			urls, err := discoverSenateVotes(congress, session)
			if err != nil {
				log.Printf("[import-votes-bulk] WARN: Senate vote discovery failed: %v", err)
				continue
			}
			if len(urls) == 0 {
				log.Printf("[import-votes-bulk] No Senate votes for %dth Congress session %d", congress, session)
				continue
			}
			log.Printf("[import-votes-bulk] Found %d Senate roll calls for %dth Congress session %d", len(urls), congress, session)

			results := downloadConcurrent(urls, destDir, cfg.Concurrency)
			for _, r := range results {
				if r.Err != nil {
					log.Printf("[import-votes-bulk] ERROR downloading %s: %v", r.URL, r.Err)
					continue
				}
				xmlPaths = append(xmlPaths, r.LocalPath)
			}
		} else {
			dir := filepath.Join(cfg.DataDir, "votes", "senate", fmt.Sprintf("%d-%d", congress, session))
			entries, err := os.ReadDir(dir)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return fmt.Errorf("reading senate cache dir %s: %w", dir, err)
			}
			for _, e := range entries {
				if !e.IsDir() && strings.HasSuffix(e.Name(), ".xml") {
					xmlPaths = append(xmlPaths, filepath.Join(dir, e.Name()))
				}
			}
		}

		if cfg.DownloadOnly {
			log.Printf("[import-votes-bulk] Download-only: %d Senate files cached for congress %d session %d",
				len(xmlPaths), congress, session)
			continue
		}

		if len(xmlPaths) == 0 {
			continue
		}

		log.Printf("[import-votes-bulk] Processing %d Senate vote files for %dth Congress session %d",
			len(xmlPaths), congress, session)

		for _, xmlPath := range xmlPaths {
			err := processSenateVoteXML(xmlPath, congress, session, sessionID, lisMap, cfg.DryRun, result)
			if err != nil {
				log.Printf("[import-votes-bulk] ERROR processing %s: %v", filepath.Base(xmlPath), err)
				result.Errors = append(result.Errors, fmt.Sprintf("parse senate %s: %v", filepath.Base(xmlPath), err))
				continue
			}
			result.FilesProcessed++
		}
	}

	log.Printf("[import-votes-bulk] Senate votes complete: %d roll calls, %d votes upserted",
		result.SenateRollCalls, result.SenateVotesUpserted)
	return nil
}

// processHouseVoteXML parses one House clerk XML file and upserts member votes.
func processHouseVoteXML(
	xmlPath string,
	congress, session int,
	sessionID uuid.UUID,
	bioguideMap map[string]uuid.UUID,
	dryRun bool,
	result *ImportFederalVotesBulkResult,
) error {
	data, err := os.ReadFile(xmlPath)
	if err != nil {
		return fmt.Errorf("reading file: %w", err)
	}

	var vote HouseRollCallVoteXML
	if err := xml.Unmarshal(data, &vote); err != nil {
		return fmt.Errorf("parsing XML: %w", err)
	}

	meta := vote.Metadata

	// Parse date: "D-Mon-YYYY" format (e.g., "3-Jan-2025", "25-Feb-2025")
	voteDate, dateErr := time.Parse("2-Jan-2006", meta.ActionDate)
	if dateErr != nil {
		log.Printf("[import-votes-bulk] WARN: cannot parse House vote date %q — using zero time", meta.ActionDate)
		voteDate = time.Time{}
	}

	// Parse session number from session string (e.g., "1st" -> 1)
	sessionNum := session
	if strings.HasPrefix(meta.Session, "2") {
		sessionNum = 2
	}

	externalVoteID := fmt.Sprintf("house-%d-%d-%d", congress, sessionNum, meta.RollCallNum)

	// Try to find matching bill in DB
	var billID *uuid.UUID
	if meta.LegisNum != "" && meta.LegisNum != "QUORUM" {
		billID = matchHouseLegisNum(meta.LegisNum, congress, sessionID)
	}

	voteResult := normalizeVoteResult(meta.VoteResult)

	result.HouseRollCalls++

	for _, rv := range vote.VoteData {
		bioguide := rv.Legislator.NameID
		if bioguide == "" {
			result.Skipped++
			continue
		}

		polID, ok := bioguideMap[bioguide]
		if !ok {
			result.Skipped++
			continue
		}

		voteRec := LegislativeVote{
			PoliticianID:   polID,
			BillID:         billID,
			SessionID:      sessionID,
			ExternalVoteID: externalVoteID,
			VoteQuestion:   meta.VoteQuestion,
			Position:       normalizeVoteCast(rv.Vote),
			VoteDate:       voteDate,
			Result:         voteResult,
			YeaCount:       meta.VoteTotals.TotalsByVote.YeaTotal,
			NayCount:       meta.VoteTotals.TotalsByVote.NayTotal,
			Source:         "house-clerk",
		}

		if dryRun {
			result.HouseVotesUpserted++
			continue
		}

		if dbErr := db.DB.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "politician_id"},
				{Name: "bill_id"},
				{Name: "session_id"},
				{Name: "external_vote_id"},
			},
			DoUpdates: clause.AssignmentColumns([]string{"position", "vote_question", "result", "source"}),
		}).Create(&voteRec).Error; dbErr != nil {
			log.Printf("[import-votes-bulk] ERROR upserting House vote %s bioguide=%s: %v",
				externalVoteID, bioguide, dbErr)
			continue
		}

		result.HouseVotesUpserted++
	}

	return nil
}

// processSenateVoteXML parses one Senate clerk XML file and upserts member votes.
func processSenateVoteXML(
	xmlPath string,
	congress, session int,
	sessionID uuid.UUID,
	lisMap map[string]uuid.UUID,
	dryRun bool,
	result *ImportFederalVotesBulkResult,
) error {
	data, err := os.ReadFile(xmlPath)
	if err != nil {
		return fmt.Errorf("reading file: %w", err)
	}

	var vote SenateRollCallVoteXML
	if err := xml.Unmarshal(data, &vote); err != nil {
		return fmt.Errorf("parsing XML: %w", err)
	}

	// Parse date: "Month D, YYYY,  HH:MM AM/PM" (note double space)
	voteDate := parseSenateVoteDate(vote.VoteDate)

	externalVoteID := fmt.Sprintf("senate-%d-%d-%d", congress, session, vote.VoteNumber)

	// Try to find matching bill in DB
	var billID *uuid.UUID
	if vote.Document.DocumentType != "" && vote.Document.DocumentNumber != "" {
		billID = matchSenateDocument(vote.Document, congress, sessionID)
	}

	voteResult := normalizeVoteResult(vote.VoteResult)

	result.SenateRollCalls++

	for _, mv := range vote.Members {
		if mv.LISMemberID == "" {
			result.Skipped++
			continue
		}

		polID, ok := lisMap[mv.LISMemberID]
		if !ok {
			result.Skipped++
			continue
		}

		voteRec := LegislativeVote{
			PoliticianID:   polID,
			BillID:         billID,
			SessionID:      sessionID,
			ExternalVoteID: externalVoteID,
			VoteQuestion:   vote.VoteQuestionText,
			Position:       normalizeVoteCast(mv.VoteCast),
			VoteDate:       voteDate,
			Result:         voteResult,
			YeaCount:       vote.Count.Yeas,
			NayCount:       vote.Count.Nays,
			Source:         "senate-clerk",
		}

		if dryRun {
			result.SenateVotesUpserted++
			continue
		}

		if dbErr := db.DB.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "politician_id"},
				{Name: "bill_id"},
				{Name: "session_id"},
				{Name: "external_vote_id"},
			},
			DoUpdates: clause.AssignmentColumns([]string{"position", "vote_question", "result", "source"}),
		}).Create(&voteRec).Error; dbErr != nil {
			log.Printf("[import-votes-bulk] ERROR upserting Senate vote %s lis=%s: %v",
				externalVoteID, mv.LISMemberID, dbErr)
			continue
		}

		result.SenateVotesUpserted++
	}

	return nil
}

// matchHouseLegisNum tries to match a House legis-num (e.g., "H R 153", "H CON RES 14")
// to a bill in the database. Returns bill UUID or nil.
func matchHouseLegisNum(legisNum string, congress int, sessionID uuid.UUID) *uuid.UUID {
	// Normalize "H R 153" → "HR-153", "H CON RES 14" → "HCONRES-14"
	// The format is space-separated tokens, last token is the number
	parts := strings.Fields(legisNum)
	if len(parts) < 2 {
		return nil
	}

	number := parts[len(parts)-1]
	typeParts := parts[:len(parts)-1]
	billType := strings.Join(typeParts, "")

	// Map House clerk abbreviations to GPO bill type codes
	billType = strings.ToUpper(billType)

	externalID := fmt.Sprintf("%d-%s-%s", congress, billType, number)

	var bill LegislativeBill
	if err := db.DB.Where("external_id = ? AND jurisdiction = ?", externalID, "federal").
		Select("id").First(&bill).Error; err != nil {
		return nil
	}
	return &bill.ID
}

// matchSenateDocument tries to match a Senate document to a bill in the database.
func matchSenateDocument(doc SenateDocumentXML, congress int, sessionID uuid.UUID) *uuid.UUID {
	if doc.DocumentNumber == "" {
		return nil
	}

	// Map Senate document types to GPO bill type codes:
	// "S." -> "S", "H.R." -> "HR", "S.J.Res." -> "SJRES", etc.
	typeMap := map[string]string{
		"S.":         "S",
		"H.R.":       "HR",
		"S.J.Res.":   "SJRES",
		"H.J.Res.":   "HJRES",
		"S.Con.Res.":  "SCONRES",
		"H.Con.Res.":  "HCONRES",
		"S.Res.":     "SRES",
		"H.Res.":     "HRES",
	}

	billType, ok := typeMap[doc.DocumentType]
	if !ok {
		return nil // nominations (PN), amendments, etc. — skip
	}

	externalID := fmt.Sprintf("%d-%s-%s", congress, billType, doc.DocumentNumber)

	var bill LegislativeBill
	if err := db.DB.Where("external_id = ? AND jurisdiction = ?", externalID, "federal").
		Select("id").First(&bill).Error; err != nil {
		return nil
	}
	return &bill.ID
}

// parseSenateVoteDate parses Senate date format: "Month D, YYYY,  HH:MM AM/PM"
// Falls back to zero time on parse failure.
func parseSenateVoteDate(dateStr string) time.Time {
	// Remove double spaces and clean up
	dateStr = strings.TrimSpace(dateStr)

	// Try several date layouts the Senate uses
	layouts := []string{
		"January 2, 2006,  03:04 PM",
		"January 2, 2006, 03:04 PM",
		"January 2, 2006",
	}

	for _, layout := range layouts {
		if t, err := time.Parse(layout, dateStr); err == nil {
			return t
		}
	}

	// Last resort: extract just the date part with regex
	re := regexp.MustCompile(`(\w+ \d+, \d{4})`)
	if m := re.FindString(dateStr); m != "" {
		if t, err := time.Parse("January 2, 2006", m); err == nil {
			return t
		}
	}

	log.Printf("[import-votes-bulk] WARN: cannot parse Senate vote date %q", dateStr)
	return time.Time{}
}

// congressSession returns the session number (1 or 2) for a given year within a congress.
func congressSession(congress, year int) int {
	startYear, _ := congressYears(congress)
	if year > startYear {
		return 2
	}
	return 1
}

// parseRollNumFromFilename extracts the roll call number from a House clerk filename.
// e.g., "roll001.xml" -> 1, "roll123.xml" -> 123
func parseRollNumFromFilename(filename string) (int, error) {
	re := regexp.MustCompile(`roll(\d+)\.xml`)
	m := re.FindStringSubmatch(filename)
	if len(m) < 2 {
		return 0, fmt.Errorf("cannot parse roll number from %q", filename)
	}
	return strconv.Atoi(m[1])
}
