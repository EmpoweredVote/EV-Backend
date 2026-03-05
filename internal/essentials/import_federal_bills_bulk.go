package essentials

import (
	"encoding/xml"
	"fmt"
	"html"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/google/uuid"
	"gorm.io/gorm/clause"
)

// ImportFederalBillsBulkConfig controls the bulk GPO bill import.
type ImportFederalBillsBulkConfig struct {
	DryRun       bool
	DownloadOnly bool   // download XML files but don't parse/import
	ParseOnly    bool   // parse from local cache, don't download
	DataDir      string // cache directory (default: ~/.ev-backend/bulk-data/)
	Concurrency  int    // download concurrency (default: 5)
	CongressNumbers []int // default [119, 118]
	BillTypes    []string // default: all 8 types
}

// ImportFederalBillsBulkResult contains counts from a bulk import run.
type ImportFederalBillsBulkResult struct {
	BillsUpserted      int
	CosponsorsUpserted int
	SummariesStored    int
	FilesProcessed     int
	Skipped            int
	Errors             []string
}

// ImportFederalBillsBulk imports federal bills from GPO bulk data XML files.
// No API keys required — uses free, public, no-rate-limit bulk downloads.
func ImportFederalBillsBulk(cfg ImportFederalBillsBulkConfig) (ImportFederalBillsBulkResult, error) {
	result := ImportFederalBillsBulkResult{}

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
	if len(cfg.BillTypes) == 0 {
		cfg.BillTypes = billTypes
	}

	log.Printf("[import-bills-bulk] Starting bulk import for congress numbers: %v", cfg.CongressNumbers)
	log.Printf("[import-bills-bulk] Data directory: %s", cfg.DataDir)
	if cfg.DryRun {
		log.Printf("[import-bills-bulk] DRY RUN — no database writes")
	}

	// Load bioguide bridge map for sponsor/cosponsor resolution
	var bioguideMap map[string]uuid.UUID
	if !cfg.DownloadOnly {
		var err error
		bioguideMap, err = loadBioguideMap()
		if err != nil {
			return result, fmt.Errorf("loading bioguide map: %w", err)
		}
		log.Printf("[import-bills-bulk] Loaded %d bioguide bridge entries", len(bioguideMap))
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

		for _, billType := range cfg.BillTypes {
			// Phase 1: Download XML files
			var xmlPaths []string
			if !cfg.ParseOnly {
				paths, err := downloadBillStatusFiles(congress, billType, cfg.DataDir, cfg.Concurrency)
				if err != nil {
					log.Printf("[import-bills-bulk] ERROR downloading %s bills for congress %d: %v", billType, congress, err)
					result.Errors = append(result.Errors, fmt.Sprintf("download %d/%s: %v", congress, billType, err))
					continue
				}
				xmlPaths = paths
			} else {
				// ParseOnly: read from local cache
				dir := filepath.Join(cfg.DataDir, "bills", fmt.Sprintf("%d", congress), billType)
				entries, err := os.ReadDir(dir)
				if err != nil {
					if os.IsNotExist(err) {
						log.Printf("[import-bills-bulk] No cached files for %dth Congress %s — run without --parse-only first", congress, billType)
						continue
					}
					result.Errors = append(result.Errors, fmt.Sprintf("reading cache dir %s: %v", dir, err))
					continue
				}
				for _, e := range entries {
					if !e.IsDir() && strings.HasSuffix(e.Name(), ".xml") {
						xmlPaths = append(xmlPaths, filepath.Join(dir, e.Name()))
					}
				}
			}

			if cfg.DownloadOnly {
				log.Printf("[import-bills-bulk] Download-only mode — %d files cached for %dth Congress %s",
					len(xmlPaths), congress, strings.ToUpper(billType))
				continue
			}

			if len(xmlPaths) == 0 {
				continue
			}

			log.Printf("[import-bills-bulk] Processing %d %s bill files for %dth Congress",
				len(xmlPaths), strings.ToUpper(billType), congress)

			// Phase 2: Parse and upsert
			for _, xmlPath := range xmlPaths {
				err := processBillStatusXML(xmlPath, congress, sessionID, bioguideMap, cfg.DryRun, &result)
				if err != nil {
					log.Printf("[import-bills-bulk] ERROR processing %s: %v", filepath.Base(xmlPath), err)
					result.Errors = append(result.Errors, fmt.Sprintf("parse %s: %v", filepath.Base(xmlPath), err))
					continue
				}
				result.FilesProcessed++

				if result.FilesProcessed%500 == 0 {
					log.Printf("[import-bills-bulk] Progress: %d files processed, %d bills upserted",
						result.FilesProcessed, result.BillsUpserted)
				}
			}
		}
	}

	log.Printf("[import-bills-bulk] Complete: %d files, %d bills, %d cosponsors, %d summaries, %d errors",
		result.FilesProcessed, result.BillsUpserted, result.CosponsorsUpserted, result.SummariesStored, len(result.Errors))

	return result, nil
}

// processBillStatusXML parses one GPO BillStatus XML file and upserts the bill,
// its sponsor, all cosponsors, and the CRS summary.
func processBillStatusXML(
	xmlPath string,
	congress int,
	sessionID uuid.UUID,
	bioguideMap map[string]uuid.UUID,
	dryRun bool,
	result *ImportFederalBillsBulkResult,
) error {
	data, err := os.ReadFile(xmlPath)
	if err != nil {
		return fmt.Errorf("reading file: %w", err)
	}

	var billStatus BillStatusXML
	if err := xml.Unmarshal(data, &billStatus); err != nil {
		return fmt.Errorf("parsing XML: %w", err)
	}

	bill := billStatus.Bill
	externalID := fmt.Sprintf("%d-%s-%s", congress, bill.Type, bill.Number)
	number := fmt.Sprintf("%s %s", bill.Type, bill.Number)

	// Derive title: prefer Display Title, fall back to bill.Title
	title := bill.Title
	for _, t := range bill.Titles {
		if t.TitleType == "Display Title" {
			title = t.Title
			break
		}
	}

	// Derive status from latest action text
	statusLabel := normalizeBillStatus(bill.LatestAction.Text)

	// Parse introduced date
	var introducedAt *time.Time
	if bill.IntroducedDate != "" {
		t, err := time.Parse("2006-01-02", bill.IntroducedDate)
		if err == nil {
			introducedAt = &t
		}
	}

	// Resolve sponsor bioguide ID to politician UUID
	var sponsorID *uuid.UUID
	if len(bill.Sponsors) > 0 {
		if polID, ok := bioguideMap[bill.Sponsors[0].BioguideID]; ok {
			sponsorID = &polID
		}
	}

	// Extract CRS summary (use most recent if multiple)
	summary := extractBestSummary(bill.Summaries)

	// Build URL
	url := bill.LegislationURL

	// Extract subject tags
	var topicTags []string
	if bill.PolicyArea.Name != "" {
		topicTags = append(topicTags, bill.PolicyArea.Name)
	}
	for _, s := range bill.Subjects.Subjects {
		if s.Name != "" {
			topicTags = append(topicTags, s.Name)
		}
	}

	if dryRun {
		log.Printf("[import-bills-bulk] DRY RUN — would upsert bill: %s (%s) sponsor_bioguide=%s summary=%d chars",
			externalID, truncate(title, 60), sponsorBioguide(bill.Sponsors), len(summary))
		result.BillsUpserted++
		result.CosponsorsUpserted += len(bill.Cosponsors)
		if summary != "" {
			result.SummariesStored++
		}
		return nil
	}

	// Upsert the bill
	rec := LegislativeBill{
		SessionID:    sessionID,
		ExternalID:   externalID,
		Jurisdiction: "federal",
		Number:       number,
		Title:        title,
		Summary:      summary,
		RawStatus:    bill.LatestAction.Text,
		StatusLabel:  statusLabel,
		SponsorID:    sponsorID,
		IntroducedAt: introducedAt,
		URL:          url,
		Source:       "gpo-bulk",
	}

	if err := db.DB.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "external_id"},
			{Name: "jurisdiction"},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			"title", "raw_status", "status_label", "summary", "sponsor_id", "url", "source",
		}),
	}).Create(&rec).Error; err != nil {
		return fmt.Errorf("upserting bill %s: %w", externalID, err)
	}

	// Query back the actual DB ID
	var existing LegislativeBill
	if err := db.DB.
		Where("external_id = ? AND jurisdiction = ?", externalID, "federal").
		Select("id").
		First(&existing).Error; err != nil {
		return fmt.Errorf("querying bill ID after upsert: %w", err)
	}
	billDBID := existing.ID
	result.BillsUpserted++
	if summary != "" {
		result.SummariesStored++
	}

	// Upsert cosponsors
	for _, cs := range bill.Cosponsors {
		polID, ok := bioguideMap[cs.BioguideID]
		if !ok {
			result.Skipped++
			continue
		}

		cosponsor := LegislativeBillCosponsor{
			BillID:       billDBID,
			PoliticianID: polID,
		}
		if dbErr := db.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&cosponsor).Error; dbErr != nil {
			log.Printf("[import-bills-bulk] ERROR upserting cosponsor for bill=%s bioguide=%s: %v",
				externalID, cs.BioguideID, dbErr)
			continue
		}
		result.CosponsorsUpserted++
	}

	return nil
}

// extractBestSummary picks the most recent CRS summary from the XML summaries.
// Returns plain text with HTML tags stripped.
func extractBestSummary(summaries []BillSummaryXML) string {
	if len(summaries) == 0 {
		return ""
	}

	// Pick the summary with the latest actionDate
	best := summaries[0]
	for _, s := range summaries[1:] {
		if s.ActionDate > best.ActionDate {
			best = s
		}
	}

	text := best.CData.Text
	if text == "" {
		return ""
	}

	// Decode HTML entities (the text is HTML-encoded in the XML)
	text = html.UnescapeString(text)

	// Strip HTML tags
	text = stripHTMLTags(text)

	return strings.TrimSpace(text)
}

// sponsorBioguide returns the bioguide ID of the first sponsor, or "none".
func sponsorBioguide(sponsors []BillSponsorXML) string {
	if len(sponsors) > 0 {
		return sponsors[0].BioguideID
	}
	return "none"
}

// truncate shortens a string to maxLen, appending "..." if truncated.
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
