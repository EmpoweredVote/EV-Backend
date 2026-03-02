package essentials

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/goccy/go-yaml"
	"github.com/google/uuid"
	"gorm.io/gorm/clause"
)

// legislatorWithLeadershipYAML extends the legislator YAML struct to include
// leadership_roles. This is a separate struct from legislatorYAML in backfill_ids.go
// to avoid conflicts while still covering the same fields.
type legislatorWithLeadershipYAML struct {
	ID struct {
		Bioguide string `yaml:"bioguide"`
	} `yaml:"id"`
	Name struct {
		Official string `yaml:"official_full"`
	} `yaml:"name"`
	LeadershipRoles []leadershipRoleYAML `yaml:"leadership_roles"`
}

// leadershipRoleYAML maps to the leadership_roles array on each legislator entry.
// This field is a TOP-LEVEL array (not nested in terms) and only ~8 current leaders have it.
type leadershipRoleYAML struct {
	Title   string `yaml:"title"`   // "Speaker of the House", "Senate Majority Leader"
	Chamber string `yaml:"chamber"` // "house" or "senate"
	Start   string `yaml:"start"`   // "YYYY-MM-DD"
	End     string `yaml:"end"`     // "YYYY-MM-DD" or empty if currently serving
}

// ImportLeadershipConfig controls the import behavior.
type ImportLeadershipConfig struct {
	DryRun bool
}

// ImportLeadershipResult contains counts from an import run.
type ImportLeadershipResult struct {
	RolesUpserted int
	Skipped       int
	Errors        []string
}

// isCurrentLeadershipRole returns true if the role has no end date or the end date
// is in the future. Historical roles (e.g., Schumer as Minority Whip 2007-2009) are
// excluded to prevent inflating the result set beyond the expected ~8 current leaders.
func isCurrentLeadershipRole(role leadershipRoleYAML) bool {
	if role.End == "" {
		return true // no end date = currently serving
	}
	end, err := time.Parse("2006-01-02", role.End)
	if err != nil {
		return false // unparseable date — skip
	}
	return end.After(time.Now())
}

// ensureFederalSession looks up the current 119th Congress session or creates it.
// This is the same logic as getOrCreateSession in import_committees.go if that plan
// has already run — both functions target the same row, so it is safe to call either.
func ensureFederalSession(congressNumber int) (uuid.UUID, error) {
	congressName := fmt.Sprintf("%dth Congress", congressNumber)
	if congressNumber == 119 {
		congressName = "119th Congress"
	}
	externalID := fmt.Sprintf("%d", congressNumber)

	var session LegislativeSession
	result := db.DB.Where("jurisdiction = ? AND external_id = ?", "federal", externalID).First(&session)
	if result.Error == nil {
		log.Printf("[import-leadership] Found existing session: %s (ID: %s)", session.Name, session.ID)
		return session.ID, nil
	}

	// Not found — create it
	now := time.Now()
	session = LegislativeSession{
		Jurisdiction: "federal",
		Name:         congressName,
		ExternalID:   externalID,
		IsCurrent:    true,
		Source:       "congress-legislators",
		StartDate:    func() *time.Time { t := time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC); return &t }(),
		EndDate:      func() *time.Time { t := time.Date(2027, 1, 3, 0, 0, 0, 0, time.UTC); return &t }(),
	}
	_ = now // unused after reassignment above
	if err := db.DB.Create(&session).Error; err != nil {
		return uuid.Nil, fmt.Errorf("creating federal session: %w", err)
	}
	log.Printf("[import-leadership] Created new session: %s (ID: %s)", session.Name, session.ID)
	return session.ID, nil
}

// ImportLeadership downloads legislators-current.yaml, extracts current leadership_roles,
// builds a bioguide-to-politician UUID cache, and upserts rows into
// essentials.legislative_leadership_roles.
//
// Expected result: ~5-10 roles imported (Speaker, Majority/Minority Leaders,
// Whips per chamber, President Pro Tempore).
func ImportLeadership(cfg ImportLeadershipConfig) (ImportLeadershipResult, error) {
	result := ImportLeadershipResult{}

	// ── Step 1: Get or create the 119th Congress session ─────────────────────
	sessionID, err := ensureFederalSession(119)
	if err != nil {
		return result, fmt.Errorf("ensuring federal session: %w", err)
	}

	// ── Step 2: Download legislators-current.yaml ─────────────────────────────
	// Reuse the legislatorsCurrentURL constant defined in backfill_ids.go.
	log.Printf("[import-leadership] Downloading %s", legislatorsCurrentURL)
	resp, err := http.Get(legislatorsCurrentURL)
	if err != nil {
		return result, fmt.Errorf("downloading congress-legislators YAML: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return result, fmt.Errorf("unexpected HTTP status %d fetching congress-legislators YAML", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return result, fmt.Errorf("reading congress-legislators YAML body: %w", err)
	}
	log.Printf("[import-leadership] Downloaded %d bytes", len(body))

	// ── Step 3: Parse YAML as []legislatorWithLeadershipYAML ─────────────────
	var legislators []legislatorWithLeadershipYAML
	if err := yaml.Unmarshal(body, &legislators); err != nil {
		return result, fmt.Errorf("parsing congress-legislators YAML: %w", err)
	}
	log.Printf("[import-leadership] Parsed %d legislators from YAML", len(legislators))

	// ── Step 4: Build bioguide-to-politician UUID cache ───────────────────────
	// Single query loading all id_type = "bioguide" bridge rows.
	var bridgeRows []LegislativePoliticianIDMap
	if dbErr := db.DB.Where("id_type = ?", "bioguide").Find(&bridgeRows).Error; dbErr != nil {
		return result, fmt.Errorf("loading bioguide bridge rows: %w", dbErr)
	}
	bioguideToUUID := make(map[string]uuid.UUID, len(bridgeRows))
	for _, row := range bridgeRows {
		bioguideToUUID[row.IDValue] = row.PoliticianID
	}
	log.Printf("[import-leadership] Loaded %d bioguide bridge entries", len(bioguideToUUID))

	// ── Step 5: Iterate and upsert current leadership roles ──────────────────
	for _, leg := range legislators {
		if len(leg.LeadershipRoles) == 0 {
			continue
		}

		for _, role := range leg.LeadershipRoles {
			// Filter to current roles only — CRITICAL to exclude historical entries
			if !isCurrentLeadershipRole(role) {
				result.Skipped++
				continue
			}

			// Look up politician UUID from bioguide cache
			politicianID, found := bioguideToUUID[leg.ID.Bioguide]
			if !found {
				log.Printf("[import-leadership] No bridge row for bioguide=%s (%s) — skipping %s",
					leg.ID.Bioguide, leg.Name.Official, role.Title)
				result.Skipped++
				continue
			}

			if cfg.DryRun {
				log.Printf("[import-leadership] DRY RUN — would upsert: %s → %s (chamber=%s)",
					leg.Name.Official, role.Title, role.Chamber)
				result.RolesUpserted++
				continue
			}

			// Parse start date
			var startDate *time.Time
			if role.Start != "" {
				t, parseErr := time.Parse("2006-01-02", role.Start)
				if parseErr == nil {
					startDate = &t
				}
			}

			// Build the leadership role record
			leadershipRole := LegislativeLeadershipRole{
				PoliticianID: politicianID,
				SessionID:    &sessionID,
				Chamber:      role.Chamber,
				Title:        role.Title,
				StartDate:    startDate,
				EndDate:      nil, // current roles have no end date
				IsCurrent:    true,
				Source:       "congress-legislators",
			}

			// Upsert on (politician_id, session_id, chamber)
			upsertErr := db.DB.Clauses(clause.OnConflict{
				Columns: []clause.Column{
					{Name: "politician_id"},
					{Name: "session_id"},
					{Name: "chamber"},
				},
				DoUpdates: clause.AssignmentColumns([]string{
					"title", "is_current", "start_date", "end_date", "source",
				}),
			}).Create(&leadershipRole).Error
			if upsertErr != nil {
				log.Printf("[import-leadership] ERROR upserting role for %s (%s): %v",
					leg.Name.Official, role.Title, upsertErr)
				result.Errors = append(result.Errors,
					fmt.Sprintf("upsert failed for %s (%s): %v", leg.Name.Official, role.Title, upsertErr))
				continue
			}

			log.Printf("[import-leadership] Upserted: %s → %s (chamber=%s)",
				leg.Name.Official, role.Title, role.Chamber)
			result.RolesUpserted++
		}
	}

	// ── Step 6: Log summary ───────────────────────────────────────────────────
	log.Printf("[import-leadership] Complete: %d upserted, %d skipped, %d errors",
		result.RolesUpserted, result.Skipped, len(result.Errors))

	return result, nil
}
