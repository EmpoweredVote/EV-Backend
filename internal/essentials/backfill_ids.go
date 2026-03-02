package essentials

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/goccy/go-yaml"
	"github.com/google/uuid"
)

const legislatorsCurrentURL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/legislators-current.yaml"

// legislatorYAML holds only the fields needed for matching against our politician records.
type legislatorYAML struct {
	ID struct {
		Bioguide string `yaml:"bioguide"`
	} `yaml:"id"`
	Name struct {
		First    string `yaml:"first"`
		Last     string `yaml:"last"`
		Official string `yaml:"official_full"`
	} `yaml:"name"`
	Terms []struct {
		Type  string `yaml:"type"`  // "rep" or "sen"
		State string `yaml:"state"` // two-letter abbreviation e.g. "IN"
	} `yaml:"terms"`
}

// BackfillConfig controls backfill behavior.
type BackfillConfig struct {
	DryRun bool
}

// BackfillResult contains counts from a backfill run.
type BackfillResult struct {
	Matched  int
	Inserted int
	Skipped  int
	Errors   []string
}

// federalPol is a lightweight projection of politicians with federal districts.
type federalPol struct {
	ID                uuid.UUID `gorm:"column:id"`
	FirstName         string    `gorm:"column:first_name"`
	LastName          string    `gorm:"column:last_name"`
	FullName          string    `gorm:"column:full_name"`
	BioguideID        string    `gorm:"column:bioguide_id"`
	RepresentingState string    `gorm:"column:representing_state"`
}

// BackfillLegislativeIDs downloads the congress-legislators YAML, matches each
// NATIONAL_UPPER / NATIONAL_LOWER politician by bioguide_id (Tier 1) or
// last_name+state (Tier 2), and populates the legislative_politician_id_map
// bridge table.
//
// NATIONAL_EXEC politicians (President, VP, Cabinet) are intentionally excluded
// because they have no bioguide_id and are not in the congress-legislators file.
func BackfillLegislativeIDs(cfg BackfillConfig) (BackfillResult, error) {
	result := BackfillResult{}

	// ── Step 1: Download legislators-current.yaml ──────────────────────────
	log.Printf("[backfill] Downloading %s", legislatorsCurrentURL)
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
	log.Printf("[backfill] Downloaded %d bytes", len(body))

	// ── Step 2: Parse YAML ─────────────────────────────────────────────────
	var legislators []legislatorYAML
	if err := yaml.Unmarshal(body, &legislators); err != nil {
		return result, fmt.Errorf("parsing congress-legislators YAML: %w", err)
	}
	log.Printf("[backfill] Parsed %d legislators from YAML", len(legislators))

	// ── Step 3: Build lookup maps ──────────────────────────────────────────
	// Tier 1: exact bioguide_id match
	bioguideMap := make(map[string]legislatorYAML, len(legislators))
	// Tier 2: last_name (lowercase) + "|" + state → slice (may have duplicates)
	nameStateMap := make(map[string][]legislatorYAML)

	for _, leg := range legislators {
		if leg.ID.Bioguide == "" {
			continue
		}
		bioguideMap[leg.ID.Bioguide] = leg

		// Use the most recent term's state for name+state matching
		if len(leg.Terms) > 0 {
			lastTerm := leg.Terms[len(leg.Terms)-1]
			key := strings.ToLower(leg.Name.Last) + "|" + strings.ToUpper(lastTerm.State)
			nameStateMap[key] = append(nameStateMap[key], leg)
		}
	}

	// ── Step 4: Query database for active federal politicians ──────────────
	var federalPols []federalPol
	dbResult := db.DB.Raw(`
		SELECT p.id, p.first_name, p.last_name, p.full_name, p.bioguide_id,
		       o.representing_state
		FROM essentials.politicians p
		JOIN essentials.offices o ON o.politician_id = p.id
		JOIN essentials.districts d ON o.district_id = d.id
		WHERE d.district_type IN ('NATIONAL_UPPER', 'NATIONAL_LOWER')
		  AND p.is_active = true
	`).Scan(&federalPols)
	if dbResult.Error != nil {
		return result, fmt.Errorf("querying federal politicians: %w", dbResult.Error)
	}
	log.Printf("[backfill] Found %d active NATIONAL_UPPER/NATIONAL_LOWER politicians in database", len(federalPols))

	// ── Step 5 & 6: Match and insert ──────────────────────────────────────
	for _, pol := range federalPols {
		var matchedBioguide string
		var isTier2Match bool

		if pol.BioguideID != "" {
			// Tier 1: politician already has bioguide_id — verify it exists in YAML
			if _, ok := bioguideMap[pol.BioguideID]; ok {
				matchedBioguide = pol.BioguideID
				log.Printf("[backfill] Tier 1 match: %s (%s)", pol.FullName, pol.BioguideID)
			} else {
				log.Printf("[backfill] WARNING: %s has bioguide_id=%s but not found in congress-legislators YAML — skipping",
					pol.FullName, pol.BioguideID)
				result.Skipped++
				continue
			}
		} else {
			// Tier 2: search by last_name + representing_state
			if pol.RepresentingState == "" {
				log.Printf("[backfill] UNMATCHED: %s has no bioguide_id and no representing_state — skipping", pol.FullName)
				result.Skipped++
				continue
			}
			key := strings.ToLower(pol.LastName) + "|" + strings.ToUpper(pol.RepresentingState)
			matches := nameStateMap[key]
			switch len(matches) {
			case 0:
				log.Printf("[backfill] UNMATCHED: %s (%s) — not found in congress-legislators YAML", pol.FullName, pol.RepresentingState)
				result.Skipped++
				continue
			case 1:
				matchedBioguide = matches[0].ID.Bioguide
				isTier2Match = true
				log.Printf("[backfill] Tier 2 match: %s (%s) → bioguide=%s", pol.FullName, pol.RepresentingState, matchedBioguide)
			default:
				log.Printf("[backfill] AMBIGUOUS: %s (%s) — %d name+state matches in YAML, manual review needed",
					pol.FullName, pol.RepresentingState, len(matches))
				result.Errors = append(result.Errors,
					fmt.Sprintf("ambiguous match: %s (%s) — %d candidates", pol.FullName, pol.RepresentingState, len(matches)))
				result.Skipped++
				continue
			}
		}

		result.Matched++

		if cfg.DryRun {
			log.Printf("[backfill] DRY RUN — would insert bridge row for %s bioguide=%s", pol.FullName, matchedBioguide)
			continue
		}

		// Check if bridge row already exists (ON CONFLICT DO NOTHING equivalent)
		var existing LegislativePoliticianIDMap
		checkResult := db.DB.Where(
			"politician_id = ? AND id_type = ? AND id_value = ?",
			pol.ID, "bioguide", matchedBioguide,
		).First(&existing)
		if checkResult.Error == nil {
			// Row already exists — skip
			log.Printf("[backfill] SKIP (already exists): %s bioguide=%s", pol.FullName, matchedBioguide)
			result.Skipped++
			continue
		}

		// Insert bridge row
		bridge := LegislativePoliticianIDMap{
			PoliticianID: pol.ID,
			IDType:       "bioguide",
			IDValue:      matchedBioguide,
			VerifiedAt:   time.Now(),
			Source:       "congress-legislators-yaml",
		}
		if insertErr := db.DB.Create(&bridge).Error; insertErr != nil {
			log.Printf("[backfill] ERROR inserting bridge row for %s: %v", pol.FullName, insertErr)
			result.Errors = append(result.Errors, fmt.Sprintf("insert failed for %s: %v", pol.FullName, insertErr))
			continue
		}
		result.Inserted++

		// Tier 2 match: also update the politician's bioguide_id field
		if isTier2Match {
			if updateErr := db.DB.Model(&Politician{}).Where("id = ?", pol.ID).Update("bioguide_id", matchedBioguide).Error; updateErr != nil {
				log.Printf("[backfill] WARNING: inserted bridge row for %s but failed to update bioguide_id: %v", pol.FullName, updateErr)
				result.Errors = append(result.Errors, fmt.Sprintf("bioguide_id update failed for %s: %v", pol.FullName, updateErr))
			} else {
				log.Printf("[backfill] Updated bioguide_id for %s → %s", pol.FullName, matchedBioguide)
			}
		}
	}

	// ── Step 8: Log summary ────────────────────────────────────────────────
	log.Printf("[backfill] Backfill complete: %d matched, %d inserted, %d skipped, %d errors",
		result.Matched, result.Inserted, result.Skipped, len(result.Errors))

	return result, nil
}
