package essentials

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/goccy/go-yaml"
	"github.com/google/uuid"
	"gorm.io/gorm/clause"
)

const (
	committeesCurrentURL   = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/committees-current.yaml"
	committeeMembershipURL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/committee-membership-current.yaml"
)

// committeeYAML represents a top-level committee entry from committees-current.yaml.
type committeeYAML struct {
	ThomasID          string             `yaml:"thomas_id"`
	Name              string             `yaml:"name"`
	Type              string             `yaml:"type"`               // "house", "senate", "joint"
	HouseCommitteeID  string             `yaml:"house_committee_id"` // alternate ID field
	SenateCommitteeID string             `yaml:"senate_committee_id"`
	Subcommittees     []subcommitteeYAML `yaml:"subcommittees"`
}

// subcommitteeYAML represents a subcommittee nested inside a committeeYAML.
type subcommitteeYAML struct {
	ThomasID string `yaml:"thomas_id"` // numeric string: "15", "22"
	Name     string `yaml:"name"`
}

// committeeMemberYAML represents one member entry within a committee membership map.
// CRITICAL: committee-membership-current.yaml is a MAP at the top level (not an array).
// Keys are thomas_id (parent committee) or composed keys (parent_thomas_id + sub_thomas_id).
type committeeMemberYAML struct {
	Name     string `yaml:"name"`
	Bioguide string `yaml:"bioguide"`
	Party    string `yaml:"party"`
	Rank     int    `yaml:"rank"`
	Title    string `yaml:"title"`   // "Chairman", "Ranking Member", "Ex Officio" — NOT yet normalized
	Chamber  string `yaml:"chamber"` // only for joint committees
}

// ImportCommitteesConfig controls import behavior.
type ImportCommitteesConfig struct {
	DryRun         bool
	CongressNumber int // default 119
}

// ImportCommitteesResult contains counts from an import run.
type ImportCommitteesResult struct {
	SessionCreated        bool
	CommitteesUpserted    int
	SubcommitteesUpserted int
	MembershipsUpserted   int
	Skipped               int
	Errors                []string
}

// normalizeCommitteeRole maps raw title strings from the YAML to one of:
// "chair", "vice_chair", "ranking_member", "ex_officio", "member".
func normalizeCommitteeRole(title string) string {
	t := strings.ToLower(strings.TrimSpace(title))
	switch {
	case strings.Contains(t, "vice") && strings.Contains(t, "chair"):
		return "vice_chair"
	case strings.Contains(t, "chair") && !strings.Contains(t, "ranking"):
		return "chair"
	case strings.Contains(t, "ranking"):
		return "ranking_member"
	case strings.Contains(t, "ex officio"):
		return "ex_officio"
	default:
		return "member"
	}
}

// getOrCreateSession returns the UUID of the legislative session for the given congress number,
// creating it if it does not exist.
func getOrCreateSession(congressNumber int) (uuid.UUID, bool, error) {
	var session LegislativeSession
	externalID := strconv.Itoa(congressNumber)

	result := db.DB.Where("jurisdiction = ? AND external_id = ?", "federal", externalID).First(&session)
	if result.Error == nil {
		// Session already exists
		return session.ID, false, nil
	}

	// Create new session
	session = LegislativeSession{
		Jurisdiction: "federal",
		Name:         fmt.Sprintf("%dth Congress", congressNumber),
		ExternalID:   externalID,
		IsCurrent:    true,
		Source:       "congress-legislators",
	}
	if createErr := db.DB.Create(&session).Error; createErr != nil {
		return uuid.Nil, false, fmt.Errorf("creating legislative session for congress %d: %w", congressNumber, createErr)
	}
	log.Printf("[import-committees] Created legislative session: %s (ID: %s)", session.Name, session.ID)
	return session.ID, true, nil
}

// fetchYAML downloads a URL and returns the raw bytes.
func fetchYAML(url string) ([]byte, error) {
	log.Printf("[import-committees] Downloading %s", url)
	resp, err := http.Get(url) //nolint:gosec
	if err != nil {
		return nil, fmt.Errorf("downloading %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected HTTP status %d fetching %s", resp.StatusCode, url)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body from %s: %w", url, err)
	}
	log.Printf("[import-committees] Downloaded %d bytes from %s", len(body), url)
	return body, nil
}

// ImportCommittees downloads the congress-legislators committee YAML files, parses them,
// and upserts committees and memberships into the database.
//
// Flow:
//  1. Ensure a LegislativeSession row exists for the current congress.
//  2. Download + parse committees-current.yaml ([]committeeYAML).
//  3. Download + parse committee-membership-current.yaml (map[string][]committeeMemberYAML).
//  4. Build bioguide→politicianID cache from the bridge table.
//  5. Upsert top-level committees.
//  6. Upsert subcommittees with parent_id.
//  7. Upsert memberships.
func ImportCommittees(cfg ImportCommitteesConfig) (ImportCommitteesResult, error) {
	result := ImportCommitteesResult{}

	if cfg.CongressNumber == 0 {
		cfg.CongressNumber = 119
	}

	// ── Step 1: Ensure legislative session ────────────────────────────────
	sessionID, created, err := getOrCreateSession(cfg.CongressNumber)
	if err != nil {
		return result, err
	}
	result.SessionCreated = created

	// ── Step 2: Download + parse committees-current.yaml ──────────────────
	committeeBody, err := fetchYAML(committeesCurrentURL)
	if err != nil {
		return result, err
	}
	var committees []committeeYAML
	if err := yaml.Unmarshal(committeeBody, &committees); err != nil {
		return result, fmt.Errorf("parsing committees YAML: %w", err)
	}
	log.Printf("[import-committees] Parsed %d top-level committees", len(committees))

	// ── Step 3: Download + parse committee-membership-current.yaml ────────
	// CRITICAL: this is a MAP[string][]committeeMemberYAML, NOT a slice.
	membershipBody, err := fetchYAML(committeeMembershipURL)
	if err != nil {
		return result, err
	}
	var membershipMap map[string][]committeeMemberYAML
	if err := yaml.Unmarshal(membershipBody, &membershipMap); err != nil {
		return result, fmt.Errorf("parsing membership YAML: %w", err)
	}
	log.Printf("[import-committees] Parsed %d committee membership entries", len(membershipMap))

	// ── Step 4: Build bioguide → politician UUID cache ────────────────────
	// Single query to load ALL bioguide bridge rows up front.
	type bridgeRow struct {
		IDValue     string    `gorm:"column:id_value"`
		PoliticianID uuid.UUID `gorm:"column:politician_id"`
	}
	var bridges []bridgeRow
	if dbErr := db.DB.Table("essentials.legislative_politician_id_map").
		Select("id_value, politician_id").
		Where("id_type = ?", "bioguide").
		Find(&bridges).Error; dbErr != nil {
		return result, fmt.Errorf("loading bioguide bridge table: %w", dbErr)
	}
	bioguideCache := make(map[string]uuid.UUID, len(bridges))
	for _, b := range bridges {
		bioguideCache[b.IDValue] = b.PoliticianID
	}
	log.Printf("[import-committees] Loaded %d bioguide bridge entries", len(bioguideCache))

	// committeeMap tracks thomas_id → DB uuid for both committees and subcommittees.
	committeeMap := make(map[string]uuid.UUID)

	// ── Step 5: Upsert top-level committees ───────────────────────────────
	for _, c := range committees {
		if c.ThomasID == "" {
			log.Printf("[import-committees] WARNING: committee with empty thomas_id (name=%s), skipping", c.Name)
			continue
		}

		// Derive chamber from type field
		chamber := strings.ToLower(c.Type)
		if chamber == "" {
			chamber = "unknown"
		}

		rec := LegislativeCommittee{
			SessionID:    &sessionID,
			ParentID:     nil,
			ExternalID:   c.ThomasID,
			Jurisdiction: "federal",
			Name:         c.Name,
			Type:         "committee",
			Chamber:      chamber,
			IsCurrent:    true,
			Source:       "congress-legislators",
		}

		if cfg.DryRun {
			log.Printf("[import-committees] DRY RUN — would upsert committee: %s (%s)", c.ThomasID, c.Name)
			result.CommitteesUpserted++
			committeeMap[c.ThomasID] = uuid.New() // placeholder UUID for dry run
			continue
		}

		if dbErr := db.DB.
			Where(LegislativeCommittee{ExternalID: c.ThomasID, Jurisdiction: "federal"}).
			Assign(LegislativeCommittee{
				Name:      c.Name,
				Type:      "committee",
				Chamber:   chamber,
				IsCurrent: true,
				SessionID: &sessionID,
			}).
			FirstOrCreate(&rec).Error; dbErr != nil {
			log.Printf("[import-committees] ERROR upserting committee %s: %v", c.ThomasID, dbErr)
			result.Errors = append(result.Errors, fmt.Sprintf("committee upsert failed: %s: %v", c.ThomasID, dbErr))
			continue
		}

		committeeMap[c.ThomasID] = rec.ID
		result.CommitteesUpserted++
		log.Printf("[import-committees] Upserted committee: %s (%s) ID=%s", c.ThomasID, c.Name, rec.ID)
	}

	// ── Step 6: Upsert subcommittees ──────────────────────────────────────
	for _, parent := range committees {
		parentDBID, ok := committeeMap[parent.ThomasID]
		if !ok {
			// Parent was skipped; skip its subcommittees too
			continue
		}

		chamber := strings.ToLower(parent.Type)
		if chamber == "" {
			chamber = "unknown"
		}

		for _, sub := range parent.Subcommittees {
			if sub.ThomasID == "" {
				continue
			}
			// Composed external ID: parent thomas_id + sub thomas_id
			composedID := parent.ThomasID + sub.ThomasID

			rec := LegislativeCommittee{
				SessionID:    &sessionID,
				ParentID:     &parentDBID,
				ExternalID:   composedID,
				Jurisdiction: "federal",
				Name:         sub.Name,
				Type:         "subcommittee",
				Chamber:      chamber,
				IsCurrent:    true,
				Source:       "congress-legislators",
			}

			if cfg.DryRun {
				log.Printf("[import-committees] DRY RUN — would upsert subcommittee: %s (%s)", composedID, sub.Name)
				result.SubcommitteesUpserted++
				committeeMap[composedID] = uuid.New()
				continue
			}

			if dbErr := db.DB.
				Where(LegislativeCommittee{ExternalID: composedID, Jurisdiction: "federal"}).
				Assign(LegislativeCommittee{
					Name:      sub.Name,
					Type:      "subcommittee",
					Chamber:   chamber,
					IsCurrent: true,
					SessionID: &sessionID,
					ParentID:  &parentDBID,
				}).
				FirstOrCreate(&rec).Error; dbErr != nil {
				log.Printf("[import-committees] ERROR upserting subcommittee %s: %v", composedID, dbErr)
				result.Errors = append(result.Errors, fmt.Sprintf("subcommittee upsert failed: %s: %v", composedID, dbErr))
				continue
			}

			committeeMap[composedID] = rec.ID
			result.SubcommitteesUpserted++
		}
	}

	log.Printf("[import-committees] Upserted %d committees, %d subcommittees",
		result.CommitteesUpserted, result.SubcommitteesUpserted)

	// ── Step 7: Upsert memberships ────────────────────────────────────────
	for thomasID, members := range membershipMap {
		committeeDBID, ok := committeeMap[thomasID]
		if !ok {
			log.Printf("[import-committees] SKIP membership key %s — committee not found in upserted set", thomasID)
			result.Skipped += len(members)
			continue
		}

		for _, member := range members {
			if member.Bioguide == "" {
				log.Printf("[import-committees] SKIP member with no bioguide in committee %s", thomasID)
				result.Skipped++
				continue
			}

			polID, found := bioguideCache[member.Bioguide]
			if !found {
				log.Printf("[import-committees] SKIP unmatched bioguide=%s in committee %s (name=%s)",
					member.Bioguide, thomasID, member.Name)
				result.Skipped++
				continue
			}

			role := normalizeCommitteeRole(member.Title)

			if cfg.DryRun {
				log.Printf("[import-committees] DRY RUN — would upsert membership: bioguide=%s committee=%s role=%s",
					member.Bioguide, thomasID, role)
				result.MembershipsUpserted++
				continue
			}

			membership := LegislativeCommitteeMembership{
				CommitteeID:    committeeDBID,
				PoliticianID:   polID,
				CongressNumber: cfg.CongressNumber,
				Role:           role,
				IsCurrent:      true,
				SessionID:      &sessionID,
			}

			if dbErr := db.DB.
				Clauses(clause.OnConflict{
					Columns: []clause.Column{
						{Name: "committee_id"},
						{Name: "politician_id"},
						{Name: "congress_number"},
					},
					DoUpdates: clause.AssignmentColumns([]string{"role", "is_current", "session_id"}),
				}).
				Create(&membership).Error; dbErr != nil {
				log.Printf("[import-committees] ERROR upserting membership bioguide=%s committee=%s: %v",
					member.Bioguide, thomasID, dbErr)
				result.Errors = append(result.Errors,
					fmt.Sprintf("membership upsert failed: bioguide=%s committee=%s: %v", member.Bioguide, thomasID, dbErr))
				continue
			}

			result.MembershipsUpserted++
		}
	}

	// ── Step 8: Log summary ───────────────────────────────────────────────
	log.Printf("[import-committees] Import complete: %d committees, %d subcommittees, %d memberships upserted, %d skipped, %d errors",
		result.CommitteesUpserted, result.SubcommitteesUpserted, result.MembershipsUpserted,
		result.Skipped, len(result.Errors))

	return result, nil
}
