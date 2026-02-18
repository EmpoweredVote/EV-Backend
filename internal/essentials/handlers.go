package essentials

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/EmpoweredVote/EV-Backend/internal/essentials/ballotready"
	"github.com/EmpoweredVote/EV-Backend/internal/essentials/cicero"
	"github.com/EmpoweredVote/EV-Backend/internal/essentials/provider"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// Type aliases for backward compatibility with existing code
// These reference the types from the cicero package
type CiceroOfficial = cicero.CiceroOfficial
type CiceroOffice = cicero.CiceroOffice
type CiceroDistrict = cicero.CiceroDistrict
type CiceroChamber = cicero.CiceroChamber
type CiceroGovernment = cicero.CiceroGovernment
type CiceroAddress = cicero.CiceroAddress
type CiceroIdentifier = cicero.CiceroIdentifier
type CiceroCommittee = cicero.CiceroCommittee
type CiceroAPIResponse = cicero.CiceroAPIResponse

type CommitteeOut struct {
	Name     string   `json:"name"`
	Position string   `json:"position"`
	URLs     []string `json:"urls"`
}

type ImageOut struct {
	URL  string `json:"url"`
	Type string `json:"type"`
}

type DegreeOut struct {
	Degree   string `json:"degree"`
	Major    string `json:"major"`
	School   string `json:"school"`
	GradYear int    `json:"grad_year"`
}

type ExperienceOut struct {
	Title        string `json:"title"`
	Organization string `json:"organization"`
	Type         string `json:"type"`
	Start        string `json:"start"`
	End          string `json:"end"`
}

// Phase B: Candidacy data DTOs

type EndorsementOut struct {
	EndorserString      string  `json:"endorser_string"`
	Recommendation      string  `json:"recommendation"` // "PRO", "CON"
	Status              string  `json:"status"`
	ElectionDate        string  `json:"election_date"`
	OrganizationName    string  `json:"organization_name,omitempty"`
	OrganizationDesc    string  `json:"organization_description,omitempty"`
	OrganizationLogoURL string  `json:"organization_logo_url,omitempty"`
	OrganizationIssue   string  `json:"organization_issue,omitempty"`
}

type StanceOut struct {
	Statement      string `json:"statement"`
	ReferenceURL   string `json:"reference_url,omitempty"`
	ElectionDate   string `json:"election_date"`
	IssueName      string `json:"issue_name"`
	IssueKey       string `json:"issue_key"`
	IssueExpanded  string `json:"issue_expanded,omitempty"`
	ParentIssueName string `json:"parent_issue_name,omitempty"`
}

type ElectionRecordOut struct {
	ElectionName    string `json:"election_name"`
	ElectionDate    string `json:"election_date"`
	PositionName    string `json:"position_name"`
	Result          string `json:"result"` // "WON", "LOST", "RUNOFF", etc.
	Withdrawn       bool   `json:"withdrawn"`
	PartyName       string `json:"party_name"`
	IsPrimary       bool   `json:"is_primary"`
	IsRunoff        bool   `json:"is_runoff"`
	IsUnexpiredTerm bool   `json:"is_unexpired_term"`
}

type CacheStatusResponse struct {
	FederalFresh bool `json:"federalFresh"`
	StateFresh   bool `json:"stateFresh"`
	LocalFresh   bool `json:"localFresh"`
	AllFresh     bool `json:"allFresh"`
	Warming      bool `json:"warming"`
}

type OfficialOut struct {
	ID                   uuid.UUID       `json:"id"`
	ExternalID           int             `json:"external_id"`
	FirstName            string          `json:"first_name"`
	MiddleInitial        string          `json:"middle_initial"`
	LastName             string          `json:"last_name"`
	PreferredName        string          `json:"preferred_name"`
	NameSuffix           string          `json:"name_suffix"`
	FullName             string          `json:"full_name"`
	Party                string          `json:"party"`
	PartyShortName       string          `json:"party_short_name,omitempty"`
	PhotoOriginURL       string          `json:"photo_origin_url"`
	WebFormURL           string          `json:"web_form_url"`
	URLs                 []string        `json:"urls"`
	EmailAddresses       []string        `json:"email_addresses"`
	OfficeTitle          string          `json:"office_title"`
	RepresentingState    string          `json:"representing_state"`
	RepresentingCity     string          `json:"representing_city"`
	DistrictType         string          `json:"district_type"`
	DistrictLabel        string          `json:"district_label"`
	MTFCC                string          `json:"mtfcc"`
	ChamberName          string          `json:"chamber_name"`
	ChamberNameFormal    string          `json:"chamber_name_formal"`
	GovernmentName       string          `json:"government_name"`
	IsElected            bool            `json:"is_elected"`
	IsAppointed          bool            `json:"is_appointed,omitempty"`
	IsVacant             bool            `json:"is_vacant,omitempty"`
	IsOffCycle           bool            `json:"is_off_cycle,omitempty"`
	Specificity          string          `json:"specificity,omitempty"`
	ElectionFrequency    string          `json:"election_frequency,omitempty"`
	Seats                int             `json:"seats,omitempty"`
	NormalizedPositionName string          `json:"normalized_position_name,omitempty"`
	PartisanType         string          `json:"partisan_type,omitempty"`
	Salary               string          `json:"salary,omitempty"`
	GeoID                string          `json:"geo_id,omitempty"`
	IsJudicial           bool            `json:"is_judicial,omitempty"`
	OCDID                string          `json:"ocd_id,omitempty"`
	Committees           []CommitteeOut  `json:"committees"`
	BioText              string          `json:"bio_text,omitempty"`
	BioguideID           string          `json:"bioguide_id,omitempty"`
	Slug                 string          `json:"slug,omitempty"`
	TotalYearsInOffice   int             `json:"total_years_in_office,omitempty"`
	OfficeDescription    string          `json:"office_description,omitempty"`
	Images               []ImageOut      `json:"images,omitempty"`
	Degrees              []DegreeOut     `json:"degrees,omitempty"`
	Experiences          []ExperienceOut `json:"experiences,omitempty"`
	IsContained          *bool           `json:"is_contained,omitempty"` // For ZIP queries: true=position fully contains ZIP, false=partial overlap
	TermStart            string          `json:"term_start,omitempty"`
	TermEnd              string          `json:"term_end,omitempty"`
}

func GetPoliticiansByZip(w http.ResponseWriter, r *http.Request) {
	zip := chi.URLParam(r, "zip")
	if !isZip5(zip) {
		http.Error(w, "Missing or invalid zip parameter", http.StatusBadRequest)
		return
	}
	handleZipLookup(w, r, zip)
}

// GetCacheStatus returns cache freshness status for a ZIP code without triggering warmers.
func GetCacheStatus(w http.ResponseWriter, r *http.Request) {
	zip := chi.URLParam(r, "zip")
	if !isZip5(zip) {
		http.Error(w, "Invalid zip parameter", http.StatusBadRequest)
		return
	}

	log.Printf("[GetCacheStatus] zip=%s from=%s", zip, r.RemoteAddr)

	t0 := time.Now()

	status, err := getCacheStatus(r.Context(), zip)
	if err != nil {
		log.Printf("[GetCacheStatus] zip=%s err=%v", zip, err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Trigger warming for stale tiers (if not already warming)
	if !status.AllFresh {
		state := zipPrefixToState(zip)

		if !status.FederalFresh {
			if tryAcquireLock(r.Context(), "federal") {
				go func() {
					defer releaseLock(context.Background(), "federal")
					if err := warmFederal(context.Background()); err != nil {
						log.Printf("[warmFederal] err=%v", err)
					}
				}()
				status.Warming = true
			}
		}

		if !status.StateFresh && state != "" {
			if tryAcquireLock(r.Context(), "state-"+state) {
				capturedZip := zip
				go func() {
					defer releaseLock(context.Background(), "state-"+state)
					if err := warmState(context.Background(), state, capturedZip); err != nil {
						log.Printf("[warmState] state=%s err=%v", state, err)
					}
				}()
				status.Warming = true
			}
		}

		if !status.LocalFresh {
			if tryAcquireLock(r.Context(), "zip-"+zip) {
				go func() {
					defer releaseLock(context.Background(), "zip-"+zip)
					if err := warmLocal(context.Background(), zip); err != nil {
						log.Printf("[warmLocal] zip=%s err=%v", zip, err)
					}
				}()
				status.Warming = true
			}
		}
	}

	// Add Server-Timing header with total query duration
	addServerTiming(w, [2]string{"total", fmt.Sprintf("%d", time.Since(t0).Milliseconds())})

	// If warming is in progress, suggest retry after 3 seconds
	if status.Warming {
		w.Header().Set("Retry-After", "3")
	}

	// Always prevent caching of status responses
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")

	writeJSON(w, status)
}

func handleZipLookup(w http.ResponseWriter, r *http.Request, zip string) {
	ctx := r.Context()

	const (
		maxAge          = 90 * 24 * time.Hour
		shortWait       = 10 * time.Second
		shortWaitTick   = 200 * time.Millisecond
		cdnTTLWhenFresh = 3600
		swrSeconds      = 86400
	)

	t0 := time.Now()
	var dbReadMs float64
	now := time.Now()

	// --- Check federal cache ---
	var federalCache FederalCache
	federalErr := db.DB.WithContext(ctx).First(&federalCache).Error
	federalFresh := federalErr == nil && now.Sub(federalCache.LastFetched) < maxAge

	if !federalFresh {
		if tryAcquireLock(ctx, "federal") {
			go func() {
				defer releaseLock(context.Background(), "federal")
				if err := warmFederal(context.Background()); err != nil {
					log.Printf("[warmFederal] err=%v", err)
				}
			}()
		}
	}

	// --- Determine state and check state cache ---
	var zipCache ZipCache
	zipCacheErr := db.DB.WithContext(ctx).Where("zip = ?", zip).First(&zipCache).Error

	var state string
	if zipCacheErr == nil && zipCache.State != "" {
		state = zipCache.State
	} else {
		// Derive state from ZIP prefix immediately (static lookup, no API call)
		state = zipPrefixToState(zip)
		// Note: warmLocal will still discover and save state to zip_caches for future lookups
	}

	var stateFresh bool
	if state != "" {
		var stateCache StateCache
		stateErr := db.DB.WithContext(ctx).Where("state = ?", state).First(&stateCache).Error
		stateFresh = stateErr == nil && now.Sub(stateCache.LastFetched) < maxAge

		if !stateFresh {
			if tryAcquireLock(ctx, "state-"+state) {
				capturedZip := zip // Capture for goroutine
				go func() {
					defer releaseLock(context.Background(), "state-"+state)
					if err := warmState(context.Background(), state, capturedZip); err != nil {
						log.Printf("[warmState] state=%s err=%v", state, err)
					}
				}()
			}
		}
	}

	// --- Check local/ZIP cache ---
	localFresh := zipCacheErr == nil && now.Sub(zipCache.LastFetched) < maxAge

	if !localFresh {
		if tryAcquireLock(ctx, "zip-"+zip) {
			go func() {
				defer releaseLock(context.Background(), "zip-"+zip)
				if err := warmLocal(context.Background(), zip); err != nil {
					log.Printf("[warmLocal] zip=%s err=%v", zip, err)
				}
			}()
		}
	}

	// --- Fetch officials from DB ---
	tDB := time.Now()
	officials, readErr := fetchOfficialsFromDB(zip, state)
	dbReadMs = float64(time.Since(tDB).Milliseconds())

	hasData := readErr == nil && len(officials) > 0
	allFresh := federalFresh && (state == "" || stateFresh) && localFresh

	// Serve immediately if all caches are fresh or if we have data
	if allFresh && hasData {
		addServerTiming(w, [2]string{"dbread", fmt.Sprintf("%d", int(dbReadMs))})
		w.Header().Set("X-Data-Status", "fresh")
		addCacheHeaders(w, cdnTTLWhenFresh, swrSeconds)
		writeJSON(w, officials)
		return
	}
	if hasData {
		addServerTiming(w, [2]string{"dbread", fmt.Sprintf("%d", int(dbReadMs))})
		w.Header().Set("X-Data-Status", "stale")
		addNoStore(w)
		writeJSON(w, officials)
		return
	}

	// Cold miss: wait briefly for warmers to populate
	tWait := time.Now()
	if warmed, ok := waitForDataMin(ctx, zip, state, shortWait, shortWaitTick, 3); ok {
		waitMs := float64(time.Since(tWait).Milliseconds())
		addServerTiming(w,
			[2]string{"dbread", fmt.Sprintf("%d", int(dbReadMs))},
			[2]string{"wait", fmt.Sprintf("%d", int(waitMs))},
			[2]string{"total", fmt.Sprintf("%d", int(time.Since(t0).Milliseconds()))},
		)
		w.Header().Set("X-Data-Status", "warmed")
		addNoStore(w)
		writeJSON(w, warmed)
		return
	}

	// Still warming
	waitMs := float64(time.Since(tWait).Milliseconds())
	addServerTiming(w,
		[2]string{"dbread", fmt.Sprintf("%d", int(dbReadMs))},
		[2]string{"wait", fmt.Sprintf("%d", int(waitMs))},
		[2]string{"total", fmt.Sprintf("%d", int(time.Since(t0).Milliseconds()))},
	)
	w.Header().Set("X-Data-Status", "warming")
	w.Header().Set("Retry-After", "3")
	w.Header().Set("Cache-Control", "no-store")
	writeJSONStatus(w, http.StatusAccepted, map[string]string{"status": "warming"})
}

// helper: write JSON with a specific HTTP status code
func writeJSONStatus(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// --- Background warmers (hierarchical caching) ----

// upsertOfficial handles the database upsert logic for a single official.
// Returns the politician ID if successful.
func upsertOfficial(ctx context.Context, off CiceroOfficial, timestamp time.Time) (uuid.UUID, error) {
	tr, err := TransformCiceroData(off)
	if err != nil {
		return uuid.Nil, err
	}

	var polID uuid.UUID

	err = db.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// ==== District (upsert + RETURNING) ====
		if tr.District != nil {
			if err := tx.Clauses(
				clause.OnConflict{
					Columns: []clause.Column{{Name: "external_id"}},
					DoUpdates: clause.AssignmentColumns([]string{
						"ocd_id", "label", "district_type", "district_id", "subtype",
						"state", "city", "mtfcc", "num_officials", "valid_from", "valid_to",
						"geo_id", "is_judicial", "has_unknown_boundaries", "retention",
					}),
				},
				clause.Returning{Columns: []clause.Column{{Name: "id"}}},
			).Create(tr.District).Error; err != nil {
				return err
			}
		}

		// ==== Government (natural-key lookup) ====
		var govID *uuid.UUID
		if tr.Government != nil {
			var existingGov Government
			err := tx.Where(
				"name = ? AND type = ? AND state = ? AND city = ?",
				tr.Government.Name, tr.Government.Type, tr.Government.State, tr.Government.City,
			).First(&existingGov).Error
			if err == nil {
				govID = &existingGov.ID
			} else if errors.Is(err, gorm.ErrRecordNotFound) {
				if err := tx.Create(tr.Government).Error; err != nil {
					return err
				}
				govID = &tr.Government.ID
			} else {
				return err
			}
		}

		// ==== Chamber (upsert + RETURNING) ====
		if tr.Chamber != nil {
			if govID != nil {
				tr.Chamber.GovernmentID = *govID
			}
			if err := tx.Clauses(
				clause.OnConflict{
					Columns: []clause.Column{{Name: "external_id"}},
					DoUpdates: clause.AssignmentColumns([]string{
						"government_id", "name", "name_formal", "official_count",
						"term_limit", "term_length", "inauguration_rules",
						"election_frequency", "election_rules", "vacancy_rules", "remarks",
						"staggered_term",
					}),
				},
				clause.Returning{Columns: []clause.Column{{Name: "id"}}},
			).Create(tr.Chamber).Error; err != nil {
				return err
			}
		}

		// ==== Politician (upsert + RETURNING id) ====
		if tr.Politician != nil {
			assign := map[string]interface{}{
				"first_name":       gorm.Expr("excluded.first_name"),
				"middle_initial":   gorm.Expr("excluded.middle_initial"),
				"last_name":        gorm.Expr("excluded.last_name"),
				"preferred_name":   gorm.Expr("excluded.preferred_name"),
				"name_suffix":      gorm.Expr("excluded.name_suffix"),
				"party":            gorm.Expr("excluded.party"),
				"web_form_url":     gorm.Expr("excluded.web_form_url"),
				"urls":             gorm.Expr("excluded.urls"),
				"photo_origin_url": gorm.Expr(`COALESCE(NULLIF(excluded.photo_origin_url, ''), "essentials"."politicians"."photo_origin_url")`),
				"notes":            gorm.Expr("excluded.notes"),
				"valid_from":       gorm.Expr("excluded.valid_from"),
				"valid_to":         gorm.Expr("excluded.valid_to"),
				"email_addresses":  gorm.Expr("excluded.email_addresses"),
				"office_id":        gorm.Expr("excluded.office_id"),
			}
			if err := tx.
				Omit("Addresses", "Identifiers", "Committees").
				Clauses(
					clause.OnConflict{
						Columns:   []clause.Column{{Name: "external_id"}},
						DoUpdates: clause.Assignments(assign),
					},
					clause.Returning{Columns: []clause.Column{{Name: "id"}}},
				).
				Create(tr.Politician).Error; err != nil {
				return err
			}
		}

		polID = tr.Politician.ID
		if polID == uuid.Nil {
			var persistedPol Politician
			if err := tx.Where("external_id = ?", off.OfficialID).First(&persistedPol).Error; err != nil {
				return err
			}
			polID = persistedPol.ID
		}

		// Resolve IDs for office
		var districtID, chamberID uuid.UUID
		if tr.District != nil {
			districtID = tr.District.ID
		} else {
			var ex District
			if err := tx.Where("external_id = ?", off.Office.District.SK).First(&ex).Error; err != nil {
				return err
			}
			districtID = ex.ID
		}
		if tr.Chamber != nil {
			chamberID = tr.Chamber.ID
		} else {
			var ex Chamber
			if err := tx.Where("external_id = ?", off.Office.Chamber.ID).First(&ex).Error; err != nil {
				return err
			}
			chamberID = ex.ID
		}

		// ==== Office ====
		office := Office{
			ID:                   tr.Politician.OfficeID,
			PoliticianID:         polID,
			ChamberID:            chamberID,
			DistrictID:           districtID,
			Title:                off.Office.Title,
			RepresentingState:    off.Office.RepresentingState,
			RepresentingCity:     off.Office.RepresentingCity,
			// New fields from BallotReady - not available in Cicero data, set to zero values
			Description:          "",
			Seats:                0,
			NormalizedPositionName: "",
			PartisanType:         "",
			Salary:               "",
			IsAppointedPosition:  false,
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "politician_id"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"chamber_id", "district_id", "title", "representing_state", "representing_city",
			}),
		}).Create(&office).Error; err != nil {
			return err
		}

		// ==== Addresses ====
		if err := tx.Where("politician_id = ?", polID).Delete(&Address{}).Error; err != nil {
			return err
		}
		if len(tr.Politician.Addresses) > 0 {
			for i := range tr.Politician.Addresses {
				tr.Politician.Addresses[i].PoliticianID = polID
			}
			if err := tx.Create(&tr.Politician.Addresses).Error; err != nil {
				return err
			}
		}

		// ==== Identifiers ====
		if err := tx.Where("politician_id = ?", polID).Delete(&Identifier{}).Error; err != nil {
			return err
		}
		if len(tr.Politician.Identifiers) > 0 {
			batch := make([]Identifier, 0, len(tr.Politician.Identifiers))
			for _, it := range tr.Politician.Identifiers {
				t := strings.TrimSpace(strings.ToLower(it.IdentifierType))
				v := strings.TrimSpace(strings.ToLower(it.IdentifierValue))
				if t == "" || v == "" {
					continue
				}
				batch = append(batch, Identifier{
					PoliticianID:    polID,
					IdentifierType:  t,
					IdentifierValue: v,
				})
			}
			if len(batch) > 0 {
				if err := tx.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "politician_id"}, {Name: "identifier_type"}, {Name: "identifier_value"}},
					DoNothing: true,
				}).Create(&batch).Error; err != nil {
					return err
				}
			}
		}

		// ==== Committees ====
		committeeIDByName := make(map[string]uuid.UUID)
		norm := func(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

		names := make([]string, 0, len(tr.Committees))
		for _, c := range tr.Committees {
			if strings.TrimSpace(c.Name) == "" {
				continue
			}
			names = append(names, c.Name)
		}
		if len(names) > 0 {
			var existing []Committee
			if err := tx.Where("LOWER(name) IN ?", names).Find(&existing).Error; err != nil {
				return err
			}
			for _, ex := range existing {
				committeeIDByName[norm(ex.Name)] = ex.ID
			}
			toCreate := make([]*Committee, 0)
			for _, c := range tr.Committees {
				k := norm(c.Name)
				if _, ok := committeeIDByName[k]; !ok {
					toCreate = append(toCreate, c)
				}
			}
			if len(toCreate) > 0 {
				if err := tx.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "name"}},
					DoNothing: true,
				}).Create(&toCreate).Error; err != nil {
					return err
				}
				for _, c := range toCreate {
					committeeIDByName[norm(c.Name)] = c.ID
				}
			}
		}

		// Dedupe and create committee joins
		type namePos struct{ Name, Position string }
		posByName := make(map[string]namePos)
		for _, kc := range off.Committees {
			name := strings.TrimSpace(kc.Name)
			if name == "" {
				continue
			}
			k := norm(name)
			pos := strings.TrimSpace(kc.Position)
			if _, ok := posByName[k]; !ok || posByName[k].Position == "" {
				posByName[k] = namePos{Name: name, Position: pos}
			}
		}

		joins := make([]PoliticianCommittee, 0, len(posByName))
		for k, np := range posByName {
			cid, ok := committeeIDByName[k]
			if !ok {
				minimal := Committee{ID: uuid.New(), Name: np.Name}
				if err := tx.Clauses(
					clause.OnConflict{Columns: []clause.Column{{Name: "name"}}, DoNothing: true},
					clause.Returning{Columns: []clause.Column{{Name: "id"}}},
				).Create(&minimal).Error; err != nil {
					return err
				}
				cid = minimal.ID
			}
			joins = append(joins, PoliticianCommittee{
				PoliticianID: polID,
				CommitteeID:  cid,
				Position:     np.Position,
			})
		}
		if len(joins) > 0 {
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "politician_id"}, {Name: "committee_id"}},
				DoUpdates: clause.AssignmentColumns([]string{"position"}),
			}).Create(&joins).Error; err != nil {
				return err
			}
		}

		// ==== Images (delete + recreate) ====
		if err := tx.Where("politician_id = ?", polID).Delete(&PoliticianImage{}).Error; err != nil {
			return err
		}
		if len(tr.Politician.Images) > 0 {
			for i := range tr.Politician.Images {
				tr.Politician.Images[i].PoliticianID = polID
			}
			if err := tx.Create(&tr.Politician.Images).Error; err != nil {
				return err
			}
		}

		// ==== Degrees (delete + recreate) ====
		if err := tx.Where("politician_id = ?", polID).Delete(&Degree{}).Error; err != nil {
			return err
		}
		if len(tr.Politician.Degrees) > 0 {
			for i := range tr.Politician.Degrees {
				tr.Politician.Degrees[i].PoliticianID = polID
			}
			if err := tx.Create(&tr.Politician.Degrees).Error; err != nil {
				return err
			}
		}

		// ==== Experiences (delete + recreate) ====
		if err := tx.Where("politician_id = ?", polID).Delete(&Experience{}).Error; err != nil {
			return err
		}
		if len(tr.Politician.Experiences) > 0 {
			for i := range tr.Politician.Experiences {
				tr.Politician.Experiences[i].PoliticianID = polID
			}
			if err := tx.Create(&tr.Politician.Experiences).Error; err != nil {
				return err
			}
		}

		return nil
	})

	return polID, err
}

// upsertNormalizedOfficial handles the database upsert logic for a NormalizedOfficial.
// UpsertNormalizedOfficial is the exported version for use by scripts/tools.
func UpsertNormalizedOfficial(ctx context.Context, off provider.NormalizedOfficial, timestamp time.Time) (uuid.UUID, error) {
	return upsertNormalizedOfficial(ctx, off, timestamp)
}

// This is the provider-agnostic version of upsertOfficial.
// Returns the politician ID if successful.
func upsertNormalizedOfficial(ctx context.Context, off provider.NormalizedOfficial, timestamp time.Time) (uuid.UUID, error) {
	tr, err := TransformNormalizedToModels(off)
	if err != nil {
		return uuid.Nil, err
	}

	var polID uuid.UUID

	err = db.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// ==== District (upsert + RETURNING) ====
		if tr.District != nil {
			if err := tx.Clauses(
				clause.OnConflict{
					Columns: []clause.Column{{Name: "external_id"}},
					DoUpdates: clause.AssignmentColumns([]string{
						"ocd_id", "label", "district_type", "district_id", "subtype",
						"state", "city", "mtfcc", "num_officials", "valid_from", "valid_to",
						"geo_id", "is_judicial", "has_unknown_boundaries", "retention",
					}),
				},
				clause.Returning{Columns: []clause.Column{{Name: "id"}}},
			).Create(tr.District).Error; err != nil {
				return err
			}
		}

		// ==== Government (natural-key lookup) ====
		var govID *uuid.UUID
		if tr.Government != nil {
			var existingGov Government
			err := tx.Where(
				"name = ? AND type = ? AND state = ? AND city = ?",
				tr.Government.Name, tr.Government.Type, tr.Government.State, tr.Government.City,
			).First(&existingGov).Error
			if err == nil {
				govID = &existingGov.ID
			} else if errors.Is(err, gorm.ErrRecordNotFound) {
				if err := tx.Create(tr.Government).Error; err != nil {
					return err
				}
				govID = &tr.Government.ID
			} else {
				return err
			}
		}

		// ==== Chamber (upsert + RETURNING) ====
		if tr.Chamber != nil {
			if govID != nil {
				tr.Chamber.GovernmentID = *govID
			}
			if err := tx.Clauses(
				clause.OnConflict{
					Columns: []clause.Column{{Name: "external_id"}},
					DoUpdates: clause.AssignmentColumns([]string{
						"government_id", "name", "name_formal", "official_count",
						"term_limit", "term_length", "inauguration_rules",
						"election_frequency", "election_rules", "vacancy_rules", "remarks",
						"staggered_term",
					}),
				},
				clause.Returning{Columns: []clause.Column{{Name: "id"}}},
			).Create(tr.Chamber).Error; err != nil {
				return err
			}
		}

		// ==== Politician (upsert + RETURNING id) ====
		if tr.Politician != nil {
			assign := map[string]interface{}{
				"first_name":          gorm.Expr("excluded.first_name"),
				"middle_initial":      gorm.Expr("excluded.middle_initial"),
				"last_name":           gorm.Expr("excluded.last_name"),
				"preferred_name":      gorm.Expr("excluded.preferred_name"),
				"name_suffix":         gorm.Expr("excluded.name_suffix"),
				"party":               gorm.Expr("excluded.party"),
				"party_short_name":    gorm.Expr("excluded.party_short_name"),
				"web_form_url":        gorm.Expr("excluded.web_form_url"),
				"urls":                gorm.Expr("excluded.urls"),
				"photo_origin_url":    gorm.Expr(`COALESCE(NULLIF(excluded.photo_origin_url, ''), "essentials"."politicians"."photo_origin_url")`),
				"notes":               gorm.Expr("excluded.notes"),
				"valid_from":          gorm.Expr("excluded.valid_from"),
				"valid_to":            gorm.Expr("excluded.valid_to"),
				"email_addresses":     gorm.Expr("excluded.email_addresses"),
				"office_id":           gorm.Expr("excluded.office_id"),
				"source":              gorm.Expr("excluded.source"),
				"bio_text":            gorm.Expr("excluded.bio_text"),
				"bioguide_id":         gorm.Expr("excluded.bioguide_id"),
				"slug":                gorm.Expr("excluded.slug"),
				"total_years_in_office": gorm.Expr("excluded.total_years_in_office"),
				"external_global_id":  gorm.Expr(`COALESCE(NULLIF(excluded.external_global_id, ''), "essentials"."politicians"."external_global_id")`),
				"is_appointed":        gorm.Expr("excluded.is_appointed"),
				"is_vacant":           gorm.Expr("excluded.is_vacant"),
				"is_off_cycle":        gorm.Expr("excluded.is_off_cycle"),
				"specificity":         gorm.Expr("excluded.specificity"),
				"last_synced":         timestamp,
			}
			if err := tx.
				Omit("Addresses", "Identifiers", "Committees").
				Clauses(
					clause.OnConflict{
						Columns:   []clause.Column{{Name: "external_id"}},
						DoUpdates: clause.Assignments(assign),
					},
					clause.Returning{Columns: []clause.Column{{Name: "id"}}},
				).
				Create(tr.Politician).Error; err != nil {
				return err
			}
		}

		polID = tr.Politician.ID
		if polID == uuid.Nil {
			var persistedPol Politician
			if err := tx.Where("external_id = ?", tr.Politician.ExternalID).First(&persistedPol).Error; err != nil {
				return err
			}
			polID = persistedPol.ID
		}

		// Resolve IDs for office
		var districtID, chamberID uuid.UUID
		if tr.District != nil {
			districtID = tr.District.ID
		} else {
			var ex District
			if err := tx.Where("external_id = ?", off.Office.District.ExternalID).First(&ex).Error; err != nil {
				return err
			}
			districtID = ex.ID
		}
		if tr.Chamber != nil {
			chamberID = tr.Chamber.ID
		} else {
			var ex Chamber
			if err := tx.Where("external_id = ?", off.Office.Chamber.ExternalID).First(&ex).Error; err != nil {
				return err
			}
			chamberID = ex.ID
		}

		// ==== Office ====
		office := Office{
			ID:                   tr.Politician.OfficeID,
			PoliticianID:         polID,
			ChamberID:            chamberID,
			DistrictID:           districtID,
			Title:                off.Office.Title,
			RepresentingState:    off.Office.RepresentingState,
			RepresentingCity:     off.Office.RepresentingCity,
			Description:          off.Office.Description,
			Seats:                off.Office.Seats,
			NormalizedPositionName: off.Office.NormalizedPositionName,
			PartisanType:         off.Office.PartisanType,
			Salary:               off.Office.Salary,
			IsAppointedPosition:  off.Office.IsAppointedPosition,
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "politician_id"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"chamber_id", "district_id", "title", "representing_state", "representing_city",
				"description", "seats", "normalized_position_name", "partisan_type", "salary", "is_appointed_position",
			}),
		}, clause.Returning{Columns: []clause.Column{{Name: "id"}}},
		).Create(&office).Error; err != nil {
			return err
		}

		// Sync politician's office_id to match the actual office ID returned by
		// the upsert. On first insert this is a no-op (same value). On re-import
		// the RETURNING clause gives back the existing office row's ID, fixing
		// the orphaned FK that would otherwise point to a never-persisted UUID.
		if err := tx.Model(&Politician{}).Where("id = ?", polID).
			Update("office_id", office.ID).Error; err != nil {
			return err
		}

		// ==== Addresses ====
		if err := tx.Where("politician_id = ?", polID).Delete(&Address{}).Error; err != nil {
			return err
		}
		if len(tr.Politician.Addresses) > 0 {
			for i := range tr.Politician.Addresses {
				tr.Politician.Addresses[i].PoliticianID = polID
			}
			if err := tx.Create(&tr.Politician.Addresses).Error; err != nil {
				return err
			}
		}

		// ==== Identifiers ====
		if err := tx.Where("politician_id = ?", polID).Delete(&Identifier{}).Error; err != nil {
			return err
		}
		if len(tr.Politician.Identifiers) > 0 {
			batch := make([]Identifier, 0, len(tr.Politician.Identifiers))
			for _, it := range tr.Politician.Identifiers {
				t := strings.TrimSpace(strings.ToLower(it.IdentifierType))
				v := strings.TrimSpace(strings.ToLower(it.IdentifierValue))
				if t == "" || v == "" {
					continue
				}
				batch = append(batch, Identifier{
					PoliticianID:    polID,
					IdentifierType:  t,
					IdentifierValue: v,
				})
			}
			if len(batch) > 0 {
				if err := tx.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "politician_id"}, {Name: "identifier_type"}, {Name: "identifier_value"}},
					DoNothing: true,
				}).Create(&batch).Error; err != nil {
					return err
				}
			}
		}

		// ==== Committees ====
		committeeIDByName := make(map[string]uuid.UUID)
		norm := func(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

		names := make([]string, 0, len(tr.Committees))
		for _, c := range tr.Committees {
			if strings.TrimSpace(c.Name) == "" {
				continue
			}
			names = append(names, c.Name)
		}
		if len(names) > 0 {
			var existing []Committee
			if err := tx.Where("LOWER(name) IN ?", names).Find(&existing).Error; err != nil {
				return err
			}
			for _, ex := range existing {
				committeeIDByName[norm(ex.Name)] = ex.ID
			}
			toCreate := make([]*Committee, 0)
			for _, c := range tr.Committees {
				k := norm(c.Name)
				if _, ok := committeeIDByName[k]; !ok {
					toCreate = append(toCreate, c)
				}
			}
			if len(toCreate) > 0 {
				if err := tx.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "name"}},
					DoNothing: true,
				}).Create(&toCreate).Error; err != nil {
					return err
				}
				for _, c := range toCreate {
					committeeIDByName[norm(c.Name)] = c.ID
				}
			}
		}

		// Build committee joins from normalized committees
		type namePos struct{ Name, Position string }
		posByName := make(map[string]namePos)
		for _, kc := range off.Committees {
			name := strings.TrimSpace(kc.Name)
			if name == "" {
				continue
			}
			k := norm(name)
			pos := strings.TrimSpace(kc.Position)
			if _, ok := posByName[k]; !ok || posByName[k].Position == "" {
				posByName[k] = namePos{Name: name, Position: pos}
			}
		}

		joins := make([]PoliticianCommittee, 0, len(posByName))
		for k, np := range posByName {
			cid, ok := committeeIDByName[k]
			if !ok {
				minimal := Committee{ID: uuid.New(), Name: np.Name}
				if err := tx.Clauses(
					clause.OnConflict{Columns: []clause.Column{{Name: "name"}}, DoNothing: true},
					clause.Returning{Columns: []clause.Column{{Name: "id"}}},
				).Create(&minimal).Error; err != nil {
					return err
				}
				cid = minimal.ID
			}
			joins = append(joins, PoliticianCommittee{
				PoliticianID: polID,
				CommitteeID:  cid,
				Position:     np.Position,
			})
		}
		if len(joins) > 0 {
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "politician_id"}, {Name: "committee_id"}},
				DoUpdates: clause.AssignmentColumns([]string{"position"}),
			}).Create(&joins).Error; err != nil {
				return err
			}
		}

		// ==== Images (delete + recreate) ====
		if err := tx.Where("politician_id = ?", polID).Delete(&PoliticianImage{}).Error; err != nil {
			return err
		}
		if len(tr.Politician.Images) > 0 {
			for i := range tr.Politician.Images {
				tr.Politician.Images[i].PoliticianID = polID
			}
			if err := tx.Create(&tr.Politician.Images).Error; err != nil {
				return err
			}
		}

		// ==== Degrees (delete + recreate) ====
		if err := tx.Where("politician_id = ?", polID).Delete(&Degree{}).Error; err != nil {
			return err
		}
		if len(tr.Politician.Degrees) > 0 {
			for i := range tr.Politician.Degrees {
				tr.Politician.Degrees[i].PoliticianID = polID
			}
			if err := tx.Create(&tr.Politician.Degrees).Error; err != nil {
				return err
			}
		}

		// ==== Experiences (delete + recreate) ====
		if err := tx.Where("politician_id = ?", polID).Delete(&Experience{}).Error; err != nil {
			return err
		}
		if len(tr.Politician.Experiences) > 0 {
			for i := range tr.Politician.Experiences {
				tr.Politician.Experiences[i].PoliticianID = polID
			}
			if err := tx.Create(&tr.Politician.Experiences).Error; err != nil {
				return err
			}
		}

		// ==== Contacts (delete + recreate) ====
		if err := tx.Where("politician_id = ?", polID).Delete(&PoliticianContact{}).Error; err != nil {
			return err
		}
		if len(tr.Politician.Contacts) > 0 {
			for i := range tr.Politician.Contacts {
				tr.Politician.Contacts[i].PoliticianID = polID
			}
			if err := tx.Create(&tr.Politician.Contacts).Error; err != nil {
				return err
			}
		}

		return nil
	})

	return polID, err
}

// upsertCandidacyData upserts candidacy data (endorsements, stances, election records) for a politician.
func upsertCandidacyData(ctx context.Context, politicianID uuid.UUID, candidacies []provider.NormalizedCandidacy) error {
	if len(candidacies) == 0 {
		return nil
	}

	return db.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, cand := range candidacies {
			// ==== ElectionRecord ====
			electionRecord := ElectionRecord{
				PoliticianID:        politicianID,
				CandidacyExternalID: cand.CandidacyExternalID,
				ElectionName:        cand.ElectionName,
				ElectionDate:        cand.ElectionDate,
				PositionName:        cand.PositionName,
				Result:              cand.Result,
				Withdrawn:           cand.Withdrawn,
				PartyName:           cand.PartyName,
				IsPrimary:           cand.IsPrimary,
				IsRunoff:            cand.IsRunoff,
				IsUnexpiredTerm:     cand.IsUnexpiredTerm,
			}

			if err := tx.Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "candidacy_external_id"}},
				DoUpdates: clause.AssignmentColumns([]string{
					"election_name", "election_date", "position_name", "result",
					"withdrawn", "party_name", "is_primary", "is_runoff", "is_unexpired_term",
				}),
			}).Create(&electionRecord).Error; err != nil {
				return fmt.Errorf("upsert election record: %w", err)
			}

			// ==== Endorsements ====
			for _, end := range cand.Endorsements {
				var orgID *uuid.UUID

				// Upsert endorser organization if present
				if end.Organization != nil {
					org := EndorserOrganization{
						ExternalID:  end.Organization.ExternalID,
						Name:        end.Organization.Name,
						Description: end.Organization.Description,
						LogoURL:     end.Organization.LogoURL,
						IssueName:   end.Organization.IssueName,
						State:       end.Organization.State,
					}

					if err := tx.Clauses(clause.OnConflict{
						Columns: []clause.Column{{Name: "external_id"}},
						DoUpdates: clause.AssignmentColumns([]string{
							"name", "description", "logo_url", "issue_name", "state",
						}),
					}).Create(&org).Error; err != nil {
						return fmt.Errorf("upsert endorser organization: %w", err)
					}
					orgID = &org.ID
				}

				// Upsert endorsement
				endorsement := Endorsement{
					PoliticianID:        politicianID,
					OrganizationID:      orgID,
					EndorserString:      end.EndorserString,
					Recommendation:      end.Recommendation,
					Status:              end.Status,
					ElectionDate:        end.ElectionDate,
					CandidacyExternalID: end.CandidacyExternalID,
				}

				if err := tx.Clauses(clause.OnConflict{
					Columns: []clause.Column{
						{Name: "politician_id"},
						{Name: "organization_id"},
						{Name: "candidacy_external_id"},
					},
					DoUpdates: clause.AssignmentColumns([]string{
						"endorser_string", "recommendation", "status", "election_date",
					}),
				}).Create(&endorsement).Error; err != nil {
					return fmt.Errorf("upsert endorsement: %w", err)
				}
			}

			// ==== Stances ====
			for _, stance := range cand.Stances {
				var issueID uuid.UUID

				// Upsert issue (with parent if present)
				if stance.Issue != nil {
					issueID, _ = upsertIssue(tx, stance.Issue)
				}

				// Upsert stance
				politicianStance := PoliticianStance{
					PoliticianID:        politicianID,
					IssueID:             issueID,
					Statement:           stance.Statement,
					ReferenceURL:        stance.ReferenceURL,
					Locale:              stance.Locale,
					CandidacyExternalID: stance.CandidacyExternalID,
					ElectionDate:        stance.ElectionDate,
				}

				if err := tx.Clauses(clause.OnConflict{
					Columns: []clause.Column{
						{Name: "politician_id"},
						{Name: "issue_id"},
						{Name: "candidacy_external_id"},
					},
					DoUpdates: clause.AssignmentColumns([]string{
						"statement", "reference_url", "locale", "election_date",
					}),
				}).Create(&politicianStance).Error; err != nil {
					return fmt.Errorf("upsert politician stance: %w", err)
				}
			}
		}

		return nil
	})
}

// upsertIssue recursively upserts an issue and its parent, returning the issue ID.
func upsertIssue(tx *gorm.DB, issue *provider.NormalizedIssue) (uuid.UUID, error) {
	if issue == nil {
		return uuid.Nil, nil
	}

	var parentID *uuid.UUID

	// Recursively upsert parent first
	if issue.Parent != nil {
		pid, err := upsertIssue(tx, issue.Parent)
		if err != nil {
			return uuid.Nil, err
		}
		parentID = &pid
	}

	// Upsert this issue
	iss := Issue{
		ExternalID:   issue.ExternalID,
		Name:         issue.Name,
		Key:          issue.Key,
		ExpandedText: issue.ExpandedText,
		ParentID:     parentID,
	}

	if err := tx.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "external_id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"name", "key", "expanded_text", "parent_id",
		}),
	}).Create(&iss).Error; err != nil {
		return uuid.Nil, fmt.Errorf("upsert issue: %w", err)
	}

	return iss.ID, nil
}

// ensureCandidacyData checks if candidacy data exists for a politician and triggers
// a background fetch if not. This implements lazy-loading for endorsements, stances, and election history.
func ensureCandidacyData(ctx context.Context, politicianID uuid.UUID, externalGlobalID string, externalID int) {
	// Quick check: do we have any election records for this politician?
	var count int64
	if err := db.DB.Model(&ElectionRecord{}).Where("politician_id = ?", politicianID).Count(&count).Error; err != nil {
		log.Printf("[ensureCandidacyData] DB count error for politician %s: %v", politicianID, err)
		return
	}

	// If we already have candidacy data, skip
	if count > 0 {
		return
	}

	// Skip if no global ID (needed for BallotReady candidacy query)
	if externalGlobalID == "" {
		log.Printf("[ensureCandidacyData] Skipping politician %s (externalID=%d): no external_global_id", politicianID, externalID)
		return
	}

	// Trigger background fetch (non-blocking)
	go func() {
		fetchCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		log.Printf("[ensureCandidacyData] Fetching candidacy data for politician %s (globalID=%s)", politicianID, externalGlobalID)

		// Only BallotReady provider supports candidacy queries
		brClient, ok := Provider.(*ballotready.BallotReadyProvider)
		if !ok {
			log.Printf("[ensureCandidacyData] Skipping: provider is not BallotReady")
			return
		}

		// Fetch candidacy data from BallotReady
		personWithCandidacies, err := brClient.Client().FetchCandidacyData(fetchCtx, externalGlobalID)
		if err != nil {
			log.Printf("[ensureCandidacyData] BallotReady fetch error for %s: %v", externalGlobalID, err)
			return
		}

		// Transform to normalized format
		normalized := ballotready.TransformCandidaciesToNormalized(personWithCandidacies)

		// Upsert to database
		if err := upsertCandidacyData(fetchCtx, politicianID, normalized); err != nil {
			log.Printf("[ensureCandidacyData] Upsert error for politician %s: %v", politicianID, err)
			return
		}

		log.Printf("[ensureCandidacyData] Successfully fetched and stored %d candidacies for politician %s", len(normalized), politicianID)
	}()
}

// warmFederal fetches and caches federal officials (NATIONAL_*).
// These are the same for all ZIP codes in the US.
func warmFederal(ctx context.Context) error {
	warmStart := time.Now()
	log.Printf("[warmFederal] starting federal data warm")

	// Use the provider abstraction if available
	if Provider != nil {
		officials, err := Provider.FetchFederal(ctx)
		if err != nil {
			return fmt.Errorf("provider fetch federal: %w", err)
		}

		log.Printf("[warmFederal] fetched %d federal officials via %s provider", len(officials), Provider.Name())

		for _, off := range officials {
			if _, err := upsertNormalizedOfficial(ctx, off, warmStart); err != nil {
				log.Printf("[warmFederal] upsert error for official %s: %v", off.ExternalID, err)
			}
		}
	} else {
		// Fallback to direct Cicero API call (legacy behavior)
		var sampleZip string
		if err := db.DB.WithContext(ctx).Raw("SELECT zip FROM essentials.zip_caches LIMIT 1").Row().Scan(&sampleZip); err != nil || sampleZip == "" {
			sampleZip = "20001"
		}

		officials, err := fetchCiceroOfficialsByTypes(sampleZip, []string{"NATIONAL_EXEC", "NATIONAL_UPPER", "NATIONAL_LOWER"})
		if err != nil {
			return fmt.Errorf("cicero fetch federal: %w", err)
		}

		log.Printf("[warmFederal] fetched %d federal officials via legacy Cicero", len(officials))

		for _, off := range officials {
			if _, err := upsertOfficial(ctx, off, warmStart); err != nil {
				log.Printf("[warmFederal] upsert error for official %d: %v", off.OfficialID, err)
			}
		}
	}

	// Update federal cache
	if err := db.DB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.Assignments(map[string]any{"last_fetched": warmStart}),
	}).Create(&FederalCache{ID: 1, LastFetched: warmStart}).Error; err != nil {
		return err
	}

	log.Printf("[warmFederal] completed in %dms", time.Since(warmStart).Milliseconds())
	return nil
}

// warmState fetches and caches state officials (STATE_*) for a specific state.
// These are the same for all ZIP codes within the state.
// Accepts a sample ZIP from the state to use for the query.
func warmState(ctx context.Context, state string, sampleZip string) error {
	warmStart := time.Now()
	log.Printf("[warmState] starting state data warm for state=%s using zip=%s", state, sampleZip)

	// Use the provider abstraction if available
	if Provider != nil {
		officials, err := Provider.FetchByState(ctx, state, sampleZip)
		if err != nil {
			return fmt.Errorf("provider fetch state: %w", err)
		}

		log.Printf("[warmState] fetched %d state officials for %s via %s provider", len(officials), state, Provider.Name())

		for _, off := range officials {
			if _, err := upsertNormalizedOfficial(ctx, off, warmStart); err != nil {
				log.Printf("[warmState] upsert error for official %s: %v", off.ExternalID, err)
			}
		}
	} else {
		// Fallback to direct Cicero API call (legacy behavior)
		officials, err := fetchCiceroOfficialsByTypes(sampleZip, []string{"STATE_EXEC", "STATE_UPPER", "STATE_LOWER"})
		if err != nil {
			return fmt.Errorf("cicero fetch state: %w", err)
		}

		// Filter to only officials from the target state
		stateOfficials := make([]CiceroOfficial, 0)
		for _, off := range officials {
			if strings.EqualFold(off.Office.RepresentingState, state) || strings.EqualFold(off.Office.District.State, state) {
				stateOfficials = append(stateOfficials, off)
			}
		}

		log.Printf("[warmState] fetched %d state officials for %s via legacy Cicero", len(stateOfficials), state)

		for _, off := range stateOfficials {
			if _, err := upsertOfficial(ctx, off, warmStart); err != nil {
				log.Printf("[warmState] upsert error for official %d: %v", off.OfficialID, err)
			}
		}
	}

	// Update state cache
	if err := db.DB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "state"}},
		DoUpdates: clause.Assignments(map[string]any{"last_fetched": warmStart}),
	}).Create(&StateCache{State: state, LastFetched: warmStart}).Error; err != nil {
		return err
	}

	log.Printf("[warmState] completed for %s in %dms", state, time.Since(warmStart).Milliseconds())
	return nil
}

// warmLocal fetches and caches local officials (LOCAL_*, COUNTY, SCHOOL, JUDICIAL) for a specific ZIP.
// This is the original warmZip logic but only for local officials.
func warmLocal(ctx context.Context, zip string) error {
	warmStart := time.Now()
	log.Printf("[warmLocal] starting local data warm for zip=%s", zip)

	var zipState string
	touched := make([]uuid.UUID, 0)

	// Use the provider abstraction if available
	if Provider != nil {
		// Fetch ALL officials for this ZIP (federal + state + local).
		// BallotReady returns everything in a single call, so capture it all
		// instead of throwing away federal/state data.
		officials, err := Provider.FetchByZip(ctx, zip, nil)
		if err != nil {
			return fmt.Errorf("provider fetch: %w", err)
		}

		log.Printf("[warmLocal] fetched %d officials (all levels) for %s via %s provider", len(officials), zip, Provider.Name())

		// Build set of local district types for zip_politicians mapping
		localTypes := make(map[string]bool, len(provider.LocalDistrictTypes))
		for _, dt := range provider.LocalDistrictTypes {
			localTypes[dt] = true
		}

		// Extract state from first official
		for _, off := range officials {
			if off.Office.RepresentingState != "" {
				zipState = off.Office.RepresentingState
				break
			}
			if off.Office.District.State != "" {
				zipState = off.Office.District.State
				break
			}
		}

		for _, off := range officials {
			polID, err := upsertNormalizedOfficial(ctx, off, warmStart)
			if err != nil {
				log.Printf("[warmLocal] upsert error for official %s: %v", off.ExternalID, err)
				continue
			}
			if polID != uuid.Nil {
				// Only create zip_politicians mapping for local district types
				if localTypes[off.Office.District.DistrictType] {
					touched = append(touched, polID)
					if err := db.DB.WithContext(ctx).Clauses(clause.OnConflict{
						Columns:   []clause.Column{{Name: "zip"}, {Name: "politician_id"}},
						DoUpdates: clause.Assignments(map[string]any{"last_seen": warmStart}),
					}).Create(&ZipPolitician{
						Zip:          zip,
						PoliticianID: polID,
						LastSeen:     warmStart,
					}).Error; err != nil {
						log.Printf("[warmLocal] zip mapping error: %v", err)
					}
				}
			}
		}

		// Fetch position containment data for BallotReady provider
		if Provider.Name() == "ballotready" {
			if brProvider, ok := Provider.(*ballotready.BallotReadyProvider); ok {
				containments, err := brProvider.Client().FetchPositionContainmentByZip(ctx, zip)
				if err != nil {
					log.Printf("[warmLocal] WARNING: failed to fetch containment data for %s: %v", zip, err)
				} else {
					// Build map: positionDatabaseID → isContained
					containmentMap := make(map[int]bool, len(containments))
					for _, c := range containments {
						containmentMap[c.PositionDatabaseID] = c.IsContained
					}

					// Update is_contained for each politician
					for _, polID := range touched {
						// Fetch the politician's district external_id (which is the position database ID)
						type districtExternalID struct {
							ExternalID int
						}
						var dist districtExternalID
						if err := db.DB.Raw(`
							SELECT d.external_id
							FROM essentials.politicians p
							JOIN essentials.offices o ON o.politician_id = p.id
							JOIN essentials.districts d ON d.id = o.district_id
							WHERE p.id = ?
						`, polID).Scan(&dist).Error; err != nil {
							log.Printf("[warmLocal] failed to get district for politician %s: %v", polID, err)
							continue
						}

						// Look up containment status
						if isContained, found := containmentMap[dist.ExternalID]; found {
							// Update the ZipPolitician record with containment status
							if err := db.DB.Model(&ZipPolitician{}).
								Where("zip = ? AND politician_id = ?", zip, polID).
								Update("is_contained", isContained).Error; err != nil {
								log.Printf("[warmLocal] failed to update containment for %s: %v", polID, err)
							}
						}
					}

					log.Printf("[warmLocal] updated containment data for %d positions in zip %s", len(containmentMap), zip)
				}
			}
		}
	} else {
		// Fallback to direct Cicero API call (legacy behavior)
		officials, err := fetchCiceroOfficialsByTypes(zip, []string{"LOCAL_EXEC", "LOCAL", "COUNTY", "SCHOOL", "JUDICIAL"})
		if err != nil {
			return fmt.Errorf("cicero fetch local: %w", err)
		}

		log.Printf("[warmLocal] fetched %d local officials for %s via legacy Cicero", len(officials), zip)

		if len(officials) > 0 {
			if officials[0].Office.RepresentingState != "" {
				zipState = officials[0].Office.RepresentingState
			} else if officials[0].Office.District.State != "" {
				zipState = officials[0].Office.District.State
			}
		}

		for _, off := range officials {
			polID, err := upsertOfficial(ctx, off, warmStart)
			if err != nil {
				log.Printf("[warmLocal] upsert error for official %d: %v", off.OfficialID, err)
				continue
			}
			if polID != uuid.Nil {
				touched = append(touched, polID)
				if err := db.DB.WithContext(ctx).Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "zip"}, {Name: "politician_id"}},
					DoUpdates: clause.Assignments(map[string]any{"last_seen": warmStart}),
				}).Create(&ZipPolitician{
					Zip:          zip,
					PoliticianID: polID,
					LastSeen:     warmStart,
				}).Error; err != nil {
					log.Printf("[warmLocal] zip mapping error: %v", err)
				}
			}
		}
	}

	// Update zip cache, state cache, federal cache, and cleanup stale mappings
	return db.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "zip"}},
			DoUpdates: clause.Assignments(map[string]any{"last_fetched": warmStart, "state": zipState}),
		}).Create(&ZipCache{Zip: zip, State: zipState, LastFetched: warmStart}).Error; err != nil {
			return err
		}
		// Remove stale local mappings for this zip
		if err := tx.Where("zip = ? AND last_seen < ?", zip, warmStart).Delete(&ZipPolitician{}).Error; err != nil {
			return err
		}
		// Also mark state cache as fresh (we already upserted state officials)
		if zipState != "" {
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "state"}},
				DoUpdates: clause.Assignments(map[string]any{"last_fetched": warmStart}),
			}).Create(&StateCache{State: zipState, LastFetched: warmStart}).Error; err != nil {
				log.Printf("[warmLocal] failed to update state cache: %v", err)
			}
		}
		// Also mark federal cache as fresh (BallotReady returns federal officials in ZIP query)
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "id"}},
			DoUpdates: clause.Assignments(map[string]any{"last_fetched": warmStart}),
		}).Create(&FederalCache{ID: 1, LastFetched: warmStart}).Error; err != nil {
			log.Printf("[warmLocal] failed to update federal cache: %v", err)
		}
		return nil
	})
}

// --- Legacy warmZip function (kept for backwards compatibility during transition) ----

func warmZip(ctx context.Context, zip string) error {
	warmStart := time.Now()

	// timings for logs
	var ciceroMs, transformMs, upsertMs, mapMs float64
	defer func() {
		log.Printf("[warmZip] zip=%s timings ms: cicero=%d transform=%d upserts=%d map=%d total=%d",
			zip,
			int(ciceroMs), int(transformMs), int(upsertMs), int(mapMs),
			int(time.Since(warmStart).Milliseconds()),
		)
	}()

	tFetch := time.Now()
	officials, err := fetchAllCiceroOfficials(zip)
	ciceroMs = float64(time.Since(tFetch).Milliseconds())
	if err != nil {
		return fmt.Errorf("cicero fetch: %w", err)
	}

	if len(officials) == 0 {
		return db.DB.WithContext(ctx).Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "zip"}},
			DoUpdates: clause.Assignments(map[string]any{"last_fetched": warmStart}),
		}).Create(&ZipCache{Zip: zip, LastFetched: warmStart}).Error
	}

	// working set for end-of-warm cleanup
	// (we also stream results immediately; touched is only for optional debugging)
	touched := make([]uuid.UUID, 0, len(officials))

	for _, off := range officials {
		tTr := time.Now()
		tr, err := TransformCiceroData(off)
		transformMs += float64(time.Since(tTr).Milliseconds())
		if err != nil {
			continue
		}

		var polID uuid.UUID

		tUp := time.Now()
		err = db.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			// ==== District (upsert + RETURNING) ====
			if tr.District != nil {
				if err := tx.Clauses(
					clause.OnConflict{
						Columns: []clause.Column{{Name: "external_id"}},
						DoUpdates: clause.AssignmentColumns([]string{
							"ocd_id", "label", "district_type", "district_id", "subtype",
							"state", "city", "num_officials", "valid_from", "valid_to",
						}),
					},
					clause.Returning{Columns: []clause.Column{{Name: "id"}}},
				).Create(tr.District).Error; err != nil {
					return err
				}
			}

			// ==== Government (keep your natural-key lookup) ====
			var govID *uuid.UUID
			if tr.Government != nil {
				var existingGov Government
				err := tx.Where(
					"name = ? AND type = ? AND state = ? AND city = ?",
					tr.Government.Name, tr.Government.Type, tr.Government.State, tr.Government.City,
				).First(&existingGov).Error
				if err == nil {
					govID = &existingGov.ID
				} else if errors.Is(err, gorm.ErrRecordNotFound) {
					if err := tx.Create(tr.Government).Error; err != nil {
						return err
					}
					govID = &tr.Government.ID
				} else {
					return err
				}
			}

			// ==== Chamber (upsert + RETURNING) ====
			if tr.Chamber != nil {
				if govID != nil {
					tr.Chamber.GovernmentID = *govID
				}
				if err := tx.Clauses(
					clause.OnConflict{
						Columns: []clause.Column{{Name: "external_id"}},
						DoUpdates: clause.AssignmentColumns([]string{
							"government_id", "name", "name_formal", "official_count",
							"term_limit", "term_length", "inauguration_rules",
							"election_frequency", "election_rules", "vacancy_rules", "remarks",
						}),
					},
					clause.Returning{Columns: []clause.Column{{Name: "id"}}},
				).Create(tr.Chamber).Error; err != nil {
					return err
				}
			}

			// ==== Politician (upsert + RETURNING id) ====
			if tr.Politician != nil {
				assign := map[string]interface{}{
					"first_name":       gorm.Expr("excluded.first_name"),
					"middle_initial":   gorm.Expr("excluded.middle_initial"),
					"last_name":        gorm.Expr("excluded.last_name"),
					"preferred_name":   gorm.Expr("excluded.preferred_name"),
					"name_suffix":      gorm.Expr("excluded.name_suffix"),
					"party":            gorm.Expr("excluded.party"),
					"web_form_url":     gorm.Expr("excluded.web_form_url"),
					"urls":             gorm.Expr("excluded.urls"),
					"photo_origin_url": gorm.Expr(`COALESCE(NULLIF(excluded.photo_origin_url, ''), "essentials"."politicians"."photo_origin_url")`),
					"notes":            gorm.Expr("excluded.notes"),
					"valid_from":       gorm.Expr("excluded.valid_from"),
					"valid_to":         gorm.Expr("excluded.valid_to"),
					"email_addresses":  gorm.Expr("excluded.email_addresses"),
					"office_id":        gorm.Expr("excluded.office_id"),
				}
				if err := tx.
					Omit("Addresses", "Identifiers", "Committees").
					Clauses(
						clause.OnConflict{
							Columns:   []clause.Column{{Name: "external_id"}},
							DoUpdates: clause.Assignments(assign),
						},
						clause.Returning{Columns: []clause.Column{{Name: "id"}}},
					).
					Create(tr.Politician).Error; err != nil {
					return err
				}
			}

			// ID now available without a separate SELECT
			polID = tr.Politician.ID
			if polID == uuid.Nil {
				// defensive fallback (shouldn't happen)
				var persistedPol Politician
				if err := tx.Where("external_id = ?", off.OfficialID).First(&persistedPol).Error; err != nil {
					return err
				}
				polID = persistedPol.ID
			}

			// Resolve IDs for office
			var districtID, chamberID uuid.UUID
			if tr.District != nil {
				districtID = tr.District.ID
			} else {
				var ex District
				if err := tx.Where("external_id = ?", off.Office.District.SK).First(&ex).Error; err != nil {
					return err
				}
				districtID = ex.ID
			}
			if tr.Chamber != nil {
				chamberID = tr.Chamber.ID
			} else {
				var ex Chamber
				if err := tx.Where("external_id = ?", off.Office.Chamber.ID).First(&ex).Error; err != nil {
					return err
				}
				chamberID = ex.ID
			}

			// ==== Office ====
			office := Office{
				ID:                tr.Politician.OfficeID,
				PoliticianID:      polID,
				ChamberID:         chamberID,
				DistrictID:        districtID,
				Title:             off.Office.Title,
				RepresentingState: off.Office.RepresentingState,
				RepresentingCity:  off.Office.RepresentingCity,
			}
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "politician_id"}},
				DoUpdates: clause.AssignmentColumns([]string{"chamber_id", "district_id", "title", "representing_state", "representing_city"}),
			}).Create(&office).Error; err != nil {
				return err
			}

			// ==== Addresses ====
			if err := tx.Where("politician_id = ?", polID).Delete(&Address{}).Error; err != nil {
				return err
			}
			if len(tr.Politician.Addresses) > 0 {
				for i := range tr.Politician.Addresses {
					tr.Politician.Addresses[i].PoliticianID = polID
				}
				if err := tx.Create(&tr.Politician.Addresses).Error; err != nil {
					return err
				}
			}

			// ==== Identifiers (batch insert, DO NOTHING on dup) ====
			if err := tx.Where("politician_id = ?", polID).Delete(&Identifier{}).Error; err != nil {
				return err
			}
			if len(tr.Politician.Identifiers) > 0 {
				batch := make([]Identifier, 0, len(tr.Politician.Identifiers))
				for _, it := range tr.Politician.Identifiers {
					t := strings.TrimSpace(strings.ToLower(it.IdentifierType))
					v := strings.TrimSpace(strings.ToLower(it.IdentifierValue))
					if t == "" || v == "" {
						continue
					}
					batch = append(batch, Identifier{
						PoliticianID:    polID,
						IdentifierType:  t,
						IdentifierValue: v,
					})
				}
				if len(batch) > 0 {
					if err := tx.Clauses(clause.OnConflict{
						Columns:   []clause.Column{{Name: "politician_id"}, {Name: "identifier_type"}, {Name: "identifier_value"}},
						DoNothing: true,
					}).Create(&batch).Error; err != nil {
						return err
					}
				}
			}

			// ==== Committees (prefetch existing by LOWER(name), then upsert join) ====
			committeeIDByName := make(map[string]uuid.UUID)
			norm := func(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

			names := make([]string, 0, len(tr.Committees))
			for _, c := range tr.Committees {
				if strings.TrimSpace(c.Name) == "" {
					continue
				}
				names = append(names, c.Name)
			}
			if len(names) > 0 {
				var existing []Committee
				if err := tx.Where("LOWER(name) IN ?", names).Find(&existing).Error; err != nil {
					return err
				}
				for _, ex := range existing {
					committeeIDByName[norm(ex.Name)] = ex.ID
				}
				// insert missing
				toCreate := make([]*Committee, 0)
				for _, c := range tr.Committees {
					k := norm(c.Name)
					if _, ok := committeeIDByName[k]; !ok {
						toCreate = append(toCreate, c)
					}
				}
				if len(toCreate) > 0 {
					if err := tx.Clauses(clause.OnConflict{
						Columns:   []clause.Column{{Name: "name"}},
						DoNothing: true,
					}).Create(&toCreate).Error; err != nil {
						return err
					}
					for _, c := range toCreate {
						committeeIDByName[norm(c.Name)] = c.ID
					}
				}
			}

			// dedupe name->position from source
			type namePos struct{ Name, Position string }
			posByName := make(map[string]namePos)
			for _, kc := range off.Committees {
				name := strings.TrimSpace(kc.Name)
				if name == "" {
					continue
				}
				k := norm(name)
				pos := strings.TrimSpace(kc.Position)
				if _, ok := posByName[k]; !ok || posByName[k].Position == "" {
					posByName[k] = namePos{Name: name, Position: pos}
				}
			}

			joins := make([]PoliticianCommittee, 0, len(posByName))
			for k, np := range posByName {
				cid, ok := committeeIDByName[k]
				if !ok {
					// last-resort minimal create
					minimal := Committee{ID: uuid.New(), Name: np.Name}
					if err := tx.Clauses(
						clause.OnConflict{Columns: []clause.Column{{Name: "name"}}, DoNothing: true},
						clause.Returning{Columns: []clause.Column{{Name: "id"}}},
					).Create(&minimal).Error; err != nil {
						return err
					}
					cid = minimal.ID
				}
				joins = append(joins, PoliticianCommittee{
					PoliticianID: polID,
					CommitteeID:  cid,
					Position:     np.Position,
				})
			}
			if len(joins) > 0 {
				if err := tx.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "politician_id"}, {Name: "committee_id"}},
					DoUpdates: clause.AssignmentColumns([]string{"position"}),
				}).Create(&joins).Error; err != nil {
					return err
				}
			}

			return nil
		})
		upsertMs += float64(time.Since(tUp).Milliseconds())
		if err == nil && polID != uuid.Nil {
			touched = append(touched, polID)

			// STREAM the mapping so waiters see partial results
			tMap := time.Now()
			if err := db.DB.WithContext(ctx).Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "zip"}, {Name: "politician_id"}},
				DoUpdates: clause.Assignments(map[string]any{"last_seen": warmStart}),
			}).Create(&ZipPolitician{
				Zip:          zip,
				PoliticianID: polID,
				LastSeen:     warmStart,
			}).Error; err != nil {
				log.Printf("[warmZip] zip=%s map upsert err=%v", zip, err)
			}
			mapMs += float64(time.Since(tMap).Milliseconds())
		}
	}

	// Refresh cache + cleanup (delete only rows not touched this warm)
	return db.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "zip"}},
			DoUpdates: clause.Assignments(map[string]any{"last_fetched": warmStart}),
		}).Create(&ZipCache{Zip: zip, LastFetched: warmStart}).Error; err != nil {
			return err
		}
		// remove stale mappings for this zip
		if err := tx.Where("zip = ? AND last_seen < ?", zip, warmStart).Delete(&ZipPolitician{}).Error; err != nil {
			return err
		}
		return nil
	})
}

// --- Helpers ---------------------------------------------------------------

func isZip5(s string) bool {
	return regexp.MustCompile(`^\d{5}$`).MatchString(s)
}

func addCacheHeaders(w http.ResponseWriter, maxAgeSeconds, swrSeconds int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d, stale-while-revalidate=%d", maxAgeSeconds, swrSeconds))
	w.Header().Set("Vary", "Accept-Encoding") // helpful once you enable gzip/br
}

func addNoStore(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	// Prevent browser/CDN from caching partial payloads
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Vary", "Accept-Encoding")
}

// Generic lock functions for deduping parallel warm operations
func tryAcquireLock(ctx context.Context, key string) bool {
	var ok bool
	if err := db.DB.WithContext(ctx).
		Raw(`SELECT pg_try_advisory_lock(hashtext(?))`, key).
		Row().Scan(&ok); err != nil {
		return false
	}
	return ok
}

func releaseLock(ctx context.Context, key string) {
	var dummy bool
	_ = db.DB.WithContext(ctx).
		Raw(`SELECT pg_advisory_unlock(hashtext(?))`, key).
		Row().Scan(&dummy)
}

// isWarmingInProgress probes an advisory lock without blocking.
// Returns true if warming is in progress (lock is held by another process).
// Returns false if not warming (lock acquired and released, or on error).
func isWarmingInProgress(ctx context.Context, key string) bool {
	var acquired bool
	if err := db.DB.WithContext(ctx).
		Raw(`SELECT pg_try_advisory_lock(hashtext(?))`, key).
		Row().Scan(&acquired); err != nil {
		return false // Assume not warming on error
	}

	if acquired {
		// Lock was acquired, so no warmer was running - release immediately
		var dummy bool
		_ = db.DB.WithContext(ctx).
			Raw(`SELECT pg_advisory_unlock(hashtext(?))`, key).
			Row().Scan(&dummy)
		return false // Not warming
	}

	return true // Lock already held by warmer
}

// getCacheStatus checks cache freshness for all tiers without triggering warmers.
// Returns cache status and error. Uses only indexed cache table lookups.
func getCacheStatus(ctx context.Context, zip string) (CacheStatusResponse, error) {
	const maxAge = 90 * 24 * time.Hour
	now := time.Now()

	// Check federal cache
	var federalCache FederalCache
	federalErr := db.DB.WithContext(ctx).First(&federalCache).Error
	if federalErr != nil && federalErr != gorm.ErrRecordNotFound {
		return CacheStatusResponse{}, fmt.Errorf("database error: %w", federalErr)
	}
	federalFresh := federalErr == nil && now.Sub(federalCache.LastFetched) < maxAge

	// Determine state and check state cache
	var zipCache ZipCache
	zipCacheErr := db.DB.WithContext(ctx).Where("zip = ?", zip).First(&zipCache).Error
	if zipCacheErr != nil && zipCacheErr != gorm.ErrRecordNotFound {
		return CacheStatusResponse{}, fmt.Errorf("database error: %w", zipCacheErr)
	}

	var state string
	if zipCacheErr == nil && zipCache.State != "" {
		state = zipCache.State
	} else {
		// Derive state from ZIP prefix immediately (static lookup, no API call)
		state = zipPrefixToState(zip)
	}

	var stateFresh bool
	if state != "" {
		var stateCache StateCache
		stateErr := db.DB.WithContext(ctx).Where("state = ?", state).First(&stateCache).Error
		if stateErr != nil && stateErr != gorm.ErrRecordNotFound {
			return CacheStatusResponse{}, fmt.Errorf("database error: %w", stateErr)
		}
		stateFresh = stateErr == nil && now.Sub(stateCache.LastFetched) < maxAge
	} else {
		stateFresh = true // No state means no state cache needed
	}

	// Check local/ZIP cache (reuse zipCache from state derivation)
	localFresh := zipCacheErr == nil && now.Sub(zipCache.LastFetched) < maxAge

	// Compute allFresh
	allFresh := federalFresh && stateFresh && localFresh

	// Check warming status for all tiers
	federalWarming := isWarmingInProgress(ctx, "federal")
	stateWarming := false
	if state != "" {
		stateWarming = isWarmingInProgress(ctx, "state-"+state)
	}
	zipWarming := isWarmingInProgress(ctx, "zip-"+zip)
	warming := federalWarming || stateWarming || zipWarming

	return CacheStatusResponse{
		FederalFresh: federalFresh,
		StateFresh:   stateFresh,
		LocalFresh:   localFresh,
		AllFresh:     allFresh,
		Warming:      warming,
	}, nil
}

// Legacy functions for backwards compatibility
func tryAcquireZipWarmLock(ctx context.Context, zip string) bool {
	return tryAcquireLock(ctx, "zip-"+zip)
}

func releaseZipWarmLock(ctx context.Context, zip string) {
	releaseLock(ctx, "zip-"+zip)
}

func waitForDataMin(ctx context.Context, zip string, state string, maxWait, tick time.Duration, minCount int) ([]OfficialOut, bool) {
	ctx, cancel := context.WithTimeout(ctx, maxWait)
	defer cancel()
	t := time.NewTicker(tick)
	defer t.Stop()

	currentState := state

	for {
		select {
		case <-ctx.Done():
			return nil, false
		case <-t.C:
			// Re-read state from ZipCache if not yet known (warmLocal may have discovered it)
			if currentState == "" {
				var zipCache ZipCache
				if err := db.DB.WithContext(ctx).Where("zip = ?", zip).First(&zipCache).Error; err == nil && zipCache.State != "" {
					currentState = zipCache.State
				}
			}
			officials, err := fetchOfficialsFromDB(zip, currentState)
			if err == nil && len(officials) >= minCount {
				return officials, true
			}
		}
	}
}

// waitForData is the legacy version that doesn't pass state
func waitForData(ctx context.Context, zip string, maxWait, tick time.Duration) ([]OfficialOut, bool) {
	return waitForDataMin(ctx, zip, "", maxWait, tick, 1)
}

// fetchOfficialsFromDB returns all officials for a zip (federal + state + local).
// Uses hierarchical queries instead of relying solely on zip_politicians mapping.
func fetchOfficialsFromDB(zip string, state string) ([]OfficialOut, error) {
	type row struct {
		ID                   uuid.UUID
		ExternalID           int
		FirstName            string
		MiddleInitial        string
		LastName             string
		PreferredName        string
		NameSuffix           string
		FullName             string
		Party                string
		PartyShortName       string
		PhotoOriginURL       string
		WebFormURL           string
		URLs                 pq.StringArray `gorm:"type:text[]"`
		EmailAddresses       pq.StringArray `gorm:"type:text[]"`
		OfficeTitle          string
		RepresentingState    string
		RepresentingCity     string
		DistrictType         string
		DistrictLabel        string
		MTFCC                string
		ChamberName          string
		ChamberNameFormal    string
		GovernmentName       string
		ElectionFrequency    string
		IsAppointed          bool
		IsVacant             bool
		IsOffCycle           bool
		Specificity          string
		Seats                int
		NormalizedPositionName string
		PartisanType         string
		Salary               string
		GeoID                string
		IsJudicial           bool
		OCDID                string
		BioText              string
		BioguideID           string
		Slug                 string
		TotalYearsInOffice   int
		OfficeDescription    string
		IsContained          *bool
		ValidFrom            string
		ValidTo              string
	}

	var rows []row

	// Build query that includes federal + state (if known) + local officials
	query := `
		SELECT
		  p.id, p.external_id, p.first_name, p.middle_initial, p.last_name,
		  p.preferred_name, p.name_suffix, p.full_name, p.party, p.party_short_name,
		  COALESCE(p.photo_custom_url, NULLIF(p.photo_origin_url, '')) AS photo_origin_url,
		  p.web_form_url, p.urls, p.email_addresses,
		  o.title AS office_title, o.representing_state, o.representing_city,
		  d.district_type, d.label AS district_label, d.mtfcc,
		  COALESCE(c.name, '') AS chamber_name,
		  COALESCE(c.name_formal, '') AS chamber_name_formal,
		  COALESCE(g.name, '') AS government_name,
		  COALESCE(c.election_frequency, '') AS election_frequency,
		  p.is_appointed, p.is_vacant, p.is_off_cycle, p.specificity,
		  o.seats, o.normalized_position_name, o.partisan_type, o.salary,
		  d.geo_id, d.is_judicial, d.ocd_id,
		  COALESCE(p.bio_text, '') AS bio_text,
		  COALESCE(p.bioguide_id, '') AS bioguide_id,
		  COALESCE(p.slug, '') AS slug,
		  COALESCE(p.total_years_in_office, 0) AS total_years_in_office,
		  COALESCE(o.description, '') AS office_description,
		  zp.is_contained,
		  COALESCE(p.valid_from, '') AS valid_from,
		  COALESCE(p.valid_to, '') AS valid_to
		FROM essentials.politicians p
		JOIN essentials.offices o ON o.politician_id = p.id
		JOIN essentials.districts d ON d.id = o.district_id
		LEFT JOIN essentials.chambers c ON c.id = o.chamber_id
		LEFT JOIN essentials.governments g ON g.id = c.government_id
		LEFT JOIN essentials.zip_politicians zp ON zp.politician_id = p.id AND zp.zip = ?
		WHERE (
		  -- Federal executive officials (President, VP, Cabinet) - nationwide
		  d.district_type = 'NATIONAL_EXEC'
	`

	args := []interface{}{}

	// Add senators and state officials filtered by state
	if state != "" {
		query += `
		  OR (
		    d.district_type IN ('NATIONAL_UPPER', 'NATIONAL_LOWER', 'STATE_EXEC', 'STATE_UPPER', 'STATE_LOWER')
		    AND (o.representing_state = ? OR d.state = ?)
		  )
		`
		args = append(args, state, state)
	}

	// Add local officials mapped to this ZIP
	query += `
		  OR p.id IN (
		    SELECT politician_id FROM essentials.zip_politicians WHERE zip = ?
		  )
		)
		ORDER BY d.district_type, o.title, p.last_name, p.first_name
	`
	// Prepend zip for LEFT JOIN, then append for WHERE clause
	args = append([]interface{}{zip}, args...)
	args = append(args, zip)

	if err := db.DB.Raw(query, args...).Scan(&rows).Error; err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return []OfficialOut{}, nil
	}

	ids := make([]uuid.UUID, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.ID)
	}

	type commRow struct {
		PoliticianID uuid.UUID
		Name         string
		Position     string
		URLs         pq.StringArray `gorm:"type:text[]"`
	}
	var commRows []commRow
	if err := db.DB.Raw(`
		SELECT pc.politician_id, cm.name, pc.position, cm.urls
		FROM essentials.politician_committees pc
		JOIN essentials.committees cm ON cm.id = pc.committee_id
		WHERE pc.politician_id = ANY(?)
	`, pq.Array(ids)).Scan(&commRows).Error; err != nil {
		return nil, err
	}

	byPol := make(map[uuid.UUID][]CommitteeOut, len(ids))
	for _, cr := range commRows {
		byPol[cr.PoliticianID] = append(byPol[cr.PoliticianID], CommitteeOut{
			Name: cr.Name, Position: cr.Position, URLs: []string(cr.URLs),
		})
	}

	// Fetch images
	type imageRow struct {
		PoliticianID uuid.UUID
		URL          string
		Type         string
	}
	var imageRows []imageRow
	if err := db.DB.Raw(`
		SELECT politician_id, url, type
		FROM essentials.politician_images
		WHERE politician_id = ANY(?)
		ORDER BY type
	`, pq.Array(ids)).Scan(&imageRows).Error; err != nil {
		return nil, err
	}

	imagesByPol := make(map[uuid.UUID][]ImageOut)
	for _, img := range imageRows {
		imagesByPol[img.PoliticianID] = append(imagesByPol[img.PoliticianID], ImageOut{
			URL:  img.URL,
			Type: img.Type,
		})
	}

	// Fetch degrees
	type degreeRow struct {
		PoliticianID uuid.UUID
		Degree       string
		Major        string
		School       string
		GradYear     int
	}
	var degreeRows []degreeRow
	if err := db.DB.Raw(`
		SELECT politician_id, degree, major, school, grad_year
		FROM essentials.degrees
		WHERE politician_id = ANY(?)
		ORDER BY grad_year DESC
	`, pq.Array(ids)).Scan(&degreeRows).Error; err != nil {
		return nil, err
	}

	degreesByPol := make(map[uuid.UUID][]DegreeOut)
	for _, deg := range degreeRows {
		degreesByPol[deg.PoliticianID] = append(degreesByPol[deg.PoliticianID], DegreeOut{
			Degree:   deg.Degree,
			Major:    deg.Major,
			School:   deg.School,
			GradYear: deg.GradYear,
		})
	}

	// Fetch experiences
	type experienceRow struct {
		PoliticianID uuid.UUID
		Title        string
		Organization string
		Type         string
		Start        string
		End          string
	}
	var experienceRows []experienceRow
	if err := db.DB.Raw(`
		SELECT politician_id, title, organization, type, start, "end"
		FROM essentials.experiences
		WHERE politician_id = ANY(?)
		ORDER BY start DESC
	`, pq.Array(ids)).Scan(&experienceRows).Error; err != nil {
		return nil, err
	}

	experiencesByPol := make(map[uuid.UUID][]ExperienceOut)
	for _, exp := range experienceRows {
		experiencesByPol[exp.PoliticianID] = append(experiencesByPol[exp.PoliticianID], ExperienceOut{
			Title:        exp.Title,
			Organization: exp.Organization,
			Type:         exp.Type,
			Start:        exp.Start,
			End:          exp.End,
		})
	}

	// Assemble final DTOs
	out := make([]OfficialOut, 0, len(rows))
	for _, r := range rows {
		out = append(out, OfficialOut{
			ID: r.ID, ExternalID: r.ExternalID,
			FirstName: r.FirstName, MiddleInitial: r.MiddleInitial, LastName: r.LastName,
			PreferredName: r.PreferredName, NameSuffix: r.NameSuffix, FullName: r.FullName,
			Party: r.Party, PartyShortName: r.PartyShortName,
			PhotoOriginURL: r.PhotoOriginURL, WebFormURL: r.WebFormURL,
			URLs: []string(r.URLs), EmailAddresses: []string(r.EmailAddresses),
			OfficeTitle: r.OfficeTitle, RepresentingState: r.RepresentingState, RepresentingCity: r.RepresentingCity,
			DistrictType: r.DistrictType, DistrictLabel: r.DistrictLabel,
			MTFCC: r.MTFCC,
			ChamberName: r.ChamberName, ChamberNameFormal: r.ChamberNameFormal,
			GovernmentName:       r.GovernmentName,
			IsElected:            isElectedPosition(r.DistrictType, r.OfficeTitle, r.ElectionFrequency),
			IsAppointed:          r.IsAppointed,
			IsVacant:             r.IsVacant,
			IsOffCycle:           r.IsOffCycle,
			Specificity:          r.Specificity,
			ElectionFrequency:    r.ElectionFrequency,
			Seats:                r.Seats,
			NormalizedPositionName: r.NormalizedPositionName,
			PartisanType:         r.PartisanType,
			Salary:               r.Salary,
			GeoID:                r.GeoID,
			IsJudicial:           r.IsJudicial,
			OCDID:                r.OCDID,
			Committees:           byPol[r.ID],
			BioText:              r.BioText,
			BioguideID:           r.BioguideID,
			Slug:                 r.Slug,
			TotalYearsInOffice:   r.TotalYearsInOffice,
			OfficeDescription:    r.OfficeDescription,
			Images:               imagesByPol[r.ID],
			Degrees:              degreesByPol[r.ID],
			Experiences:          experiencesByPol[r.ID],
			IsContained:          r.IsContained,
			TermStart:            r.ValidFrom,
			TermEnd:              r.ValidTo,
		})
	}

	return out, nil
}

// fetchStatewideFromDB returns only truly statewide officials: NATIONAL_EXEC (president, VP,
// cabinet), NATIONAL_UPPER (US senators), and STATE_EXEC (governor, lt gov, AG). These
// officials represent everyone in the state. District-specific types (NATIONAL_LOWER,
// STATE_UPPER, STATE_LOWER) are excluded — they should come from geofence matches.
func fetchStatewideFromDB(state string) ([]OfficialOut, error) {
	return fetchFederalAndStateFromDBFiltered(state, []string{"NATIONAL_EXEC", "NATIONAL_UPPER", "STATE_EXEC"})
}

// fetchFederalAndStateFromDB returns federal officials (nationwide) plus state-level officials
// for the given state from the DB cache. Unlike fetchOfficialsFromDB, this does NOT require
// a ZIP code or the zip_politicians mapping — it queries by district type directly.
func fetchFederalAndStateFromDB(state string) ([]OfficialOut, error) {
	return fetchFederalAndStateFromDBFiltered(state, []string{"NATIONAL_UPPER", "NATIONAL_LOWER", "STATE_EXEC", "STATE_UPPER", "STATE_LOWER"})
}

// fetchFederalAndStateFromDBFiltered is the shared implementation that accepts specific
// district types to filter by (in addition to always including NATIONAL_EXEC).
func fetchFederalAndStateFromDBFiltered(state string, stateFilteredTypes []string) ([]OfficialOut, error) {
	type row struct {
		ID                   uuid.UUID
		ExternalID           int
		FirstName            string
		MiddleInitial        string
		LastName             string
		PreferredName        string
		NameSuffix           string
		FullName             string
		Party                string
		PartyShortName       string
		PhotoOriginURL       string
		WebFormURL           string
		URLs                 pq.StringArray `gorm:"type:text[]"`
		EmailAddresses       pq.StringArray `gorm:"type:text[]"`
		OfficeTitle          string
		RepresentingState    string
		RepresentingCity     string
		DistrictType         string
		DistrictLabel        string
		MTFCC                string
		ChamberName          string
		ChamberNameFormal    string
		GovernmentName       string
		ElectionFrequency    string
		IsAppointed          bool
		IsVacant             bool
		IsOffCycle           bool
		Specificity          string
		Seats                int
		NormalizedPositionName string
		PartisanType         string
		Salary               string
		GeoID                string
		IsJudicial           bool
		OCDID                string
		BioText              string
		BioguideID           string
		Slug                 string
		TotalYearsInOffice   int
		OfficeDescription    string
		ValidFrom            string
		ValidTo              string
	}

	var rows []row

	query := `
		SELECT
		  p.id, p.external_id, p.first_name, p.middle_initial, p.last_name,
		  p.preferred_name, p.name_suffix, p.full_name, p.party, p.party_short_name,
		  COALESCE(p.photo_custom_url, NULLIF(p.photo_origin_url, '')) AS photo_origin_url,
		  p.web_form_url, p.urls, p.email_addresses,
		  o.title AS office_title, o.representing_state, o.representing_city,
		  d.district_type, d.label AS district_label, d.mtfcc,
		  c.name AS chamber_name, c.name_formal AS chamber_name_formal,
		  g.name AS government_name,
		  COALESCE(c.election_frequency, '') AS election_frequency,
		  p.is_appointed, p.is_vacant, p.is_off_cycle, p.specificity,
		  o.seats, o.normalized_position_name, o.partisan_type, o.salary,
		  d.geo_id, d.is_judicial, d.ocd_id,
		  COALESCE(p.bio_text, '') AS bio_text,
		  COALESCE(p.bioguide_id, '') AS bioguide_id,
		  COALESCE(p.slug, '') AS slug,
		  COALESCE(p.total_years_in_office, 0) AS total_years_in_office,
		  COALESCE(o.description, '') AS office_description,
		  COALESCE(p.valid_from, '') AS valid_from,
		  COALESCE(p.valid_to, '') AS valid_to
		FROM essentials.politicians p
		JOIN essentials.offices o ON o.politician_id = p.id
		JOIN essentials.districts d ON d.id = o.district_id
		JOIN essentials.chambers c ON c.id = o.chamber_id
		JOIN essentials.governments g ON g.id = c.government_id
		WHERE (
		  d.district_type = 'NATIONAL_EXEC'
		  OR (
		    d.district_type = ANY(?)
		    AND (o.representing_state = ? OR d.state = ?)
		  )
		)
		ORDER BY d.district_type, o.title, p.last_name, p.first_name
	`

	if err := db.DB.Raw(query, pq.Array(stateFilteredTypes), state, state).Scan(&rows).Error; err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return []OfficialOut{}, nil
	}

	ids := make([]uuid.UUID, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.ID)
	}

	type commRow struct {
		PoliticianID uuid.UUID
		Name         string
		Position     string
		URLs         pq.StringArray `gorm:"type:text[]"`
	}
	var commRows []commRow
	if err := db.DB.Raw(`
		SELECT pc.politician_id, cm.name, pc.position, cm.urls
		FROM essentials.politician_committees pc
		JOIN essentials.committees cm ON cm.id = pc.committee_id
		WHERE pc.politician_id = ANY(?)
	`, pq.Array(ids)).Scan(&commRows).Error; err != nil {
		return nil, err
	}

	byPol := make(map[uuid.UUID][]CommitteeOut, len(ids))
	for _, cr := range commRows {
		byPol[cr.PoliticianID] = append(byPol[cr.PoliticianID], CommitteeOut{
			Name: cr.Name, Position: cr.Position, URLs: []string(cr.URLs),
		})
	}

	// Fetch images
	type imageRow struct {
		PoliticianID uuid.UUID
		URL          string
		Type         string
	}
	var imageRows []imageRow
	if err := db.DB.Raw(`
		SELECT politician_id, url, type
		FROM essentials.politician_images
		WHERE politician_id = ANY(?)
		ORDER BY type
	`, pq.Array(ids)).Scan(&imageRows).Error; err != nil {
		return nil, err
	}

	imagesByPol := make(map[uuid.UUID][]ImageOut)
	for _, img := range imageRows {
		imagesByPol[img.PoliticianID] = append(imagesByPol[img.PoliticianID], ImageOut{
			URL:  img.URL,
			Type: img.Type,
		})
	}

	// Fetch degrees
	type degreeRow struct {
		PoliticianID uuid.UUID
		Degree       string
		Major        string
		School       string
		GradYear     int
	}
	var degreeRows []degreeRow
	if err := db.DB.Raw(`
		SELECT politician_id, degree, major, school, grad_year
		FROM essentials.degrees
		WHERE politician_id = ANY(?)
		ORDER BY grad_year DESC
	`, pq.Array(ids)).Scan(&degreeRows).Error; err != nil {
		return nil, err
	}

	degreesByPol := make(map[uuid.UUID][]DegreeOut)
	for _, deg := range degreeRows {
		degreesByPol[deg.PoliticianID] = append(degreesByPol[deg.PoliticianID], DegreeOut{
			Degree:   deg.Degree,
			Major:    deg.Major,
			School:   deg.School,
			GradYear: deg.GradYear,
		})
	}

	// Fetch experiences
	type experienceRow struct {
		PoliticianID uuid.UUID
		Title        string
		Organization string
		Type         string
		Start        string
		End          string
	}
	var experienceRows []experienceRow
	if err := db.DB.Raw(`
		SELECT politician_id, title, organization, type, start, "end"
		FROM essentials.experiences
		WHERE politician_id = ANY(?)
		ORDER BY start DESC
	`, pq.Array(ids)).Scan(&experienceRows).Error; err != nil {
		return nil, err
	}

	experiencesByPol := make(map[uuid.UUID][]ExperienceOut)
	for _, exp := range experienceRows {
		experiencesByPol[exp.PoliticianID] = append(experiencesByPol[exp.PoliticianID], ExperienceOut{
			Title:        exp.Title,
			Organization: exp.Organization,
			Type:         exp.Type,
			Start:        exp.Start,
			End:          exp.End,
		})
	}

	out := make([]OfficialOut, 0, len(rows))
	for _, r := range rows {
		out = append(out, OfficialOut{
			ID: r.ID, ExternalID: r.ExternalID,
			FirstName: r.FirstName, MiddleInitial: r.MiddleInitial, LastName: r.LastName,
			PreferredName: r.PreferredName, NameSuffix: r.NameSuffix, FullName: r.FullName,
			Party: r.Party, PartyShortName: r.PartyShortName,
			PhotoOriginURL: r.PhotoOriginURL, WebFormURL: r.WebFormURL,
			URLs: []string(r.URLs), EmailAddresses: []string(r.EmailAddresses),
			OfficeTitle: r.OfficeTitle, RepresentingState: r.RepresentingState, RepresentingCity: r.RepresentingCity,
			DistrictType: r.DistrictType, DistrictLabel: r.DistrictLabel,
			MTFCC: r.MTFCC,
			ChamberName: r.ChamberName, ChamberNameFormal: r.ChamberNameFormal,
			GovernmentName:       r.GovernmentName,
			IsElected:            isElectedPosition(r.DistrictType, r.OfficeTitle, r.ElectionFrequency),
			IsAppointed:          r.IsAppointed,
			IsVacant:             r.IsVacant,
			IsOffCycle:           r.IsOffCycle,
			Specificity:          r.Specificity,
			ElectionFrequency:    r.ElectionFrequency,
			Seats:                r.Seats,
			NormalizedPositionName: r.NormalizedPositionName,
			PartisanType:         r.PartisanType,
			Salary:               r.Salary,
			GeoID:                r.GeoID,
			IsJudicial:           r.IsJudicial,
			OCDID:                r.OCDID,
			Committees:           byPol[r.ID],
			BioText:              r.BioText,
			BioguideID:           r.BioguideID,
			Slug:                 r.Slug,
			TotalYearsInOffice:   r.TotalYearsInOffice,
			OfficeDescription:    r.OfficeDescription,
			Images:               imagesByPol[r.ID],
			Degrees:              degreesByPol[r.ID],
			Experiences:          experiencesByPol[r.ID],
			TermStart:            r.ValidFrom,
			TermEnd:              r.ValidTo,
		})
	}

	return out, nil
}

// isElectedPosition determines if a position is elected based on district type, title, and election frequency.
func isElectedPosition(districtType, officeTitle, electionFrequency string) bool {
	dt := strings.ToUpper(districtType)
	title := strings.ToLower(officeTitle)

	// Legislative positions are always elected
	if dt == "NATIONAL_UPPER" || dt == "NATIONAL_LOWER" ||
		dt == "STATE_UPPER" || dt == "STATE_LOWER" {
		return true
	}

	// Executive elected positions by title
	electedTitles := []string{
		"president", "vice president",
		"governor", "lieutenant governor", "lt. governor",
		"mayor", "county executive",
		"attorney general", "secretary of state", "treasurer",
		"comptroller", "auditor", "superintendent",
	}
	for _, et := range electedTitles {
		if strings.Contains(title, et) {
			return true
		}
	}

	// If election_frequency is set and not empty, it's likely an elected position
	if electionFrequency != "" && !strings.Contains(strings.ToLower(electionFrequency), "appointed") {
		return true
	}

	return false
}

// fetchCiceroOfficialsByTypes returns officials for a ZIP filtered by specific district types.
// This is a legacy fallback used when the provider is not initialized.
// It uses the cicero package's client directly.
func fetchCiceroOfficialsByTypes(zip string, districtTypes []string) ([]CiceroOfficial, error) {
	client := cicero.NewClient(provider.LoadFromEnv().CiceroKey)
	return client.FetchOfficialsByZip(context.Background(), zip, districtTypes)
}

// fetchAllCiceroOfficials returns all officials for a ZIP (all district types).
// Kept for backwards compatibility with the legacy warmZip function.
func fetchAllCiceroOfficials(zip string) ([]CiceroOfficial, error) {
	return fetchCiceroOfficialsByTypes(zip, provider.AllDistrictTypes)
}

// addressSearchNamespace is a fixed UUID v5 namespace for generating deterministic IDs
// from external IDs when bypassing the database.
var addressSearchNamespace = uuid.MustParse("b7e9c4a1-3f82-4d56-9e01-af2c68d17b34")

// normalizedToOfficialOut converts a NormalizedOfficial directly to an OfficialOut,
// bypassing the database. Used for address-based lookups where results are returned immediately.
func normalizedToOfficialOut(off provider.NormalizedOfficial) OfficialOut {
	extID, _ := strconv.Atoi(off.ExternalID)

	// Generate a deterministic UUID from the external ID so React keys are unique/stable
	id := uuid.NewSHA1(addressSearchNamespace, []byte(off.ExternalID))

	// Build full name from parts if not available
	fullName := strings.TrimSpace(off.FirstName + " " + off.LastName)

	committees := make([]CommitteeOut, 0, len(off.Committees))
	for _, c := range off.Committees {
		committees = append(committees, CommitteeOut{
			Name:     c.Name,
			Position: c.Position,
			URLs:     c.URLs,
		})
	}

	// Map images
	images := make([]ImageOut, 0, len(off.Images))
	for _, img := range off.Images {
		images = append(images, ImageOut{
			URL:  img.URL,
			Type: img.Type,
		})
	}

	// Map degrees
	degrees := make([]DegreeOut, 0, len(off.Degrees))
	for _, deg := range off.Degrees {
		degrees = append(degrees, DegreeOut{
			Degree:   deg.Degree,
			Major:    deg.Major,
			School:   deg.School,
			GradYear: deg.GradYear,
		})
	}

	// Map experiences
	experiences := make([]ExperienceOut, 0, len(off.Experiences))
	for _, exp := range off.Experiences {
		experiences = append(experiences, ExperienceOut{
			Title:        exp.Title,
			Organization: exp.Organization,
			Type:         exp.Type,
			Start:        exp.Start,
			End:          exp.End,
		})
	}

	return OfficialOut{
		ID:                   id,
		ExternalID:           extID,
		FirstName:            off.FirstName,
		MiddleInitial:        off.MiddleInitial,
		LastName:             off.LastName,
		PreferredName:        off.PreferredName,
		NameSuffix:           off.NameSuffix,
		FullName:             fullName,
		Party:                off.Party,
		PartyShortName:       off.PartyShortName,
		PhotoOriginURL:       off.PhotoOriginURL,
		WebFormURL:           off.WebFormURL,
		URLs:                 off.URLs,
		EmailAddresses:       off.EmailAddresses,
		OfficeTitle:          off.Office.Title,
		RepresentingState:    off.Office.RepresentingState,
		RepresentingCity:     off.Office.RepresentingCity,
		DistrictType:         off.Office.District.DistrictType,
		DistrictLabel:        off.Office.District.Label,
		MTFCC:                off.Office.District.MTFCC,
		ChamberName:          off.Office.Chamber.Name,
		ChamberNameFormal:    off.Office.Chamber.NameFormal,
		GovernmentName:       off.Office.Chamber.Government.Name,
		IsElected:            isElectedPosition(off.Office.District.DistrictType, off.Office.Title, off.Office.Chamber.ElectionFrequency),
		IsAppointed:          off.IsAppointed,
		IsVacant:             off.IsVacant,
		IsOffCycle:           off.IsOffCycle,
		Specificity:          off.Specificity,
		ElectionFrequency:    off.Office.Chamber.ElectionFrequency,
		Seats:                off.Office.Seats,
		NormalizedPositionName: off.Office.NormalizedPositionName,
		PartisanType:         off.Office.PartisanType,
		Salary:               off.Office.Salary,
		GeoID:                off.Office.District.GeoID,
		IsJudicial:           off.Office.District.IsJudicial,
		OCDID:                off.Office.District.OCDID,
		Committees:           committees,
		BioText:              off.BioText,
		BioguideID:           off.BioguideID,
		Slug:                 off.Slug,
		TotalYearsInOffice:   off.TotalYearsInOffice,
		OfficeDescription:    off.Office.Description,
		Images:               images,
		Degrees:              degrees,
		Experiences:          experiences,
		TermStart:            off.ValidFrom,
		TermEnd:              off.ValidTo,
	}
}

// SearchPoliticians handles POST /politicians/search.
// Accepts {"query": "..."} and detects ZIP vs address.
// ZIP queries delegate to the existing warm/cache flow.
// Address queries call BallotReady directly for precise results.
func SearchPoliticians(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Query string `json:"query"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	query := strings.TrimSpace(body.Query)
	if query == "" {
		http.Error(w, "Query is required", http.StatusBadRequest)
		return
	}

	// If the query is a 5-digit ZIP, delegate to existing ZIP flow
	if isZip5(query) {
		handleZipLookup(w, r, query)
		return
	}

	// Try geofence-based lookup: Google geocoding → PostGIS point-in-polygon → database
	if GeoClient != nil {
		geoResult, geoErr := GeoClient.Geocode(r.Context(), query)
		if geoErr == nil {
			log.Printf("[SearchPoliticians] Google geocoded %q → (%.6f, %.6f) %s", query, geoResult.Lat, geoResult.Lng, geoResult.Formatted)

			// Find all geofences (districts) that contain this point
			geoMatches, err := FindGeoIDsByPoint(r.Context(), geoResult.Lat, geoResult.Lng)
			if err != nil {
				log.Printf("[SearchPoliticians] geofence lookup error: %v", err)
			} else if len(geoMatches) > 0 {
				geoIDs := make([]string, len(geoMatches))
				for i, m := range geoMatches {
					geoIDs[i] = m.GeoID + "(" + m.MTFCC + ")"
				}
				log.Printf("[SearchPoliticians] found %d geofences for point: %v", len(geoMatches), geoIDs)

				// Look up politicians by geo matches (with MTFCC disambiguation)
				officials, err := FindPoliticiansByGeoMatches(r.Context(), geoMatches)
				if err != nil {
					log.Printf("[SearchPoliticians] politician lookup error: %v", err)
				} else if len(officials) > 0 {
					// Supplement geofence results with federal + state officials from DB cache
					geoState := strings.ToUpper(geoResult.State)
					if geoState == "" {
						// Fall back to state from geofence-matched officials
						for _, o := range officials {
							if o.RepresentingState != "" {
								geoState = strings.ToUpper(o.RepresentingState)
								break
							}
						}
					}

					if geoState != "" {
						seenExtIDs := make(map[int]bool, len(officials))
						for _, o := range officials {
							if o.ExternalID != 0 {
								seenExtIDs[o.ExternalID] = true
							}
						}

						supplemental, supErr := fetchStatewideFromDB(geoState)
						if supErr == nil {
							for _, s := range supplemental {
								if !seenExtIDs[s.ExternalID] {
									officials = append(officials, s)
									seenExtIDs[s.ExternalID] = true
								}
							}
						} else {
							log.Printf("[SearchPoliticians] geofence supplemental fetch error: %v", supErr)
						}

						// Kick background warmers if federal/state caches are stale
						now := time.Now()
						const maxAge = 90 * 24 * time.Hour

						var federalCache FederalCache
						if err := db.DB.First(&federalCache).Error; err != nil || now.Sub(federalCache.LastFetched) >= maxAge {
							if tryAcquireLock(r.Context(), "federal") {
								go func() {
									defer releaseLock(context.Background(), "federal")
									if err := warmFederal(context.Background()); err != nil {
										log.Printf("[SearchPoliticians] warmFederal err=%v", err)
									}
								}()
							}
						}

						var stateCache StateCache
						if err := db.DB.Where("state = ?", geoState).First(&stateCache).Error; err != nil || now.Sub(stateCache.LastFetched) >= maxAge {
							if tryAcquireLock(r.Context(), "state-"+geoState) {
								go func() {
									defer releaseLock(context.Background(), "state-"+geoState)
									sampleZip := geoResult.Zip
									if sampleZip == "" {
										if err := db.DB.Raw("SELECT zip FROM essentials.zip_caches WHERE state = ? LIMIT 1", geoState).Row().Scan(&sampleZip); err != nil || sampleZip == "" {
											sampleZip = "20001"
										}
									}
									if err := warmState(context.Background(), geoState, sampleZip); err != nil {
										log.Printf("[SearchPoliticians] warmState err=%v", err)
									}
								}()
							}
						}
					}

					log.Printf("[SearchPoliticians] ✓ Served %d officials (%d geofence + supplemental) for state=%s", len(officials), len(geoMatches), geoState)
					w.Header().Set("X-Data-Status", "fresh-local")
					w.Header().Set("X-Geofence-Count", fmt.Sprintf("%d", len(geoMatches)))
					writeJSON(w, officials)
					return
				} else {
					log.Printf("[SearchPoliticians] no politicians found for geo-IDs (area not pre-populated)")
				}
			} else {
				log.Printf("[SearchPoliticians] no geofences found at (%.6f, %.6f) — area not imported yet", geoResult.Lat, geoResult.Lng)
			}
		} else {
			log.Printf("[SearchPoliticians] Google geocoding failed for %q: %v", query, geoErr)
		}
	}

	// Fallback: BallotReady address lookup (for areas not pre-populated with geofence data)
	brProvider, ok := Provider.(*ballotready.BallotReadyProvider)
	if !ok {
		http.Error(w, "Address search requires the BallotReady provider", http.StatusServiceUnavailable)
		return
	}

	ctx := r.Context()
	nodes, err := brProvider.Client().FetchOfficeHoldersByAddress(ctx, query)
	if err != nil {
		log.Printf("[SearchPoliticians] address lookup error: %v", err)
		http.Error(w, "Failed to fetch politicians for address", http.StatusBadGateway)
		return
	}

	normalized := ballotready.TransformBatch(nodes)

	// 1. Build response immediately (fast, uses SHA1-based UUIDs)
	out := make([]OfficialOut, 0, len(normalized))
	for _, off := range normalized {
		out = append(out, normalizedToOfficialOut(off))
	}

	// 2. Fast batch lookup: resolve real DB UUIDs for known politicians
	extIDs := make([]int, 0, len(out))
	for _, o := range out {
		if o.ExternalID != 0 {
			extIDs = append(extIDs, o.ExternalID)
		}
	}
	if len(extIDs) > 0 {
		type idRow struct {
			ID         uuid.UUID
			ExternalID int
		}
		var rows []idRow
		db.DB.Raw(`SELECT id, external_id FROM essentials.politicians WHERE external_id = ANY(?)`, pq.Array(extIDs)).Scan(&rows)
		lookup := make(map[int]uuid.UUID, len(rows))
		for _, r := range rows {
			lookup[r.ExternalID] = r.ID
		}
		for i := range out {
			if dbID, ok := lookup[out[i].ExternalID]; ok {
				out[i].ID = dbID
			}
		}
	}

	// 3. Supplement with federal + state officials from DB cache
	var searchState string
	for _, o := range out {
		if o.RepresentingState != "" {
			searchState = strings.ToUpper(o.RepresentingState)
			break
		}
	}

	if searchState != "" {
		seenExtIDs := make(map[int]bool, len(out))
		for _, o := range out {
			if o.ExternalID != 0 {
				seenExtIDs[o.ExternalID] = true
			}
		}

		supplemental, err := fetchFederalAndStateFromDB(searchState)
		if err == nil {
			for _, s := range supplemental {
				if !seenExtIDs[s.ExternalID] {
					out = append(out, s)
					seenExtIDs[s.ExternalID] = true
				}
			}
		}

		// Kick background warmers if caches are stale
		now := time.Now()
		const maxAge = 90 * 24 * time.Hour

		var federalCache FederalCache
		if err := db.DB.First(&federalCache).Error; err != nil || now.Sub(federalCache.LastFetched) >= maxAge {
			if tryAcquireLock(ctx, "federal") {
				go func() {
					defer releaseLock(context.Background(), "federal")
					if err := warmFederal(context.Background()); err != nil {
						log.Printf("[SearchPoliticians] warmFederal err=%v", err)
					}
				}()
			}
		}

		var stateCache StateCache
		if err := db.DB.Where("state = ?", searchState).First(&stateCache).Error; err != nil || now.Sub(stateCache.LastFetched) >= maxAge {
			if tryAcquireLock(ctx, "state-"+searchState) {
				go func() {
					defer releaseLock(context.Background(), "state-"+searchState)
					// Use a common ZIP from BallotReady results for the state warmer
					sampleZip := ""
					if err := db.DB.Raw("SELECT zip FROM essentials.zip_caches WHERE state = ? LIMIT 1", searchState).Row().Scan(&sampleZip); err != nil || sampleZip == "" {
						sampleZip = "20001" // fallback
					}
					if err := warmState(context.Background(), searchState, sampleZip); err != nil {
						log.Printf("[SearchPoliticians] warmState err=%v", err)
					}
				}()
			}
		}
	}

	// 4. Background upsert (non-blocking)
	go func() {
		bgCtx := context.Background()
		ts := time.Now()
		for _, off := range normalized {
			if _, err := upsertNormalizedOfficial(bgCtx, off, ts); err != nil {
				log.Printf("[SearchPoliticians] bg upsert error for %s: %v", off.ExternalID, err)
			}
		}
	}()

	w.Header().Set("X-Data-Status", "fresh")
	writeJSON(w, out)
}

// PoliticianProfileOut is the enriched response for a single politician profile.
type PoliticianProfileOut struct {
	OfficialOut
	Addresses   []Address    `json:"addresses"`
	Identifiers []Identifier `json:"identifiers"`
	Notes       []string     `json:"notes"`
}

func GetPoliticianByID(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "Missing id parameter", http.StatusBadRequest)
		return
	}

	// Validate UUID format
	parsedID, err := uuid.Parse(id)
	if err != nil {
		http.Error(w, "Invalid id format", http.StatusBadRequest)
		return
	}

	// 1. Fetch politician with joined office/district/chamber/government (same pattern as GetAllPoliticians)
	type row struct {
		ID                   uuid.UUID
		ExternalID           int
		ExternalGlobalID     string
		FirstName            string
		MiddleInitial        string
		LastName             string
		PreferredName        string
		NameSuffix           string
		FullName             string
		Party                string
		PartyShortName       string
		PhotoOriginURL       string
		WebFormURL           string
		URLs                 pq.StringArray `gorm:"type:text[]"`
		EmailAddresses       pq.StringArray `gorm:"type:text[]"`
		Notes                pq.StringArray `gorm:"type:text[]"`
		OfficeTitle          string
		RepresentingState    string
		RepresentingCity     string
		DistrictType         string
		DistrictLabel        string
		MTFCC                string
		ChamberName          string
		ChamberNameFormal    string
		GovernmentName       string
		ElectionFrequency    string
		IsAppointed          bool
		IsVacant             bool
		IsOffCycle           bool
		Specificity          string
		Seats                int
		NormalizedPositionName string
		PartisanType         string
		Salary               string
		GeoID                string
		IsJudicial           bool
		OCDID                string
		BioText              string
		BioguideID           string
		Slug                 string
		TotalYearsInOffice   int
		OfficeDescription    string
		ValidFrom            string
		ValidTo              string
	}

	var r0 row
	if err := db.DB.Raw(`
		SELECT
		  p.id, p.external_id, COALESCE(p.external_global_id, '') AS external_global_id,
		  p.first_name, p.middle_initial, p.last_name,
		  p.preferred_name, p.name_suffix, p.full_name, p.party, p.party_short_name,
		  COALESCE(p.photo_custom_url, NULLIF(p.photo_origin_url, '')) AS photo_origin_url,
		  p.web_form_url, p.urls, p.email_addresses, p.notes,
		  COALESCE(p.bio_text, '') AS bio_text,
		  COALESCE(p.bioguide_id, '') AS bioguide_id,
		  COALESCE(p.slug, '') AS slug,
		  COALESCE(p.total_years_in_office, 0) AS total_years_in_office,
		  p.is_appointed, p.is_vacant, p.is_off_cycle, p.specificity,
		  o.title AS office_title, o.representing_state, o.representing_city,
		  COALESCE(o.description, '') AS office_description,
		  o.seats, o.normalized_position_name, o.partisan_type, o.salary,
		  d.district_type, d.label AS district_label, d.mtfcc,
		  d.geo_id, d.is_judicial, d.ocd_id,
		  c.name AS chamber_name, c.name_formal AS chamber_name_formal,
		  g.name AS government_name,
		  COALESCE(c.election_frequency, '') AS election_frequency,
		  COALESCE(p.valid_from, '') AS valid_from,
		  COALESCE(p.valid_to, '') AS valid_to
		FROM essentials.politicians p
		JOIN essentials.offices o ON o.politician_id = p.id
		JOIN essentials.districts d ON d.id = o.district_id
		JOIN essentials.chambers c ON c.id = o.chamber_id
		JOIN essentials.governments g ON g.id = c.government_id
		WHERE p.id = ?
	`, parsedID).Scan(&r0).Error; err != nil {
		http.Error(w, "DB fetch error", http.StatusInternalServerError)
		return
	}
	if r0.ID == uuid.Nil {
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}

	// 2. Fetch addresses
	var addresses []Address
	db.DB.Where("politician_id = ?", parsedID).Find(&addresses)

	// 3. Fetch identifiers
	var identifiers []Identifier
	db.DB.Where("politician_id = ?", parsedID).Find(&identifiers)

	// 4. Fetch committees with resolved names
	type commRow struct {
		Name     string
		Position string
		URLs     pq.StringArray `gorm:"type:text[]"`
	}
	var commRows []commRow
	db.DB.Raw(`
		SELECT cm.name, pc.position, cm.urls
		FROM essentials.politician_committees pc
		JOIN essentials.committees cm ON cm.id = pc.committee_id
		WHERE pc.politician_id = ?
	`, parsedID).Scan(&commRows)

	committees := make([]CommitteeOut, 0, len(commRows))
	for _, cr := range commRows {
		committees = append(committees, CommitteeOut{
			Name:     cr.Name,
			Position: cr.Position,
			URLs:     []string(cr.URLs),
		})
	}

	// 5. Fetch images
	var imageRows []PoliticianImage
	db.DB.Where("politician_id = ?", parsedID).Find(&imageRows)
	images := make([]ImageOut, 0, len(imageRows))
	for _, img := range imageRows {
		images = append(images, ImageOut{URL: img.URL, Type: img.Type})
	}

	// 6. Fetch degrees
	var degreeRows []Degree
	db.DB.Where("politician_id = ?", parsedID).Find(&degreeRows)
	degrees := make([]DegreeOut, 0, len(degreeRows))
	for _, deg := range degreeRows {
		degrees = append(degrees, DegreeOut{
			Degree:   deg.Degree,
			Major:    deg.Major,
			School:   deg.School,
			GradYear: deg.GradYear,
		})
	}

	// 7. Fetch experiences
	var expRows []Experience
	db.DB.Where("politician_id = ?", parsedID).Find(&expRows)
	experiences := make([]ExperienceOut, 0, len(expRows))
	for _, exp := range expRows {
		experiences = append(experiences, ExperienceOut{
			Title:        exp.Title,
			Organization: exp.Organization,
			Type:         exp.Type,
			Start:        exp.Start,
			End:          exp.End,
		})
	}

	// 8. Trigger lazy-fetch for candidacy data (endorsements, stances, elections)
	ensureCandidacyData(r.Context(), parsedID, r0.ExternalGlobalID, r0.ExternalID)

	// 9. Assemble profile response
	profile := PoliticianProfileOut{
		OfficialOut: OfficialOut{
			ID: r0.ID, ExternalID: r0.ExternalID,
			FirstName: r0.FirstName, MiddleInitial: r0.MiddleInitial, LastName: r0.LastName,
			PreferredName: r0.PreferredName, NameSuffix: r0.NameSuffix, FullName: r0.FullName,
			Party: r0.Party, PartyShortName: r0.PartyShortName,
			PhotoOriginURL: r0.PhotoOriginURL, WebFormURL: r0.WebFormURL,
			URLs: []string(r0.URLs), EmailAddresses: []string(r0.EmailAddresses),
			OfficeTitle: r0.OfficeTitle, RepresentingState: r0.RepresentingState, RepresentingCity: r0.RepresentingCity,
			DistrictType: r0.DistrictType, DistrictLabel: r0.DistrictLabel,
			MTFCC: r0.MTFCC,
			ChamberName: r0.ChamberName, ChamberNameFormal: r0.ChamberNameFormal,
			GovernmentName:       r0.GovernmentName,
			IsElected:            isElectedPosition(r0.DistrictType, r0.OfficeTitle, r0.ElectionFrequency),
			IsAppointed:          r0.IsAppointed,
			IsVacant:             r0.IsVacant,
			IsOffCycle:           r0.IsOffCycle,
			Specificity:          r0.Specificity,
			ElectionFrequency:    r0.ElectionFrequency,
			Seats:                r0.Seats,
			NormalizedPositionName: r0.NormalizedPositionName,
			PartisanType:         r0.PartisanType,
			Salary:               r0.Salary,
			GeoID:                r0.GeoID,
			IsJudicial:           r0.IsJudicial,
			OCDID:                r0.OCDID,
			Committees:           committees,
			BioText:              r0.BioText,
			BioguideID:           r0.BioguideID,
			Slug:                 r0.Slug,
			TotalYearsInOffice:   r0.TotalYearsInOffice,
			OfficeDescription:    r0.OfficeDescription,
			Images:               images,
			Degrees:              degrees,
			Experiences:          experiences,
			TermStart:            r0.ValidFrom,
			TermEnd:              r0.ValidTo,
		},
		Addresses:   addresses,
		Identifiers: identifiers,
		Notes:       []string(r0.Notes),
	}

	writeJSON(w, profile)
}

// GetPoliticianEndorsements returns all endorsements for a politician.
func GetPoliticianEndorsements(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "Missing id parameter", http.StatusBadRequest)
		return
	}

	parsedID, err := uuid.Parse(id)
	if err != nil {
		http.Error(w, "Invalid id format", http.StatusBadRequest)
		return
	}

	type row struct {
		EndorserString      string
		Recommendation      string
		Status              string
		ElectionDate        string
		OrganizationName    string
		OrganizationDesc    string
		OrganizationLogoURL string
		OrganizationIssue   string
	}

	var rows []row
	if err := db.DB.Raw(`
		SELECT
		  e.endorser_string,
		  e.recommendation,
		  e.status,
		  e.election_date,
		  COALESCE(o.name, '') AS organization_name,
		  COALESCE(o.description, '') AS organization_desc,
		  COALESCE(o.logo_url, '') AS organization_logo_url,
		  COALESCE(o.issue_name, '') AS organization_issue
		FROM essentials.endorsements e
		LEFT JOIN essentials.endorser_organizations o ON o.id = e.organization_id
		WHERE e.politician_id = ?
		ORDER BY e.election_date DESC
	`, parsedID).Scan(&rows).Error; err != nil {
		http.Error(w, "DB fetch error", http.StatusInternalServerError)
		return
	}

	result := make([]EndorsementOut, 0, len(rows))
	for _, r := range rows {
		result = append(result, EndorsementOut{
			EndorserString:      r.EndorserString,
			Recommendation:      r.Recommendation,
			Status:              r.Status,
			ElectionDate:        r.ElectionDate,
			OrganizationName:    r.OrganizationName,
			OrganizationDesc:    r.OrganizationDesc,
			OrganizationLogoURL: r.OrganizationLogoURL,
			OrganizationIssue:   r.OrganizationIssue,
		})
	}

	writeJSON(w, result)
}

// GetPoliticianStances returns all stances for a politician.
func GetPoliticianStances(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "Missing id parameter", http.StatusBadRequest)
		return
	}

	parsedID, err := uuid.Parse(id)
	if err != nil {
		http.Error(w, "Invalid id format", http.StatusBadRequest)
		return
	}

	type row struct {
		Statement       string
		ReferenceURL    string
		ElectionDate    string
		IssueName       string
		IssueKey        string
		IssueExpanded   string
		ParentIssueName string
	}

	var rows []row
	if err := db.DB.Raw(`
		SELECT
		  s.statement,
		  s.reference_url,
		  s.election_date,
		  i.name AS issue_name,
		  i.key AS issue_key,
		  COALESCE(i.expanded_text, '') AS issue_expanded,
		  COALESCE(p.name, '') AS parent_issue_name
		FROM essentials.politician_stances s
		JOIN essentials.issues i ON i.id = s.issue_id
		LEFT JOIN essentials.issues p ON p.id = i.parent_id
		WHERE s.politician_id = ?
		ORDER BY s.election_date DESC
	`, parsedID).Scan(&rows).Error; err != nil {
		http.Error(w, "DB fetch error", http.StatusInternalServerError)
		return
	}

	result := make([]StanceOut, 0, len(rows))
	for _, r := range rows {
		result = append(result, StanceOut{
			Statement:       r.Statement,
			ReferenceURL:    r.ReferenceURL,
			ElectionDate:    r.ElectionDate,
			IssueName:       r.IssueName,
			IssueKey:        r.IssueKey,
			IssueExpanded:   r.IssueExpanded,
			ParentIssueName: r.ParentIssueName,
		})
	}

	writeJSON(w, result)
}

// GetPoliticianElections returns election history for a politician.
func GetPoliticianElections(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "Missing id parameter", http.StatusBadRequest)
		return
	}

	parsedID, err := uuid.Parse(id)
	if err != nil {
		http.Error(w, "Invalid id format", http.StatusBadRequest)
		return
	}

	var records []ElectionRecord
	if err := db.DB.Where("politician_id = ?", parsedID).Order("election_date DESC").Find(&records).Error; err != nil {
		http.Error(w, "DB fetch error", http.StatusInternalServerError)
		return
	}

	result := make([]ElectionRecordOut, 0, len(records))
	for _, rec := range records {
		result = append(result, ElectionRecordOut{
			ElectionName:    rec.ElectionName,
			ElectionDate:    rec.ElectionDate,
			PositionName:    rec.PositionName,
			Result:          rec.Result,
			Withdrawn:       rec.Withdrawn,
			PartyName:       rec.PartyName,
			IsPrimary:       rec.IsPrimary,
			IsRunoff:        rec.IsRunoff,
			IsUnexpiredTerm: rec.IsUnexpiredTerm,
		})
	}

	writeJSON(w, result)
}

// GetAllPoliticians returns a (paged) list of politicians across the DB,
// in the same flattened shape as GetPoliticiansByZip (OfficialOut).
// Query params:
//
//	q       -> case-insensitive search on name (full/first/last)
//	state   -> filter by representing_state (e.g., "WA")
//	limit   -> default 100, max 500
//	offset  -> default 0
func GetAllPoliticians(w http.ResponseWriter, r *http.Request) {
	type row struct {
		ID                uuid.UUID
		ExternalID        int
		FirstName         string
		MiddleInitial     string
		LastName          string
		PreferredName     string
		NameSuffix        string
		FullName          string
		Party             string
		PhotoOriginURL    string
		WebFormURL        string
		URLs              pq.StringArray `gorm:"type:text[]"`
		EmailAddresses    pq.StringArray `gorm:"type:text[]"`
		OfficeTitle       string
		RepresentingState string
		RepresentingCity  string
		DistrictType      string
		DistrictLabel     string
		MTFCC             string
		ChamberName       string
		ChamberNameFormal string
		GovernmentName    string
		ElectionFrequency string
	}

	// ---- parse filters/paging
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	state := strings.TrimSpace(r.URL.Query().Get("state"))

	limitStr := strings.TrimSpace(r.URL.Query().Get("limit"))
	offsetStr := strings.TrimSpace(r.URL.Query().Get("offset"))

	applyLimit := false
	limit := 0
	offset := 0

	// Allow ?limit=all to explicitly request no limit
	if limitStr != "" && strings.ToLower(limitStr) != "all" {
		if n, err := strconv.Atoi(limitStr); err == nil && n > 0 {
			// Cap to a reasonable max to protect the DB (tune as needed)
			const maxLimit = 5000
			if n > maxLimit {
				n = maxLimit
			}
			limit = n
			applyLimit = true

			if offsetStr != "" {
				if off, err := strconv.Atoi(offsetStr); err == nil && off >= 0 {
					offset = off
				}
			}
		}
	}

	// ---- build WHERE dynamically
	where := []string{"1=1"}
	args := []any{}

	if q != "" {
		where = append(where, `(p.full_name ILIKE ? OR p.first_name ILIKE ? OR p.last_name ILIKE ? OR (p.first_name || ' ' || p.last_name) ILIKE ?)`)
		pat := "%" + q + "%"
		args = append(args, pat, pat, pat, pat)
	}
	if state != "" {
		where = append(where, `o.representing_state = ?`)
		args = append(args, state)
	}

	baseSQL := fmt.Sprintf(`
	SELECT
	  p.id, p.external_id, p.first_name, p.middle_initial, p.last_name,
	  p.preferred_name, p.name_suffix, p.full_name, p.party,
	  COALESCE(p.photo_custom_url, NULLIF(p.photo_origin_url, '')) AS photo_origin_url,
	  p.web_form_url, p.urls, p.email_addresses,
	  o.title AS office_title, o.representing_state, o.representing_city,
	  d.district_type, d.label AS district_label, d.mtfcc,
	  c.name AS chamber_name, c.name_formal AS chamber_name_formal,
	  g.name AS government_name,
	  COALESCE(c.election_frequency, '') AS election_frequency
	FROM essentials.politicians p
	JOIN essentials.offices o   ON o.politician_id = p.id
	JOIN essentials.districts d ON d.id = o.district_id
	JOIN essentials.chambers c  ON c.id = o.chamber_id
	JOIN essentials.governments g ON g.id = c.government_id
	WHERE %s
	ORDER BY d.district_type, o.title, p.last_name, p.first_name
`, strings.Join(where, " AND "))

	var sql string
	if applyLimit {
		sql = fmt.Sprintf("%s LIMIT %d OFFSET %d", baseSQL, limit, offset)
	} else {
		sql = baseSQL // No LIMIT/OFFSET → return all
	}

	var rows []row
	if err := db.DB.Raw(sql, args...).Scan(&rows).Error; err != nil {
		http.Error(w, "DB fetch error", http.StatusInternalServerError)
		return
	}

	if len(rows) == 0 {
		writeJSON(w, []OfficialOut{})
		return
	}

	// Batch load committees (like fetchOfficialsFromDB)
	ids := make([]uuid.UUID, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.ID)
	}

	type commRow struct {
		PoliticianID uuid.UUID
		Name         string
		Position     string
		URLs         pq.StringArray `gorm:"type:text[]"`
	}
	var commRows []commRow
	if err := db.DB.Raw(`
		SELECT pc.politician_id, cm.name, pc.position, cm.urls
		FROM essentials.politician_committees pc
		JOIN essentials.committees cm ON cm.id = pc.committee_id
		WHERE pc.politician_id = ANY(?)
	`, pq.Array(ids)).Scan(&commRows).Error; err != nil {
		http.Error(w, "DB fetch error", http.StatusInternalServerError)
		return
	}

	byPol := make(map[uuid.UUID][]CommitteeOut, len(ids))
	for _, cr := range commRows {
		byPol[cr.PoliticianID] = append(byPol[cr.PoliticianID], CommitteeOut{
			Name: cr.Name, Position: cr.Position, URLs: []string(cr.URLs),
		})
	}

	// Assemble final DTOs
	out := make([]OfficialOut, 0, len(rows))
	for _, r := range rows {
		out = append(out, OfficialOut{
			ID: r.ID, ExternalID: r.ExternalID,
			FirstName: r.FirstName, MiddleInitial: r.MiddleInitial, LastName: r.LastName,
			PreferredName: r.PreferredName, NameSuffix: r.NameSuffix, FullName: r.FullName,
			Party: r.Party, PhotoOriginURL: r.PhotoOriginURL, WebFormURL: r.WebFormURL,
			URLs: []string(r.URLs), EmailAddresses: []string(r.EmailAddresses),
			OfficeTitle: r.OfficeTitle, RepresentingState: r.RepresentingState, RepresentingCity: r.RepresentingCity,
			DistrictType: r.DistrictType, DistrictLabel: r.DistrictLabel,
			MTFCC: r.MTFCC,
			ChamberName: r.ChamberName, ChamberNameFormal: r.ChamberNameFormal,
			GovernmentName:    r.GovernmentName,
			IsElected:         isElectedPosition(r.DistrictType, r.OfficeTitle, r.ElectionFrequency),
			ElectionFrequency: r.ElectionFrequency,
			Committees:        byPol[r.ID],
		})
	}

	writeJSON(w, out)
}

// --- Candidates endpoint (Phase D) -----------------------------------------

// CandidateOut represents a single candidate in an upcoming election race.
// The district_type field uses the same internal enum as OfficialOut for
// compatibility with the frontend's classify.js system.
type CandidateOut struct {
	ExternalID        int    `json:"external_id"`
	FirstName         string `json:"first_name"`
	LastName          string `json:"last_name"`
	FullName          string `json:"full_name"`
	PhotoOriginURL    string `json:"photo_origin_url,omitempty"`
	OfficeTitle       string `json:"office_title"`
	DistrictType      string `json:"district_type"`
	Party             string `json:"party,omitempty"`
	PartyShortName    string `json:"party_short_name,omitempty"`
	IsCandidate       bool   `json:"is_candidate"`
	ElectionDate      string `json:"election_date"`
	ElectionName      string `json:"election_name"`
	IsPrimary         bool   `json:"is_primary"`
	IsRunoff          bool   `json:"is_runoff"`
	RepresentingState string `json:"representing_state,omitempty"`
	ChamberName       string `json:"chamber_name,omitempty"`
}

// raceChamberName derives a chamber name from BallotReady race position data.
// Uses normalizedPosition.name first, then falls back to the position name directly.
func raceChamberName(pos ballotready.RacePositionNode) string {
	if pos.NormalizedPosition != nil && pos.NormalizedPosition.Name != "" {
		return pos.NormalizedPosition.Name
	}
	// Fallback: use position name directly (covers most cases)
	return pos.Name
}

// levelToDistrictType maps a BallotReady position level and name to an internal
// district type string compatible with the frontend's classify.js tier system.
func levelToDistrictType(level, posName string) string {
	name := strings.ToLower(posName)
	switch strings.ToUpper(level) {
	case "FEDERAL":
		if strings.Contains(name, "senate") || strings.Contains(name, "senator") {
			return "NATIONAL_UPPER"
		}
		if strings.Contains(name, "house") || strings.Contains(name, "representative") {
			return "NATIONAL_LOWER"
		}
		return "NATIONAL_EXEC"
	case "STATE":
		if strings.Contains(name, "senate") || strings.Contains(name, "senator") {
			return "STATE_UPPER"
		}
		if strings.Contains(name, "house") || strings.Contains(name, "representative") || strings.Contains(name, "assembly") {
			return "STATE_LOWER"
		}
		return "STATE_EXEC"
	case "LOCAL":
		if strings.Contains(name, "school") {
			return "SCHOOL"
		}
		if strings.Contains(name, "county") {
			return "COUNTY"
		}
		return "LOCAL"
	default:
		return "LOCAL"
	}
}

// GetCandidatesByZip returns a flat list of candidates in upcoming elections for a ZIP code.
// Fetches from BallotReady races query; only future elections are included (electionDayGte=today).
// Returns an empty array (not an error) when no upcoming races exist or when BallotReady is unavailable.
func GetCandidatesByZip(w http.ResponseWriter, r *http.Request) {
	zip := chi.URLParam(r, "zip")
	if !isZip5(zip) {
		http.Error(w, "Missing or invalid zip parameter", http.StatusBadRequest)
		return
	}

	log.Printf("[GetCandidatesByZip] zip=%s", zip)

	brProvider, ok := Provider.(*ballotready.BallotReadyProvider)
	if !ok {
		// BallotReady not configured — return empty array (graceful degradation)
		writeJSON(w, []CandidateOut{})
		return
	}

	races, err := brProvider.Client().FetchRacesByZip(r.Context(), zip)
	if err != nil {
		log.Printf("[GetCandidatesByZip] error fetching races: %v", err)
		// Return empty array on error (graceful degradation)
		writeJSON(w, []CandidateOut{})
		return
	}

	var candidates []CandidateOut
	for _, race := range races {
		for _, c := range race.Candidacies {
			// Skip withdrawn candidacies
			if c.Withdrawn {
				continue
			}

			// Get best available photo URL (prefer "default" type)
			var photoURL string
			for _, img := range c.Candidate.Images {
				if img.Type == "default" || photoURL == "" {
					photoURL = img.URL
				}
			}

			// Get party info from first party entry
			var partyName, partyShort string
			if len(c.Parties) > 0 {
				partyName = c.Parties[0].Name
				partyShort = c.Parties[0].ShortName
			}

			candidates = append(candidates, CandidateOut{
				ExternalID:        c.DatabaseID,
				FirstName:         c.Candidate.FirstName,
				LastName:          c.Candidate.LastName,
				FullName:          c.Candidate.FullName,
				PhotoOriginURL:    photoURL,
				OfficeTitle:       race.Position.Name,
				DistrictType:      levelToDistrictType(race.Position.Level, race.Position.Name),
				Party:             partyName,
				PartyShortName:    partyShort,
				IsCandidate:       true,
				ElectionDate:      race.Election.Date,
				ElectionName:      race.Election.Name,
				IsPrimary:         race.IsPrimary,
				IsRunoff:          race.IsRunoff,
				RepresentingState: race.Position.State,
				ChamberName:       raceChamberName(race.Position),
			})
		}
	}

	if candidates == nil {
		candidates = []CandidateOut{}
	}

	writeJSON(w, candidates)
}
