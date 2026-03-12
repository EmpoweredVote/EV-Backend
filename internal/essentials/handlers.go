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

type ContactOut struct {
	ContactType string `json:"contact_type"`
	Source      string `json:"source"`
	Phone       string `json:"phone,omitempty"`
	Email       string `json:"email,omitempty"`
	Fax         string `json:"fax,omitempty"`
	WebsiteURL  string `json:"website_url,omitempty"`
	SyncedAt    string `json:"synced_at,omitempty"`
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
	IsActive        bool   `json:"is_active"`
}

// Legislative data DTOs (Phase 55)

type LegislativeCommitteeAssignmentOut struct {
	CommitteeName  string `json:"committee_name"`
	Role           string `json:"role"`            // "member", "chair", "vice_chair", "ranking_member", "ex_officio"
	Chamber        string `json:"chamber"`
	CongressNumber int    `json:"congress_number"`
	IsCurrent      bool   `json:"is_current"`
	ParentName     string `json:"parent_name,omitempty"` // populated for subcommittees
	CommitteeType  string `json:"committee_type"`        // "committee", "subcommittee", "joint"
}

type LegislativeLeadershipRoleOut struct {
	Title     string `json:"title"`
	Chamber   string `json:"chamber"`
	IsCurrent bool   `json:"is_current"`
	StartDate string `json:"start_date,omitempty"`
	EndDate   string `json:"end_date,omitempty"`
}

type LegislativeBillOut struct {
	ExternalID   string `json:"external_id"`
	Number       string `json:"number"`
	Title        string `json:"title"`
	Summary      string `json:"summary,omitempty"`
	StatusLabel  string `json:"status_label"`
	IntroducedAt string `json:"introduced_at,omitempty"`
	IsSponsor    bool   `json:"is_sponsor"`
	URL          string `json:"url,omitempty"`
	Source       string `json:"source"`
}

type LegislativeVoteOut struct {
	VoteQuestion string `json:"vote_question"`
	Position     string `json:"position"`
	VoteDate     string `json:"vote_date"`
	Result       string `json:"result"`
	BillTitle    string `json:"bill_title,omitempty"`
	BillNumber   string `json:"bill_number,omitempty"`
	BillURL      string `json:"bill_url,omitempty"`
	Source       string `json:"source"`
}

type LegislativeSummaryOut struct {
	RecentBills []LegislativeBillOut `json:"recent_bills"`
	RecentVotes []LegislativeVoteOut `json:"recent_votes"`
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
	DistrictID           string          `json:"district_id,omitempty"`
	MTFCC                string          `json:"mtfcc"`
	ChamberName          string          `json:"chamber_name"`
	ChamberNameFormal    string          `json:"chamber_name_formal"`
	GovernmentName       string          `json:"government_name"`
	IsElected            bool            `json:"is_elected"`
	IsAppointed          bool            `json:"is_appointed,omitempty"`
	IsVacant             bool            `json:"is_vacant,omitempty"`
	VacantSince          *time.Time      `json:"vacant_since,omitempty"`
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
	TermDatePrecision    string          `json:"term_date_precision,omitempty"` // "year", "month", or "day"
	GovernmentBodyName   string          `json:"government_body_name,omitempty"`
	GovernmentBodyURL    string          `json:"government_body_url,omitempty"`
}

func GetPoliticiansByZip(w http.ResponseWriter, r *http.Request) {
	zip := chi.URLParam(r, "zip")
	if !isZip5(zip) {
		http.Error(w, "Missing or invalid zip parameter", http.StatusBadRequest)
		return
	}
	handleZipLookup(w, r, zip)
}

func handleZipLookup(w http.ResponseWriter, r *http.Request, zip string) {
	state := zipPrefixToState(zip)
	officials, err := fetchOfficialsFromDB(zip, state)
	if err != nil {
		http.Error(w, "DB fetch error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("X-Data-Status", "fresh")
	writeJSON(w, officials)
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
			PoliticianID:         &polID,
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
			PoliticianID:         &polID,
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
		DistrictIDText       string
		MTFCC                string
		ChamberName          string
		ChamberNameFormal    string
		GovernmentName       string
		ElectionFrequency    string
		IsAppointed          bool
		IsVacantOffice       bool
		VacantSince          *time.Time
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
		TermDatePrecision    string
		GovernmentBodyName   string
		GovernmentBodyURL    string
	}

	var rows []row

	// Build query that includes federal + state (if known) + local officials.
	// Uses offices as the base table with a LEFT JOIN to politicians so that
	// vacant offices (o.is_vacant = true) are returned even without an active politician.
	query := `
		SELECT
		  COALESCE(p.id, o.id) AS id,
		  COALESCE(p.external_id, 0) AS external_id,
		  COALESCE(p.first_name, '') AS first_name,
		  COALESCE(p.middle_initial, '') AS middle_initial,
		  COALESCE(p.last_name, '') AS last_name,
		  COALESCE(p.preferred_name, '') AS preferred_name,
		  COALESCE(p.name_suffix, '') AS name_suffix,
		  COALESCE(p.full_name, '') AS full_name,
		  COALESCE(p.party, '') AS party,
		  COALESCE(p.party_short_name, '') AS party_short_name,
		  COALESCE(p.photo_custom_url, NULLIF(p.photo_origin_url, '')) AS photo_origin_url,
		  COALESCE(p.web_form_url, '') AS web_form_url,
		  COALESCE(p.urls, '{}') AS urls,
		  COALESCE(p.email_addresses, '{}') AS email_addresses,
		  o.title AS office_title, o.representing_state, o.representing_city,
		  d.district_type, d.label AS district_label,
		  COALESCE(d.district_id, '') AS district_id_text,
		  d.mtfcc,
		  COALESCE(c.name, '') AS chamber_name,
		  COALESCE(c.name_formal, '') AS chamber_name_formal,
		  COALESCE(g.name, '') AS government_name,
		  COALESCE(c.election_frequency, '') AS election_frequency,
		  COALESCE(p.is_appointed, false) AS is_appointed,
		  o.is_vacant AS is_vacant_office,
		  o.vacant_since,
		  COALESCE(p.is_off_cycle, false) AS is_off_cycle,
		  COALESCE(p.specificity, '') AS specificity,
		  o.seats, o.normalized_position_name, o.partisan_type, o.salary,
		  d.geo_id, d.is_judicial, d.ocd_id,
		  COALESCE(p.bio_text, '') AS bio_text,
		  COALESCE(p.bioguide_id, '') AS bioguide_id,
		  COALESCE(p.slug, '') AS slug,
		  COALESCE(p.total_years_in_office, 0) AS total_years_in_office,
		  COALESCE(NULLIF(o.description, ''), pd_specific.description, pd_generic.description, '') AS office_description,
		  zp.is_contained,
		  COALESCE(p.valid_from, '') AS valid_from,
		  COALESCE(p.valid_to, '') AS valid_to,
		  COALESCE(p.term_date_precision, '') AS term_date_precision,
		  COALESCE(gb.display_name, '') AS government_body_name,
		  COALESCE(gb.website_url, '') AS government_body_url
		FROM essentials.offices o
		LEFT JOIN essentials.politicians p ON o.politician_id = p.id
		JOIN essentials.districts d ON d.id = o.district_id
		LEFT JOIN essentials.chambers c ON c.id = o.chamber_id
		LEFT JOIN essentials.governments g ON g.id = c.government_id
		LEFT JOIN essentials.position_descriptions pd_specific
		  ON pd_specific.normalized_position_name = COALESCE(NULLIF(o.normalized_position_name, ''), o.title)
		  AND pd_specific.district_type = d.district_type
		LEFT JOIN essentials.position_descriptions pd_generic
		  ON pd_generic.normalized_position_name = COALESCE(NULLIF(o.normalized_position_name, ''), o.title)
		  AND pd_generic.district_type = ''
		LEFT JOIN essentials.government_bodies gb
		  ON gb.state = d.state
		  AND gb.geo_id = d.geo_id
		  AND gb.body_key = COALESCE(NULLIF(c.name_formal, ''), c.name, '')
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

	// Add local officials mapped to this ZIP (vacant offices without politician_id won't match)
	query += `
		  OR p.id IN (
		    SELECT politician_id FROM essentials.zip_politicians WHERE zip = ?
		  )
		)
		AND (p.is_active = true OR o.is_vacant = true)
		AND COALESCE(p.is_incumbent, true) = true
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
			DistrictID:           r.DistrictIDText,
			MTFCC: r.MTFCC,
			ChamberName: r.ChamberName, ChamberNameFormal: r.ChamberNameFormal,
			GovernmentName:       r.GovernmentName,
			IsElected:            isElectedPosition(r.DistrictType, r.OfficeTitle, r.ElectionFrequency),
			IsAppointed:          r.IsAppointed,
			IsVacant:             r.IsVacantOffice,
			VacantSince:          r.VacantSince,
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
			TermDatePrecision:    r.TermDatePrecision,
			GovernmentBodyName:   r.GovernmentBodyName,
			GovernmentBodyURL:    r.GovernmentBodyURL,
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
		DistrictIDText       string
		MTFCC                string
		ChamberName          string
		ChamberNameFormal    string
		GovernmentName       string
		ElectionFrequency    string
		IsAppointed          bool
		IsVacantOffice       bool
		VacantSince          *time.Time
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
		TermDatePrecision    string
		GovernmentBodyName   string
		GovernmentBodyURL    string
	}

	var rows []row

	// Uses offices as the base table with LEFT JOIN to politicians so that
	// vacant offices (o.is_vacant = true) are returned even without an active politician.
	query := `
		SELECT
		  COALESCE(p.id, o.id) AS id,
		  COALESCE(p.external_id, 0) AS external_id,
		  COALESCE(p.first_name, '') AS first_name,
		  COALESCE(p.middle_initial, '') AS middle_initial,
		  COALESCE(p.last_name, '') AS last_name,
		  COALESCE(p.preferred_name, '') AS preferred_name,
		  COALESCE(p.name_suffix, '') AS name_suffix,
		  COALESCE(p.full_name, '') AS full_name,
		  COALESCE(p.party, '') AS party,
		  COALESCE(p.party_short_name, '') AS party_short_name,
		  COALESCE(p.photo_custom_url, NULLIF(p.photo_origin_url, '')) AS photo_origin_url,
		  COALESCE(p.web_form_url, '') AS web_form_url,
		  COALESCE(p.urls, '{}') AS urls,
		  COALESCE(p.email_addresses, '{}') AS email_addresses,
		  o.title AS office_title, o.representing_state, o.representing_city,
		  d.district_type, d.label AS district_label,
		  COALESCE(d.district_id, '') AS district_id_text,
		  d.mtfcc,
		  COALESCE(c.name, '') AS chamber_name, COALESCE(c.name_formal, '') AS chamber_name_formal,
		  COALESCE(g.name, '') AS government_name,
		  COALESCE(c.election_frequency, '') AS election_frequency,
		  COALESCE(p.is_appointed, false) AS is_appointed,
		  o.is_vacant AS is_vacant_office,
		  o.vacant_since,
		  COALESCE(p.is_off_cycle, false) AS is_off_cycle,
		  COALESCE(p.specificity, '') AS specificity,
		  o.seats, o.normalized_position_name, o.partisan_type, o.salary,
		  d.geo_id, d.is_judicial, d.ocd_id,
		  COALESCE(p.bio_text, '') AS bio_text,
		  COALESCE(p.bioguide_id, '') AS bioguide_id,
		  COALESCE(p.slug, '') AS slug,
		  COALESCE(p.total_years_in_office, 0) AS total_years_in_office,
		  COALESCE(NULLIF(o.description, ''), pd_specific.description, pd_generic.description, '') AS office_description,
		  COALESCE(p.valid_from, '') AS valid_from,
		  COALESCE(p.valid_to, '') AS valid_to,
		  COALESCE(p.term_date_precision, '') AS term_date_precision,
		  COALESCE(gb.display_name, '') AS government_body_name,
		  COALESCE(gb.website_url, '') AS government_body_url
		FROM essentials.offices o
		LEFT JOIN essentials.politicians p ON o.politician_id = p.id
		JOIN essentials.districts d ON d.id = o.district_id
		LEFT JOIN essentials.chambers c ON c.id = o.chamber_id
		LEFT JOIN essentials.governments g ON g.id = c.government_id
		LEFT JOIN essentials.position_descriptions pd_specific
		  ON pd_specific.normalized_position_name = COALESCE(NULLIF(o.normalized_position_name, ''), o.title)
		  AND pd_specific.district_type = d.district_type
		LEFT JOIN essentials.position_descriptions pd_generic
		  ON pd_generic.normalized_position_name = COALESCE(NULLIF(o.normalized_position_name, ''), o.title)
		  AND pd_generic.district_type = ''
		LEFT JOIN essentials.government_bodies gb
		  ON gb.state = d.state
		  AND gb.geo_id = d.geo_id
		  AND gb.body_key = COALESCE(NULLIF(c.name_formal, ''), c.name, '')
		WHERE (
		  d.district_type = 'NATIONAL_EXEC'
		  OR (
		    d.district_type = ANY(?)
		    AND (o.representing_state = ? OR d.state = ?)
		  )
		)
		AND (p.is_active = true OR o.is_vacant = true)
		AND COALESCE(p.is_incumbent, true) = true
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
			DistrictID:           r.DistrictIDText,
			MTFCC: r.MTFCC,
			ChamberName: r.ChamberName, ChamberNameFormal: r.ChamberNameFormal,
			GovernmentName:       r.GovernmentName,
			IsElected:            isElectedPosition(r.DistrictType, r.OfficeTitle, r.ElectionFrequency),
			IsAppointed:          r.IsAppointed,
			IsVacant:             r.IsVacantOffice,
			VacantSince:          r.VacantSince,
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
			TermDatePrecision:    r.TermDatePrecision,
			GovernmentBodyName:   r.GovernmentBodyName,
			GovernmentBodyURL:    r.GovernmentBodyURL,
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
// Accepts {"query": "..."} and geocodes the query to detect area vs point queries.
// Area queries (city, ZIP, county) use ST_Intersects boundary overlap to find all overlapping districts.
// Point queries (street addresses) use ST_Covers point-in-polygon (unchanged behavior).
// ZIP codes route through this geocode → area-intersection path instead of the old zip_politicians warming flow.
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

	// All queries (including ZIP codes) go through geocode → area/point detection.
	// ZIP codes will return result type "postal_code" which triggers area intersection.
	// This replaces the old ZIP-specific warm/cache delegation.

	// Search requires geocoding — no fallback without API key.
	if GeoClient == nil {
		http.Error(w, "Address search requires geocoding service configuration", http.StatusServiceUnavailable)
		return
	}

	// Geocode the query to coordinates + structured location data.
	geoResult, geoErr := GeoClient.Geocode(r.Context(), query)
	if geoErr != nil {
		log.Printf("[SearchPoliticians] Google geocoding failed for %q: %v", query, geoErr)
		if strings.Contains(geoErr.Error(), "could not determine US state") {
			http.Error(w, "Address must be within the United States", http.StatusUnprocessableEntity)
			return
		}
		http.Error(w, "Could not resolve address", http.StatusBadRequest)
		return
	}
	log.Printf("[SearchPoliticians] Google geocoded %q → (%.6f, %.6f) types=%v %s",
		query, geoResult.Lat, geoResult.Lng, geoResult.ResultTypes, geoResult.Formatted)

	var officials []OfficialOut
	var geoMatchCount int

	if geoResult.IsAreaQuery() {
		// AREA QUERY: city, ZIP, county — use boundary intersection to find all overlapping districts.
		log.Printf("[SearchPoliticians] area query detected (types=%v) — using boundary intersection", geoResult.ResultTypes)

		areaGeoID, areaMTFCC, found := ResolveAreaBoundary(r.Context(), geoResult)
		if found {
			log.Printf("[SearchPoliticians] resolved area boundary: geo_id=%s mtfcc=%s", areaGeoID, areaMTFCC)

			areaMatches, err := FindGeoIDsByAreaIntersection(r.Context(), areaGeoID, areaMTFCC)
			if err != nil {
				log.Printf("[SearchPoliticians] area intersection error: %v", err)
			} else {
				geoMatchCount = len(areaMatches)
				log.Printf("[SearchPoliticians] area intersection found %d overlapping districts", geoMatchCount)

				if geoMatchCount > 0 {
					officials, err = FindPoliticiansByGeoMatches(r.Context(), areaMatches)
					if err != nil {
						log.Printf("[SearchPoliticians] politician lookup from area matches error: %v", err)
						officials = nil
					}
				}
			}
		} else {
			log.Printf("[SearchPoliticians] no area boundary found in DB — falling back to point-in-polygon")
		}

		// Fallback: if area resolution failed or no boundary exists, try point-in-polygon.
		if len(officials) == 0 {
			pointMatches, err := FindGeoIDsByPoint(r.Context(), geoResult.Lat, geoResult.Lng)
			if err != nil {
				log.Printf("[SearchPoliticians] point-in-polygon fallback error: %v", err)
			} else if len(pointMatches) > 0 {
				geoMatchCount = len(pointMatches)
				officials, err = FindPoliticiansByGeoMatches(r.Context(), pointMatches)
				if err != nil {
					log.Printf("[SearchPoliticians] politician lookup from point fallback error: %v", err)
					officials = nil
				}
			}
		}
	} else {
		// POINT QUERY: specific street address — use existing point-in-polygon (unchanged behavior).
		log.Printf("[SearchPoliticians] point query detected (types=%v) — using point-in-polygon", geoResult.ResultTypes)

		pointMatches, err := FindGeoIDsByPoint(r.Context(), geoResult.Lat, geoResult.Lng)
		if err != nil {
			log.Printf("[SearchPoliticians] geofence lookup error: %v", err)
		} else {
			geoMatchCount = len(pointMatches)
			if geoMatchCount > 0 {
				officials, err = FindPoliticiansByGeoMatches(r.Context(), pointMatches)
				if err != nil {
					log.Printf("[SearchPoliticians] politician lookup error: %v", err)
					officials = nil
				}
			}
		}
	}

	if len(officials) > 0 {
		// Supplement with federal + state officials, deduplicating by external_id.
		geoState := strings.ToUpper(geoResult.State)
		if geoState == "" {
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
				log.Printf("[SearchPoliticians] supplemental fetch error: %v", supErr)
			}
		}

		log.Printf("[SearchPoliticians] served %d officials (%d geo matches) for state=%s query=%q",
			len(officials), geoMatchCount, strings.ToUpper(geoResult.State), query)
		w.Header().Set("X-Data-Status", "fresh")
		w.Header().Set("X-Geofence-Count", fmt.Sprintf("%d", geoMatchCount))
		w.Header().Set("X-Formatted-Address", geoResult.Formatted)
		writeJSON(w, officials)
		return
	}

	// No local geofence data — return federal + state officials from cache.
	geoState := strings.ToUpper(geoResult.State)
	if geoState == "" {
		http.Error(w, "Address must be within the United States", http.StatusUnprocessableEntity)
		return
	}

	federalState, err := fetchFederalAndStateFromDB(geoState)
	if err != nil {
		log.Printf("[SearchPoliticians] fallback DB fetch error for state=%s: %v", geoState, err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	log.Printf("[SearchPoliticians] no-geofence fallback: returned %d federal+state officials for state=%s", len(federalState), geoState)
	w.Header().Set("X-Data-Status", "no-geofence-data")
	w.Header().Set("X-Formatted-Address", geoResult.Formatted)
	writeJSON(w, federalState)
}

// PoliticianProfileOut is the enriched response for a single politician profile.
type PoliticianProfileOut struct {
	OfficialOut
	Addresses   []Address    `json:"addresses"`
	Identifiers []Identifier `json:"identifiers"`
	Notes       []string     `json:"notes"`
	Contacts    []ContactOut `json:"contacts"`
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
		DistrictIDText       string
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
		TermDatePrecision    string
	}

	var r0 row
	if err := db.DB.Raw(`
		SELECT
		  p.id, p.external_id,
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
		  COALESCE(NULLIF(o.description, ''), pd_specific.description, pd_generic.description, '') AS office_description,
		  o.seats, o.normalized_position_name, o.partisan_type, o.salary,
		  d.district_type, d.label AS district_label,
		  COALESCE(d.district_id, '') AS district_id_text,
		  d.mtfcc,
		  d.geo_id, d.is_judicial, d.ocd_id,
		  COALESCE(c.name, '') AS chamber_name, COALESCE(c.name_formal, '') AS chamber_name_formal,
		  COALESCE(g.name, '') AS government_name,
		  COALESCE(c.election_frequency, '') AS election_frequency,
		  COALESCE(p.valid_from, '') AS valid_from,
		  COALESCE(p.valid_to, '') AS valid_to,
		  COALESCE(p.term_date_precision, '') AS term_date_precision
		FROM essentials.politicians p
		JOIN essentials.offices o ON o.politician_id = p.id
		JOIN essentials.districts d ON d.id = o.district_id
		LEFT JOIN essentials.chambers c ON c.id = o.chamber_id
		LEFT JOIN essentials.governments g ON g.id = c.government_id
		LEFT JOIN essentials.position_descriptions pd_specific
		  ON pd_specific.normalized_position_name = COALESCE(NULLIF(o.normalized_position_name, ''), o.title)
		  AND pd_specific.district_type = d.district_type
		LEFT JOIN essentials.position_descriptions pd_generic
		  ON pd_generic.normalized_position_name = COALESCE(NULLIF(o.normalized_position_name, ''), o.title)
		  AND pd_generic.district_type = ''
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

	// 7b. Fetch contacts
	var contactRows []PoliticianContact
	db.DB.Where("politician_id = ?", parsedID).Find(&contactRows)
	contacts := make([]ContactOut, 0, len(contactRows))
	for _, c := range contactRows {
		if c.Phone == "" && c.Email == "" && c.Fax == "" && c.WebsiteURL == "" {
			continue
		}
		syncedAt := ""
		if c.ContactSyncedAt != nil && !c.ContactSyncedAt.IsZero() {
			syncedAt = c.ContactSyncedAt.Format(time.RFC3339)
		}
		contacts = append(contacts, ContactOut{
			ContactType: c.ContactType,
			Source:      c.Source,
			Phone:       c.Phone,
			Email:       c.Email,
			Fax:         c.Fax,
			WebsiteURL:  c.WebsiteURL,
			SyncedAt:    syncedAt,
		})
	}

	// 8. Assemble profile response
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
			DistrictID:           r0.DistrictIDText,
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
			TermDatePrecision:    r0.TermDatePrecision,
		},
		Addresses:   addresses,
		Identifiers: identifiers,
		Notes:       []string(r0.Notes),
		Contacts:    contacts,
	}

	writeJSON(w, profile)
}

// GetBuildingPhoto returns the building photo for a city by Census GEOID.
func GetBuildingPhoto(w http.ResponseWriter, r *http.Request) {
	geoID := chi.URLParam(r, "geo_id")
	if geoID == "" {
		http.Error(w, "Missing geo_id parameter", http.StatusBadRequest)
		return
	}

	var photo BuildingPhoto
	result := db.DB.Where("place_geoid = ?", geoID).First(&photo)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]interface{}{
		"place_geoid": photo.PlaceGeoid,
		"url":         photo.URL,
		"license":     photo.License,
		"attribution": photo.Attribution,
		"source_url":  photo.SourceURL,
		"fetched_at":  photo.FetchedAt.Format(time.RFC3339),
	})
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
			IsActive:        rec.IsActive,
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
	ID                string            `json:"id"`                         // Politician UUID as string
	ExternalID        int               `json:"external_id"`
	FirstName         string            `json:"first_name"`
	LastName          string            `json:"last_name"`
	FullName          string            `json:"full_name"`
	PhotoOriginURL    string            `json:"photo_origin_url,omitempty"`
	Images            []PoliticianImage `json:"images,omitempty"`           // Full images array
	OfficeTitle       string            `json:"office_title"`
	DistrictType      string            `json:"district_type"`
	Party             string            `json:"party,omitempty"`
	PartyShortName    string            `json:"party_short_name,omitempty"`
	IsCandidate       bool              `json:"is_candidate"`
	ElectionDate      string            `json:"election_date"`
	ElectionName      string            `json:"election_name"`
	IsPrimary         bool              `json:"is_primary"`
	IsRunoff          bool              `json:"is_runoff"`
	RepresentingState string            `json:"representing_state,omitempty"`
	ChamberName       string            `json:"chamber_name,omitempty"`
	DistrictID        string            `json:"district_id,omitempty"`      // For subtitle (e.g. "District 11")
}

// GetCandidatesByZip returns active candidates from the election_records table.
// No live API call is made. Candidates are filtered by is_active=true and withdrawn=false.
// Results include candidates of all geographic tiers; frontend classify.js handles tier grouping.
// For local candidates, ZIP filtering is applied via zip_politicians.
// For state/federal candidates, filtering is by representing_state matching the derived state.
func GetCandidatesByZip(w http.ResponseWriter, r *http.Request) {
	zip := chi.URLParam(r, "zip")
	if !isZip5(zip) {
		http.Error(w, "Missing or invalid zip parameter", http.StatusBadRequest)
		return
	}

	log.Printf("[GetCandidatesByZip] zip=%s (DB-only)", zip)

	// Derive state from ZIP for state-level candidate filtering
	state := zipPrefixToState(zip)

	type candidateRow struct {
		PoliticianID  uuid.UUID
		ExternalID        int
		FirstName         string
		LastName          string
		FullName          string
		PhotoOriginURL    string
		OfficeTitle       string
		DistrictType      string
		Party             string
		PartyShortName    string
		ElectionDate      string
		ElectionName      string
		IsPrimary         bool
		IsRunoff          bool
		RepresentingState string
		ChamberName       string
		DistrictIDText    string
	}

	var rows []candidateRow
	if err := db.DB.Raw(`
		SELECT
		  p.id AS politician_id,
		  p.external_id,
		  p.first_name, p.last_name, p.full_name,
		  COALESCE(p.photo_custom_url, NULLIF(p.photo_origin_url, '')) AS photo_origin_url,
		  o.title AS office_title,
		  d.district_type,
		  COALESCE(p.party, '') AS party,
		  COALESCE(p.party_short_name, '') AS party_short_name,
		  er.election_date,
		  er.election_name,
		  er.is_primary,
		  er.is_runoff,
		  COALESCE(o.representing_state, '') AS representing_state,
		  COALESCE(c.name, '') AS chamber_name,
		  COALESCE(d.district_id, '') AS district_id_text
		FROM essentials.election_records er
		JOIN essentials.politicians p ON p.id = er.politician_id
		JOIN essentials.offices o ON o.politician_id = p.id
		JOIN essentials.districts d ON d.id = o.district_id
		LEFT JOIN essentials.chambers c ON c.id = o.chamber_id
		WHERE er.is_active = true
		  AND er.withdrawn = false
		  AND (
		    -- Local: politician has a zip_politicians entry for this ZIP
		    (d.district_type IN ('LOCAL_EXEC','LOCAL','COUNTY','SCHOOL','JUDICIAL')
		     AND EXISTS (
		       SELECT 1 FROM essentials.zip_politicians zp
		       WHERE zp.politician_id = p.id AND zp.zip = ?
		     ))
		    OR
		    -- State and federal: match by representing_state
		    (d.district_type IN ('STATE_EXEC','STATE_UPPER','STATE_LOWER','NATIONAL_EXEC','NATIONAL_UPPER','NATIONAL_LOWER')
		     AND (o.representing_state = ? OR ? = ''))
		  )
		ORDER BY er.election_date ASC
	`, zip, state, state).Scan(&rows).Error; err != nil {
		log.Printf("[GetCandidatesByZip] DB error: %v", err)
		writeJSON(w, []CandidateOut{})
		return
	}

	// Batch-fetch images for all candidate politician IDs
	polIDs := make([]uuid.UUID, 0, len(rows))
	for _, row := range rows {
		polIDs = append(polIDs, row.PoliticianID)
	}
	imagesByPolID := fetchImagesByPoliticianIDs(polIDs)

	candidates := make([]CandidateOut, 0, len(rows))
	for _, row := range rows {
		candidates = append(candidates, CandidateOut{
			ID:                row.PoliticianID.String(),
			ExternalID:        row.ExternalID,
			FirstName:         row.FirstName,
			LastName:          row.LastName,
			FullName:          row.FullName,
			PhotoOriginURL:    row.PhotoOriginURL,
			Images:            imagesByPolID[row.PoliticianID],
			OfficeTitle:       row.OfficeTitle,
			DistrictType:      row.DistrictType,
			Party:             row.Party,
			PartyShortName:    row.PartyShortName,
			IsCandidate:       true,
			ElectionDate:      row.ElectionDate,
			ElectionName:      row.ElectionName,
			IsPrimary:         row.IsPrimary,
			IsRunoff:          row.IsRunoff,
			RepresentingState: row.RepresentingState,
			ChamberName:       row.ChamberName,
			DistrictID:        row.DistrictIDText,
		})
	}

	writeJSON(w, candidates)
}

// fetchImagesByPoliticianIDs batch-fetches images for a set of politician UUIDs
// and returns them grouped by politician_id for efficient attachment to CandidateOut.
func fetchImagesByPoliticianIDs(polIDs []uuid.UUID) map[uuid.UUID][]PoliticianImage {
	result := make(map[uuid.UUID][]PoliticianImage, len(polIDs))
	if len(polIDs) == 0 {
		return result
	}
	var images []PoliticianImage
	db.DB.Where("politician_id IN ?", polIDs).Find(&images)
	for _, img := range images {
		result[img.PoliticianID] = append(result[img.PoliticianID], img)
	}
	return result
}

// --- SearchCandidates endpoint (address-based candidate search) ---------------

// SearchCandidates accepts a POST with { "query": "..." } (address or ZIP string),
// geocodes the query, finds matching geofences, and returns active candidates whose
// districts overlap with the resolved geography.
func SearchCandidates(w http.ResponseWriter, r *http.Request) {
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

	if GeoClient == nil {
		http.Error(w, "Address search requires geocoding service configuration", http.StatusServiceUnavailable)
		return
	}

	geoResult, geoErr := GeoClient.Geocode(r.Context(), query)
	if geoErr != nil {
		log.Printf("[SearchCandidates] geocoding failed for %q: %v", query, geoErr)
		writeJSON(w, []CandidateOut{})
		return
	}
	log.Printf("[SearchCandidates] geocoded %q → (%.6f, %.6f) types=%v",
		query, geoResult.Lat, geoResult.Lng, geoResult.ResultTypes)

	// Resolve matching geo_ids using the same area/point strategy as SearchPoliticians
	var geoMatches []GeoMatch

	if geoResult.IsAreaQuery() {
		areaGeoID, areaMTFCC, found := ResolveAreaBoundary(r.Context(), geoResult)
		if found {
			areaMatches, err := FindGeoIDsByAreaIntersection(r.Context(), areaGeoID, areaMTFCC)
			if err == nil && len(areaMatches) > 0 {
				geoMatches = areaMatches
			}
		}
		// Fallback to point-in-polygon if area resolution failed
		if len(geoMatches) == 0 {
			pointMatches, err := FindGeoIDsByPoint(r.Context(), geoResult.Lat, geoResult.Lng)
			if err == nil {
				geoMatches = pointMatches
			}
		}
	} else {
		pointMatches, err := FindGeoIDsByPoint(r.Context(), geoResult.Lat, geoResult.Lng)
		if err == nil {
			geoMatches = pointMatches
		}
	}

	// Extract geo_ids from matches
	geoIDs := make([]string, 0, len(geoMatches))
	for _, m := range geoMatches {
		geoIDs = append(geoIDs, m.GeoID)
	}

	// Derive state for supplemental state/federal candidate lookup
	geoState := strings.ToUpper(geoResult.State)

	type candidateRow struct {
		PoliticianID  uuid.UUID
		ExternalID        int
		FirstName         string
		LastName          string
		FullName          string
		PhotoOriginURL    string
		OfficeTitle       string
		DistrictType      string
		Party             string
		PartyShortName    string
		ElectionDate      string
		ElectionName      string
		IsPrimary         bool
		IsRunoff          bool
		RepresentingState string
		ChamberName       string
		DistrictIDText    string
	}

	var rows []candidateRow

	if len(geoIDs) > 0 {
		// Query candidates whose districts match the resolved geo_ids (local/state/federal)
		if err := db.DB.Raw(`
			SELECT
			  p.id AS politician_id,
			  p.external_id,
			  p.first_name, p.last_name, p.full_name,
			  COALESCE(p.photo_custom_url, NULLIF(p.photo_origin_url, '')) AS photo_origin_url,
			  o.title AS office_title,
			  d.district_type,
			  COALESCE(p.party, '') AS party,
			  COALESCE(p.party_short_name, '') AS party_short_name,
			  er.election_date,
			  er.election_name,
			  er.is_primary,
			  er.is_runoff,
			  COALESCE(o.representing_state, '') AS representing_state,
			  COALESCE(c.name, '') AS chamber_name,
			  COALESCE(d.district_id, '') AS district_id_text
			FROM essentials.election_records er
			JOIN essentials.politicians p ON p.id = er.politician_id
			JOIN essentials.offices o ON o.politician_id = p.id
			JOIN essentials.districts d ON d.id = o.district_id
			LEFT JOIN essentials.chambers c ON c.id = o.chamber_id
			WHERE er.is_active = true
			  AND er.withdrawn = false
			  AND d.geo_id IN ?
			ORDER BY er.election_date ASC
		`, geoIDs).Scan(&rows).Error; err != nil {
			log.Printf("[SearchCandidates] DB error: %v", err)
		}
	}

	// Supplement with state/federal active candidates by representing_state
	if geoState != "" {
		seenPolIDs := make(map[uuid.UUID]bool, len(rows))
		for _, row := range rows {
			seenPolIDs[row.PoliticianID] = true
		}

		var suppRows []candidateRow
		if err := db.DB.Raw(`
			SELECT
			  p.id AS politician_id,
			  p.external_id,
			  p.first_name, p.last_name, p.full_name,
			  COALESCE(p.photo_custom_url, NULLIF(p.photo_origin_url, '')) AS photo_origin_url,
			  o.title AS office_title,
			  d.district_type,
			  COALESCE(p.party, '') AS party,
			  COALESCE(p.party_short_name, '') AS party_short_name,
			  er.election_date,
			  er.election_name,
			  er.is_primary,
			  er.is_runoff,
			  COALESCE(o.representing_state, '') AS representing_state,
			  COALESCE(c.name, '') AS chamber_name,
			  COALESCE(d.district_id, '') AS district_id_text
			FROM essentials.election_records er
			JOIN essentials.politicians p ON p.id = er.politician_id
			JOIN essentials.offices o ON o.politician_id = p.id
			JOIN essentials.districts d ON d.id = o.district_id
			LEFT JOIN essentials.chambers c ON c.id = o.chamber_id
			WHERE er.is_active = true
			  AND er.withdrawn = false
			  AND d.district_type IN ('STATE_EXEC','STATE_UPPER','STATE_LOWER','NATIONAL_EXEC','NATIONAL_UPPER','NATIONAL_LOWER')
			  AND (o.representing_state = ? OR d.district_type = 'NATIONAL_EXEC')
			ORDER BY er.election_date ASC
		`, geoState).Scan(&suppRows).Error; err != nil {
			log.Printf("[SearchCandidates] supplemental query error: %v", err)
		} else {
			for _, sr := range suppRows {
				if !seenPolIDs[sr.PoliticianID] {
					rows = append(rows, sr)
					seenPolIDs[sr.PoliticianID] = true
				}
			}
		}
	}

	// Batch-fetch images for all candidate politician IDs
	polIDs := make([]uuid.UUID, 0, len(rows))
	for _, row := range rows {
		polIDs = append(polIDs, row.PoliticianID)
	}
	imagesByPolID := fetchImagesByPoliticianIDs(polIDs)

	candidates := make([]CandidateOut, 0, len(rows))
	for _, row := range rows {
		candidates = append(candidates, CandidateOut{
			ID:                row.PoliticianID.String(),
			ExternalID:        row.ExternalID,
			FirstName:         row.FirstName,
			LastName:          row.LastName,
			FullName:          row.FullName,
			PhotoOriginURL:    row.PhotoOriginURL,
			Images:            imagesByPolID[row.PoliticianID],
			OfficeTitle:       row.OfficeTitle,
			DistrictType:      row.DistrictType,
			Party:             row.Party,
			PartyShortName:    row.PartyShortName,
			IsCandidate:       true,
			ElectionDate:      row.ElectionDate,
			ElectionName:      row.ElectionName,
			IsPrimary:         row.IsPrimary,
			IsRunoff:          row.IsRunoff,
			RepresentingState: row.RepresentingState,
			ChamberName:       row.ChamberName,
			DistrictID:        row.DistrictIDText,
		})
	}

	log.Printf("[SearchCandidates] returned %d candidates for query=%q", len(candidates), query)
	writeJSON(w, candidates)
}

// ──────────────────────────────────────────────────
// GetQuotes: serves Read & Rank data
// ──────────────────────────────────────────────────

// QuoteOut matches the Read & Rank Quote interface: { id, text, candidateId, issue, sourceUrl, sourceName }
type QuoteOut struct {
	ID          string `json:"id"`
	Text        string `json:"text"`
	CandidateID string `json:"candidateId,omitempty"`
	Issue       string `json:"issue"`
	SourceURL   string `json:"sourceUrl,omitempty"`
	SourceName  string `json:"sourceName,omitempty"`
}

// CandidateReadRankOut matches the Read & Rank Candidate interface.
type CandidateReadRankOut struct {
	ID               string `json:"id"`
	Name             string `json:"name"`
	Party            string `json:"party"`
	Office           string `json:"office"`
	Photo            string `json:"photo"`
	AlignmentPercent int    `json:"alignmentPercent"`
	IssuesAligned    int    `json:"issuesAligned"`
	TotalIssues      int    `json:"totalIssues"`
}

// IssueOut matches the Read & Rank IssueData interface: { id, title, question }
type IssueOut struct {
	ID       string `json:"id"`
	Title    string `json:"title"`
	Question string `json:"question"`
}

// GetQuotes returns quotes, candidates, and issues for the Read & Rank feature.
// All three arrays are derived from the essentials.quotes table, joined with
// essentials.politicians, essentials.offices, and compass.topics.
func GetQuotes(w http.ResponseWriter, r *http.Request) {
	politicianIDStr := r.URL.Query().Get("politician_id")
	var filterPoliticianID *uuid.UUID
	if politicianIDStr != "" {
		pid, err := uuid.Parse(politicianIDStr)
		if err != nil {
			http.Error(w, "invalid politician_id", http.StatusBadRequest)
			return
		}
		filterPoliticianID = &pid
	}

	type quoteRow struct {
		ID          string
		QuoteText   string
		PoliticianID string
		TopicKey    string
		SourceURL   string
		SourceName  string
		FullName    string
		Party       string
		Photo       string
		OfficeTitle string
	}

	var rows []quoteRow
	mainSQL := `
		SELECT
		  q.id::text,
		  q.quote_text,
		  q.politician_id::text,
		  q.topic_key,
		  COALESCE(q.source_url, '') AS source_url,
		  COALESCE(q.source_name, '') AS source_name,
		  p.full_name,
		  COALESCE(p.party, '') AS party,
		  COALESCE(p.photo_custom_url, p.photo_origin_url, '') AS photo,
		  COALESCE(o.title, '') AS office_title
		FROM essentials.quotes q
		JOIN essentials.politicians p ON p.id = q.politician_id
		LEFT JOIN LATERAL (
		  SELECT title FROM essentials.offices
		  WHERE politician_id = p.id
		  ORDER BY id DESC
		  LIMIT 1
		) o ON true`
	var mainArgs []interface{}
	if filterPoliticianID != nil {
		mainSQL += " WHERE q.politician_id = ?"
		mainArgs = append(mainArgs, filterPoliticianID.String())
	}
	mainSQL += " ORDER BY p.full_name, q.topic_key"
	if err := db.DB.Raw(mainSQL, mainArgs...).Scan(&rows).Error; err != nil {
		log.Printf("[GetQuotes] DB error: %v", err)
		http.Error(w, "DB fetch error", http.StatusInternalServerError)
		return
	}

	// Build quotes array and collect unique politician IDs and topic keys
	quotes := make([]QuoteOut, 0, len(rows))
	candidateMap := make(map[string]*CandidateReadRankOut)
	topicKeySet := make(map[string]struct{})

	for _, row := range rows {
		quotes = append(quotes, QuoteOut{
			ID:          row.ID,
			Text:        row.QuoteText,
			CandidateID: row.PoliticianID,
			Issue:       row.TopicKey,
			SourceURL:   row.SourceURL,
			SourceName:  row.SourceName,
		})

		topicKeySet[row.TopicKey] = struct{}{}

		if _, seen := candidateMap[row.PoliticianID]; !seen {
			candidateMap[row.PoliticianID] = &CandidateReadRankOut{
				ID:               row.PoliticianID,
				Name:             row.FullName,
				Party:            row.Party,
				Office:           row.OfficeTitle,
				Photo:            row.Photo,
				AlignmentPercent: 0,
				IssuesAligned:    0,
				TotalIssues:      0,
			}
		}
		candidateMap[row.PoliticianID].TotalIssues++
	}

	// TotalIssues should be distinct topics per candidate, not total quote count
	// Recount using topic_key per politician_id
	type topicCount struct {
		PoliticianID string
		TopicKey     string
	}
	var topicCounts []topicCount
	countSQL := `SELECT DISTINCT politician_id::text, topic_key FROM essentials.quotes`
	var countArgs []interface{}
	if filterPoliticianID != nil {
		countSQL += " WHERE politician_id = ?"
		countArgs = append(countArgs, *filterPoliticianID)
	}
	if err := db.DB.Raw(countSQL, countArgs...).Scan(&topicCounts).Error; err != nil {
		log.Printf("[GetQuotes] topic count error: %v", err)
		// Continue with whatever TotalIssues we have
	} else {
		// Reset and recount
		for _, c := range candidateMap {
			c.TotalIssues = 0
		}
		for _, tc := range topicCounts {
			if c, ok := candidateMap[tc.PoliticianID]; ok {
				c.TotalIssues++
			}
		}
	}

	// Build candidates array (ordered by name for stability)
	candidates := make([]CandidateReadRankOut, 0, len(candidateMap))
	for _, c := range candidateMap {
		candidates = append(candidates, *c)
	}

	// Collect topic keys for querying compass.topics
	topicKeys := make([]string, 0, len(topicKeySet))
	for k := range topicKeySet {
		topicKeys = append(topicKeys, k)
	}

	// Query compass.topics for matching topic_keys
	type topicRow struct {
		TopicKey     string
		Title        string
		QuestionText string
	}
	var topicRows []topicRow
	if len(topicKeys) > 0 {
		if err := db.DB.Raw(`
			SELECT topic_key, title, COALESCE(question_text, '') AS question_text
			FROM compass.topics
			WHERE topic_key = ANY(?)
			ORDER BY title
		`, pq.Array(topicKeys)).Scan(&topicRows).Error; err != nil {
			log.Printf("[GetQuotes] topics query error: %v", err)
			// Non-fatal: return empty issues array
		}
	}

	issues := make([]IssueOut, 0, len(topicRows))
	for _, t := range topicRows {
		issues = append(issues, IssueOut{
			ID:       t.TopicKey,
			Title:    t.Title,
			Question: t.QuestionText,
		})
	}

	writeJSON(w, map[string]any{
		"quotes":     quotes,
		"candidates": candidates,
		"issues":     issues,
	})
}

// ──────────────────────────────────────────────────
// Position Descriptions CRUD (admin)
// ──────────────────────────────────────────────────

func ListPositionDescriptions(w http.ResponseWriter, r *http.Request) {
	var descs []PositionDescription
	if err := db.DB.Order("normalized_position_name, district_type").Find(&descs).Error; err != nil {
		http.Error(w, "DB error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, descs)
}

func UpsertPositionDescription(w http.ResponseWriter, r *http.Request) {
	var input struct {
		NormalizedPositionName string `json:"normalized_position_name"`
		DistrictType           string `json:"district_type"` // empty string = generic
		Description            string `json:"description"`
		Source                 string `json:"source"`
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	if input.NormalizedPositionName == "" || input.Description == "" {
		http.Error(w, "normalized_position_name and description are required", http.StatusBadRequest)
		return
	}
	if input.Source == "" {
		input.Source = "manual"
	}

	desc := PositionDescription{
		NormalizedPositionName: input.NormalizedPositionName,
		DistrictType:           input.DistrictType,
		Description:            input.Description,
		Source:                 input.Source,
	}

	// Upsert on the unique (normalized_position_name, district_type) pair
	if err := db.DB.Where(
		"normalized_position_name = ? AND district_type = ?",
		input.NormalizedPositionName, input.DistrictType,
	).Assign(desc).FirstOrCreate(&desc).Error; err != nil {
		http.Error(w, "DB error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	writeJSON(w, desc)
}

func DeletePositionDescription(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	parsedID, err := uuid.Parse(id)
	if err != nil {
		http.Error(w, "Invalid id", http.StatusBadRequest)
		return
	}

	if err := db.DB.Delete(&PositionDescription{}, parsedID).Error; err != nil {
		http.Error(w, "DB error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// Phase 55: Legislative data handlers

// GetPoliticianCommittees returns committee assignments for a politician.
// Joins committee_memberships with committees and optionally the parent committee
// for subcommittee name resolution.
func GetPoliticianCommittees(w http.ResponseWriter, r *http.Request) {
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
		CommitteeName  string
		Role           string
		Chamber        string
		CongressNumber int
		IsCurrent      bool
		ParentName     string
		CommitteeType  string
	}

	var rows []row
	if err := db.DB.Raw(`
SELECT
    c.name AS committee_name,
    m.role,
    c.chamber,
    m.congress_number,
    m.is_current,
    COALESCE(p.name, '') AS parent_name,
    c.type AS committee_type
FROM essentials.legislative_committee_memberships m
JOIN essentials.legislative_committees c ON c.id = m.committee_id
LEFT JOIN essentials.legislative_committees p ON p.id = c.parent_id
WHERE m.politician_id = ?
ORDER BY m.is_current DESC, c.chamber, c.type, c.name
`, parsedID).Scan(&rows).Error; err != nil {
		http.Error(w, "DB fetch error", http.StatusInternalServerError)
		return
	}

	result := make([]LegislativeCommitteeAssignmentOut, 0, len(rows))
	for _, r := range rows {
		result = append(result, LegislativeCommitteeAssignmentOut{
			CommitteeName:  r.CommitteeName,
			Role:           r.Role,
			Chamber:        r.Chamber,
			CongressNumber: r.CongressNumber,
			IsCurrent:      r.IsCurrent,
			ParentName:     r.ParentName,
			CommitteeType:  r.CommitteeType,
		})
	}

	writeJSON(w, result)
}

// GetPoliticianLeadership returns legislative leadership roles for a politician.
// Sorts current roles first, then by start_date descending.
func GetPoliticianLeadership(w http.ResponseWriter, r *http.Request) {
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
		Title     string
		Chamber   string
		IsCurrent bool
		StartDate *time.Time
		EndDate   *time.Time
	}

	var rows []row
	if err := db.DB.Raw(`
SELECT title, chamber, is_current, start_date, end_date
FROM essentials.legislative_leadership_roles
WHERE politician_id = ?
ORDER BY is_current DESC, start_date DESC
`, parsedID).Scan(&rows).Error; err != nil {
		http.Error(w, "DB fetch error", http.StatusInternalServerError)
		return
	}

	result := make([]LegislativeLeadershipRoleOut, 0, len(rows))
	for _, r := range rows {
		out := LegislativeLeadershipRoleOut{
			Title:     r.Title,
			Chamber:   r.Chamber,
			IsCurrent: r.IsCurrent,
		}
		if r.StartDate != nil {
			out.StartDate = r.StartDate.Format("2006-01-02")
		}
		if r.EndDate != nil {
			out.EndDate = r.EndDate.Format("2006-01-02")
		}
		result = append(result, out)
	}

	writeJSON(w, result)
}

// Phase 56: Bills, votes, and legislative summary handlers

// GetPoliticianBills returns sponsored and cosponsored legislation for a politician.
// Query params:
//   - all=true: include bills with "Introduced" status (default: filter them out)
//   - limit=N: max results (default 50, max 250)
func GetPoliticianBills(w http.ResponseWriter, r *http.Request) {
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

	// Parse query params
	includeAll := r.URL.Query().Get("all") == "true"
	pageLimit := 50
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if n, err := strconv.Atoi(limitStr); err == nil && n > 0 {
			pageLimit = n
			if pageLimit > 250 {
				pageLimit = 250
			}
		}
	}

	type row struct {
		ExternalID   string
		Number       string
		Title        string
		Summary      string
		StatusLabel  string
		IntroducedAt *time.Time
		IsSponsor    bool
		URL          string
		Source       string
	}

	// Build status filter: exclude "Introduced" unless ?all=true
	statusFilter := ""
	if !includeAll {
		statusFilter = "AND b.status_label != 'Introduced'"
	}

	query := fmt.Sprintf(`
SELECT
    b.external_id,
    b.number,
    b.title,
    b.summary,
    b.status_label,
    b.introduced_at,
    CASE WHEN b.sponsor_id = ? THEN true ELSE false END AS is_sponsor,
    b.url,
    b.source
FROM essentials.legislative_bills b
WHERE (b.sponsor_id = ?
       OR b.id IN (SELECT bill_id FROM essentials.legislative_bill_cosponsors WHERE politician_id = ?))
%s
ORDER BY b.introduced_at DESC NULLS LAST
LIMIT ?
`, statusFilter)

	var rows []row
	if err := db.DB.Raw(query, parsedID, parsedID, parsedID, pageLimit).Scan(&rows).Error; err != nil {
		http.Error(w, "DB fetch error", http.StatusInternalServerError)
		return
	}

	result := make([]LegislativeBillOut, 0, len(rows))
	for _, r := range rows {
		out := LegislativeBillOut{
			ExternalID:  r.ExternalID,
			Number:      r.Number,
			Title:       r.Title,
			Summary:     r.Summary,
			StatusLabel: r.StatusLabel,
			IsSponsor:   r.IsSponsor,
			URL:         r.URL,
			Source:      r.Source,
		}
		if r.IntroducedAt != nil {
			out.IntroducedAt = r.IntroducedAt.Format("2006-01-02")
		}
		result = append(result, out)
	}

	writeJSON(w, result)
}

// GetPoliticianVotes returns voting record for a politician.
// Query params:
//   - limit=N: max results (default 50, max 250)
func GetPoliticianVotes(w http.ResponseWriter, r *http.Request) {
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

	pageLimit := 50
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if n, err := strconv.Atoi(limitStr); err == nil && n > 0 {
			pageLimit = n
			if pageLimit > 250 {
				pageLimit = 250
			}
		}
	}

	type row struct {
		VoteQuestion string
		Position     string
		VoteDate     time.Time
		Result       string
		BillTitle    string
		BillNumber   string
		BillURL      string
		Source       string
	}

	var rows []row
	if err := db.DB.Raw(`
SELECT
    v.vote_question,
    v.position,
    v.vote_date,
    v.result,
    COALESCE(b.title, '') AS bill_title,
    COALESCE(b.number, '') AS bill_number,
    COALESCE(b.url, '') AS bill_url,
    v.source
FROM essentials.legislative_votes v
LEFT JOIN essentials.legislative_bills b ON b.id = v.bill_id
WHERE v.politician_id = ?
ORDER BY v.vote_date DESC
LIMIT ?
`, parsedID, pageLimit).Scan(&rows).Error; err != nil {
		http.Error(w, "DB fetch error", http.StatusInternalServerError)
		return
	}

	result := make([]LegislativeVoteOut, 0, len(rows))
	for _, r := range rows {
		result = append(result, LegislativeVoteOut{
			VoteQuestion: r.VoteQuestion,
			Position:     r.Position,
			VoteDate:     r.VoteDate.Format("2006-01-02"),
			Result:       r.Result,
			BillTitle:    r.BillTitle,
			BillNumber:   r.BillNumber,
			BillURL:      r.BillURL,
			Source:       r.Source,
		})
	}

	writeJSON(w, result)
}

// GetPoliticianLegislativeSummary returns a bounded overview for initial profile render.
// Returns 5 recent bills (advanced only) + 10 recent votes in a single response.
func GetPoliticianLegislativeSummary(w http.ResponseWriter, r *http.Request) {
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

	// Fetch 5 recent bills (advanced only — exclude "Introduced")
	type billRow struct {
		ExternalID   string
		Number       string
		Title        string
		Summary      string
		StatusLabel  string
		IntroducedAt *time.Time
		IsSponsor    bool
		URL          string
		Source       string
	}

	var billRows []billRow
	db.DB.Raw(`
SELECT
    b.external_id,
    b.number,
    b.title,
    b.summary,
    b.status_label,
    b.introduced_at,
    CASE WHEN b.sponsor_id = ? THEN true ELSE false END AS is_sponsor,
    b.url,
    b.source
FROM essentials.legislative_bills b
WHERE (b.sponsor_id = ?
       OR b.id IN (SELECT bill_id FROM essentials.legislative_bill_cosponsors WHERE politician_id = ?))
AND b.status_label != 'Introduced'
ORDER BY b.introduced_at DESC NULLS LAST
LIMIT 5
`, parsedID, parsedID, parsedID).Scan(&billRows)

	bills := make([]LegislativeBillOut, 0, len(billRows))
	for _, r := range billRows {
		out := LegislativeBillOut{
			ExternalID:  r.ExternalID,
			Number:      r.Number,
			Title:       r.Title,
			Summary:     r.Summary,
			StatusLabel: r.StatusLabel,
			IsSponsor:   r.IsSponsor,
			URL:         r.URL,
			Source:      r.Source,
		}
		if r.IntroducedAt != nil {
			out.IntroducedAt = r.IntroducedAt.Format("2006-01-02")
		}
		bills = append(bills, out)
	}

	// Fetch 10 recent votes
	type voteRow struct {
		VoteQuestion string
		Position     string
		VoteDate     time.Time
		Result       string
		BillTitle    string
		BillNumber   string
		BillURL      string
		Source       string
	}

	var voteRows []voteRow
	db.DB.Raw(`
SELECT
    v.vote_question,
    v.position,
    v.vote_date,
    v.result,
    COALESCE(b.title, '') AS bill_title,
    COALESCE(b.number, '') AS bill_number,
    COALESCE(b.url, '') AS bill_url,
    v.source
FROM essentials.legislative_votes v
LEFT JOIN essentials.legislative_bills b ON b.id = v.bill_id
WHERE v.politician_id = ?
ORDER BY v.vote_date DESC
LIMIT 10
`, parsedID).Scan(&voteRows)

	votes := make([]LegislativeVoteOut, 0, len(voteRows))
	for _, r := range voteRows {
		votes = append(votes, LegislativeVoteOut{
			VoteQuestion: r.VoteQuestion,
			Position:     r.Position,
			VoteDate:     r.VoteDate.Format("2006-01-02"),
			Result:       r.Result,
			BillTitle:    r.BillTitle,
			BillNumber:   r.BillNumber,
			BillURL:      r.BillURL,
			Source:       r.Source,
		})
	}

	writeJSON(w, LegislativeSummaryOut{
		RecentBills: bills,
		RecentVotes: votes,
	})
}
