package essentials

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
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

type CiceroAPIResponse struct {
	Response struct {
		Results struct {
			Candidates []struct {
				MatchAddress   string `json:"match_addr"`
				MatchCity      string `json:"match_city"`
				MatchSubregion string `json:"match_subregion"`
				MatchRegion    string `json:"match_region"`
				MatchPostal    string `json:"match_postal"`
				Count          struct {
					From  int `json:"from"`
					To    int `json:"to"`
					Total int `json:"total"`
				} `json:"count"`
				Officials []CiceroOfficial `json:"officials"`
			} `json:"candidates"`
		} `json:"results"`
	} `json:"response"`
}

type CiceroOffice struct {
	Title             string         `json:"title"`
	RepresentingState string         `json:"representing_state"`
	RepresentingCity  string         `json:"representing_city"`
	District          CiceroDistrict `json:"district"`
	Chamber           CiceroChamber  `json:"chamber"`
}

type CiceroDistrict struct {
	Type           string `json:"district_type"`
	DistrictID     string `json:"district_id"`
	SK             int    `json:"sk"`
	OCDID          string `json:"ocd_id"`
	Subtype        string `json:"subtype"`
	Label          string `json:"label"`
	State          string `json:"state"`
	City           string `json:"city"`
	NumOfficials   int    `json:"num_officials"`
	ValidFrom      string `json:"valid_from"` // can change to time.Time if you want
	ValidTo        string `json:"valid_to"`
	LastUpdateDate string `json:"last_update_date"`
}

type CiceroChamber struct {
	ID                int              `json:"id"`
	Name              string           `json:"name"`
	NameFormal        string           `json:"name_formal"`
	OfficialCount     int              `json:"official_count" gorm:"omitempty"`
	TermLimit         string           `json:"term_limit"`
	TermLength        string           `json:"term_length"`
	InaugurationRules string           `json:"inauguration_rules"`
	ElectionFrequency string           `json:"election_frequency"`
	ElectionRules     string           `json:"election_rules"`
	VacancyRules      string           `json:"vacancy_rules"`
	Remarks           string           `json:"remarks"`
	Government        CiceroGovernment `json:"government"`
}

type CiceroGovernment struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	State string `json:"state"`
	City  string `json:"city"`
}

type CiceroAddress struct {
	Address1   string `json:"address_1"`
	Address2   string `json:"address_2"`
	Address3   string `json:"address_3"`
	State      string `json:"state"`
	PostalCode string `json:"postal_code"`
	Phone1     string `json:"phone_1"`
	Phone2     string `json:"phone_2"`
}

type CiceroIdentifier struct {
	ID              int    `json:"id"`
	IdentifierType  string `json:"identifier_type"` // Consider changing to something like Platform for clarity
	IdentifierValue string `json:"identifier_value"`
}

type CiceroCommittee struct {
	Name                 string             `json:"name"`
	URLs                 []string           `json:"urls"`
	CommitteeIdentifiers []CiceroIdentifier `json:"committee_identifiers"`
	Position             string             `json:"position"`
}

type CiceroOfficial struct {
	OfficialID     int                `json:"id"`
	FirstName      string             `json:"first_name"`
	MiddleInitial  string             `json:"middle_initial"`
	LastName       string             `json:"last_name"`
	PreferredName  string             `json:"preferred_name"`
	NameSuffix     string             `json:"name_suffix"`
	Party          string             `json:"party"`
	WebFormURL     *string            `json:"web_form_url"`
	Urls           []string           `json:"urls"`
	EmailAddresses []string           `json:"email_addresses"`
	PhotoOriginURL string             `json:"photo_origin_url"`
	Notes          []string           `json:"notes" `
	ValidFrom      string             `json:"valid_from"`
	ValidTo        string             `json:"valid_to"`
	Office         CiceroOffice       `json:"office"`
	Addresses      []CiceroAddress    `json:"addresses"`
	Identifiers    []CiceroIdentifier `json:"identifiers"`
	Committees     []CiceroCommittee  `json:"committees"`
}

type CommitteeOut struct {
	Name     string   `json:"name"`
	Position string   `json:"position"`
	URLs     []string `json:"urls"`
}

type OfficialOut struct {
	ID                uuid.UUID      `json:"id"`
	ExternalID        int            `json:"external_id"`
	FirstName         string         `json:"first_name"`
	MiddleInitial     string         `json:"middle_initial"`
	LastName          string         `json:"last_name"`
	PreferredName     string         `json:"preferred_name"`
	NameSuffix        string         `json:"name_suffix"`
	FullName          string         `json:"full_name"`
	Party             string         `json:"party"`
	PhotoOriginURL    string         `json:"photo_origin_url"`
	WebFormURL        string         `json:"web_form_url"`
	URLs              []string       `json:"urls"`
	EmailAddresses    []string       `json:"email_addresses"`
	OfficeTitle       string         `json:"office_title"`
	RepresentingState string         `json:"representing_state"`
	RepresentingCity  string         `json:"representing_city"`
	DistrictType      string         `json:"district_type"`
	DistrictLabel     string         `json:"district_label"`
	ChamberName       string         `json:"chamber_name"`
	ChamberNameFormal string         `json:"chamber_name_formal"`
	GovernmentName    string         `json:"government_name"`
	Committees        []CommitteeOut `json:"committees"`
}

func GetPoliticiansByZip(w http.ResponseWriter, r *http.Request) {
	zip := chi.URLParam(r, "zip")
	if zip == "" {
		http.Error(w, "Missing zip parameter", http.StatusBadRequest)
		return
	}

	now := time.Now()
	const maxAge = 7 * 24 * time.Hour

	// Try cache
	var cache ZipCache
	err := db.DB.Where("zip = ?", zip).First(&cache).Error
	if err == nil && now.Sub(cache.LastFetched) < maxAge {
		officials, err := fetchOfficialsFromDB(zip)
		if err != nil {
			http.Error(w, "DB fetch error", http.StatusInternalServerError)
			return
		}
		if len(officials) > 0 {
			writeJSON(w, officials)
			return
		}
		// Fresh cache but empty mapping -> treat as stale and refresh
	}

	// 2) Not cached or stale => call Cicero, upsert, and refresh cache
	officials, err := fetchAllCiceroOfficials(zip)
	if err != nil {
		http.Error(w, "Failed to contact Cicero API", http.StatusInternalServerError)
		return
	}

	if len(officials) == 0 {
		// Update an empty cache so we don't spam Cicero
		_ = db.DB.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "zip"}},
			DoUpdates: clause.Assignments(map[string]any{"last_fetched": now}),
		}).Create(&ZipCache{Zip: zip, LastFetched: now}).Error

		writeJSON(w, []OfficialOut{})
		return
	}

	// Upsert + collect politician IDs we touched for this zip
	touched := make([]uuid.UUID, 0, len(officials))

	for _, off := range officials {
		tr, err := TransformCiceroData(off)
		if err != nil {
			continue
		}

		var polID uuid.UUID

		// Run your existing transaction + capture politician UUID for this official
		err = db.DB.Transaction(func(tx *gorm.DB) error {
			// ==== District ====
			if tr.District != nil {
				if err := tx.Clauses(clause.OnConflict{
					Columns: []clause.Column{{Name: "external_id"}},
					DoUpdates: clause.AssignmentColumns([]string{
						"ocd_id", "label", "district_type", "district_id", "subtype",
						"state", "city", "num_officials", "valid_from", "valid_to",
					}),
				}).Create(tr.District).Error; err != nil {
					return err
				}
			}

			// ==== Government (natural key) ====
			var govID *uuid.UUID
			if tr.Government != nil {
				var existingGov Government
				err := tx.Where(
					"name = ? AND type = ? AND state = ? AND city = ?",
					tr.Government.Name, tr.Government.Type, tr.Government.State, tr.Government.City,
				).First(&existingGov).Error
				if err == nil {
					govID = &existingGov.ID
				} else {
					if err := tx.Create(tr.Government).Error; err != nil {
						return err
					}
					govID = &tr.Government.ID
				}
			}

			// ==== Chamber ====
			if tr.Chamber != nil {
				if govID != nil {
					tr.Chamber.GovernmentID = *govID
				}
				if err := tx.Clauses(clause.OnConflict{
					Columns: []clause.Column{{Name: "external_id"}},
					DoUpdates: clause.AssignmentColumns([]string{
						"government_id", "name", "name_formal", "official_count",
						"term_limit", "term_length", "inauguration_rules",
						"election_frequency", "election_rules", "vacancy_rules", "remarks",
					}),
				}).Create(tr.Chamber).Error; err != nil {
					return err
				}
			}

			// ==== Politician ====
			// ==== Politician ====
			if tr.Politician != nil {
				// Build per-column assignments so we can protect photo_origin_url from being overwritten by empty values.
				assign := map[string]interface{}{
					"first_name":     gorm.Expr("excluded.first_name"),
					"middle_initial": gorm.Expr("excluded.middle_initial"),
					"last_name":      gorm.Expr("excluded.last_name"),
					"preferred_name": gorm.Expr("excluded.preferred_name"),
					"name_suffix":    gorm.Expr("excluded.name_suffix"),
					"party":          gorm.Expr("excluded.party"),
					"web_form_url":   gorm.Expr("excluded.web_form_url"),
					"urls":           gorm.Expr("excluded.urls"),
					// Keep existing value when EXCLUDED is NULL or empty string:
					"photo_origin_url": gorm.Expr(
						`COALESCE(NULLIF(excluded.photo_origin_url, ''), "essentials"."politicians"."photo_origin_url")`,
					),
					"notes":           gorm.Expr("excluded.notes"),
					"valid_from":      gorm.Expr("excluded.valid_from"),
					"valid_to":        gorm.Expr("excluded.valid_to"),
					"email_addresses": gorm.Expr("excluded.email_addresses"),
					"office_id":       gorm.Expr("excluded.office_id"),
				}

				if err := tx.
					Omit("Addresses", "Identifiers", "Committees").
					Clauses(clause.OnConflict{
						Columns:   []clause.Column{{Name: "external_id"}},
						DoUpdates: clause.Assignments(assign),
					}).
					Create(tr.Politician).Error; err != nil {
					return err
				}
			}

			var persistedPol Politician
			if err := tx.Where("external_id = ?", off.OfficialID).First(&persistedPol).Error; err != nil {
				return err
			}
			polID = persistedPol.ID

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

			// ==== Office (upsert by politician_id) ====
			office := Office{
				ID:                tr.Politician.OfficeID,
				PoliticianID:      persistedPol.ID,
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
			if err := tx.Where("politician_id = ?", persistedPol.ID).Delete(&Address{}).Error; err != nil {
				return err
			}
			if len(tr.Politician.Addresses) > 0 {
				for i := range tr.Politician.Addresses {
					tr.Politician.Addresses[i].PoliticianID = persistedPol.ID
				}
				if err := tx.Create(&tr.Politician.Addresses).Error; err != nil {
					return err
				}
			}

			// ==== Identifiers ====
			if err := tx.Where("politician_id = ?", persistedPol.ID).Delete(&Identifier{}).Error; err != nil {
				return err
			}
			if len(tr.Politician.Identifiers) > 0 {
				seen := make(map[string]struct{}, len(tr.Politician.Identifiers))
				for _, it := range tr.Politician.Identifiers {
					t := strings.TrimSpace(it.IdentifierType)
					v := strings.TrimSpace(it.IdentifierValue)
					if t == "" || v == "" {
						continue
					}
					key := strings.ToLower(t) + "||" + strings.ToLower(v)
					if _, ok := seen[key]; ok {
						continue
					}
					seen[key] = struct{}{}

					if err := tx.Exec(
						`INSERT INTO essentials.identifiers
               (politician_id, identifier_type, identifier_value)
             VALUES (?, ?, ?)
             ON CONFLICT DO NOTHING`, // <- catch-all
						persistedPol.ID, t, v,
					).Error; err != nil {
						return err
					}
				}
			}

			// ==== Committees & joins ====
			// Upsert committees by name (case-insensitive) and build name->ID map
			committeeIDByName := make(map[string]uuid.UUID)
			norm := func(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

			for _, c := range tr.Committees {
				if strings.TrimSpace(c.Name) == "" {
					continue
				}
				var existing Committee
				if err := tx.Where("LOWER(name) = LOWER(?)", c.Name).First(&existing).Error; err == nil {
					// update URLs if changed
					if !committeeEqual(*c, existing) {
						existing.URLs = c.URLs
						if err := tx.Save(&existing).Error; err != nil {
							return err
						}
					}
					committeeIDByName[norm(c.Name)] = existing.ID
				} else if errors.Is(err, gorm.ErrRecordNotFound) {
					if err := tx.Create(c).Error; err != nil {
						return err
					}
					committeeIDByName[norm(c.Name)] = c.ID
				} else {
					return err
				}
			}

			// Decide one position per committee from the raw Cicero payload,
			// preferring the first non-empty position
			type namePos struct {
				Name     string
				Position string
			}
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

			// Insert/Upsert joins using the IDs resolved above (never using transform IDs)
			for k, np := range posByName {
				cid, ok := committeeIDByName[k]
				if !ok {
					// Fallback: resolve by name again (in case transform filtered something)
					var existing Committee
					if err := tx.Where("LOWER(name) = LOWER(?)", np.Name).First(&existing).Error; err == nil {
						cid = existing.ID
					} else if errors.Is(err, gorm.ErrRecordNotFound) {
						// Last resort: create a minimal committee row
						minimal := &Committee{ID: uuid.New(), Name: np.Name}
						if err := tx.Create(minimal).Error; err != nil {
							return err
						}
						cid = minimal.ID
					} else {
						return err
					}
				}

				row := PoliticianCommittee{
					PoliticianID: persistedPol.ID,
					CommitteeID:  cid,
					Position:     np.Position,
				}
				if err := tx.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "politician_id"}, {Name: "committee_id"}},
					DoUpdates: clause.AssignmentColumns([]string{"position"}),
				}).Create(&row).Error; err != nil {
					return err
				}
			}

			return nil
		})
		if err == nil && polID != uuid.Nil {
			touched = append(touched, polID)
		}
	}

	// Refresh cache atomically
	_ = db.DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "zip"}},
			DoUpdates: clause.Assignments(map[string]any{"last_fetched": now}),
		}).Create(&ZipCache{Zip: zip, LastFetched: now}).Error; err != nil {
			return err
		}

		// Replace mapping
		if err := tx.Where("zip = ?", zip).Delete(&ZipPolitician{}).Error; err != nil {
			return err
		}
		for _, id := range touched {
			if err := tx.Create(&ZipPolitician{
				Zip: zip, PoliticianID: id, LastSeen: now,
			}).Error; err != nil {
				return err
			}
		}
		return nil
	})

	// Return fresh data from DB
	officialsOut, err := fetchOfficialsFromDB(zip)
	if err != nil {
		http.Error(w, "DB fetch error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, officialsOut)
}

// fetchOfficialsFromDB returns all officials for a zip using the zip cache mapping.
func fetchOfficialsFromDB(zip string) ([]OfficialOut, error) {
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
		ChamberName       string
		ChamberNameFormal string
		GovernmentName    string
	}

	var rows []row
	if err := db.DB.Raw(`
		SELECT
		  p.id, p.external_id, p.first_name, p.middle_initial, p.last_name,
		  p.preferred_name, p.name_suffix, p.full_name, p.party,
		  COALESCE(p.photo_custom_url, NULLIF(p.photo_origin_url, '')) AS photo_origin_url, 
		  p.web_form_url, p.urls, p.email_addresses,
		  o.title AS office_title, o.representing_state, o.representing_city,
		  d.district_type, d.label AS district_label,
		  c.name AS chamber_name, c.name_formal AS chamber_name_formal,
		  g.name AS government_name
		FROM essentials.politicians p
		JOIN essentials.offices o ON o.politician_id = p.id
		JOIN essentials.districts d ON d.id = o.district_id
		JOIN essentials.chambers c ON c.id = o.chamber_id
		JOIN essentials.governments g ON g.id = c.government_id
		JOIN essentials.zip_politicians zp ON zp.politician_id = p.id
		WHERE zp.zip = ?
		ORDER BY o.title, p.last_name, p.first_name
	`, zip).Scan(&rows).Error; err != nil {
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
			ChamberName: r.ChamberName, ChamberNameFormal: r.ChamberNameFormal,
			GovernmentName: r.GovernmentName,
			Committees:     byPol[r.ID],
		})
	}

	return out, nil
}

// fetchAllCiceroOfficials returns all officials for a ZIP using max/offset paging.
func fetchAllCiceroOfficials(zip string) ([]CiceroOfficial, error) {
	const pageMax = 199

	apiURL := "https://app.cicerodata.com/v3.1/official"

	// Static params
	base := url.Values{}
	base.Set("search_postal", zip)
	base.Set("search_country", "US")
	base.Set("format", "json")
	base.Set("key", os.Getenv("CICERO_KEY"))
	base.Set("max", strconv.Itoa(pageMax))

	districtTypes := []string{"NATIONAL_EXEC", "NATIONAL_UPPER", "NATIONAL_LOWER", "STATE_EXEC", "STATE_UPPER", "STATE_LOWER", "LOCAL_EXEC", "LOCAL", "COUNTY", "SCHOOL", "JUDICIAL"}
	for _, dt := range districtTypes {
		base.Add("district_type", dt)
	}

	var all []CiceroOfficial
	offset := 0

	for {
		params := url.Values{}
		for k, vs := range base {
			for _, v := range vs {
				params.Add(k, v)
			}
		}
		params.Set("offset", strconv.Itoa(offset))

		fullURL := fmt.Sprintf("%s?%s", apiURL, params.Encode())

		resp, err := http.Get(fullURL)
		if err != nil {
			return nil, fmt.Errorf("cicero request failed: %w", err)
		}
		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("cicero status %d", resp.StatusCode)
		}

		var page CiceroAPIResponse
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("decode cicero: %w", err)
		}
		_ = resp.Body.Close()

		// Flatten this page
		pageCount := 0
		for _, c := range page.Response.Results.Candidates {
			all = append(all, c.Officials...)
			pageCount += len(c.Officials)
		}

		// Stop if this page returned less than requested max
		if pageCount < pageMax {
			break
		}

		offset += pageCount
	}

	return all, nil
}

func GetPoliticianByID(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "Missing id parameter", http.StatusBadRequest)
		return
	}

	var politician Politician
	err := db.DB.Preload("Addresses").Preload("Identifiers").Preload("Committees").First(&politician, "id = ?", id).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}
		http.Error(w, "DB fetch error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, politician)
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
		ChamberName       string
		ChamberNameFormal string
		GovernmentName    string
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
	  d.district_type, d.label AS district_label,
	  c.name AS chamber_name, c.name_formal AS chamber_name_formal,
	  g.name AS government_name
	FROM essentials.politicians p
	JOIN essentials.offices o   ON o.politician_id = p.id
	JOIN essentials.districts d ON d.id = o.district_id
	JOIN essentials.chambers c  ON c.id = o.chamber_id
	JOIN essentials.governments g ON g.id = c.government_id
	WHERE %s
	ORDER BY o.title, p.last_name, p.first_name
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
			ChamberName: r.ChamberName, ChamberNameFormal: r.ChamberNameFormal,
			GovernmentName: r.GovernmentName,
			Committees:     byPol[r.ID],
		})
	}

	writeJSON(w, out)
}
