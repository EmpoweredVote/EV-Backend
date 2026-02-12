package essentials

import (
	"context"
	"fmt"
	"strings"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

// GeoMatch represents a geofence hit: a geo_id + its MTFCC feature class code.
// The MTFCC is needed to disambiguate SLDU vs SLDL boundaries that share geo_ids.
type GeoMatch struct {
	GeoID string
	MTFCC string
}

// mtfccToDistrictTypes maps Census MTFCC codes to BallotReady district types.
// Used to prevent cross-matching when SLDU and SLDL share the same geo_id.
var mtfccToDistrictTypes = map[string][]string{
	"G5210": {"STATE_UPPER"},                 // State Legislative District (Upper)
	"G5220": {"STATE_LOWER"},                 // State Legislative District (Lower)
	"G5200": {"NATIONAL_LOWER"},              // Congressional District
	"G4020": {"COUNTY", "JUDICIAL"},          // County — also used for county-level judicial
	"G4040": {"LOCAL", "LOCAL_EXEC"},         // County Subdivision (township)
	"G4110": {"LOCAL", "LOCAL_EXEC"},         // Incorporated Place (city/town)
	"G5420": {"SCHOOL"},                      // Unified School District
}

// FindGeoIDsByPoint performs a PostGIS point-in-polygon query to find all
// geofences (districts) that contain the given lat/lng coordinate.
// Returns geo_id + MTFCC pairs for disambiguation.
func FindGeoIDsByPoint(ctx context.Context, lat, lng float64) ([]GeoMatch, error) {
	query := `
		SELECT geo_id, COALESCE(mtfcc, '') as mtfcc
		FROM essentials.geofence_boundaries
		WHERE ST_Contains(
			geometry,
			ST_SetSRID(ST_MakePoint($1, $2), 4326)
		)
	`

	rows, err := db.DB.WithContext(ctx).Raw(query, lng, lat).Rows()
	if err != nil {
		return nil, fmt.Errorf("geofence lookup query failed: %w", err)
	}
	defer rows.Close()

	var matches []GeoMatch
	for rows.Next() {
		var m GeoMatch
		if err := rows.Scan(&m.GeoID, &m.MTFCC); err != nil {
			return nil, fmt.Errorf("scan geo match: %w", err)
		}
		matches = append(matches, m)
	}

	return matches, nil
}

// FindPoliticiansByGeoMatches looks up all politicians whose districts match
// the given geo matches. Uses MTFCC→district_type mapping to prevent
// cross-matching between SLDU and SLDL when they share the same geo_id.
func FindPoliticiansByGeoMatches(ctx context.Context, matches []GeoMatch) ([]OfficialOut, error) {
	if len(matches) == 0 {
		return []OfficialOut{}, nil
	}

	// Build WHERE conditions: for each geo match, allow only compatible district types
	// This prevents SLDU geo_id 18046 from matching SLDL district 46 (and vice versa)
	var conditions []string
	var args []interface{}
	argIdx := 1

	// Track state FIPS codes from county matches to include state-level judicial officials
	stateFIPSSeen := make(map[string]bool)

	for _, m := range matches {
		if allowedTypes, ok := mtfccToDistrictTypes[m.MTFCC]; ok {
			// Known MTFCC: restrict to matching district types
			conditions = append(conditions, fmt.Sprintf(
				"(d.geo_id = $%d AND d.district_type = ANY($%d))",
				argIdx, argIdx+1,
			))
			args = append(args, m.GeoID, pq.Array(allowedTypes))
			argIdx += 2

			// County match: extract state FIPS for state-level judicial lookup
			if m.MTFCC == "G4020" && len(m.GeoID) >= 2 {
				stateFIPSSeen[m.GeoID[:2]] = true
			}
		} else {
			// Unknown MTFCC: match any district type for this geo_id
			conditions = append(conditions, fmt.Sprintf("d.geo_id = $%d", argIdx))
			args = append(args, m.GeoID)
			argIdx++
		}
	}

	// Include state-level judicial officials (Supreme Court, Appeals Court)
	// whose geo_id is the state FIPS code (e.g. "18" for Indiana).
	// These are retention ballot judges that all voters in the state see.
	for fips := range stateFIPSSeen {
		conditions = append(conditions, fmt.Sprintf(
			"(d.geo_id = $%d AND d.district_type = 'JUDICIAL')",
			argIdx,
		))
		args = append(args, fips)
		argIdx++
	}

	whereClause := strings.Join(conditions, " OR ")

	query := fmt.Sprintf(`
		SELECT DISTINCT ON (p.id)
			p.id,
			p.external_id,
			p.first_name,
			p.middle_initial,
			p.last_name,
			p.preferred_name,
			p.name_suffix,
			p.full_name,
			p.party,
			p.party_short_name,
			p.photo_origin_url,
			p.web_form_url,
			p.urls,
			p.email_addresses,
			o.title as office_title,
			o.representing_state,
			o.representing_city,
			d.district_type,
			d.label as district_label,
			d.mtfcc,
			ch.name as chamber_name,
			ch.name_formal as chamber_name_formal,
			g.name as government_name,
			p.is_appointed,
			p.is_vacant,
			p.is_off_cycle,
			p.specificity,
			ch.election_frequency,
			o.seats,
			o.normalized_position_name,
			o.partisan_type,
			o.salary,
			d.geo_id,
			d.is_judicial,
			d.ocd_id,
			p.bio_text,
			p.bioguide_id,
			p.slug
		FROM essentials.politicians p
		JOIN essentials.offices o ON o.politician_id = p.id
		JOIN essentials.districts d ON o.district_id = d.id
		LEFT JOIN essentials.chambers ch ON o.chamber_id = ch.id
		LEFT JOIN essentials.governments g ON ch.government_id = g.id
		WHERE (%s)
		ORDER BY p.id, d.district_type
	`, whereClause)

	rows, err := db.DB.WithContext(ctx).Raw(query, args...).Rows()
	if err != nil {
		return nil, fmt.Errorf("politicians lookup failed: %w", err)
	}
	defer rows.Close()

	var officials []OfficialOut
	for rows.Next() {
		var off OfficialOut
		var urls, emails pq.StringArray
		if err := rows.Scan(
			&off.ID,
			&off.ExternalID,
			&off.FirstName,
			&off.MiddleInitial,
			&off.LastName,
			&off.PreferredName,
			&off.NameSuffix,
			&off.FullName,
			&off.Party,
			&off.PartyShortName,
			&off.PhotoOriginURL,
			&off.WebFormURL,
			&urls,
			&emails,
			&off.OfficeTitle,
			&off.RepresentingState,
			&off.RepresentingCity,
			&off.DistrictType,
			&off.DistrictLabel,
			&off.MTFCC,
			&off.ChamberName,
			&off.ChamberNameFormal,
			&off.GovernmentName,
			&off.IsAppointed,
			&off.IsVacant,
			&off.IsOffCycle,
			&off.Specificity,
			&off.ElectionFrequency,
			&off.Seats,
			&off.NormalizedPositionName,
			&off.PartisanType,
			&off.Salary,
			&off.GeoID,
			&off.IsJudicial,
			&off.OCDID,
			&off.BioText,
			&off.BioguideID,
			&off.Slug,
		); err != nil {
			return nil, fmt.Errorf("scan official: %w", err)
		}

		off.URLs = []string(urls)
		off.EmailAddresses = []string(emails)

		// Derive IsElected (simple logic: not appointed = elected)
		off.IsElected = !off.IsAppointed

		officials = append(officials, off)
	}

	if len(officials) == 0 {
		return officials, nil
	}

	// Batch-load images for all politicians
	ids := make([]uuid.UUID, 0, len(officials))
	for _, o := range officials {
		ids = append(ids, o.ID)
	}

	type imageRow struct {
		PoliticianID uuid.UUID
		URL          string
		Type         string
	}
	var imageRows []imageRow
	if err := db.DB.WithContext(ctx).Raw(`
		SELECT politician_id, url, type
		FROM essentials.politician_images
		WHERE politician_id = ANY(?)
		ORDER BY type
	`, pq.Array(ids)).Scan(&imageRows).Error; err != nil {
		return nil, fmt.Errorf("fetch images: %w", err)
	}

	imagesByPol := make(map[uuid.UUID][]ImageOut)
	for _, img := range imageRows {
		imagesByPol[img.PoliticianID] = append(imagesByPol[img.PoliticianID], ImageOut{
			URL:  img.URL,
			Type: img.Type,
		})
	}

	for i := range officials {
		officials[i].Images = imagesByPol[officials[i].ID]
	}

	return officials, nil
}
