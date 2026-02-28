package essentials

import (
	"context"
	"fmt"
	"strings"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/EmpoweredVote/EV-Backend/internal/essentials/geocoding"
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
	"G4120": {"LOCAL", "LOCAL_EXEC"},         // Consolidated City (e.g. Nashville-Davidson)
	"G5400": {"SCHOOL"},                      // Elementary School District
	"G5410": {"SCHOOL"},                      // Secondary School District
	"G5420": {"SCHOOL"},                      // Unified School District
	"X0001": {"LOCAL"},                       // City council sub-districts (BallotReady custom MTFCC)
}

// FindGeoIDsByPoint performs a PostGIS point-in-polygon query to find all
// geofences (districts) that contain the given lat/lng coordinate.
// Returns geo_id + MTFCC pairs for disambiguation.
func FindGeoIDsByPoint(ctx context.Context, lat, lng float64) ([]GeoMatch, error) {
	query := `
		SELECT geo_id, COALESCE(mtfcc, '') as mtfcc
		FROM essentials.geofence_boundaries
		WHERE ST_Covers(
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
			COALESCE(p.first_name, '') AS first_name,
			COALESCE(p.middle_initial, '') AS middle_initial,
			COALESCE(p.last_name, '') AS last_name,
			COALESCE(p.preferred_name, '') AS preferred_name,
			COALESCE(p.name_suffix, '') AS name_suffix,
			COALESCE(p.full_name, '') AS full_name,
			COALESCE(p.party, '') AS party,
			COALESCE(p.party_short_name, '') AS party_short_name,
			COALESCE(p.photo_origin_url, '') AS photo_origin_url,
			COALESCE(p.web_form_url, '') AS web_form_url,
			p.urls,
			p.email_addresses,
			COALESCE(o.title, '') AS office_title,
			COALESCE(o.representing_state, '') AS representing_state,
			COALESCE(o.representing_city, '') AS representing_city,
			COALESCE(d.district_type, '') AS district_type,
			COALESCE(d.label, '') AS district_label,
			COALESCE(d.mtfcc, '') AS mtfcc,
			COALESCE(ch.name, '') AS chamber_name,
			COALESCE(ch.name_formal, '') AS chamber_name_formal,
			COALESCE(g.name, '') AS government_name,
			COALESCE(p.is_appointed, false) AS is_appointed,
			COALESCE(p.is_vacant, false) AS is_vacant,
			COALESCE(p.is_off_cycle, false) AS is_off_cycle,
			COALESCE(p.specificity, '') AS specificity,
			COALESCE(ch.election_frequency, '') AS election_frequency,
			COALESCE(o.seats, 0) AS seats,
			COALESCE(o.normalized_position_name, '') AS normalized_position_name,
			COALESCE(o.partisan_type, '') AS partisan_type,
			COALESCE(o.salary, '') AS salary,
			COALESCE(d.geo_id, '') AS geo_id,
			COALESCE(d.is_judicial, false) AS is_judicial,
			COALESCE(d.ocd_id, '') AS ocd_id,
			COALESCE(p.bio_text, '') AS bio_text,
			COALESCE(p.bioguide_id, '') AS bioguide_id,
			COALESCE(p.slug, '') AS slug,
			COALESCE(d.district_id, '') AS district_id_text
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
			&off.DistrictID,
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

// FindGeoIDsByAreaIntersection finds all geofence boundaries (districts) that
// spatially intersect the boundary of a queried area (city, ZIP, county).
// The area is identified by looking up its boundary from geofence_boundaries
// using the provided geo_id and MTFCC, then finding all other boundaries that
// ST_Intersects with it.
//
// If the area boundary is not found in the database, returns nil, nil (no error)
// to allow fallback to point-in-polygon.
func FindGeoIDsByAreaIntersection(ctx context.Context, areaGeoID, areaMTFCC string) ([]GeoMatch, error) {
	query := `
		SELECT DISTINCT gb2.geo_id, COALESCE(gb2.mtfcc, '') as mtfcc
		FROM essentials.geofence_boundaries gb1
		JOIN essentials.geofence_boundaries gb2
		  ON ST_Intersects(gb1.geometry, gb2.geometry)
		WHERE gb1.geo_id = $1
		  AND gb1.mtfcc = $2
		  AND gb2.geo_id != gb1.geo_id
	`

	rows, err := db.DB.WithContext(ctx).Raw(query, areaGeoID, areaMTFCC).Rows()
	if err != nil {
		return nil, fmt.Errorf("area intersection query failed: %w", err)
	}
	defer rows.Close()

	var matches []GeoMatch
	for rows.Next() {
		var m GeoMatch
		if err := rows.Scan(&m.GeoID, &m.MTFCC); err != nil {
			return nil, fmt.Errorf("scan area geo match: %w", err)
		}
		matches = append(matches, m)
	}

	return matches, nil
}

// ResolveAreaBoundary determines the geo_id and MTFCC for an area boundary
// based on geocoding results. Uses the area's geofence_boundaries record.
//
// Resolution priority:
// 1. ZIP: geo_id is the 5-digit ZIP code, MTFCC is looked up from the boundary table
// 2. City: look up by MTFCC G4110 (incorporated place) or G4120 (consolidated city)
//    matching on state + name
// 3. County: look up by MTFCC G4020 matching on state + name
//
// Returns geo_id, mtfcc, found. If not found, returns "", "", false.
func ResolveAreaBoundary(ctx context.Context, geoResult *geocoding.Result) (string, string, bool) {
	// Try ZIP first (simplest — ZIP geo_id is the ZIP code itself)
	if geoResult.Zip != "" {
		var count int64
		db.DB.WithContext(ctx).Raw(
			"SELECT COUNT(*) FROM essentials.geofence_boundaries WHERE geo_id = ?",
			geoResult.Zip,
		).Scan(&count)
		if count > 0 {
			// ZIP boundaries may have empty MTFCC or a ZCTA code
			var mtfcc string
			db.DB.WithContext(ctx).Raw(
				"SELECT COALESCE(mtfcc, '') FROM essentials.geofence_boundaries WHERE geo_id = ? LIMIT 1",
				geoResult.Zip,
			).Scan(&mtfcc)
			return geoResult.Zip, mtfcc, true
		}
	}

	// Try city boundary (G4110 = Incorporated Place, G4120 = Consolidated City)
	if geoResult.City != "" && geoResult.State != "" {
		var geoID, mtfcc string
		err := db.DB.WithContext(ctx).Raw(`
			SELECT geo_id, mtfcc
			FROM essentials.geofence_boundaries
			WHERE state = ? AND mtfcc IN ('G4110', 'G4120')
			  AND LOWER(name) = LOWER(?)
			LIMIT 1
		`, geoResult.State, geoResult.City).Row().Scan(&geoID, &mtfcc)
		if err == nil && geoID != "" {
			return geoID, mtfcc, true
		}
	}

	// Try county boundary (G4020)
	if geoResult.County != "" && geoResult.State != "" {
		// Match on county name prefix (handles "Monroe County" in DB vs "Monroe County" from geocoder)
		countyName := geoResult.County
		var geoID string
		err := db.DB.WithContext(ctx).Raw(`
			SELECT geo_id
			FROM essentials.geofence_boundaries
			WHERE state = ? AND mtfcc = 'G4020'
			  AND LOWER(name) LIKE LOWER(?)
			LIMIT 1
		`, geoResult.State, countyName+"%").Row().Scan(&geoID)
		if err == nil && geoID != "" {
			return geoID, "G4020", true
		}
	}

	return "", "", false
}
