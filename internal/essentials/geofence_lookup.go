package essentials

import (
	"context"
	"fmt"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

// FindGeoIDsByPoint performs a PostGIS point-in-polygon query to find all
// geofences (districts) that contain the given lat/lng coordinate.
// Returns a list of Geo-IDs that can be used to look up politicians.
func FindGeoIDsByPoint(ctx context.Context, lat, lng float64) ([]string, error) {
	var geoIDs []string

	// PostGIS query: ST_Contains checks if the geometry contains the point
	// ST_SetSRID creates a point in WGS84 (SRID 4326)
	query := `
		SELECT geo_id
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

	for rows.Next() {
		var geoID string
		if err := rows.Scan(&geoID); err != nil {
			return nil, fmt.Errorf("scan geo_id: %w", err)
		}
		geoIDs = append(geoIDs, geoID)
	}

	return geoIDs, nil
}

// FindPoliticiansByGeoIDs looks up all politicians whose districts match
// the given Geo-IDs. This is the second step after FindGeoIDsByPoint.
func FindPoliticiansByGeoIDs(ctx context.Context, geoIDs []string) ([]OfficialOut, error) {
	if len(geoIDs) == 0 {
		return []OfficialOut{}, nil
	}

	// Use the existing fetchOfficialsFromDB pattern but filtered by geo_ids
	// This reuses the complex query logic that already exists
	query := `
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
		WHERE d.geo_id = ANY($1)
		ORDER BY p.id, d.district_type
	`

	rows, err := db.DB.WithContext(ctx).Raw(query, geoIDs).Rows()
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
