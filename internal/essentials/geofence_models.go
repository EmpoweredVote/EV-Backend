package essentials

import (
	"github.com/google/uuid"
)

// GeofenceBoundary stores the actual polygon geometry for geographic lookups.
// This enables point-in-polygon queries to determine which districts contain a given lat/lng.
type GeofenceBoundary struct {
	ID       uuid.UUID `gorm:"type:uuid;default:uuid_generate_v4();primaryKey" json:"id"`
	GeoID    string    `gorm:"size:50" json:"geo_id"` // Census GEOID - primary link to districts (unique constraint managed manually)
	OCDID    string    `gorm:"index;size:255" json:"ocd_id"`      // Open Civic Data ID
	Name     string    `json:"name"`
	State    string    `gorm:"index;size:2" json:"state"`
	MTFCC    string    `gorm:"index;size:10" json:"mtfcc"` // MAF/TIGER Feature Class Code

	// Geometry stored as PostGIS GEOMETRY type
	// This will be a POLYGON or MULTIPOLYGON in WGS84 (SRID 4326)
	Geometry string `gorm:"type:geometry(Geometry,4326)" json:"-"`

	// Metadata
	Source      string `json:"source"`       // e.g., "census_tiger_2024"
	ValidFrom   string `json:"valid_from"`   // Date range for validity
	ValidTo     string `json:"valid_to"`
	ImportedAt  string `json:"imported_at"`
}

func (GeofenceBoundary) TableName() string {
	return "essentials.geofence_boundaries"
}
