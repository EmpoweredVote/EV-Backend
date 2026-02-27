package essentials

import (
	"log"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/EmpoweredVote/EV-Backend/internal/essentials/geocoding"

	// Import cicero to register it via init()
	_ "github.com/EmpoweredVote/EV-Backend/internal/essentials/cicero"
)

// GeoClient is the Google Maps geocoding client (nil if API key not set).
var GeoClient *geocoding.Client

func Init() {
	// Ensure the essentials schema exists
	if err := db.EnsureSchema(db.DB, "essentials"); err != nil {
		log.Fatal("Failed to ensure schema essentials: ", err)
	}

	if err := db.DB.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`).Error; err != nil {
		log.Fatal("Failed to enable uuid-ossp extension:", err)
	}

	// Enable PostGIS extension for geospatial queries
	if err := db.DB.Exec(`CREATE EXTENSION IF NOT EXISTS postgis`).Error; err != nil {
		log.Fatal("Failed to enable postgis extension:", err)
	}

	// Drop legacy cache tracking tables (no longer used after BallotReady warmer removal)
	db.DB.Exec("DROP TABLE IF EXISTS essentials.federal_cache")
	db.DB.Exec("DROP TABLE IF EXISTS essentials.state_caches")
	db.DB.Exec("DROP TABLE IF EXISTS essentials.zip_caches")

	if err := db.DB.AutoMigrate(
		&Politician{},
		&Office{},
		&Chamber{},
		&District{},
		&Government{},
		&Address{},
		&Identifier{},
		&Committee{},
		&PoliticianCommittee{},
		&ZipPolitician{},
		&PoliticianImage{},
		&Degree{},
		&Experience{},
		// Phase B: Candidacy data models
		&EndorserOrganization{},
		&Endorsement{},
		&Issue{},
		&PoliticianStance{},
		&ElectionRecord{},
		&PoliticianContact{},
		&Quote{},
		&PositionDescription{},
		// Phase 39: Building photos for city hall imagery
		&BuildingPhoto{},
		// &GeofenceBoundary{}, // Table already exists, managed manually to avoid GORM constraint issues
	); err != nil {
		log.Fatal("Failed to auto-migrate tables", err)
	}

	// Create spatial index on geofence_boundaries geometry column
	if err := db.DB.Exec(`
		CREATE INDEX IF NOT EXISTS idx_geofence_boundaries_geometry
		ON essentials.geofence_boundaries USING GIST (geometry);
	`).Error; err != nil {
		log.Fatal("Failed to create spatial index:", err)
	}

	// Remove duplicate (geo_id, mtfcc) rows, keeping the most recently imported one.
	// Required before CREATE UNIQUE INDEX if import scripts ran without the constraint.
	if err := db.DB.Exec(`
		DELETE FROM essentials.geofence_boundaries
		WHERE id NOT IN (
		    SELECT DISTINCT ON (geo_id, mtfcc) id
		    FROM essentials.geofence_boundaries
		    ORDER BY geo_id, mtfcc, id DESC
		)
	`).Error; err != nil {
		log.Printf("[essentials] WARNING: Dedup cleanup failed (table may not exist yet): %v", err)
	}

	// Composite unique constraint: prevents duplicate rows when importing
	// multiple MTFCC layer types that share the same geo_id value.
	// Required for ON CONFLICT idempotency in import scripts (Phases 34-35).
	if err := db.DB.Exec(`
		CREATE UNIQUE INDEX IF NOT EXISTS idx_geofence_boundaries_geo_id_mtfcc
		ON essentials.geofence_boundaries (geo_id, mtfcc);
	`).Error; err != nil {
		log.Fatal("Failed to create geofence_boundaries unique constraint:", err)
	}

	// Case insensitive unique for committees.name
	if err := db.DB.Exec(`
        CREATE UNIQUE INDEX IF NOT EXISTS committees_name_ci_unique
        ON essentials.committees (LOWER(name));
    `).Error; err != nil {
		log.Fatal("Failed to create committees_name_ci_unique", err)
	}

	// Initialize Google Maps geocoding client
	var err error
	GeoClient, err = geocoding.NewClient()
	if err != nil {
		log.Printf("[essentials] WARNING: Failed to initialize Google Maps geocoding: %v", err)
	} else if GeoClient != nil {
		log.Printf("[essentials] Initialized Google Maps geocoding client")
	} else {
		log.Printf("[essentials] Google Maps geocoding disabled (GOOGLE_MAPS_API_KEY not set)")
	}
}
