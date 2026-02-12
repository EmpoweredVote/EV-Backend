package essentials

import (
	"log"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/EmpoweredVote/EV-Backend/internal/essentials/geocoding"
	"github.com/EmpoweredVote/EV-Backend/internal/essentials/provider"

	// Import providers to register them via init()
	_ "github.com/EmpoweredVote/EV-Backend/internal/essentials/ballotready"
	_ "github.com/EmpoweredVote/EV-Backend/internal/essentials/cicero"
)

// Provider is the active politician data provider.
// It is initialized in Init() based on environment configuration.
var Provider provider.OfficialProvider

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
		&FederalCache{},
		&StateCache{},
		&ZipCache{},
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

	// Case insensitive unique for committees.name
	if err := db.DB.Exec(`
        CREATE UNIQUE INDEX IF NOT EXISTS committees_name_ci_unique
        ON essentials.committees (LOWER(name));
    `).Error; err != nil {
		log.Fatal("Failed to create committees_name_ci_unique", err)
	}

	// Initialize the politician data provider
	cfg := provider.LoadFromEnv()
	var err error
	Provider, err = provider.NewProvider(cfg)
	if err != nil {
		log.Printf("[essentials] WARNING: Failed to initialize %s provider: %v", cfg.Provider, err)
		log.Printf("[essentials] Provider-based warming will be disabled")
		Provider = nil
	} else {
		log.Printf("[essentials] Initialized %s provider", Provider.Name())
	}

	// Initialize Google Maps geocoding client
	GeoClient, err = geocoding.NewClient()
	if err != nil {
		log.Printf("[essentials] WARNING: Failed to initialize Google Maps geocoding: %v", err)
	} else if GeoClient != nil {
		log.Printf("[essentials] Initialized Google Maps geocoding client")
	} else {
		log.Printf("[essentials] Google Maps geocoding disabled (GOOGLE_MAPS_API_KEY not set)")
	}
}
