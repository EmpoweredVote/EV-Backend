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
		// Phase 73: Government body display names and website URLs
		&GovernmentBody{},
		// Phase 54: Legislative data foundation
		&LegislativeSession{},
		&LegislativeCommittee{},
		&LegislativeCommitteeMembership{},
		&LegislativeLeadershipRole{},
		&LegislativeBill{},
		&LegislativeBillCosponsor{},
		&LegislativeVote{},
		&LegislativePoliticianIDMap{},
		// &GeofenceBoundary{}, // Table already exists, managed manually to avoid GORM constraint issues
	); err != nil {
		log.Fatal("Failed to auto-migrate tables", err)
	}

	// Phase 73: Populate chamber_name_formal for Indiana chambers (idempotent)
	db.DB.Exec(`UPDATE essentials.chambers SET name_formal = 'Monroe County Council' WHERE name LIKE 'Monroe County Council%' AND (name_formal = '' OR name_formal IS NULL)`)
	// Quick-8: Rename Commission name_formal to its legal name (unconditional so it also fixes previously set old value)
	db.DB.Exec(`UPDATE essentials.chambers SET name_formal = 'Monroe County Board of Commissioners' WHERE name LIKE 'Monroe County Commission%'`)
	db.DB.Exec(`UPDATE essentials.chambers SET name_formal = 'Bloomington Common Council' WHERE name LIKE 'Bloomington City Common Council%' AND (name_formal = '' OR name_formal IS NULL)`)

	// Phase 76: Group city executive/clerk chambers under shared body_key
	db.DB.Exec(`UPDATE essentials.chambers SET name_formal = 'City of Bloomington'
	  WHERE name IN ('Bloomington City Mayor', 'Bloomington City Clerk')
	  AND (name_formal = '' OR name_formal IS NULL)`)

	// Phase 76: Group township chambers under shared body_key
	db.DB.Exec(`UPDATE essentials.chambers SET name_formal = 'Bloomington Township'
	  WHERE name LIKE 'Monroe County: Bloomington Township%'
	  AND (name_formal = '' OR name_formal IS NULL)`)

	// Phase 76: Group school board district chambers under shared body_key
	db.DB.Exec(`UPDATE essentials.chambers SET name_formal = 'Monroe County Community School Corporation'
	  WHERE name LIKE 'Monroe County Community School Board%'
	  AND (name_formal = '' OR name_formal IS NULL)`)

	// Phase 76: Group judicial chambers under shared body_keys
	db.DB.Exec(`UPDATE essentials.chambers SET name_formal = 'Monroe County Circuit Court'
	  WHERE name LIKE 'Indiana Circuit Court Judge - 10th Circuit (Monroe County)%'
	  AND (name_formal = '' OR name_formal IS NULL)`)
	db.DB.Exec(`UPDATE essentials.chambers SET name_formal = 'Indiana Court of Appeals'
	  WHERE name LIKE 'Indiana Appeals Court Judge%'
	  AND (name_formal = '' OR name_formal IS NULL)`)
	db.DB.Exec(`UPDATE essentials.chambers SET name_formal = 'Indiana Supreme Court'
	  WHERE name LIKE 'Indiana Supreme Court Justice%'
	  AND (name_formal = '' OR name_formal IS NULL)`)

	// Phase 74: Seed individual county office chambers with shared body_key (idempotent)
	// Individual single-office county officials share 'Monroe County Government' so they resolve
	// to one government_bodies row via the JOIN on COALESCE(NULLIF(c.name_formal, ''), c.name, '')
	db.DB.Exec(`UPDATE essentials.chambers SET name_formal = 'Monroe County Government'
	  WHERE name IN (
	    'Monroe County Assessor', 'Monroe County Auditor', 'Monroe County Circuit Court Clerk',
	    'Monroe County Coroner', 'Monroe County Prosecuting Attorney', 'Monroe County Recorder',
	    'Monroe County Sheriff', 'Monroe County Surveyor', 'Monroe County Treasurer'
	  ) AND (name_formal = '' OR name_formal IS NULL)`)

	// Phase 74: Seed government_bodies with verified official website URLs (idempotent)
	// Uses ON CONFLICT DO NOTHING so manually-corrected URLs are preserved on restart.
	// All rows use state='IN' (abbreviation) to match d.state in the districts table JOIN.
	db.DB.Exec("DELETE FROM essentials.government_bodies WHERE state='18'") // clean up old FIPS-coded rows

	// Quick-8: Rename Commission body_key to match new name_formal (idempotent — no-op if already renamed)
	db.DB.Exec(`UPDATE essentials.government_bodies
	  SET body_key = 'Monroe County Board of Commissioners',
	      display_name = 'Monroe County Board of Commissioners',
	      website_url = 'https://www.in.gov/counties/monroe/government/commissioners/'
	  WHERE state = 'IN' AND geo_id = '18105' AND body_key = 'Monroe County Commission'`)

	db.DB.Exec(`
	  INSERT INTO essentials.government_bodies (state, geo_id, body_key, display_name, website_url)
	  VALUES
	    -- Monroe County Board of Commissioners (all 3 commissioners use geo_id 18105)
	    ('IN', '18105',        'Monroe County Board of Commissioners',   'Monroe County Board of Commissioners',   'https://www.in.gov/counties/monroe/government/commissioners/'),
	    -- Monroe County Council — At-Large (3 members, geo_id 18105)
	    ('IN', '18105',        'Monroe County Council',      'Monroe County Council',      'https://www.in.gov/counties/monroe/government/council/'),
	    -- Monroe County Council — District 1
	    ('IN', '1810500001',   'Monroe County Council',      'Monroe County Council',      'https://www.in.gov/counties/monroe/government/council/'),
	    -- Monroe County Council — District 2
	    ('IN', '1810500002',   'Monroe County Council',      'Monroe County Council',      'https://www.in.gov/counties/monroe/government/council/'),
	    -- Monroe County Council — District 3
	    ('IN', '1810500003',   'Monroe County Council',      'Monroe County Council',      'https://www.in.gov/counties/monroe/government/council/'),
	    -- Monroe County Council — District 4
	    ('IN', '1810500004',   'Monroe County Council',      'Monroe County Council',      'https://www.in.gov/counties/monroe/government/council/'),
	    -- Individual county-wide elected officials (Sheriff, Assessor, Auditor, Coroner, etc.)
	    ('IN', '18105',        'Monroe County Government',   'Monroe County Government',   'https://www.in.gov/counties/monroe/'),
	    -- Bloomington Common Council — At-Large (3 members, geo_id 1805860)
	    ('IN', '1805860',      'Bloomington Common Council', 'Bloomington Common Council', 'https://bloomington.in.gov/council'),
	    -- Bloomington Common Council — District 1
	    ('IN', '180586000001', 'Bloomington Common Council', 'Bloomington Common Council', 'https://bloomington.in.gov/council'),
	    -- Bloomington Common Council — District 2
	    ('IN', '180586000002', 'Bloomington Common Council', 'Bloomington Common Council', 'https://bloomington.in.gov/council'),
	    -- Bloomington Common Council — District 3
	    ('IN', '180586000003', 'Bloomington Common Council', 'Bloomington Common Council', 'https://bloomington.in.gov/council'),
	    -- Bloomington Common Council — District 4
	    ('IN', '180586000004', 'Bloomington Common Council', 'Bloomington Common Council', 'https://bloomington.in.gov/council'),
	    -- Bloomington Common Council — District 5
	    ('IN', '180586000005', 'Bloomington Common Council', 'Bloomington Common Council', 'https://bloomington.in.gov/council'),
	    -- Bloomington Common Council — District 6
	    ('IN', '180586000006', 'Bloomington Common Council', 'Bloomington Common Council', 'https://bloomington.in.gov/council'),
	    -- City of Bloomington (Mayor + Clerk, geo_id 1805860)
	    ('IN', '1805860',      'City of Bloomington',        'City of Bloomington',        'https://bloomington.in.gov/'),
	    -- Bloomington Township (Trustee + Board, geo_id 1810505878)
	    ('IN', '1810505878',   'Bloomington Township',       'Bloomington Township',       ''),
	    -- Monroe County Community School Corporation (7 districts)
	    ('IN', '180063000001', 'Monroe County Community School Corporation', 'Monroe County Community School Corporation', 'https://www.mccsc.edu/'),
	    ('IN', '180063000002', 'Monroe County Community School Corporation', 'Monroe County Community School Corporation', 'https://www.mccsc.edu/'),
	    ('IN', '180063000003', 'Monroe County Community School Corporation', 'Monroe County Community School Corporation', 'https://www.mccsc.edu/'),
	    ('IN', '180063000004', 'Monroe County Community School Corporation', 'Monroe County Community School Corporation', 'https://www.mccsc.edu/'),
	    ('IN', '180063000005', 'Monroe County Community School Corporation', 'Monroe County Community School Corporation', 'https://www.mccsc.edu/'),
	    ('IN', '180063000006', 'Monroe County Community School Corporation', 'Monroe County Community School Corporation', 'https://www.mccsc.edu/'),
	    ('IN', '180063000007', 'Monroe County Community School Corporation', 'Monroe County Community School Corporation', 'https://www.mccsc.edu/'),
	    -- Monroe County Circuit Court (9 seats, geo_id 18105)
	    ('IN', '18105',        'Monroe County Circuit Court', 'Monroe County Circuit Court', 'https://www.in.gov/courts/circuit/monroe/'),
	    -- Indiana Court of Appeals (statewide, geo_id 18)
	    ('IN', '18',           'Indiana Court of Appeals',   'Indiana Court of Appeals',   'https://www.in.gov/courts/appeals/'),
	    -- Indiana Supreme Court (statewide, geo_id 18)
	    ('IN', '18',           'Indiana Supreme Court',      'Indiana Supreme Court',      'https://www.in.gov/courts/supreme/')
	  ON CONFLICT (state, geo_id, body_key) DO NOTHING
	`)

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
