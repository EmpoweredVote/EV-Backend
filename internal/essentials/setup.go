package essentials

import (
	"log"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/EmpoweredVote/EV-Backend/internal/essentials/provider"

	// Import providers to register them via init()
	_ "github.com/EmpoweredVote/EV-Backend/internal/essentials/ballotready"
	_ "github.com/EmpoweredVote/EV-Backend/internal/essentials/cicero"
)

// Provider is the active politician data provider.
// It is initialized in Init() based on environment configuration.
var Provider provider.OfficialProvider

func Init() {
	// Ensure the essentials schema exists
	if err := db.EnsureSchema(db.DB, "essentials"); err != nil {
		log.Fatal("Failed to ensure schema essentials: ", err)
	}

	if err := db.DB.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`).Error; err != nil {
		log.Fatal("Failed to enable uuid-ossp extension:", err)
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
	); err != nil {
		log.Fatal("Failed to auto-migrate tables", err)
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
}
