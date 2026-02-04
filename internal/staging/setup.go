package staging

import (
	"log"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
)

func Init() {
	// Ensure the staging schema exists
	if err := db.EnsureSchema(db.DB, "staging"); err != nil {
		log.Fatal("Failed to ensure schema staging: ", err)
	}

	// Create required extensions
	if err := db.DB.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`).Error; err != nil {
		log.Fatal("Failed to enable uuid-ossp extension:", err)
	}

	// Auto-migrate all staging models
	if err := db.DB.AutoMigrate(
		&StagingStance{},
		&StagingPolitician{},
		&ReviewLog{},
	); err != nil {
		log.Fatal("Failed to auto-migrate staging tables: ", err)
	}

	log.Println("Staging module initialized")
}
