package campaign_finance

import (
	"log"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
)

func Init() {
	if err := db.EnsureSchema(db.DB, "transparent_motivations"); err != nil {
		log.Fatal("campaign_finance: failed to create schema: ", err)
	}

	if err := db.DB.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`).Error; err != nil {
		log.Fatal("campaign_finance: failed to enable uuid-ossp extension:", err)
	}

	if err := db.DB.AutoMigrate(
		&PoliticianSource{},
		&Donor{},
		&Committee{},
		&Contribution{},
		&DataSourceMetadata{},
		&SourceAuditLog{},
		&IngestionRun{},
	); err != nil {
		log.Fatal("campaign_finance: failed to migrate tables:", err)
	}
}
