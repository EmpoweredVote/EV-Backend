package essentials

import (
	"log"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
)

func Init() {
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
		&ZipCache{},
		&ZipPolitician{},
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
}
