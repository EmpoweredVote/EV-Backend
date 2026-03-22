package treasury

import (
	"log"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
)

func Init() {
	// Ensure the treasury schema exists
	if err := db.EnsureSchema(db.DB, "treasury"); err != nil {
		log.Fatal("Failed to ensure schema treasury: ", err)
	}

	// Create required extensions
	if err := db.DB.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`).Error; err != nil {
		log.Fatal("Failed to enable uuid-ossp extension:", err)
	}

	// Auto-migrate all treasury models
	if err := db.DB.AutoMigrate(
		&Municipality{},
		&Budget{},
		&BudgetCategory{},
		&BudgetLineItem{},
	); err != nil {
		log.Fatal("Failed to auto-migrate treasury tables: ", err)
	}

	// Drop old indexes that AutoMigrate won't remove
	db.DB.Exec(`DROP INDEX IF EXISTS treasury.idx_budget_city_year`)
	db.DB.Exec(`DROP INDEX IF EXISTS treasury.idx_cities_name`)

	// Create composite unique index for municipalities (SCHM-02)
	// Ensures name+state+entity_type is unique (a city and county can share a name in the same state)
	if err := db.DB.Exec(`
		CREATE UNIQUE INDEX IF NOT EXISTS idx_municipality_name_state_type
		ON treasury.municipalities (name, state, entity_type)
	`).Error; err != nil {
		log.Fatal("Failed to create idx_municipality_name_state_type: ", err)
	}

	// Create three-column unique index for budgets (SCHM-01)
	// Note: GORM may create this from struct tags, but explicit is safer
	if err := db.DB.Exec(`
		CREATE UNIQUE INDEX IF NOT EXISTS idx_budget_municipality_year_type
		ON treasury.budgets (municipality_id, fiscal_year, dataset_type)
	`).Error; err != nil {
		log.Fatal("Failed to create idx_budget_municipality_year_type: ", err)
	}

	// Create index for efficient category tree queries
	if err := db.DB.Exec(`
		CREATE INDEX IF NOT EXISTS idx_category_tree
		ON treasury.budget_categories (budget_id, parent_id, sort_order);
	`).Error; err != nil {
		log.Fatal("Failed to create idx_category_tree: ", err)
	}

	log.Println("Treasury module initialized")
}
