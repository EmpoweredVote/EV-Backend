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
		&City{},
		&Budget{},
		&BudgetCategory{},
		&BudgetLineItem{},
	); err != nil {
		log.Fatal("Failed to auto-migrate treasury tables: ", err)
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
