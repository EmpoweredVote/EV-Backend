package compass

import (
	"log"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
)

func Init() {
	// Ensure the compass schema exists first
	if err := db.EnsureSchema(db.DB, "compass"); err != nil {
		log.Fatal("Failed to create compass schema: ", err)
	}

	if err := db.DB.AutoMigrate(&Topic{}, &Answer{}, &Stance{}, &Category{}, &Context{}); err != nil {
		log.Fatal("Failed to auto-migrate tables", err)
	}
}