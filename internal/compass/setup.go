package compass

import (
	"log"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
)

func Init() {
	if err := db.DB.AutoMigrate(&Topic{}, &Answer{}, &Stance{}, &Category{}); err != nil {
		log.Fatal("Failed to auto-migrate tables", err)
	}
}