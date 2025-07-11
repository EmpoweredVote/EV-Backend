package auth

import (
	"log"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
)

func Init() {
	if err := db.DB.AutoMigrate(&User{}, &Session{}); err != nil {
		log.Fatal("Failed to auto-migrate tables", err)
	}
}