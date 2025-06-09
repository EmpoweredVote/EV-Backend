package compass

import (
	"log"

	"github.com/DoyleJ11/auth-system/internal/db"
)

func Init() {
	if err := db.DB.AutoMigrate(&Topic{}, &Answer{}, &Stance{}); err != nil {
		log.Fatal("Failed to auto-migrate tables", err)
	}
}