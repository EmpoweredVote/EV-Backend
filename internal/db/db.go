package db

import (
	"fmt"
	"log"
	"os"

	// "github.com/EmpoweredVote/EV-Backend/internal/auth"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var DB *gorm.DB

func Connect() {
	dsn := os.Getenv("DATABASE_URL")
	connection, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Warn)})
	if err != nil {
		log.Fatal("Failed to connect to database ", err)
	}

	DB = connection
	fmt.Println("Connected to database")
}
