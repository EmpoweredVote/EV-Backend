package db

import (
	"fmt"
	"log"
	"os"

	"github.com/DoyleJ11/auth-system/models"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var DB * gorm.DB

func Connect() {
	dsn := os.Getenv("DATABASE_URL")
	connection, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Failed to connect to database ", err)
	}

	DB = connection
	fmt.Println("Connected to database")

	if err := connection.AutoMigrate(&models.User{}, &models.Session{}); err != nil {
		log.Fatal("Failed to auto-migrate tables", err)
	}
}