package main

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	godotenv.Load(".env.local")

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL not set")
	}

	db, err := gorm.Open(postgres.Open(dbURL), &gorm.Config{})
	if err != nil {
		log.Fatalf("DB connection error: %v", err)
	}

	// Delete the cache entry for 47403
	result := db.Exec("DELETE FROM essentials.zip_caches WHERE zip = '47403'")
	if result.Error != nil {
		log.Fatalf("Error deleting cache: %v", result.Error)
	}

	fmt.Printf("✓ Deleted cache for ZIP 47403 (affected rows: %d)\n", result.RowsAffected)
	fmt.Println("\nNext time you request ZIP 47403, it will fetch fresh data from BallotReady including federal officials.")
	fmt.Println("Visit: http://localhost:5050/essentials/politicians/47403")
}
