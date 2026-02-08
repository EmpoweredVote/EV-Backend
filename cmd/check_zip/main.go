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

	// Check ALL politicians for this ZIP to see what district types exist
	type Result struct {
		FullName     string
		Party        string
		PositionName string
		DistrictType string
	}

	var results []Result
	query := `
		SELECT
			p.full_name,
			p.party,
			o.title as position_name,
			d.district_type
		FROM essentials.zip_politicians zp
		JOIN essentials.politicians p ON zp.politician_id = p.id
		JOIN essentials.offices o ON p.id = o.politician_id
		JOIN essentials.districts d ON o.district_id = d.id
		WHERE zp.zip = '47403'
		ORDER BY d.district_type, p.last_name
	`

	if err := db.Raw(query).Scan(&results).Error; err != nil {
		log.Fatalf("Query error: %v", err)
	}

	// Group by district type
	byType := make(map[string][]Result)
	for _, r := range results {
		byType[r.DistrictType] = append(byType[r.DistrictType], r)
	}

	fmt.Printf("Total politicians in database for ZIP 47403: %d\n\n", len(results))
	
	for distType, pols := range byType {
		fmt.Printf("=== %s (%d) ===\n", distType, len(pols))
		for _, p := range pols {
			fmt.Printf("  - %s (%s) | %s\n", p.FullName, p.Party, p.PositionName)
		}
		fmt.Println()
	}
}
