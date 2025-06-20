package main

import (
	"log"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/EmpoweredVote/EV-Backend/internal/seeds"
)

func main() {
	db.Connect()

	if err := seeds.SeedAll(); err != nil {
		log.Fatalf("❌ Seeding failed: %v", err)
	}
}