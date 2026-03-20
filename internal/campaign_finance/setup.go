package campaign_finance

import (
	"log"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
)

// FECIngestFunc is the function signature for running FEC ingestion for a
// given politician source and election cycle. It is injected at startup by
// main.go (or tests) to avoid an import cycle between campaign_finance and
// campaign_finance/adapter.
type FECIngestFunc func(ps PoliticianSource, cycle string) error

// fecIngestFn is the package-level FEC ingestion function, set via SetFECIngestFunc.
// It is nil until the main app calls SetFECIngestFunc at startup.
var fecIngestFn FECIngestFunc

// SetFECIngestFunc registers the FEC ingestion implementation.
// Call this from main.go after connecting to the DB:
//
//	campaign_finance.SetFECIngestFunc(func(ps campaign_finance.PoliticianSource, cycle string) error {
//	    return adapter.RunIngestion(fec.New(cycle), ps, cycle)
//	})
func SetFECIngestFunc(fn FECIngestFunc) {
	fecIngestFn = fn
}

func Init() {
	if err := db.EnsureSchema(db.DB, "transparent_motivations"); err != nil {
		log.Fatal("campaign_finance: failed to create schema: ", err)
	}

	if err := db.DB.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`).Error; err != nil {
		log.Fatal("campaign_finance: failed to enable uuid-ossp extension:", err)
	}

	if err := db.DB.AutoMigrate(
		&PoliticianSource{},
		&Donor{},
		&Committee{},
		&Contribution{},
		&DataSourceMetadata{},
		&SourceAuditLog{},
		&IngestionRun{},
	); err != nil {
		log.Fatal("campaign_finance: failed to migrate tables:", err)
	}
}
