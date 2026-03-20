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

// IndianaNormalizeRowFunc is the function signature for normalizing a single
// Indiana raw row map into a Contribution. Injected at startup by main.go to
// avoid an import cycle: campaign_finance imports adapter/indiana which imports
// campaign_finance.
type IndianaNormalizeRowFunc func(rec map[string]interface{}, ps PoliticianSource) (Contribution, error)

// indianaNormalizeRowFn is set via SetIndianaBackfillFunc. Nil until startup.
var indianaNormalizeRowFn IndianaNormalizeRowFunc

// SetIndianaBackfillFunc registers the Indiana normalization function.
// Call this from main.go after campaign_finance.Init():
//
//	campaign_finance.SetIndianaBackfillFunc(func(rec map[string]interface{}, ps campaign_finance.PoliticianSource) (campaign_finance.Contribution, error) {
//	    return indiana.NormalizeRow(rec, ps)
//	})
func SetIndianaBackfillFunc(fn IndianaNormalizeRowFunc) {
	indianaNormalizeRowFn = fn
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
		&UnresolvedContribution{},
	); err != nil {
		log.Fatal("campaign_finance: failed to migrate tables:", err)
	}

	// Add skipped_no_change to IngestionRun status CHECK constraint.
	// GORM AutoMigrate does not modify CHECK constraints — raw SQL required.
	migrationResult := db.DB.Exec(`
DO $$
DECLARE
    constraint_name text;
BEGIN
    SELECT con.conname INTO constraint_name
    FROM pg_constraint con
    JOIN pg_attribute att ON att.attnum = ANY(con.conkey)
        AND att.attrelid = con.conrelid
    WHERE con.conrelid = 'transparent_motivations.ingestion_runs'::regclass
        AND con.contype = 'c'
        AND att.attname = 'status';

    IF constraint_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE transparent_motivations.ingestion_runs DROP CONSTRAINT %I', constraint_name);
    END IF;

    ALTER TABLE transparent_motivations.ingestion_runs
        ADD CONSTRAINT ingestion_runs_status_check
        CHECK (status IN ('running','completed','completed_with_warning','failed','skipped_no_change'));
END $$;
`)
	if migrationResult.Error != nil {
		log.Printf("warning: skipped_no_change migration: %v", migrationResult.Error)
	}

	// Add status CHECK constraint for unresolved_contributions.
	// GORM AutoMigrate does not modify CHECK constraints — raw SQL required.
	// Existing rows written before this migration have no status column yet;
	// the column default 'active' handles them once AutoMigrate adds the column.
	unresolvedStatusMigration := db.DB.Exec(`
DO $$
DECLARE
    constraint_name text;
BEGIN
    SELECT con.conname INTO constraint_name
    FROM pg_constraint con
    JOIN pg_attribute att ON att.attnum = ANY(con.conkey)
        AND att.attrelid = con.conrelid
    WHERE con.conrelid = 'transparent_motivations.unresolved_contributions'::regclass
        AND con.contype = 'c'
        AND att.attname = 'status';

    IF constraint_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE transparent_motivations.unresolved_contributions DROP CONSTRAINT %I', constraint_name);
    END IF;

    ALTER TABLE transparent_motivations.unresolved_contributions
        ADD CONSTRAINT unresolved_contributions_status_check
        CHECK (status IN ('active','dismissed','resolved'));
END $$;
`)
	if unresolvedStatusMigration.Error != nil {
		log.Printf("warning: unresolved_contributions status migration: %v", unresolvedStatusMigration.Error)
	}
}
