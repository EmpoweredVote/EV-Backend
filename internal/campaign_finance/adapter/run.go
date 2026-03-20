package adapter

import (
	"fmt"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/campaign_finance"
	"github.com/EmpoweredVote/EV-Backend/internal/db"
)

// RunIngestion executes the full Fetch → Normalize → Upsert pipeline for one
// politician/cycle pair. It creates an IngestionRun row at start and finalizes
// it with all counters at completion.
//
// If TotalFetched < 95% of TotalExpected the run is marked completed_with_warning
// rather than completed — this signals a potential truncation in the FEC data feed.
func RunIngestion(adapterImpl SourceAdapter, ps campaign_finance.PoliticianSource, cycle string) error {
	run := campaign_finance.IngestionRun{
		AdapterName:        adapterImpl.Name(),
		PoliticianSourceID: &ps.ID,
		ElectionCycle:      cycle,
		StartedAt:          time.Now(),
		Status:             "running",
	}

	if err := db.DB.Create(&run).Error; err != nil {
		return fmt.Errorf("RunIngestion: create ingestion_run: %w", err)
	}

	// Fetch
	fetchResult, err := adapterImpl.Fetch(ps)
	if err != nil {
		failRun(&run, err)
		return fmt.Errorf("RunIngestion: fetch: %w", err)
	}

	// Normalize
	normalizeResult, err := adapterImpl.Normalize(fetchResult, ps)
	if err != nil {
		failRun(&run, err)
		return fmt.Errorf("RunIngestion: normalize: %w", err)
	}

	// Upsert
	upsertResult, err := adapterImpl.Upsert(normalizeResult)
	if err != nil {
		failRun(&run, err)
		return fmt.Errorf("RunIngestion: upsert: %w", err)
	}

	// Finalize run
	now := time.Now()
	run.CompletedAt = &now
	run.DurationMs = now.Sub(run.StartedAt).Milliseconds()
	run.RecordsFetched = fetchResult.TotalFetched
	run.RecordsInserted = upsertResult.Inserted
	// Combined: memo/amendment skips from normalizer + duplicate skips from upsert
	run.RecordsSkipped = normalizeResult.Skipped + upsertResult.Skipped
	run.RecordsUnresolved = upsertResult.Unresolved
	run.ErrorCount = upsertResult.Errors

	// Populate ETag/download metadata if adapter provides it (Cal-Access does).
	type etagProvider interface {
		ZIPETag() string
		ZIPDownloadedAt() time.Time
	}
	if ep, ok := adapterImpl.(etagProvider); ok {
		run.SourceETag = ep.ZIPETag()
		dlAt := ep.ZIPDownloadedAt()
		if !dlAt.IsZero() {
			run.ZIPDownloadedAt = &dlAt
		}
	}

	// Completeness check: warn if fetched < 95% of expected
	if fetchResult.TotalExpected > 0 &&
		fetchResult.TotalFetched < int(float64(fetchResult.TotalExpected)*0.95) {
		run.Status = "completed_with_warning"
		run.Notes = fmt.Sprintf(
			"fetched %d of expected %d (%.0f%%)",
			fetchResult.TotalFetched,
			fetchResult.TotalExpected,
			float64(fetchResult.TotalFetched)/float64(fetchResult.TotalExpected)*100,
		)
	} else {
		run.Status = "completed"
	}

	// Skip threshold: warn if >1% of examined rows were skipped (Cal-Access locked decision).
	if normalizeResult.TotalParsed > 0 && normalizeResult.Skipped > 0 {
		skipRate := float64(normalizeResult.Skipped) / float64(normalizeResult.TotalParsed)
		if skipRate > 0.01 {
			run.Status = "completed_with_warning"
			skipNote := fmt.Sprintf("skip threshold exceeded: %d/%d rows skipped (%.1f%%)",
				normalizeResult.Skipped, normalizeResult.TotalParsed, skipRate*100)
			if run.Notes != "" {
				run.Notes += "; " + skipNote
			} else {
				run.Notes = skipNote
			}
		}
	}

	if err := db.DB.Save(&run).Error; err != nil {
		return fmt.Errorf("RunIngestion: save final ingestion_run: %w", err)
	}

	return nil
}

// failRun marks an IngestionRun as failed and persists it. Used by RunIngestion
// error paths to ensure every run has a terminal status in the audit log.
func failRun(run *campaign_finance.IngestionRun, err error) {
	now := time.Now()
	run.CompletedAt = &now
	run.DurationMs = now.Sub(run.StartedAt).Milliseconds()
	run.Status = "failed"
	run.Notes = err.Error()
	db.DB.Save(run)
}
