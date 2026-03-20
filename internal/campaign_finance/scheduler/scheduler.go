package scheduler

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Config holds all dependencies for the scheduler.
// All fields are optional at the scheduler level — missing values degrade
// gracefully (no Redis → mutex fallback; no HC URL → no pings; no DB → no
// quiet-period skip).
type Config struct {
	// RedisURL is the value of UPSTASH_REDIS_URL. A "rediss://" prefix enables TLS.
	RedisURL string

	// HealthcheckURLs maps adapter name to a Healthchecks.io check URL.
	// e.g. "fec" -> "https://hc-ping.com/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
	HealthcheckURLs map[string]string

	// FECIngestFn runs FEC ingestion for all confirmed politician sources.
	// Must match func() error.
	FECIngestFn func() error

	// DB is used to query the last successful FEC run time for quiet-period
	// cadence switching. If nil, ShouldSkipQuietPeriod always returns false.
	DB *gorm.DB
}

// Scheduler wraps a gocron.Scheduler with project-specific lifecycle management.
type Scheduler struct {
	inner gocron.Scheduler
}

// New creates and configures the gocron scheduler with the FEC ingestion job.
// Redis locker is attempted first; on any failure a warning is logged and the
// in-process mutex fallback is used (single-instance protection only).
// Returns an error only if gocron itself cannot be initialized.
func New(cfg Config) (*Scheduler, error) {
	// --- Distributed locker setup ---
	// FEC max runtime is ~5 min; 10 min TTL = 2x so a crashed-run lock expires
	// before the next 15-minute trigger fires.
	locker, err := BuildLocker(cfg.RedisURL, 10*time.Minute)
	if err != nil {
		log.Printf("scheduler: Redis locker unavailable (%v), using in-process mutex fallback", err)
		locker = NewMutexLocker()
	}

	// --- gocron scheduler ---
	s, err := gocron.NewScheduler(
		gocron.WithDistributedLocker(locker),
		gocron.WithLocation(time.UTC),
		gocron.WithLogger(gocron.NewLogger(gocron.LogLevelWarn)),
	)
	if err != nil {
		return nil, fmt.Errorf("create gocron scheduler: %w", err)
	}

	// --- FEC job function ---
	fecJobFn := func() {
		now := time.Now().UTC()
		if ShouldSkipQuietPeriod(now, lastSuccessfulFECRun(cfg.DB)) {
			log.Printf("scheduler: fec-ingest quiet period, last run within 24h, skipping")
			return
		}

		ctx := context.Background()
		hcURL := ""
		if cfg.HealthcheckURLs != nil {
			hcURL = cfg.HealthcheckURLs["fec"]
		}

		var ingestFn func() error
		if cfg.FECIngestFn != nil {
			ingestFn = cfg.FECIngestFn
		} else {
			ingestFn = func() error { return nil }
		}

		if err := withRetryOn429(func() error {
			return RunWithHealthcheck(ctx, hcURL, ingestFn)
		}); err != nil {
			log.Printf("scheduler: fec-ingest failed after retries: %v", err)
		}
	}

	// --- Register FEC job at */15 cron ---
	_, err = s.NewJob(
		gocron.CronJob("*/15 * * * *", false),
		gocron.NewTask(fecJobFn),
		gocron.WithName("fec-ingest"),
		gocron.WithEventListeners(
			gocron.AfterLockError(func(_ uuid.UUID, name string, lockErr error) {
				log.Printf("scheduler: %s lock held by another instance, skipping (%v)", name, lockErr)
			}),
			gocron.AfterJobRunsWithError(func(_ uuid.UUID, name string, runErr error) {
				log.Printf("scheduler: %s failed: %v", name, runErr)
			}),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("register fec-ingest job: %w", err)
	}

	return &Scheduler{inner: s}, nil
}

// Start begins the scheduler's background goroutine. Non-blocking.
func (s *Scheduler) Start() {
	s.inner.Start()
}

// Stop gracefully shuts down the scheduler, waiting for running jobs to finish.
func (s *Scheduler) Stop() error {
	return s.inner.Shutdown()
}

// lastSuccessfulFECRun queries the ingestion_runs table for the most recent
// completed or completed_with_warning FEC run. Returns zero time if DB is nil
// or no run is found (which causes ShouldSkipQuietPeriod to return false,
// triggering a run).
func lastSuccessfulFECRun(db *gorm.DB) time.Time {
	if db == nil {
		return time.Time{}
	}

	type row struct {
		CompletedAt time.Time
	}
	var r row
	result := db.Raw(`
		SELECT completed_at
		FROM transparent_motivations.ingestion_runs
		WHERE adapter_name = 'fec'
		  AND status IN ('completed', 'completed_with_warning')
		ORDER BY completed_at DESC
		LIMIT 1
	`).Scan(&r)

	if result.Error != nil || result.RowsAffected == 0 {
		return time.Time{}
	}
	return r.CompletedAt
}

// withRetryOn429 runs fn with exponential backoff when a 429 rate-limit error
// is returned by the FEC API. Non-429 errors are returned immediately.
// Parameters: 30s initial delay, 2x multiplier, 5-minute cap, 5 attempts max.
func withRetryOn429(fn func() error) error {
	const maxAttempts = 5
	const initialDelay = 30 * time.Second
	const maxDelay = 5 * time.Minute

	delay := initialDelay

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}

		// Only retry on 429 responses.
		if !strings.Contains(err.Error(), "429") {
			return err
		}

		if attempt == maxAttempts {
			break
		}

		// Jitter: add up to 25% of delay.
		jitter := time.Duration(rand.Int64N(int64(delay / 4)))
		sleep := delay + jitter
		log.Printf("scheduler: FEC 429 rate limit, retry %d/%d in %s", attempt, maxAttempts-1, sleep.Round(time.Second))
		time.Sleep(sleep)

		// Double delay, cap at maxDelay.
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}

	return fmt.Errorf("FEC: exhausted retries after 429 responses")
}
