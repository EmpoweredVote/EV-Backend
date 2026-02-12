package essentials

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/essentials/provider"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

// ImportJob tracks the progress of a bulk ZIP import.
type ImportJob struct {
	ID          string     `json:"id"`
	Status      string     `json:"status"` // "running", "completed", "failed"
	TotalZips   int        `json:"total_zips"`
	Completed   int        `json:"completed"`
	Failed      int        `json:"failed"`
	CurrentZip  string     `json:"current_zip,omitempty"`
	FailedZips  []string   `json:"failed_zips,omitempty"`
	DelayMs     int        `json:"delay_between_ms"`
	StartedAt   time.Time  `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

var (
	importJobs   = make(map[string]*ImportJob)
	importJobsMu sync.Mutex
)

var zipRegex = regexp.MustCompile(`^\d{5}$`)

// StartBulkImport handles POST /admin/import
// Accepts {"zips": ["47401", ...], "delay_between_ms": 3000}
func StartBulkImport(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Zips         []string `json:"zips"`
		DelayBetween int      `json:"delay_between_ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if len(body.Zips) == 0 {
		http.Error(w, "At least one ZIP code is required", http.StatusBadRequest)
		return
	}
	if len(body.Zips) > 500 {
		http.Error(w, "Maximum 500 ZIP codes per import", http.StatusBadRequest)
		return
	}

	// Validate all ZIPs
	for _, z := range body.Zips {
		if !zipRegex.MatchString(z) {
			http.Error(w, fmt.Sprintf("Invalid ZIP code: %s", z), http.StatusBadRequest)
			return
		}
	}

	delay := body.DelayBetween
	if delay <= 0 {
		delay = 3000 // default 3 seconds
	}

	job := &ImportJob{
		ID:        uuid.New().String(),
		Status:    "running",
		TotalZips: len(body.Zips),
		DelayMs:   delay,
		StartedAt: time.Now(),
	}

	importJobsMu.Lock()
	importJobs[job.ID] = job
	importJobsMu.Unlock()

	go runBulkImport(job, body.Zips)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"job_id": job.ID,
		"status": "running",
	})
}

// GetImportStatus handles GET /admin/import/{jobID}
func GetImportStatus(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "jobID")

	importJobsMu.Lock()
	job, ok := importJobs[jobID]
	importJobsMu.Unlock()

	if !ok {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	importJobsMu.Lock()
	snapshot := *job
	if job.FailedZips != nil {
		snapshot.FailedZips = make([]string, len(job.FailedZips))
		copy(snapshot.FailedZips, job.FailedZips)
	}
	importJobsMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(snapshot)
}

// ListImportJobs handles GET /admin/import
func ListImportJobs(w http.ResponseWriter, r *http.Request) {
	importJobsMu.Lock()
	jobs := make([]ImportJob, 0, len(importJobs))
	for _, job := range importJobs {
		snapshot := *job
		if job.FailedZips != nil {
			snapshot.FailedZips = make([]string, len(job.FailedZips))
			copy(snapshot.FailedZips, job.FailedZips)
		}
		jobs = append(jobs, snapshot)
	}
	importJobsMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}

// WarmZip exports warmLocal for use by CLI tools (e.g., cmd/bulk-import).
func WarmZip(zip string) error {
	return warmLocal(context.Background(), zip)
}

// WarmZipWith imports a ZIP using a specific provider (e.g., Cicero for seeding)
// without changing the live Provider setting.
func WarmZipWith(p provider.OfficialProvider, zip string) error {
	origProvider := Provider
	Provider = p
	err := warmLocal(context.Background(), zip)
	Provider = origProvider
	return err
}

// runBulkImport processes ZIPs sequentially with a delay between each.
func runBulkImport(job *ImportJob, zips []string) {
	ctx := context.Background()
	delay := time.Duration(job.DelayMs) * time.Millisecond

	log.Printf("[BulkImport] job=%s starting import of %d ZIPs", job.ID, len(zips))

	for i, zip := range zips {
		importJobsMu.Lock()
		job.CurrentZip = zip
		importJobsMu.Unlock()

		log.Printf("[BulkImport] job=%s processing ZIP %s (%d/%d)", job.ID, zip, i+1, len(zips))

		if err := warmLocal(ctx, zip); err != nil {
			log.Printf("[BulkImport] job=%s ZIP %s failed: %v", job.ID, zip, err)
			importJobsMu.Lock()
			job.Failed++
			job.FailedZips = append(job.FailedZips, zip)
			importJobsMu.Unlock()
		} else {
			importJobsMu.Lock()
			job.Completed++
			importJobsMu.Unlock()
		}

		// Delay between ZIPs to avoid rate limiting (skip after last)
		if i < len(zips)-1 {
			time.Sleep(delay)
		}
	}

	now := time.Now()
	importJobsMu.Lock()
	job.CurrentZip = ""
	job.CompletedAt = &now
	if job.Failed > 0 {
		job.Status = "completed_with_errors"
	} else {
		job.Status = "completed"
	}
	importJobsMu.Unlock()

	log.Printf("[BulkImport] job=%s finished — completed=%d failed=%d", job.ID, job.Completed, job.Failed)
}
