package campaign_finance

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/go-chi/chi/v5"
)

// fecIngestAllFn is the package-level FEC ingest-all closure, set by
// SetFECIngestAllFunc at startup. It runs FEC ingestion across all confirmed
// FEC politician sources for the current election cycle.
var fecIngestAllFn func() error

// SetFECIngestAllFunc registers the FEC ingest-all closure built in main.go.
// Must be called after Init() and after SetFECIngestFunc, before serving requests.
func SetFECIngestAllFunc(fn func() error) {
	fecIngestAllFn = fn
}

// adapterDBNames maps the URL-friendly adapter name used in the endpoint path
// to the adapter_name column value stored in the ingestion_runs table.
var adapterDBNames = map[string]string{
	"fec":       "fec",
	"cal-access": "cal_access",
	"indiana":   "indiana",
	"socrata":   "la_socrata",
}

// DispatchAdapter runs the named adapter's ingest-all function.
// Returns an error if the adapter name is unknown, the function is nil
// (not configured), or the ingestion itself fails.
// Callers can test for "unknown adapter" by checking strings.HasPrefix(err.Error(), "unknown adapter").
func DispatchAdapter(name string) error {
	switch name {
	case "fec":
		if fecIngestAllFn == nil {
			return fmt.Errorf("fec ingestion not configured")
		}
		return fecIngestAllFn()
	case "cal-access":
		if calAccessIngestAllFn == nil {
			return fmt.Errorf("cal-access ingestion not configured")
		}
		_, err := calAccessIngestAllFn()
		return err
	case "indiana":
		if indianaIngestAllFn == nil {
			return fmt.Errorf("indiana ingestion not configured")
		}
		_, _, err := indianaIngestAllFn()
		return err
	case "socrata":
		if socrataIngestAllFn == nil {
			return fmt.Errorf("socrata ingestion not configured")
		}
		_, err := socrataIngestAllFn()
		return err
	default:
		return fmt.Errorf("unknown adapter: %s", name)
	}
}

// AdminIngestHandler handles POST /admin/ingest/{adapter}.
//
// Authentication: X-Admin-Token header must match the ADMIN_INGEST_TOKEN
// environment variable. Returns 401 if the env var is unset or the token
// does not match.
//
// On all adapter outcomes (success, lock-held, other error) the handler
// returns HTTP 200 to prevent automatic retry cascades from load balancers
// or EventBridge retry policies. The JSON body's "status" field distinguishes
// outcomes:
//
//	{"status": "ok",      "adapter": "fec",  "ingestion_run_id": 42}
//	{"status": "skipped", "adapter": "fec",  "reason": "lock held by another instance"}
//	{"status": "failed",  "adapter": "fec",  "error": "..."}
func AdminIngestHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// --- Token authentication ---
	expectedToken := os.Getenv("ADMIN_INGEST_TOKEN")
	if expectedToken == "" {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": "admin endpoint not configured"})
		return
	}
	if r.Header.Get("X-Admin-Token") != expectedToken {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"})
		return
	}

	// --- Adapter dispatch ---
	adapterName := chi.URLParam(r, "adapter")
	err := DispatchAdapter(adapterName)

	if err != nil {
		// Unknown adapter → 400
		if strings.HasPrefix(err.Error(), "unknown adapter") {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		// Lock contention → 200 skipped (prevents retry floods)
		if strings.Contains(err.Error(), "lock") {
			json.NewEncoder(w).Encode(map[string]string{
				"status":  "skipped",
				"adapter": adapterName,
				"reason":  "lock held by another instance",
			})
			return
		}

		// Other error → 200 failed (prevents retry floods)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "failed",
			"adapter": adapterName,
			"error":   err.Error(),
		})
		return
	}

	// --- Success: fetch most recent IngestionRun for this adapter ---
	dbName, ok := adapterDBNames[adapterName]
	if !ok {
		// Adapter ran fine but we don't know its DB name — shouldn't happen.
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "ok",
			"adapter": adapterName,
		})
		return
	}

	var run IngestionRun
	if err := db.DB.
		Where("adapter_name = ?", dbName).
		Order("started_at DESC").
		First(&run).Error; err != nil {
		// Ingestion ran but we couldn't retrieve the run — non-fatal.
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "ok",
			"adapter": adapterName,
		})
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":           "ok",
		"adapter":          adapterName,
		"ingestion_run_id": run.ID,
	})
}
