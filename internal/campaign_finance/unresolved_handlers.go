package campaign_finance

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/google/uuid"
	"gorm.io/gorm/clause"
)

// UnresolvedQueueEntry is the shape returned by UnresolvedQueueHandler.
// It groups all rows for a given (external_id, adapter_name) pair.
type UnresolvedQueueEntry struct {
	ExternalID        string    `json:"external_id"`
	AdapterName       string    `json:"adapter_name"`
	Status            string    `json:"status"`
	ContributionCount int64     `json:"contribution_count"`
	FirstSeenAt       time.Time `json:"first_seen_at"`
	LastSeenAt        time.Time `json:"last_seen_at"`
	CandidateName     string    `json:"candidate_name"`
}

// UnresolvedQueueHandler handles GET /campaign-finance/admin/unresolved.
// Returns a grouped list of unresolved contributions, sorted by contribution_count descending.
//
// Query params:
//   - ?source=indiana — optional filter by adapter_name
//   - ?show=active (default) or ?show=dismissed
func UnresolvedQueueHandler(w http.ResponseWriter, r *http.Request) {
	showStatus := r.URL.Query().Get("show")
	if showStatus == "" {
		showStatus = "active"
	}
	source := r.URL.Query().Get("source")

	query := `
SELECT
    external_id,
    adapter_name,
    status,
    COUNT(*) AS contribution_count,
    MIN(created_at) AS first_seen_at,
    MAX(created_at) AS last_seen_at,
    MAX(raw_row->>'CandidateName') AS candidate_name
FROM transparent_motivations.unresolved_contributions
WHERE status = ?`

	args := []interface{}{showStatus}

	if source != "" {
		query += ` AND adapter_name = ?`
		args = append(args, source)
	}

	query += `
GROUP BY external_id, adapter_name, status
ORDER BY contribution_count DESC`

	var entries []UnresolvedQueueEntry
	if err := db.DB.Raw(query, args...).Scan(&entries).Error; err != nil {
		http.Error(w, fmt.Sprintf("query failed: %v", err), http.StatusInternalServerError)
		return
	}
	if entries == nil {
		entries = []UnresolvedQueueEntry{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entries)
}

// resolveRequest is the request body for ResolveUnresolvedHandler.
type resolveRequest struct {
	AdapterName string `json:"adapter_name"`
	ExternalID  string `json:"external_id"`
	PoliticianID string `json:"politician_id"`
}

// ResolveUnresolvedHandler handles POST /campaign-finance/admin/unresolved/resolve.
// Creates a PoliticianSource linking the politician to the adapter's external_id,
// then backfills all active unresolved contributions into the contributions table.
func ResolveUnresolvedHandler(w http.ResponseWriter, r *http.Request) {
	var req resolveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if req.AdapterName == "" || req.ExternalID == "" || req.PoliticianID == "" {
		http.Error(w, "adapter_name, external_id, and politician_id are required", http.StatusBadRequest)
		return
	}

	// Only Indiana backfill is supported today. Other adapters can be added
	// by registering their NormalizeRow equivalent via SetIndianaBackfillFunc
	// or a similar mechanism in the future.
	if req.AdapterName != "indiana" {
		http.Error(w, fmt.Sprintf("backfill not supported for adapter: %s", req.AdapterName), http.StatusBadRequest)
		return
	}
	if indianaNormalizeRowFn == nil {
		http.Error(w, "indiana backfill function not registered (server misconfiguration)", http.StatusInternalServerError)
		return
	}

	politicianUUID, err := uuid.Parse(req.PoliticianID)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid politician_id UUID: %v", err), http.StatusBadRequest)
		return
	}

	// Look up the politician name for the response.
	type politicianRow struct {
		Name string
	}
	var pol politicianRow
	if err := db.DB.Raw(`SELECT name FROM essentials.politicians WHERE id = ?`, politicianUUID).Scan(&pol).Error; err != nil {
		http.Error(w, fmt.Sprintf("politician lookup failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Find or create PoliticianSource for (politician_id, adapter_name).
	var ps PoliticianSource
	lookupErr := db.DB.
		Where("essentials_politician_id = ? AND source_system = ?", politicianUUID, req.AdapterName).
		First(&ps).Error

	if lookupErr != nil {
		// Create a new PoliticianSource.
		ps = PoliticianSource{
			EssentialsPoliticianID: politicianUUID,
			SourceSystem:           req.AdapterName,
			ExternalID:             req.ExternalID,
			ResearchStatus:         "confirmed",
		}
		if err := db.DB.Create(&ps).Error; err != nil {
			http.Error(w, fmt.Sprintf("create PoliticianSource failed: %v", err), http.StatusInternalServerError)
			return
		}
	}

	// Fetch all active unresolved rows for this (adapter_name, external_id).
	var unresolved []UnresolvedContribution
	if err := db.DB.
		Where("adapter_name = ? AND external_id = ? AND status = 'active'", req.AdapterName, req.ExternalID).
		Find(&unresolved).Error; err != nil {
		http.Error(w, fmt.Sprintf("fetch unresolved rows failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Backfill each row into contributions via NormalizeRow.
	contributions := make([]Contribution, 0, len(unresolved))
	for _, u := range unresolved {
		var rec map[string]interface{}
		if err := json.Unmarshal(u.RawRow, &rec); err != nil {
			// Skip rows that can't be deserialized.
			continue
		}
		contrib, err := indianaNormalizeRowFn(rec, ps)
		if err != nil {
			// Skip rows that can't be normalized.
			continue
		}
		contributions = append(contributions, contrib)
	}

	// Upsert contributions with ON CONFLICT DO NOTHING.
	if len(contributions) > 0 {
		if err := db.DB.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "data_source"}, {Name: "source_transaction_id"}},
			DoNothing: true,
		}).CreateInBatches(&contributions, 100).Error; err != nil {
			http.Error(w, fmt.Sprintf("upsert contributions failed: %v", err), http.StatusInternalServerError)
			return
		}
	}

	// Mark all matched unresolved rows as resolved.
	if err := db.DB.Model(&UnresolvedContribution{}).
		Where("adapter_name = ? AND external_id = ? AND status = 'active'", req.AdapterName, req.ExternalID).
		Update("status", "resolved").Error; err != nil {
		http.Error(w, fmt.Sprintf("update unresolved status failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"linked":              true,
		"contributions_moved": len(contributions),
		"politician_name":     pol.Name,
	})
}

// dismissRequest is the request body for DismissUnresolvedHandler and RestoreUnresolvedHandler.
type dismissRequest struct {
	AdapterName string `json:"adapter_name"`
	ExternalID  string `json:"external_id"`
}

// DismissUnresolvedHandler handles POST /campaign-finance/admin/unresolved/dismiss.
// Marks active unresolved contributions as dismissed.
func DismissUnresolvedHandler(w http.ResponseWriter, r *http.Request) {
	var req dismissRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if req.AdapterName == "" || req.ExternalID == "" {
		http.Error(w, "adapter_name and external_id are required", http.StatusBadRequest)
		return
	}

	result := db.DB.Model(&UnresolvedContribution{}).
		Where("adapter_name = ? AND external_id = ? AND status = 'active'", req.AdapterName, req.ExternalID).
		Update("status", "dismissed")
	if result.Error != nil {
		http.Error(w, fmt.Sprintf("dismiss failed: %v", result.Error), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"dismissed":     true,
		"rows_affected": result.RowsAffected,
	})
}

// RestoreUnresolvedHandler handles POST /campaign-finance/admin/unresolved/restore.
// Reverses a dismiss by setting status back to active.
func RestoreUnresolvedHandler(w http.ResponseWriter, r *http.Request) {
	var req dismissRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if req.AdapterName == "" || req.ExternalID == "" {
		http.Error(w, "adapter_name and external_id are required", http.StatusBadRequest)
		return
	}

	result := db.DB.Model(&UnresolvedContribution{}).
		Where("adapter_name = ? AND external_id = ? AND status = 'dismissed'", req.AdapterName, req.ExternalID).
		Update("status", "active")
	if result.Error != nil {
		http.Error(w, fmt.Sprintf("restore failed: %v", result.Error), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"restored":      true,
		"rows_affected": result.RowsAffected,
	})
}
