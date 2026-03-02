package campaign_finance

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/EmpoweredVote/EV-Backend/internal/utils"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

// adminUser is a local struct to look up username from app_auth.users.
type adminUser struct {
	UserID   string `gorm:"primaryKey"`
	Username string
}

func (adminUser) TableName() string { return "app_auth.users" }

// writeAuditLog records a change to a PoliticianSource for accountability.
// userID is the string user ID from context; username is fetched from DB.
func writeAuditLog(sourceID uuid.UUID, userIDStr string, action string, oldVal, newVal interface{}) {
	oldJSON, _ := json.Marshal(oldVal)
	newJSON, _ := json.Marshal(newVal)

	// Parse the user ID string into a UUID for the audit record.
	parsedUserID, err := uuid.Parse(userIDStr)
	if err != nil {
		parsedUserID = uuid.Nil
	}

	// Best-effort username lookup — do not fail the request on error.
	var u adminUser
	username := userIDStr
	if dbErr := db.DB.First(&u, "user_id = ?", userIDStr).Error; dbErr == nil {
		username = u.Username
	}

	entry := SourceAuditLog{
		PoliticianSourceID: sourceID,
		ChangedByUserID:    parsedUserID,
		ChangedByUsername:  username,
		Action:             action,
		OldValue:           oldJSON,
		NewValue:           newJSON,
		ChangedAt:          time.Now(),
	}
	db.DB.Create(&entry)
}

// ListSourcesHandler handles GET /campaign-finance/admin/sources
// Optional query params: politician_id, source_system, research_status
func ListSourcesHandler(w http.ResponseWriter, r *http.Request) {
	query := db.DB.Model(&PoliticianSource{})

	if pid := r.URL.Query().Get("politician_id"); pid != "" {
		query = query.Where("essentials_politician_id = ?", pid)
	}
	if ss := r.URL.Query().Get("source_system"); ss != "" {
		query = query.Where("source_system = ?", ss)
	}
	if rs := r.URL.Query().Get("research_status"); rs != "" {
		query = query.Where("research_status = ?", rs)
	}

	var sources []PoliticianSource
	if err := query.Order("created_at DESC").Find(&sources).Error; err != nil {
		http.Error(w, "Failed to query sources", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(sources)
}

// CreateSourceHandler handles POST /campaign-finance/admin/sources
func CreateSourceHandler(w http.ResponseWriter, r *http.Request) {
	var input PoliticianSource
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if input.EssentialsPoliticianID == uuid.Nil {
		http.Error(w, "essentials_politician_id is required", http.StatusBadRequest)
		return
	}
	if input.SourceSystem == "" {
		http.Error(w, "source_system is required", http.StatusBadRequest)
		return
	}

	// Assign a new ID; let the DB default handle it if zero.
	if input.ID == uuid.Nil {
		input.ID = uuid.New()
	}

	if err := db.DB.Create(&input).Error; err != nil {
		http.Error(w, "Failed to create source", http.StatusInternalServerError)
		return
	}

	userIDStr, _ := utils.GetUserIDFromContext(r.Context())
	writeAuditLog(input.ID, userIDStr, "CREATE", nil, input)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(input)
}

// UpdateSourceHandler handles PUT /campaign-finance/admin/sources/{id}
func UpdateSourceHandler(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, "Invalid UUID", http.StatusBadRequest)
		return
	}

	var existing PoliticianSource
	if err := db.DB.First(&existing, "id = ?", id).Error; err != nil {
		http.Error(w, "Source not found", http.StatusNotFound)
		return
	}
	oldRecord := existing

	var input PoliticianSource
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Apply only provided fields — preserve ID.
	input.ID = existing.ID
	if err := db.DB.Save(&input).Error; err != nil {
		http.Error(w, "Failed to update source", http.StatusInternalServerError)
		return
	}

	userIDStr, _ := utils.GetUserIDFromContext(r.Context())
	writeAuditLog(existing.ID, userIDStr, "UPDATE", oldRecord, input)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(input)
}

// DeleteSourceHandler handles DELETE /campaign-finance/admin/sources/{id}
func DeleteSourceHandler(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, "Invalid UUID", http.StatusBadRequest)
		return
	}

	var existing PoliticianSource
	if err := db.DB.First(&existing, "id = ?", id).Error; err != nil {
		http.Error(w, "Source not found", http.StatusNotFound)
		return
	}

	if err := db.DB.Delete(&existing).Error; err != nil {
		http.Error(w, "Failed to delete source", http.StatusInternalServerError)
		return
	}

	userIDStr, _ := utils.GetUserIDFromContext(r.Context())
	writeAuditLog(existing.ID, userIDStr, "DELETE", existing, nil)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"deleted": true})
}
