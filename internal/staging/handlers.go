package staging

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/compass"
	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/EmpoweredVote/EV-Backend/internal/essentials"
	"github.com/EmpoweredVote/EV-Backend/internal/utils"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const LockDuration = 10 * time.Minute

// PoliticianOut is the unified shape the data-entry frontend expects.
type PoliticianOut struct {
	ID          string `json:"id"`
	ExternalID  string `json:"external_id,omitempty"`
	FullName    string `json:"full_name"`
	Party       string `json:"party"`
	Office      string `json:"office"`
	OfficeLevel string `json:"office_level"`
	State       string `json:"state"`
	District    string `json:"district"`
	Status      string `json:"status,omitempty"`
	AddedBy     string `json:"added_by,omitempty"`
	Source      string `json:"source"` // "essentials" or "staging"
}

// GetAllData returns all data needed by the data-entry frontend
func GetAllData(w http.ResponseWriter, r *http.Request) {
	// Get stances
	var stances []StagingStance
	if err := db.DB.Find(&stances).Error; err != nil {
		http.Error(w, "Failed to fetch stances: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Get topics from compass
	var topics []compass.Topic
	if err := db.DB.Preload("Stances").Find(&topics).Error; err != nil {
		http.Error(w, "Failed to fetch topics: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Build unified politician list from essentials + staging
	var politicians []PoliticianOut

	// Fetch essentials politicians with office/district info
	type essRow struct {
		ID           string
		ExternalID   int
		FullName     string
		Party        string
		OfficeTitle  string
		DistrictType string
		State        string
		DistrictLabel string
	}
	var essRows []essRow
	if err := db.DB.
		Table("essentials.politicians p").
		Select(`p.id, p.external_id, p.full_name, p.party,
				COALESCE(o.title, '') as office_title,
				COALESCE(d.district_type, '') as district_type,
				COALESCE(o.representing_state, '') as state,
				COALESCE(d.label, '') as district_label`).
		Joins("LEFT JOIN essentials.offices o ON o.politician_id = p.id").
		Joins("LEFT JOIN essentials.districts d ON d.id = o.district_id").
		Order("p.full_name ASC").
		Find(&essRows).Error; err != nil {
		http.Error(w, "Failed to fetch essentials politicians: "+err.Error(), http.StatusInternalServerError)
		return
	}

	for _, row := range essRows {
		politicians = append(politicians, PoliticianOut{
			ID:          row.ID,
			ExternalID:  strconv.Itoa(row.ExternalID),
			FullName:    row.FullName,
			Party:       row.Party,
			Office:      row.OfficeTitle,
			OfficeLevel: districtTypeToLevel(row.DistrictType),
			State:       row.State,
			District:    row.DistrictLabel,
			Source:      "essentials",
		})
	}

	// Also include staging politicians (manually added, not yet in essentials)
	var stagingPoliticians []StagingPolitician
	if err := db.DB.Find(&stagingPoliticians).Error; err != nil {
		http.Error(w, "Failed to fetch staging politicians: "+err.Error(), http.StatusInternalServerError)
		return
	}
	for _, sp := range stagingPoliticians {
		extID := ""
		if sp.ExternalID != nil {
			extID = *sp.ExternalID
		}
		politicians = append(politicians, PoliticianOut{
			ID:          sp.ID.String(),
			ExternalID:  extID,
			FullName:    sp.FullName,
			Party:       sp.Party,
			Office:      sp.Office,
			OfficeLevel: sp.OfficeLevel,
			State:       sp.State,
			District:    sp.District,
			Status:      sp.Status,
			AddedBy:     sp.AddedBy,
			Source:      "staging",
		})
	}

	response := map[string]interface{}{
		"stances":     stances,
		"politicians": politicians,
		"topics":      topics,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// districtTypeToLevel maps BallotReady district types to simple level labels.
func districtTypeToLevel(dt string) string {
	switch dt {
	case "NATIONAL_EXEC", "NATIONAL_UPPER", "NATIONAL_LOWER":
		return "federal"
	case "STATE_EXEC", "STATE_UPPER", "STATE_LOWER":
		return "state"
	case "LOCAL_EXEC", "LOCAL", "COUNTY", "SCHOOL", "JUDICIAL":
		return "local"
	default:
		return dt
	}
}

// ListStances returns all stances with optional filtering
func ListStances(w http.ResponseWriter, r *http.Request) {
	query := db.DB.Model(&StagingStance{})

	// Filter by status
	if status := r.URL.Query().Get("status"); status != "" {
		query = query.Where("status = ?", status)
	}

	// Filter by topic
	if topicKey := r.URL.Query().Get("topic_key"); topicKey != "" {
		query = query.Where("topic_key = ?", topicKey)
	}

	// Filter by author
	if addedBy := r.URL.Query().Get("added_by"); addedBy != "" {
		query = query.Where("added_by = ?", addedBy)
	}

	var stances []StagingStance
	if err := query.Order("created_at DESC").Find(&stances).Error; err != nil {
		http.Error(w, "Failed to fetch stances: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stances)
}

// GetReviewQueue returns stances that need review
func GetReviewQueue(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var stances []StagingStance
	// Get stances needing review that weren't added by this user
	// and haven't been reviewed by this user yet
	if err := db.DB.Where("status = ?", "needs_review").
		Where("added_by != ?", userID).
		Order("created_at ASC").
		Find(&stances).Error; err != nil {
		http.Error(w, "Failed to fetch review queue: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Filter out stances already reviewed by this user
	var filteredStances []StagingStance
	for _, stance := range stances {
		alreadyReviewed := false
		for _, reviewer := range stance.ReviewedBy {
			if reviewer == userID {
				alreadyReviewed = true
				break
			}
		}
		if !alreadyReviewed {
			filteredStances = append(filteredStances, stance)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(filteredStances)
}

// CreateStance creates a new stance in draft status
func CreateStance(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		PoliticianExternalID *string  `json:"politician_external_id,omitempty"`
		PoliticianName       string   `json:"politician_name"`
		TopicKey             string   `json:"topic_key"`
		Value                int      `json:"value"`
		Reasoning            string   `json:"reasoning"`
		Sources              []string `json:"sources"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.PoliticianName == "" || req.TopicKey == "" || req.Value < 1 || req.Value > 5 {
		http.Error(w, "politician_name, topic_key, and value (1-5) are required", http.StatusBadRequest)
		return
	}

	// Generate context key
	polID := ""
	if req.PoliticianExternalID != nil {
		polID = *req.PoliticianExternalID
	} else {
		polID = req.PoliticianName
	}
	contextKey := fmt.Sprintf("%s_%s", polID, req.TopicKey)

	// Check if stance already exists
	var existing StagingStance
	if err := db.DB.First(&existing, "context_key = ?", contextKey).Error; err == nil {
		http.Error(w, "Stance already exists for this politician/topic", http.StatusConflict)
		return
	}

	stance := StagingStance{
		ContextKey:           contextKey,
		PoliticianExternalID: req.PoliticianExternalID,
		PoliticianName:       req.PoliticianName,
		TopicKey:             req.TopicKey,
		Value:                req.Value,
		Reasoning:            req.Reasoning,
		Sources:              req.Sources,
		Status:               "draft",
		AddedBy:              userID,
		ReviewedBy:           pq.StringArray{},
	}

	if err := db.DB.Create(&stance).Error; err != nil {
		http.Error(w, "Failed to create stance: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(stance)
}

// GetStance returns a single stance by ID
func GetStance(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	var stance StagingStance
	if err := db.DB.First(&stance, "id = ?", id).Error; err != nil {
		http.Error(w, "Stance not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stance)
}

// UpdateStance updates a stance (only author can update in draft status)
func UpdateStance(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	var stance StagingStance
	if err := db.DB.First(&stance, "id = ?", id).Error; err != nil {
		http.Error(w, "Stance not found", http.StatusNotFound)
		return
	}

	// Only author can update, and only in draft status
	if stance.AddedBy != userID {
		http.Error(w, "Only the author can update this stance", http.StatusForbidden)
		return
	}
	if stance.Status != "draft" {
		http.Error(w, "Cannot update stance after submission", http.StatusBadRequest)
		return
	}

	var req struct {
		Value     *int     `json:"value,omitempty"`
		Reasoning *string  `json:"reasoning,omitempty"`
		Sources   []string `json:"sources,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	updates := map[string]interface{}{}
	if req.Value != nil {
		if *req.Value < 1 || *req.Value > 5 {
			http.Error(w, "Value must be between 1 and 5", http.StatusBadRequest)
			return
		}
		updates["value"] = *req.Value
	}
	if req.Reasoning != nil {
		updates["reasoning"] = *req.Reasoning
	}
	if req.Sources != nil {
		updates["sources"] = pq.StringArray(req.Sources)
	}

	if err := db.DB.Model(&stance).Updates(updates).Error; err != nil {
		http.Error(w, "Failed to update stance: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
}

// SubmitForReview changes status from draft to needs_review
func SubmitForReview(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	var stance StagingStance
	if err := db.DB.First(&stance, "id = ?", id).Error; err != nil {
		http.Error(w, "Stance not found", http.StatusNotFound)
		return
	}

	// Only author can submit
	if stance.AddedBy != userID {
		http.Error(w, "Only the author can submit this stance", http.StatusForbidden)
		return
	}

	if stance.Status != "draft" {
		http.Error(w, "Stance is not in draft status", http.StatusBadRequest)
		return
	}

	stance.Status = "needs_review"
	if err := db.DB.Save(&stance).Error; err != nil {
		http.Error(w, "Failed to submit stance: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "submitted"})
}

// ApproveStance adds an approval (auto-promotes to approved at 2)
func ApproveStance(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	tx := db.DB.Begin()
	if tx.Error != nil {
		http.Error(w, "Failed to start transaction", http.StatusInternalServerError)
		return
	}

	var stance StagingStance
	if err := tx.First(&stance, "id = ?", id).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Stance not found", http.StatusNotFound)
		return
	}

	// Check if stance is in review
	if stance.Status != "needs_review" {
		tx.Rollback()
		http.Error(w, "Stance is not in review status", http.StatusBadRequest)
		return
	}

	// Check that reviewer hasn't already approved
	for _, reviewer := range stance.ReviewedBy {
		if reviewer == userID {
			tx.Rollback()
			http.Error(w, "You have already reviewed this stance", http.StatusBadRequest)
			return
		}
	}

	// Can't review your own submission
	if stance.AddedBy == userID {
		tx.Rollback()
		http.Error(w, "Cannot review your own submission", http.StatusForbidden)
		return
	}

	// Update review tracking
	stance.ReviewCount++
	stance.ReviewedBy = append(stance.ReviewedBy, userID)
	now := time.Now()
	stance.LastReviewedAt = &now

	// Log the review action
	reviewLog := ReviewLog{
		StanceID:     stance.ID,
		ReviewerName: userID,
		Action:       "approved",
		CreatedAt:    now,
	}
	if err := tx.Create(&reviewLog).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Failed to log review: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// If 2 approvals, promote to production
	if stance.ReviewCount >= 2 {
		stance.Status = "approved"
		stance.ApprovedAt = &now

		// Promote to compass schema
		answerID, err := promoteToCompass(tx, stance)
		if err != nil {
			tx.Rollback()
			http.Error(w, "Failed to promote to compass: "+err.Error(), http.StatusInternalServerError)
			return
		}
		stance.ApprovedToAnswerID = &answerID
	}

	if err := tx.Save(&stance).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Failed to save stance: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := tx.Commit().Error; err != nil {
		http.Error(w, "Failed to commit: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":       stance.Status,
		"review_count": stance.ReviewCount,
		"approved":     stance.Status == "approved",
	})
}

// promoteToCompass creates/updates records in compass.answers when a stance is approved
func promoteToCompass(tx *gorm.DB, stance StagingStance) (string, error) {
	// Resolve topic ID from topic_key
	var topic compass.Topic
	if err := tx.First(&topic, "topic_key = ?", stance.TopicKey).Error; err != nil {
		return "", fmt.Errorf("topic not found: %s", stance.TopicKey)
	}

	// Require a politician_external_id to link to essentials
	if stance.PoliticianExternalID == nil {
		return "", errors.New("politician_external_id is required for approval")
	}

	// Convert external_id string to int and resolve the essentials politician UUID
	extID, err := strconv.Atoi(*stance.PoliticianExternalID)
	if err != nil {
		return "", fmt.Errorf("invalid politician_external_id %q: %w", *stance.PoliticianExternalID, err)
	}

	var politician essentials.Politician
	if err := tx.Select("id").First(&politician, "external_id = ?", extID).Error; err != nil {
		return "", fmt.Errorf("politician not found in essentials for external_id %d: %w", extID, err)
	}

	// Create or update the answer
	answerID := uuid.NewString()
	answer := compass.Answer{
		ID:           answerID,
		PoliticianID: politician.ID,
		TopicID:      topic.ID,
		Value:        float64(stance.Value),
	}

	if err := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{"value"}),
	}).Create(&answer).Error; err != nil {
		return "", err
	}

	// Also create a context entry if reasoning/sources provided
	if stance.Reasoning != "" || len(stance.Sources) > 0 {
		context := compass.Context{
			ID:           uuid.NewString(),
			PoliticianID: politician.ID,
			TopicID:      topic.ID,
			Reasoning:    stance.Reasoning,
			Sources:      stance.Sources,
		}
		if err := tx.Create(&context).Error; err != nil {
			// Non-fatal - context is supplementary
			fmt.Printf("Warning: failed to create context: %v\n", err)
		}
	}

	return answerID, nil
}

// RejectStance rejects a stance
func RejectStance(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	var req struct {
		Comment string `json:"comment"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	var stance StagingStance
	if err := db.DB.First(&stance, "id = ?", id).Error; err != nil {
		http.Error(w, "Stance not found", http.StatusNotFound)
		return
	}

	if stance.Status != "needs_review" {
		http.Error(w, "Stance is not in review status", http.StatusBadRequest)
		return
	}

	// Can't reject your own submission
	if stance.AddedBy == userID {
		http.Error(w, "Cannot reject your own submission", http.StatusForbidden)
		return
	}

	stance.Status = "rejected"
	now := time.Now()
	stance.LastReviewedAt = &now

	// Log the rejection
	reviewLog := ReviewLog{
		StanceID:     stance.ID,
		ReviewerName: userID,
		Action:       "rejected",
		Comment:      req.Comment,
		CreatedAt:    now,
	}

	tx := db.DB.Begin()
	tx.Create(&reviewLog)
	tx.Save(&stance)
	if err := tx.Commit().Error; err != nil {
		http.Error(w, "Failed to reject stance: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "rejected"})
}

// EditAndResubmit allows a reviewer to edit a stance and resubmit
func EditAndResubmit(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	var req struct {
		Value     int      `json:"value"`
		Reasoning string   `json:"reasoning"`
		Sources   []string `json:"sources"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	var stance StagingStance
	if err := db.DB.First(&stance, "id = ?", id).Error; err != nil {
		http.Error(w, "Stance not found", http.StatusNotFound)
		return
	}

	if stance.Status != "needs_review" {
		http.Error(w, "Stance is not in review status", http.StatusBadRequest)
		return
	}

	// Store previous value for logging
	previousValue := stance.Value

	// Editor becomes the new author, review process resets
	stance.Value = req.Value
	stance.Reasoning = req.Reasoning
	stance.Sources = req.Sources
	stance.AddedBy = userID
	stance.ReviewCount = 0
	stance.ReviewedBy = pq.StringArray{}
	stance.LastReviewedAt = nil
	// Status stays as needs_review

	// Log the edit
	reviewLog := ReviewLog{
		StanceID:      stance.ID,
		ReviewerName:  userID,
		Action:        "edited",
		PreviousValue: &previousValue,
		NewValue:      &req.Value,
		CreatedAt:     time.Now(),
	}

	tx := db.DB.Begin()
	tx.Create(&reviewLog)
	tx.Save(&stance)
	if err := tx.Commit().Error; err != nil {
		http.Error(w, "Failed to edit stance: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "resubmitted"})
}

// AcquireLock acquires a 10-minute lock on a stance
func AcquireLock(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	var stance StagingStance
	if err := db.DB.First(&stance, "id = ?", id).Error; err != nil {
		http.Error(w, "Stance not found", http.StatusNotFound)
		return
	}

	now := time.Now()

	// Check if currently locked by someone else
	if stance.LockedBy != nil && stance.LockedAt != nil {
		if now.Sub(*stance.LockedAt) < LockDuration && *stance.LockedBy != userID {
			expiresAt := stance.LockedAt.Add(LockDuration)
			http.Error(w, fmt.Sprintf("Locked by %s until %s", *stance.LockedBy, expiresAt.Format(time.RFC3339)), http.StatusConflict)
			return
		}
	}

	// Acquire lock
	stance.LockedBy = &userID
	stance.LockedAt = &now

	if err := db.DB.Save(&stance).Error; err != nil {
		http.Error(w, "Failed to acquire lock: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"locked":     true,
		"locked_by":  userID,
		"expires_at": now.Add(LockDuration).Format(time.RFC3339),
	})
}

// ReleaseLock releases a lock on a stance
func ReleaseLock(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	var stance StagingStance
	if err := db.DB.First(&stance, "id = ?", id).Error; err != nil {
		http.Error(w, "Stance not found", http.StatusNotFound)
		return
	}

	// Only the lock holder can release (or if lock is expired)
	if stance.LockedBy != nil && *stance.LockedBy != userID {
		if stance.LockedAt != nil && time.Since(*stance.LockedAt) < LockDuration {
			http.Error(w, "You do not hold this lock", http.StatusForbidden)
			return
		}
	}

	stance.LockedBy = nil
	stance.LockedAt = nil

	if err := db.DB.Save(&stance).Error; err != nil {
		http.Error(w, "Failed to release lock: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "released"})
}

// ListPoliticians returns all staged politicians
func ListPoliticians(w http.ResponseWriter, r *http.Request) {
	query := db.DB.Model(&StagingPolitician{})

	if status := r.URL.Query().Get("status"); status != "" {
		query = query.Where("status = ?", status)
	}

	var politicians []StagingPolitician
	if err := query.Order("created_at DESC").Find(&politicians).Error; err != nil {
		http.Error(w, "Failed to fetch politicians: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(politicians)
}

// CreatePolitician creates a new staged politician
func CreatePolitician(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		ExternalID  *string         `json:"external_id,omitempty"`
		FullName    string          `json:"full_name"`
		Party       string          `json:"party"`
		Office      string          `json:"office"`
		OfficeLevel string          `json:"office_level"`
		State       string          `json:"state"`
		District    string          `json:"district"`
		BioText     string          `json:"bio_text"`
		PhotoURL    string          `json:"photo_url"`
		Contacts    json.RawMessage `json:"contacts"`
		Degrees     json.RawMessage `json:"degrees"`
		Experiences json.RawMessage `json:"experiences"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.FullName == "" || req.Office == "" {
		http.Error(w, "full_name and office are required", http.StatusBadRequest)
		return
	}

	politician := StagingPolitician{
		ExternalID:  req.ExternalID,
		FullName:    req.FullName,
		Party:       req.Party,
		Office:      req.Office,
		OfficeLevel: req.OfficeLevel,
		State:       req.State,
		District:    req.District,
		BioText:     req.BioText,
		PhotoURL:    req.PhotoURL,
		Contacts:    JSONB(req.Contacts),
		Degrees:     JSONB(req.Degrees),
		Experiences: JSONB(req.Experiences),
		Status:      "draft",
		AddedBy:     userID,
		ReviewedBy:  pq.StringArray{},
	}

	if err := db.DB.Create(&politician).Error; err != nil {
		http.Error(w, "Failed to create politician: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(politician)
}

// GetPolitician returns a single staged politician
func GetPolitician(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	var politician StagingPolitician
	if err := db.DB.First(&politician, "id = ?", id).Error; err != nil {
		http.Error(w, "Politician not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(politician)
}

// UpdatePolitician allows the original author to update a pending politician
func UpdatePolitician(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	var politician StagingPolitician
	if err := db.DB.First(&politician, "id = ?", id).Error; err != nil {
		http.Error(w, "Politician not found", http.StatusNotFound)
		return
	}

	if politician.AddedBy != userID {
		http.Error(w, "Only the author can update this politician", http.StatusForbidden)
		return
	}
	if politician.Status != "draft" && politician.Status != "pending" {
		http.Error(w, "Cannot update politician after submission", http.StatusBadRequest)
		return
	}

	var req struct {
		FullName    *string         `json:"full_name,omitempty"`
		Party       *string         `json:"party,omitempty"`
		Office      *string         `json:"office,omitempty"`
		OfficeLevel *string         `json:"office_level,omitempty"`
		State       *string         `json:"state,omitempty"`
		District    *string         `json:"district,omitempty"`
		BioText     *string         `json:"bio_text,omitempty"`
		PhotoURL    *string         `json:"photo_url,omitempty"`
		Contacts    json.RawMessage `json:"contacts,omitempty"`
		Degrees     json.RawMessage `json:"degrees,omitempty"`
		Experiences json.RawMessage `json:"experiences,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	updates := map[string]interface{}{}
	if req.FullName != nil {
		updates["full_name"] = *req.FullName
	}
	if req.Party != nil {
		updates["party"] = *req.Party
	}
	if req.Office != nil {
		updates["office"] = *req.Office
	}
	if req.OfficeLevel != nil {
		updates["office_level"] = *req.OfficeLevel
	}
	if req.State != nil {
		updates["state"] = *req.State
	}
	if req.District != nil {
		updates["district"] = *req.District
	}
	if req.BioText != nil {
		updates["bio_text"] = *req.BioText
	}
	if req.PhotoURL != nil {
		updates["photo_url"] = *req.PhotoURL
	}
	if req.Contacts != nil {
		updates["contacts"] = JSONB(req.Contacts)
	}
	if req.Degrees != nil {
		updates["degrees"] = JSONB(req.Degrees)
	}
	if req.Experiences != nil {
		updates["experiences"] = JSONB(req.Experiences)
	}

	if err := db.DB.Model(&politician).Updates(updates).Error; err != nil {
		http.Error(w, "Failed to update politician: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
}

// ApprovePolitician approves and merges a politician to essentials (admin only)
func ApprovePolitician(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	var politician StagingPolitician
	if err := db.DB.First(&politician, "id = ?", id).Error; err != nil {
		http.Error(w, "Politician not found", http.StatusNotFound)
		return
	}

	if politician.Status != "pending" && politician.Status != "draft" && politician.Status != "needs_review" {
		http.Error(w, "Politician is not in a reviewable status", http.StatusBadRequest)
		return
	}

	tx := db.DB.Begin()
	if tx.Error != nil {
		http.Error(w, "Failed to start transaction", http.StatusInternalServerError)
		return
	}

	mergedID, err := promoteToEssentials(tx, politician)
	if err != nil {
		tx.Rollback()
		http.Error(w, "Failed to promote politician: "+err.Error(), http.StatusInternalServerError)
		return
	}

	now := time.Now()
	politician.Status = "approved"
	politician.ReviewedBy = append(politician.ReviewedBy, userID)
	politician.ApprovedAt = &now
	politician.MergedToID = &mergedID

	if err := tx.Save(&politician).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Failed to approve politician: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := tx.Commit().Error; err != nil {
		http.Error(w, "Failed to commit: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "approved",
		"merged_to": mergedID.String(),
	})
}

// RejectPolitician rejects a politician (admin only)
func RejectPolitician(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	var politician StagingPolitician
	if err := db.DB.First(&politician, "id = ?", id).Error; err != nil {
		http.Error(w, "Politician not found", http.StatusNotFound)
		return
	}

	if politician.Status != "pending" && politician.Status != "draft" && politician.Status != "needs_review" {
		http.Error(w, "Politician is not in a rejectable status", http.StatusBadRequest)
		return
	}

	politician.Status = "rejected"
	politician.ReviewedBy = append(politician.ReviewedBy, userID)

	if err := db.DB.Save(&politician).Error; err != nil {
		http.Error(w, "Failed to reject politician: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "rejected"})
}

// SubmitPoliticianForReview changes status from draft/pending to needs_review
func SubmitPoliticianForReview(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	var politician StagingPolitician
	if err := db.DB.First(&politician, "id = ?", id).Error; err != nil {
		http.Error(w, "Politician not found", http.StatusNotFound)
		return
	}

	if politician.AddedBy != userID {
		http.Error(w, "Only the author can submit this politician", http.StatusForbidden)
		return
	}

	if politician.Status != "draft" && politician.Status != "pending" {
		http.Error(w, "Politician is not in draft status", http.StatusBadRequest)
		return
	}

	politician.Status = "needs_review"
	if err := db.DB.Save(&politician).Error; err != nil {
		http.Error(w, "Failed to submit politician: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "submitted"})
}

// GetPoliticianReviewQueue returns politicians that need review
func GetPoliticianReviewQueue(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var politicians []StagingPolitician
	if err := db.DB.Where("status = ?", "needs_review").
		Where("added_by != ?", userID).
		Order("created_at ASC").
		Find(&politicians).Error; err != nil {
		http.Error(w, "Failed to fetch review queue: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Filter out politicians already reviewed by this user or locked by someone else
	now := time.Now()
	var filtered []StagingPolitician
	for _, p := range politicians {
		alreadyReviewed := false
		for _, reviewer := range p.ReviewedBy {
			if reviewer == userID {
				alreadyReviewed = true
				break
			}
		}
		if alreadyReviewed {
			continue
		}

		// Check if locked by someone else
		if p.LockedBy != nil && p.LockedAt != nil {
			if now.Sub(*p.LockedAt) < LockDuration && *p.LockedBy != userID {
				continue
			}
		}

		filtered = append(filtered, p)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(filtered)
}

// ApprovePoliticianReview adds an approval (auto-promotes to essentials at 2)
func ApprovePoliticianReview(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	tx := db.DB.Begin()
	if tx.Error != nil {
		http.Error(w, "Failed to start transaction", http.StatusInternalServerError)
		return
	}

	var politician StagingPolitician
	if err := tx.First(&politician, "id = ?", id).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Politician not found", http.StatusNotFound)
		return
	}

	if politician.Status != "needs_review" {
		tx.Rollback()
		http.Error(w, "Politician is not in review status", http.StatusBadRequest)
		return
	}

	for _, reviewer := range politician.ReviewedBy {
		if reviewer == userID {
			tx.Rollback()
			http.Error(w, "You have already reviewed this politician", http.StatusBadRequest)
			return
		}
	}

	if politician.AddedBy == userID {
		tx.Rollback()
		http.Error(w, "Cannot review your own submission", http.StatusForbidden)
		return
	}

	politician.ReviewCount++
	politician.ReviewedBy = append(politician.ReviewedBy, userID)
	now := time.Now()
	politician.LastReviewedAt = &now

	reviewLog := PoliticianReviewLog{
		PoliticianID: politician.ID,
		ReviewerName: userID,
		Action:       "approved",
		CreatedAt:    now,
	}
	if err := tx.Create(&reviewLog).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Failed to log review: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// If 2 approvals, promote to essentials
	if politician.ReviewCount >= 2 {
		politician.Status = "approved"
		politician.ApprovedAt = &now

		mergedID, err := promoteToEssentials(tx, politician)
		if err != nil {
			tx.Rollback()
			http.Error(w, "Failed to promote to essentials: "+err.Error(), http.StatusInternalServerError)
			return
		}
		politician.MergedToID = &mergedID
	}

	if err := tx.Save(&politician).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Failed to save politician: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := tx.Commit().Error; err != nil {
		http.Error(w, "Failed to commit: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":       politician.Status,
		"review_count": politician.ReviewCount,
		"approved":     politician.Status == "approved",
	})
}

// RejectPoliticianReview rejects a politician during peer review
func RejectPoliticianReview(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	var req struct {
		Comment string `json:"comment"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	var politician StagingPolitician
	if err := db.DB.First(&politician, "id = ?", id).Error; err != nil {
		http.Error(w, "Politician not found", http.StatusNotFound)
		return
	}

	if politician.Status != "needs_review" {
		http.Error(w, "Politician is not in review status", http.StatusBadRequest)
		return
	}

	if politician.AddedBy == userID {
		http.Error(w, "Cannot reject your own submission", http.StatusForbidden)
		return
	}

	politician.Status = "rejected"
	now := time.Now()
	politician.LastReviewedAt = &now

	reviewLog := PoliticianReviewLog{
		PoliticianID: politician.ID,
		ReviewerName: userID,
		Action:       "rejected",
		Comment:      req.Comment,
		CreatedAt:    now,
	}

	tx := db.DB.Begin()
	tx.Create(&reviewLog)
	tx.Save(&politician)
	if err := tx.Commit().Error; err != nil {
		http.Error(w, "Failed to reject politician: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "rejected"})
}

// EditAndResubmitPolitician allows a reviewer to edit and resubmit a politician
func EditAndResubmitPolitician(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	var req struct {
		FullName    *string         `json:"full_name,omitempty"`
		Party       *string         `json:"party,omitempty"`
		Office      *string         `json:"office,omitempty"`
		OfficeLevel *string         `json:"office_level,omitempty"`
		State       *string         `json:"state,omitempty"`
		District    *string         `json:"district,omitempty"`
		BioText     *string         `json:"bio_text,omitempty"`
		PhotoURL    *string         `json:"photo_url,omitempty"`
		Contacts    json.RawMessage `json:"contacts,omitempty"`
		Degrees     json.RawMessage `json:"degrees,omitempty"`
		Experiences json.RawMessage `json:"experiences,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	var politician StagingPolitician
	if err := db.DB.First(&politician, "id = ?", id).Error; err != nil {
		http.Error(w, "Politician not found", http.StatusNotFound)
		return
	}

	if politician.Status != "needs_review" {
		http.Error(w, "Politician is not in review status", http.StatusBadRequest)
		return
	}

	// Apply updates
	if req.FullName != nil {
		politician.FullName = *req.FullName
	}
	if req.Party != nil {
		politician.Party = *req.Party
	}
	if req.Office != nil {
		politician.Office = *req.Office
	}
	if req.OfficeLevel != nil {
		politician.OfficeLevel = *req.OfficeLevel
	}
	if req.State != nil {
		politician.State = *req.State
	}
	if req.District != nil {
		politician.District = *req.District
	}
	if req.BioText != nil {
		politician.BioText = *req.BioText
	}
	if req.PhotoURL != nil {
		politician.PhotoURL = *req.PhotoURL
	}
	if req.Contacts != nil {
		politician.Contacts = JSONB(req.Contacts)
	}
	if req.Degrees != nil {
		politician.Degrees = JSONB(req.Degrees)
	}
	if req.Experiences != nil {
		politician.Experiences = JSONB(req.Experiences)
	}

	// Editor becomes new author, reset reviews
	politician.AddedBy = userID
	politician.ReviewCount = 0
	politician.ReviewedBy = pq.StringArray{}
	politician.LastReviewedAt = nil
	// Status stays as needs_review

	reviewLog := PoliticianReviewLog{
		PoliticianID: politician.ID,
		ReviewerName: userID,
		Action:       "edited",
		CreatedAt:    time.Now(),
	}

	tx := db.DB.Begin()
	tx.Create(&reviewLog)
	tx.Save(&politician)
	if err := tx.Commit().Error; err != nil {
		http.Error(w, "Failed to edit politician: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "resubmitted"})
}

// AcquirePoliticianLock acquires a 10-minute lock on a politician
func AcquirePoliticianLock(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	var politician StagingPolitician
	if err := db.DB.First(&politician, "id = ?", id).Error; err != nil {
		http.Error(w, "Politician not found", http.StatusNotFound)
		return
	}

	now := time.Now()

	if politician.LockedBy != nil && politician.LockedAt != nil {
		if now.Sub(*politician.LockedAt) < LockDuration && *politician.LockedBy != userID {
			expiresAt := politician.LockedAt.Add(LockDuration)
			http.Error(w, fmt.Sprintf("Locked by %s until %s", *politician.LockedBy, expiresAt.Format(time.RFC3339)), http.StatusConflict)
			return
		}
	}

	politician.LockedBy = &userID
	politician.LockedAt = &now

	if err := db.DB.Save(&politician).Error; err != nil {
		http.Error(w, "Failed to acquire lock: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"locked":     true,
		"locked_by":  userID,
		"expires_at": now.Add(LockDuration).Format(time.RFC3339),
	})
}

// ReleasePoliticianLock releases a lock on a politician
func ReleasePoliticianLock(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	var politician StagingPolitician
	if err := db.DB.First(&politician, "id = ?", id).Error; err != nil {
		http.Error(w, "Politician not found", http.StatusNotFound)
		return
	}

	if politician.LockedBy != nil && *politician.LockedBy != userID {
		if politician.LockedAt != nil && time.Since(*politician.LockedAt) < LockDuration {
			http.Error(w, "You do not hold this lock", http.StatusForbidden)
			return
		}
	}

	politician.LockedBy = nil
	politician.LockedAt = nil

	if err := db.DB.Save(&politician).Error; err != nil {
		http.Error(w, "Failed to release lock: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "released"})
}

// promoteToEssentials creates records in the essentials schema from a staged politician.
func promoteToEssentials(tx *gorm.DB, sp StagingPolitician) (uuid.UUID, error) {
	now := time.Now()

	// Map office_level to a BallotReady-style district_type for the essentials schema
	districtType := officeLevelToDistrictType(sp.OfficeLevel)

	// Create district
	district := essentials.District{
		Label:        sp.District,
		DistrictType: districtType,
		State:        sp.State,
	}
	if err := tx.Create(&district).Error; err != nil {
		return uuid.Nil, fmt.Errorf("create district: %w", err)
	}

	// Create chamber (minimal — just enough for the office chain)
	chamber := essentials.Chamber{
		Name: sp.Office,
	}
	if err := tx.Create(&chamber).Error; err != nil {
		return uuid.Nil, fmt.Errorf("create chamber: %w", err)
	}

	// Create politician
	pol := essentials.Politician{
		FullName:   sp.FullName,
		Party:      sp.Party,
		BioText:    sp.BioText,
		Source:     "staging",
		LastSynced: now,
	}
	if err := tx.Create(&pol).Error; err != nil {
		return uuid.Nil, fmt.Errorf("create politician: %w", err)
	}

	// Create office linking politician → chamber → district
	office := essentials.Office{
		PoliticianID:      pol.ID,
		ChamberID:         chamber.ID,
		DistrictID:        district.ID,
		Title:             sp.Office,
		RepresentingState: sp.State,
	}
	if err := tx.Create(&office).Error; err != nil {
		return uuid.Nil, fmt.Errorf("create office: %w", err)
	}

	// Create photo if provided
	if sp.PhotoURL != "" {
		img := essentials.PoliticianImage{
			PoliticianID: pol.ID,
			URL:          sp.PhotoURL,
			Type:         "default",
		}
		if err := tx.Create(&img).Error; err != nil {
			return uuid.Nil, fmt.Errorf("create image: %w", err)
		}
	}

	// Unpack contacts JSONB → essentials.PoliticianContact rows
	if len(sp.Contacts) > 0 {
		var contacts []struct {
			Type   string `json:"type"`
			Value  string `json:"value"`
			Source string `json:"source"`
		}
		if err := json.Unmarshal(sp.Contacts, &contacts); err == nil {
			for _, c := range contacts {
				contact := essentials.PoliticianContact{
					PoliticianID: pol.ID,
					Source:       "staging",
				}
				switch c.Type {
				case "email":
					contact.Email = c.Value
				case "phone":
					contact.Phone = c.Value
				case "fax":
					contact.Fax = c.Value
				case "website":
					contact.ContactType = "website"
					contact.Email = c.Value
				}
				if err := tx.Create(&contact).Error; err != nil {
					return uuid.Nil, fmt.Errorf("create contact: %w", err)
				}
			}
		}
	}

	// Unpack degrees JSONB → essentials.Degree rows
	if len(sp.Degrees) > 0 {
		var degrees []struct {
			Degree   string `json:"degree"`
			Major    string `json:"major"`
			School   string `json:"school"`
			GradYear int    `json:"grad_year"`
		}
		if err := json.Unmarshal(sp.Degrees, &degrees); err == nil {
			for _, d := range degrees {
				deg := essentials.Degree{
					PoliticianID: pol.ID,
					Degree:       d.Degree,
					Major:        d.Major,
					School:       d.School,
					GradYear:     d.GradYear,
				}
				if err := tx.Create(&deg).Error; err != nil {
					return uuid.Nil, fmt.Errorf("create degree: %w", err)
				}
			}
		}
	}

	// Unpack experiences JSONB → essentials.Experience rows
	if len(sp.Experiences) > 0 {
		var experiences []struct {
			Title        string `json:"title"`
			Organization string `json:"organization"`
			Type         string `json:"type"`
			Start        string `json:"start"`
			End          string `json:"end"`
		}
		if err := json.Unmarshal(sp.Experiences, &experiences); err == nil {
			for _, e := range experiences {
				exp := essentials.Experience{
					PoliticianID: pol.ID,
					Title:        e.Title,
					Organization: e.Organization,
					Type:         e.Type,
					Start:        e.Start,
					End:          e.End,
				}
				if err := tx.Create(&exp).Error; err != nil {
					return uuid.Nil, fmt.Errorf("create experience: %w", err)
				}
			}
		}
	}

	return pol.ID, nil
}

// officeLevelToDistrictType maps the simple office_level values used in staging
// to BallotReady-style district types used in essentials.
func officeLevelToDistrictType(level string) string {
	switch level {
	case "federal":
		return "NATIONAL_LOWER"
	case "state":
		return "STATE_LOWER"
	case "municipal", "local":
		return "LOCAL"
	case "school_district":
		return "SCHOOL"
	default:
		return "LOCAL"
	}
}
