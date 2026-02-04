package staging

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/compass"
	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/EmpoweredVote/EV-Backend/internal/utils"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const LockDuration = 10 * time.Minute

// GetAllData returns all data needed by the data-entry frontend
func GetAllData(w http.ResponseWriter, r *http.Request) {
	// Get stances
	var stances []StagingStance
	if err := db.DB.Find(&stances).Error; err != nil {
		http.Error(w, "Failed to fetch stances: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Get politicians from staging
	var stagingPoliticians []StagingPolitician
	if err := db.DB.Find(&stagingPoliticians).Error; err != nil {
		http.Error(w, "Failed to fetch staging politicians: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Get topics from compass
	var topics []compass.Topic
	if err := db.DB.Preload("Stances").Find(&topics).Error; err != nil {
		http.Error(w, "Failed to fetch topics: "+err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"stances":     stances,
		"politicians": stagingPoliticians,
		"topics":      topics,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
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

	// For now, we require a politician_external_id to link to essentials
	// In the future, this could also match by name
	if stance.PoliticianExternalID == nil {
		return "", errors.New("politician_external_id is required for approval")
	}

	// Create or update the answer
	answerID := uuid.NewString()
	answer := compass.Answer{
		ID:      answerID,
		TopicID: topic.ID,
		Value:   stance.Value,
		// Note: PoliticianID would need to be resolved from external_id
		// This is a simplified version - full implementation would lookup in essentials
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
			ID:        uuid.NewString(),
			TopicID:   topic.ID,
			Reasoning: stance.Reasoning,
			Sources:   stance.Sources,
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
		ExternalID  *string `json:"external_id,omitempty"`
		FullName    string  `json:"full_name"`
		Party       string  `json:"party"`
		Office      string  `json:"office"`
		OfficeLevel string  `json:"office_level"`
		State       string  `json:"state"`
		District    string  `json:"district"`
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
		Status:      "pending",
		AddedBy:     userID,
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

	if politician.Status != "pending" {
		http.Error(w, "Politician is not in pending status", http.StatusBadRequest)
		return
	}

	// TODO: Actually merge to essentials.politicians
	// For now, just mark as approved
	politician.Status = "approved"
	politician.ReviewedBy = &userID

	if err := db.DB.Save(&politician).Error; err != nil {
		http.Error(w, "Failed to approve politician: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "approved"})
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

	if politician.Status != "pending" {
		http.Error(w, "Politician is not in pending status", http.StatusBadRequest)
		return
	}

	politician.Status = "rejected"
	politician.ReviewedBy = &userID

	if err := db.DB.Save(&politician).Error; err != nil {
		http.Error(w, "Failed to reject politician: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "rejected"})
}
