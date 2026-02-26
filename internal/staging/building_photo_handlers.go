package staging

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/EmpoweredVote/EV-Backend/internal/essentials"
	"github.com/EmpoweredVote/EV-Backend/internal/utils"
	"github.com/go-chi/chi/v5"
	"gorm.io/gorm"
)

// JurisdictionGap represents a local jurisdiction missing a building photo
type JurisdictionGap struct {
	GeoID           string `json:"geo_id"`
	City            string `json:"city"`
	State           string `json:"state"`
	DistrictType    string `json:"district_type"`
	PoliticianCount int    `json:"politician_count"`
}

// GetBuildingPhotoGaps returns local jurisdictions that have no building photo
func GetBuildingPhotoGaps(w http.ResponseWriter, r *http.Request) {
	stateFilter := r.URL.Query().Get("state")

	query := `
		SELECT d.geo_id, d.city, d.state, d.district_type, COUNT(DISTINCT p.id) as politician_count
		FROM essentials.districts d
		JOIN essentials.offices o ON o.district_id = d.id
		JOIN essentials.politicians p ON o.politician_id = p.id
		WHERE d.district_type IN ('LOCAL', 'LOCAL_EXEC', 'COUNTY')
		  AND d.geo_id != ''
		  AND d.geo_id IS NOT NULL
		  AND d.geo_id NOT IN (SELECT place_geoid FROM essentials.building_photos)
		  AND d.geo_id NOT IN (SELECT place_geoid FROM staging.building_photos WHERE status IN ('draft', 'needs_review'))
	`
	args := []interface{}{}

	if stateFilter != "" {
		query += " AND d.state = ?"
		args = append(args, stateFilter)
	}

	query += " GROUP BY d.geo_id, d.city, d.state, d.district_type ORDER BY d.state, d.city"

	var gaps []JurisdictionGap
	if err := db.DB.Raw(query, args...).Scan(&gaps).Error; err != nil {
		http.Error(w, "Failed to fetch gaps: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if gaps == nil {
		gaps = []JurisdictionGap{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(gaps)
}

// CreateBuildingPhoto creates a new staging building photo
func CreateBuildingPhoto(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var input struct {
		PlaceGeoid  string `json:"place_geoid"`
		PlaceName   string `json:"place_name"`
		State       string `json:"state"`
		URL         string `json:"url"`
		SourceURL   string `json:"source_url"`
		License     string `json:"license"`
		Attribution string `json:"attribution"`
	}

	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	if input.PlaceGeoid == "" || input.PlaceName == "" || input.URL == "" || input.License == "" || input.Attribution == "" {
		http.Error(w, "place_geoid, place_name, url, license, and attribution are required", http.StatusBadRequest)
		return
	}

	photo := StagingBuildingPhoto{
		PlaceGeoid:  input.PlaceGeoid,
		PlaceName:   input.PlaceName,
		State:       input.State,
		URL:         input.URL,
		SourceURL:   input.SourceURL,
		License:     input.License,
		Attribution: input.Attribution,
		Status:      "draft",
		AddedBy:     userID,
	}

	if err := db.DB.Create(&photo).Error; err != nil {
		http.Error(w, "Failed to create building photo: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(photo)
}

// ListBuildingPhotos returns all staging building photos
func ListBuildingPhotos(w http.ResponseWriter, r *http.Request) {
	statusFilter := r.URL.Query().Get("status")

	var photos []StagingBuildingPhoto
	q := db.DB.Order("created_at DESC")
	if statusFilter != "" {
		q = q.Where("status = ?", statusFilter)
	}

	if err := q.Find(&photos).Error; err != nil {
		http.Error(w, "Failed to fetch building photos: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if photos == nil {
		photos = []StagingBuildingPhoto{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(photos)
}

// GetBuildingPhotoByID returns a single staging building photo
func GetBuildingPhotoByID(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	var photo StagingBuildingPhoto
	if err := db.DB.First(&photo, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			http.Error(w, "Building photo not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(photo)
}

// UpdateBuildingPhoto updates a draft building photo
func UpdateBuildingPhoto(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	var photo StagingBuildingPhoto
	if err := db.DB.First(&photo, "id = ?", id).Error; err != nil {
		http.Error(w, "Building photo not found", http.StatusNotFound)
		return
	}

	if photo.AddedBy != userID && !isAdmin(userID) {
		http.Error(w, "Only the author or admin can edit", http.StatusForbidden)
		return
	}

	if photo.Status != "draft" && photo.Status != "rejected" {
		http.Error(w, "Can only edit draft or rejected photos", http.StatusBadRequest)
		return
	}

	var input struct {
		URL         *string `json:"url"`
		SourceURL   *string `json:"source_url"`
		License     *string `json:"license"`
		Attribution *string `json:"attribution"`
	}

	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	if input.URL != nil {
		photo.URL = *input.URL
	}
	if input.SourceURL != nil {
		photo.SourceURL = *input.SourceURL
	}
	if input.License != nil {
		photo.License = *input.License
	}
	if input.Attribution != nil {
		photo.Attribution = *input.Attribution
	}

	if err := db.DB.Save(&photo).Error; err != nil {
		http.Error(w, "Failed to update: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
}

// SubmitBuildingPhotoForReview changes status from draft to needs_review
func SubmitBuildingPhotoForReview(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	var photo StagingBuildingPhoto
	if err := db.DB.First(&photo, "id = ?", id).Error; err != nil {
		http.Error(w, "Building photo not found", http.StatusNotFound)
		return
	}

	if photo.AddedBy != userID {
		http.Error(w, "Only the author can submit for review", http.StatusForbidden)
		return
	}

	if photo.Status != "draft" {
		http.Error(w, "Building photo is not in draft status", http.StatusBadRequest)
		return
	}

	photo.Status = "needs_review"
	if err := db.DB.Save(&photo).Error; err != nil {
		http.Error(w, "Failed to submit: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "submitted"})
}

// GetBuildingPhotoReviewQueue returns building photos needing review
func GetBuildingPhotoReviewQueue(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var photos []StagingBuildingPhoto
	if err := db.DB.Where("status = ?", "needs_review").
		Where("added_by != ?", userID).
		Order("created_at ASC").
		Find(&photos).Error; err != nil {
		http.Error(w, "Failed to fetch review queue: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Filter out already reviewed by this user
	var filtered []StagingBuildingPhoto
	for _, p := range photos {
		alreadyReviewed := false
		for _, reviewer := range p.ReviewedBy {
			if reviewer == userID {
				alreadyReviewed = true
				break
			}
		}
		if !alreadyReviewed {
			filtered = append(filtered, p)
		}
	}

	if filtered == nil {
		filtered = []StagingBuildingPhoto{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(filtered)
}

// ApproveBuildingPhotoReview adds a peer approval. Requires 1 approval to promote.
// Admins can approve their own submissions.
func ApproveBuildingPhotoReview(w http.ResponseWriter, r *http.Request) {
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

	var photo StagingBuildingPhoto
	if err := tx.First(&photo, "id = ?", id).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Building photo not found", http.StatusNotFound)
		return
	}

	if photo.Status != "needs_review" {
		tx.Rollback()
		http.Error(w, "Building photo is not in review status", http.StatusBadRequest)
		return
	}

	admin := isAdmin(userID)

	for _, reviewer := range photo.ReviewedBy {
		if reviewer == userID {
			tx.Rollback()
			http.Error(w, "You have already reviewed this photo", http.StatusBadRequest)
			return
		}
	}

	// Non-admins cannot approve their own submission
	if photo.AddedBy == userID && !admin {
		tx.Rollback()
		http.Error(w, "Cannot review your own submission", http.StatusForbidden)
		return
	}

	photo.ReviewCount++
	photo.ReviewedBy = append(photo.ReviewedBy, userID)
	now := time.Now()
	photo.LastReviewedAt = &now

	action := "approved"
	if admin {
		action = "admin_approved"
	}
	reviewLog := BuildingPhotoReviewLog{
		BuildingPhotoID: photo.ID,
		ReviewerName:    userID,
		Action:          action,
		CreatedAt:       now,
	}
	if err := tx.Create(&reviewLog).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Failed to log review: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// 1 approval promotes to essentials.building_photos
	approved := photo.ReviewCount >= 1
	if approved {
		photo.Status = "approved"
		photo.ApprovedAt = &now

		// Upsert into essentials.building_photos
		bp := essentials.BuildingPhoto{
			PlaceGeoid:  photo.PlaceGeoid,
			URL:         photo.URL,
			SourceURL:   photo.SourceURL,
			License:     photo.License,
			Attribution: photo.Attribution,
			FetchedAt:   now,
		}
		if err := tx.Save(&bp).Error; err != nil {
			tx.Rollback()
			http.Error(w, "Failed to promote to essentials: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if err := tx.Save(&photo).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Failed to save review: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := tx.Commit().Error; err != nil {
		http.Error(w, "Failed to commit: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":       photo.Status,
		"review_count": photo.ReviewCount,
		"approved":     approved,
	})
}

// RejectBuildingPhotoReview rejects a building photo during review
func RejectBuildingPhotoReview(w http.ResponseWriter, r *http.Request) {
	userID, ok := utils.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	var photo StagingBuildingPhoto
	if err := db.DB.First(&photo, "id = ?", id).Error; err != nil {
		http.Error(w, "Building photo not found", http.StatusNotFound)
		return
	}

	if photo.Status != "needs_review" {
		http.Error(w, "Building photo is not in review status", http.StatusBadRequest)
		return
	}

	var body struct {
		Comment string `json:"comment"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	now := time.Now()
	photo.Status = "rejected"
	photo.ReviewedBy = append(photo.ReviewedBy, userID)
	photo.LastReviewedAt = &now

	if err := db.DB.Save(&photo).Error; err != nil {
		http.Error(w, "Failed to reject: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Log the rejection
	reviewLog := BuildingPhotoReviewLog{
		BuildingPhotoID: photo.ID,
		ReviewerName:    userID,
		Action:          "rejected",
		Comment:         body.Comment,
		CreatedAt:       now,
	}
	db.DB.Create(&reviewLog)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "rejected"})
}

// AdminApproveBuildingPhoto allows admin to immediately approve (skip peer review)
func AdminApproveBuildingPhoto(w http.ResponseWriter, r *http.Request) {
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

	var photo StagingBuildingPhoto
	if err := tx.First(&photo, "id = ?", id).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Building photo not found", http.StatusNotFound)
		return
	}

	if photo.Status != "draft" && photo.Status != "needs_review" {
		tx.Rollback()
		http.Error(w, "Building photo is not in a reviewable status", http.StatusBadRequest)
		return
	}

	now := time.Now()
	photo.Status = "approved"
	photo.ReviewedBy = append(photo.ReviewedBy, userID)
	photo.ApprovedAt = &now
	photo.ReviewCount++

	// Upsert into essentials.building_photos
	bp := essentials.BuildingPhoto{
		PlaceGeoid:  photo.PlaceGeoid,
		URL:         photo.URL,
		SourceURL:   photo.SourceURL,
		License:     photo.License,
		Attribution: photo.Attribution,
		FetchedAt:   now,
	}
	if err := tx.Save(&bp).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Failed to promote to essentials: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := tx.Save(&photo).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Failed to approve: "+err.Error(), http.StatusInternalServerError)
		return
	}

	reviewLog := BuildingPhotoReviewLog{
		BuildingPhotoID: photo.ID,
		ReviewerName:    userID,
		Action:          "admin_approved",
		CreatedAt:       now,
	}
	tx.Create(&reviewLog)

	if err := tx.Commit().Error; err != nil {
		http.Error(w, "Failed to commit: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "approved",
		"approved": true,
	})
}
