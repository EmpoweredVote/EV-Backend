package compass

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/EmpoweredVote/EV-Backend/internal/auth"
	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func TopicHandler(w http.ResponseWriter, r *http.Request) {
	var topics []Topic

	result := db.DB.Preload("Stances", func(db *gorm.DB) *gorm.DB {
			return db.Order("value ASC")
		}).Preload("Categories").Find(&topics)

	if result.Error != nil {
		http.Error(w, "DB error: "+result.Error.Error(), http.StatusInternalServerError)
		return
	}
	if len(topics) == 0 {
		http.Error(w, "No topics found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(topics); err != nil {
    	http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}
}

func TopicBatchHandler(w http.ResponseWriter, r *http.Request) {
	var topics []Topic

	var filterRequest struct {
		IDs  []string	`json:"ids"`
	}

	err := json.NewDecoder(r.Body).Decode(&filterRequest)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	var validIDs []uuid.UUID
	for _, id := range filterRequest.IDs {
		parsed, err := uuid.Parse(id)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid UUID format: %s", id), http.StatusBadRequest)
        	return
		}
		validIDs = append(validIDs, parsed)
	}

	err = db.DB.Where("id IN ?", validIDs).Preload("Stances", func(db *gorm.DB) *gorm.DB {
		return db.Order("value ASC")
	}).Find(&topics).Error
	if err != nil {
		http.Error(w, "Invalid Topic", http.StatusNotFound)
		return
	}

	if len(topics) < len(validIDs) {
    	log.Printf("Warning: Some topic IDs were not found")
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(topics); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}
}

func TopicUpdateHandler(w http.ResponseWriter, r *http.Request) {

	var topicRequest struct {
		ID          string  `json:"ID"`
		Title       *string `json:"Title,omitempty"`
		ShortTitle  *string `json:"ShortTitle,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&topicRequest); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	var topic Topic
	if err := db.DB.First(&topic, "id = ?", topicRequest.ID).Error; err != nil {
		http.Error(w, "Topic not found", http.StatusNotFound)
		return
	}

	updates := map[string]interface{}{}
	if topicRequest.Title != nil {
		updates["Title"] = *topicRequest.Title
	}
	if topicRequest.ShortTitle != nil {
		updates["ShortTitle"] = *topicRequest.ShortTitle
	}

	if err := db.DB.Model(&topic).Updates(updates).Error; err != nil {
		http.Error(w, "Failed to update topic", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Topic updated successfully")
}

func StanceUpdateHandler(w http.ResponseWriter, r *http.Request) {
	var updates []struct {
		ID string `json:"ID"`
		Text string `json:"Text"`
	}

	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	for _, update := range updates {
		if err := db.DB.Model(&Stance{}).Where("id = ?", update.ID).Update("text", update.Text).Error; err != nil {
			http.Error(w, "Failed to update stance "+update.ID, http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Stances updated successfully")
}


func CategoryHandler(w http.ResponseWriter, r *http.Request) {
	var categories []Category

	result := db.DB.Preload("Topics").Find(&categories)
	if result.Error != nil {
		http.Error(w, "DB error: "+result.Error.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(categories); err != nil {
    	http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}
}

func UpdateTopicCategoriesHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TopicID string   `json:"topic_id"`
		Add	    []string `json:"add,omitempty"`
		Remove  []string `json:"remove,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	topicUUID, err := uuid.Parse(req.TopicID)
	if err != nil {
		http.Error(w, "Invalid topic_id format", http.StatusBadRequest)
		return
	}

	var topic Topic
	if err := db.DB.Preload("Categories").First(&topic, "id = ?", topicUUID).Error; err != nil {
		http.Error(w, "Topic not found", http.StatusNotFound)
		return
	}

	addSet := make(map[uuid.UUID]bool)
	for _, id := range req.Add {
		if parsed, err := uuid.Parse(id); err == nil {
			addSet[parsed] = true
		}
	}

	removeSet := make(map[uuid.UUID]bool)
	for _, id := range req.Remove {
		if parsed, err := uuid.Parse(id); err == nil {
			removeSet[parsed] = true
		}
	}

	var newCategories []Category
	for _, cat := range topic.Categories {
		if !removeSet[cat.ID] {
			newCategories = append(newCategories, cat)
		}
	}
	for id := range addSet {
		newCategories = append(newCategories, Category{ID: id})
	}

	if err := db.DB.Model(&topic).Association("Categories").Replace(&newCategories); err != nil {
		http.Error(w, "Failed to update categories", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "Topic categories updated successfully")
}


func AnswerHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// GET logic

		var answers []Answer

		var response[] struct {
			TopicID string `json:"topic_id"`
			Value   int    `json:"value"`
		}

		cookie, err := r.Cookie("session_id")
		if err != nil {
			http.Error(w, "Session not found", http.StatusUnauthorized)
			return
		}

		session, err := auth.SessionInfo{}.FindSessionByID(cookie.Value)
		if err != nil {
			http.Error(w, "Invalid session", http.StatusUnauthorized)
			return
		}

		err = db.DB.Model(&answers).Find(&response, "user_id = ?", session.UserID).Error
		if err != nil {
			http.Error(w, "Answers not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		}

	
	case http.MethodPost:
		// POST logic
		var input struct {
			TopicID string `json:"topic_id"`
			Value   int    `json:"value"`
		}

		if err := json.NewDecoder(r.Body).Decode(&input); err != nil{
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		cookie, err := r.Cookie("session_id")
		if err != nil {
			http.Error(w, "Session not found", http.StatusUnauthorized)
			return
		}

		session, err := auth.SessionInfo{}.FindSessionByID(cookie.Value)
		if err != nil {
			http.Error(w, "Invalid session", http.StatusUnauthorized)
			return
		}

		var existing Answer
		err = db.DB.Where("user_id = ? AND topic_id = ?", session.UserID, input.TopicID).First(&existing).Error

		if err == nil {
			// If no error, answer already exists, update it
			err = db.DB.Model(&existing).Update("value", input.Value).Error
			if err != nil {
				http.Error(w, "Failed to update answer", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			fmt.Fprintln(w, "Answer updated successfully")
			return
		} 

		if err == gorm.ErrRecordNotFound {
			newAnswer := Answer{
				ID:      uuid.NewString(),
				UserID:  session.UserID,
				TopicID: input.TopicID,
				Value:   input.Value,
			}
			if err = db.DB.Create(&newAnswer).Error; err != nil {
				http.Error(w, "Failed to create answer", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(newAnswer)
			return
		}
		http.Error(w, "DB error", http.StatusInternalServerError)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func AnswerBatchHander(w http.ResponseWriter, r *http.Request) {
	var answers []Answer

	var filterRequest struct {
		IDs  []string	`json:"ids"`
	}

	err := json.NewDecoder(r.Body).Decode(&filterRequest)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	var validIDs []uuid.UUID
	for _, id := range filterRequest.IDs {
		parsed, err := uuid.Parse(id)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid UUID format: %s", id), http.StatusBadRequest)
        	return
		}
		validIDs = append(validIDs, parsed)
	}

	cookie, err := r.Cookie("session_id")
	if err != nil {
		http.Error(w, "Session not found", http.StatusUnauthorized)
		return
	}

	session, err := auth.SessionInfo{}.FindSessionByID(cookie.Value)
	if err != nil {
		http.Error(w, "Invalid session", http.StatusUnauthorized)
		return
	}

	err = db.DB.Where("user_id = ? AND topic_id IN ? AND value != 0", session.UserID, validIDs).Find(&answers).Error
	if err != nil {
		http.Error(w, "Couldn't find answers", http.StatusInternalServerError)
		return
	}


	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(answers); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}
}

func CompareHandler(w http.ResponseWriter, r *http.Request) {
	var answers []Answer

	var request struct {
		UserID   	string    `json:"user_id"`
		TopicIDs	[]string  `json:"ids"`
	}
	
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	var validIDs []uuid.UUID
	for _, id := range request.TopicIDs {
		parsed, err := uuid.Parse(id)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid UUID format: %s", id), http.StatusBadRequest)
        	return
		}
		validIDs = append(validIDs, parsed)
	}

	err = db.DB.Where("user_id = ? AND topic_id IN ?", request.UserID, validIDs).Find(&answers).Error
	if err != nil {
		http.Error(w, "Couldn't find answers", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(answers); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}

}