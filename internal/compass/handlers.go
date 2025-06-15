package compass

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/DoyleJ11/auth-system/internal/auth"
	"github.com/DoyleJ11/auth-system/internal/db"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func TopicHandler(w http.ResponseWriter, r *http.Request) {
	var topics []Topic

	result := db.DB.Preload("Stances").Preload("Categories").Find(&topics)
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

	err = db.DB.Where("id IN ?", validIDs).Preload("Stances").Find(&topics).Error
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

func CompareHandler(w http.ResponseWriter, r *http.Request) {

	
}