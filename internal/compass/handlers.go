package compass

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"sort"

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


func StancesUpdateHandlerOLD(w http.ResponseWriter, r *http.Request) {
	type stanceType struct {
        ID   	string  `json:"id"`
        Text 	string 	`json:"text"`
		TopicID string  `json:"topic_id"`
		Value 	int     `json:"value"`
    }

	var updatedStances []struct {
		TopicID string 	 `json:"topic_id"`
		Stances []stanceType `json:"stances"`
	}

	if err := json.NewDecoder(r.Body).Decode(&updatedStances); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	for _, group := range updatedStances {
		var oldStances []stanceType
		topic := group.TopicID
		if err := db.DB.Model(&Stance{}).Where("topic_id = ?", topic).Find(&oldStances).Error;
		err != nil {
			http.Error(w, "Failed to find stances", http.StatusInternalServerError)
			return
		}

		// Sort both slices by value
		sort.Slice(group.Stances, func(i, j int) bool {
			return group.Stances[i].Value < group.Stances[j].Value
		})

		sort.Slice(oldStances, func(i, j int) bool {
			return oldStances[i].Value < oldStances[j].Value
		})

		for i, stance := range group.Stances {
			if i < len(oldStances) {
				if oldStances[i].Text == stance.Text &&
				oldStances[i].Value == stance.Value &&
				oldStances[i].ID == stance.ID {
					fmt.Printf("Stance %d: unchanged\n", i)
					continue
				}

				if oldStances[i].ID == stance.ID {
					// We did not continue so stance or val must be diff if ID is the smae
					update := Stance{Text: stance.Text, Value: stance.Value}
					if err := db.DB.Model(&Stance{}).Where("id = ? AND topic_id = ?",stance.ID, topic).Select("Text", "Value").Updates(update).Error
					err != nil {
						http.Error(w, "Failed to update stance", http.StatusInternalServerError)
						return
					}
					fmt.Printf("Stance %d: updated\n", i)
					continue
				} 

				// We know the ID doesn't match & the text or Value don't match. delete old stance
				if err := db.DB.Delete(&Stance{}, "id = ?", oldStances[i].ID).Error;
				err != nil {
					http.Error(w, "Failed to delete stance", http.StatusInternalServerError)
					return
				}

				// Create new stance
				if err := db.DB.Create(stance).Error;
				err != nil {
					http.Error(w, "Failed to create stance", http.StatusInternalServerError)
					return
				}
				fmt.Printf("Stance %d: deleted + created\n", i)
				continue
			}

			if err := db.DB.Create(&stance).Error; err != nil {
				http.Error(w, "Failed to create new stance", http.StatusInternalServerError)
				return
			}
			fmt.Printf("Stance %d: new created\n", i)
		}
}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Stances updated successfully")
}

func StancesUpdateHandler(w http.ResponseWriter, r *http.Request) {
	type updated struct {
		ID    string `json:"id"`
		Text  string `json:"text"`
		Value int    `json:"value"`
	}

	type added struct {
		Text  string `json:"text"`
		Value int    `json:"value"`
	}

	type removed struct {
		ID    string `json:"id"`
		Value int    `json:"value"`
	}

	var request struct {
		TopicID uuid.UUID `json:"topic_id"`
		Updated []updated `json:"updated"`
		Added   []added   `json:"added"`
		Removed []removed `json:"removed"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		log.Printf("❌ JSON decode failed: %v\n", err)
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	log.Println("🔁 Starting DB transaction...")
	tx := db.DB.Begin()
	if tx.Error != nil {
		log.Printf("❌ Failed to begin transaction: %v\n", tx.Error)
		http.Error(w, "Failed to start transaction", http.StatusInternalServerError)
		return
	}

	// 🔄 Updated stances
	for _, updatedStance := range request.Updated {
		update := Stance{Text: updatedStance.Text, Value: updatedStance.Value}
		if err := tx.Model(&Stance{}).
			Where("id = ? AND topic_id = ?", updatedStance.ID, request.TopicID).
			Select("Text", "Value").
			Updates(update).Error; err != nil {
			log.Printf("❌ Failed to update stance ID %s: %v\n", updatedStance.ID, err)
			tx.Rollback()
			http.Error(w, "Failed to update stance", http.StatusInternalServerError)
			return
		}
		log.Printf("✅ Updated stance ID %s\n", updatedStance.ID)
	}

	// ➕ Added stances
	for _, addedStance := range request.Added {
		newID := uuid.NewString()
		newStance := Stance{
			ID:      newID,
			TopicID: request.TopicID,
			Text:    addedStance.Text,
			Value:   addedStance.Value,
		}

		if err := tx.Model(&Stance{}).
			Where("topic_id = ? AND value >= ?", request.TopicID, addedStance.Value).
			Update("value", gorm.Expr("value + ?", 1)).Error; err != nil {
			log.Printf("❌ Failed to increment values before insert: %v\n", err)
			tx.Rollback()
			http.Error(w, "Failed to increment stances", http.StatusInternalServerError)
			return
		}

		if err := tx.Create(&newStance).Error; err != nil {
			log.Printf("❌ Failed to create stance (text: %s): %v\n", addedStance.Text, err)
			tx.Rollback()
			http.Error(w, "Failed to create stance", http.StatusInternalServerError)
			return
		}
		log.Printf("✅ Created new stance with ID %s\n", newID)
	}

	// ❌ Removed stances
	for _, removedStance := range request.Removed {
		if err := tx.Model(&Stance{}).
			Where("topic_id = ? AND value > ?", request.TopicID, removedStance.Value).
			Update("value", gorm.Expr("value - ?", 1)).Error; err != nil {
			log.Printf("❌ Failed to decrement values after removal of ID %s: %v\n", removedStance.ID, err)
			tx.Rollback()
			http.Error(w, "Failed to decrement stances", http.StatusInternalServerError)
			return
		}

		if err := tx.Delete(&Stance{}, "id = ?", removedStance.ID).Error; err != nil {
			log.Printf("❌ Failed to delete stance ID %s: %v\n", removedStance.ID, err)
			tx.Rollback()
			http.Error(w, "Failed to delete stance", http.StatusInternalServerError)
			return
		}
		log.Printf("✅ Deleted stance ID %s\n", removedStance.ID)
	}

	if err := tx.Commit().Error; err != nil {
		log.Printf("❌ Transaction commit failed: %v\n", err)
		http.Error(w, "Transaction commit failed", http.StatusInternalServerError)
		return
	}

	log.Printf("✅ Transaction committed successfully for topic %s\n", request.TopicID)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "Stances updated successfully")
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

func ContextHandler(w http.ResponseWriter, r *http.Request) {

	var request struct {
		UserID   	string    `json:"user_id"`
		TopicID		string    `json:"topic_id"`
		Reasoning	string	  `json:"reasoning"`
		Sources		[]string  `json:"sources"`
	}
	
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Verify user making request has Admin role
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

	var user auth.User
	err = db.DB.Where("user_id = ? AND role = ?", session.UserID, "admin").Take(&user).Error
	if err != nil {
		http.Error(w, "Invalid permissions. Admin only", http.StatusUnauthorized)
		return
	}
	// Verify user is empowered
	var empoweredUser auth.User
	err = db.DB.Where("user_id = ? AND account_type = ?", request.UserID, "empowered").Take(&empoweredUser).Error
	if err != nil {
		http.Error(w, "User not empowered.", http.StatusUnauthorized)
		return
	}


	var existing Context
	err = db.DB.Where("user_id = ? AND topic_id = ?", request.UserID, request.TopicID).First(&existing).Error

	if err == nil {
		// If no error, context already exists, update it
		update := Context{Reasoning: request.Reasoning, Sources: request.Sources}
		err = db.DB.Model(&existing).Select("Reasoning", "Sources").Updates(update).Error
		if err != nil {
			http.Error(w, "Failed to update context", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "Context updated successfully")
		return
	} 

	if err == gorm.ErrRecordNotFound {
		newContext := Context{
			ID:      	uuid.NewString(),
			UserID:  	request.UserID,
			TopicID: 	request.TopicID,
			Reasoning:  request.Reasoning,
			Sources:    request.Sources,
		}
		if err = db.DB.Create(&newContext).Error; err != nil {
			http.Error(w, "Failed to create context", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(newContext)
		return
	}
	http.Error(w, "DB error", http.StatusInternalServerError)
}

func GetContextHandler(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	topicID := r.URL.Query().Get("topic_id")
	var contexts []Context

	if userID == "" || topicID == "" {
		err := db.DB.Find(&contexts).Error
		if err != nil {
			http.Error(w, "Failed to return context", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(contexts)
		return
	}


	var ctx Context
	err := db.DB.Where("user_id = ? AND topic_id = ?", userID, topicID).First(&ctx).Error
	if err != nil {
		http.Error(w, "Couldn't find context", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ctx)
}
