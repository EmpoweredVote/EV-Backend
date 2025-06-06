package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/DoyleJ11/auth-system/db"
	"github.com/DoyleJ11/auth-system/middleware"
	"github.com/DoyleJ11/auth-system/models"
)

func MeHandler(w http.ResponseWriter, r *http.Request) {
	var user models.User
	// var session models.Session

	userID := r.Context().Value(middleware.ContextUserIDKey)
	userIDStr, ok := userID.(string)
	if ok {
		err := db.DB.Preload("Session").First(&user, "user_id = ?", userIDStr).Error
		if err != nil {
			http.Error(w, "Couldn't find user", http.StatusNotFound)
			return
		}
		
		w.Header().Set("Content-Type", "text/json")
		json.NewEncoder(w).Encode(user)

	} else {
		http.Error(w, "Failed converting ID to string", http.StatusInternalServerError)
		return
	}
}