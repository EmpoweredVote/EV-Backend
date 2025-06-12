package auth

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/DoyleJ11/auth-system/internal/db"
	"github.com/DoyleJ11/auth-system/internal/utils"
	"golang.org/x/crypto/bcrypt"
)

func RegisterHandler(w http.ResponseWriter, r *http.Request) {
	var user User

		// Only allow POST requests
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	err := json.NewDecoder(r.Body).Decode(&user)
	if err != nil {
		http.Error(w, "Invalid Request Format", http.StatusBadRequest)
		return
	}

	// Check if request has username & password
	if user.Username == ""|| user.Password == "" {
		http.Error(w, "Username and password are required", http.StatusBadRequest)
		return
	}

	// Check if username is taken
	var existing User
	err = db.DB.First(&existing, "username = ?", user.Username).Error
	if err == nil {
		http.Error(w, "Username already taken", http.StatusConflict)
		return
	}

	// Hash password
	hashed, err := bcrypt.GenerateFromPassword([]byte(user.Password), bcrypt.DefaultCost)
	if err != nil {
		http.Error(w, "Server error hashing password", http.StatusInternalServerError)
		return
	}
	user.HashedPassword = string(hashed)
	user.UserID = utils.GenerateUUID()

	// Clear user password
	user.Password = ""

	// Save to DB
	if err := db.DB.Create(&user).Error; err != nil {
		http.Error(w, "Failed to register user", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
	"user_id": user.UserID,
	"username": user.Username,
})
}


func LoginHandler(w http.ResponseWriter, r *http.Request) {
	var user User
	var session Session
	var existing Session

		// Only allow POST requests
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}


	err := json.NewDecoder(r.Body).Decode(&user)
	if err != nil {
		//Handle err
		http.Error(w, "Invalid Data", http.StatusBadRequest)
		return
	}

	// Search for matching username
	err = db.DB.First(&user, "username = ?", user.Username).Error
	if err != nil {
		http.Error(w, "Invalid Credentials", http.StatusUnauthorized)
		return
	}

	// Compare hashed password with plaintext password
	err = bcrypt.CompareHashAndPassword([]byte(user.HashedPassword), []byte(user.Password))
	if err != nil {
		http.Error(w, "Invalid Credentials", http.StatusUnauthorized)
		return
	} 

	// Passwords matched, set cookie
	uuid := utils.GenerateUUID()
	http.SetCookie(w, &http.Cookie{
		Name: "session_id",
		Value: uuid,
		Path: "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   false,
	})

	// Search db to see if session cookie already exists, update DB with new session_id if true
	db.DB.Where("user_id = ?", user.UserID).First(&existing)
	if existing.UserID != "" {
		fmt.Println(existing)
		db.DB.Model(&existing).Updates(Session{
			SessionID: uuid,
			ExpiresAt: time.Now().Add(6 * time.Hour),
		})
	} else {
		session.SessionID = uuid
		session.UserID = user.UserID
		session.ExpiresAt = time.Now().Add(6 * time.Hour)
		db.DB.Create(&session)
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "Login successful")
}

func LogoutHandler(w http.ResponseWriter, r *http.Request) {
	var session Session

	// Only allow POST requests
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get session_id from cookie
	cookie, err := r.Cookie("session_id")
	if err != nil {
		http.Error(w, "Couldn't find cookie", http.StatusUnauthorized)
		return
	}

	// search sessions for session_id
	err = db.DB.First(&session, "session_id = ?", cookie.Value).Error
	if err != nil {
		http.Error(w, "Couldn't find session", http.StatusUnauthorized)
		return
	} else {
		// If no err, delete session record
		db.DB.Delete(&session)

		// Replace the cookie with new expired/empty cookie
		deletedCookie := &http.Cookie{
			Name: "session_id",
			Value: "",
			MaxAge: 0,
			Path: "/",
		}
		http.SetCookie(w, deletedCookie)

		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "Logout successful")
	}
}

type MeResponse struct {
	UserID 		string 	`json:"user_id"`
	Username 	string  `json:"username"`
}

func MeHandler(w http.ResponseWriter, r *http.Request) {
	var user User

	// Only allow GET requests
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := utils.GetUserIDFromContext(r.Context())
	if ok {
		err := db.DB.First(&user, "user_id = ?", userID).Error
		if err != nil {
			http.Error(w, "Couldn't find user", http.StatusNotFound)
			return
		}

		response := MeResponse {
			UserID: userID,
			Username: user.Username,
		}
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)

	} else {
		http.Error(w, "Failed converting ID to string", http.StatusInternalServerError)
		return
	}
}

func UpdatePasswordHandler(w http.ResponseWriter, r *http.Request) {
	// Middleware checks user is logged in (valid session)
	// We ask user to enter current password, hash it & compare to current hashed pass
	// We then take the user's new password, hash it & update it in the user records

	type UpdatePassword struct {
		CurrentPassword string  `json:"current_password"`
		NewPassword 	string 	`json:"new_password"`
	}

	var user User
	var updatepass UpdatePassword
	var session Session

	// Only allow POST requests
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get session cookie
	cookie, err := r.Cookie("session_id")
	if err != nil {
		http.Error(w, "Couldn't find cookie", http.StatusUnauthorized)
		return
	}

	// Search db for matching session_id
	err = db.DB.First(&session, "session_id = ?", cookie.Value).Error
	if err != nil {
		http.Error(w, "Couldn't find session", http.StatusUnauthorized)
		return
	}

	// search users table for a userID matching session userID
	err = db.DB.First(&user, "user_id = ?", session.UserID).Error
	if err != nil {
		http.Error(w, "Couldn't find user", http.StatusUnauthorized)
		return
	}

	// Check we have both old & new password
	err = json.NewDecoder(r.Body).Decode(&updatepass)
	if err != nil {
		http.Error(w, "Current and new password are required", http.StatusBadRequest)
		return
	}

	// Make sure user's current password matches stored hash before updating
	err = bcrypt.CompareHashAndPassword([]byte(user.HashedPassword), []byte(updatepass.CurrentPassword))
	if err != nil {
		http.Error(w, "Invalid current password", http.StatusUnauthorized)
		return
	}

	// Hash new password
	hashed, err := bcrypt.GenerateFromPassword([]byte(updatepass.NewPassword), bcrypt.DefaultCost)
	if err != nil {
		http.Error(w, "Server error hashing password", http.StatusInternalServerError)
		return
	}

	// Update stored hashed_password
	db.DB.Model(&user).Update("hashed_password", string(hashed))

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "Password updated")
}