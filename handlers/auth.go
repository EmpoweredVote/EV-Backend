package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/DoyleJ11/auth-system/db"
	"github.com/DoyleJ11/auth-system/models"
	"github.com/DoyleJ11/auth-system/utils"
	"golang.org/x/crypto/bcrypt"
)

func RootHandler(w http.ResponseWriter, r *http.Request) {
	response := "Server is up!"
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintln(w, response)
}

func LoginHandler(w http.ResponseWriter, r *http.Request) {
	var user models.User
	var session models.Session
	var existing models.Session

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
	log.Printf("%+v", user) // Testing only

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
	})

	// Search db to see if session cookie already exists, update DB with new session_id if true
	db.DB.Where("user_id = ?", user.UserID).First(&existing)
	if existing.UserID != "" {
		db.DB.Model(&existing).Update("session_id", uuid)
	} else {
		session.SessionID = uuid
		session.UserID = user.UserID
		db.DB.Create(&session)
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "Login successful")
}

func RegisterHandler(w http.ResponseWriter, r *http.Request) {
	var user models.User

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
	var existing models.User
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

func LogoutHandler(w http.ResponseWriter, r *http.Request) {
	

}