package auth

import (
	"net/http"

	"github.com/DoyleJ11/auth-system/internal/middleware"
)

func SetupRoutes() {
	sessionFetcher := SessionInfo{}

	http.HandleFunc("/login", LoginHandler)
	http.HandleFunc("/register", RegisterHandler)
	http.Handle("/logout", middleware.SessionMiddleware(sessionFetcher)(http.HandlerFunc(LogoutHandler)))
	http.Handle("/me", middleware.SessionMiddleware(sessionFetcher)(http.HandlerFunc(MeHandler)))
}