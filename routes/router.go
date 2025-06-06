package routes

import (
	"net/http"

	"github.com/DoyleJ11/auth-system/handlers"
	"github.com/DoyleJ11/auth-system/middleware"
)

func SetupRoutes() {
	http.HandleFunc("/", handlers.RootHandler)
	http.HandleFunc("/login", handlers.LoginHandler)
	http.HandleFunc("/register", handlers.RegisterHandler)
	http.Handle("/logout", middleware.SessionMiddleware(http.HandlerFunc(handlers.LogoutHandler)))
	http.Handle("/me", middleware.SessionMiddleware(http.HandlerFunc(handlers.MeHandler)))
}