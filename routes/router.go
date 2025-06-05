package routes

import (
	"net/http"

	"github.com/DoyleJ11/auth-system/handlers"
)

func SetupRoutes() {
	http.HandleFunc("/", handlers.RootHandler)
	http.HandleFunc("/login", handlers.LoginHandler)
	http.HandleFunc("/register", handlers.RegisterHandler)
}