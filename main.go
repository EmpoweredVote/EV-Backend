package main

import (
	"fmt"
	"net/http"

	"github.com/DoyleJ11/auth-system/internal/auth"
	"github.com/DoyleJ11/auth-system/internal/compass"
	"github.com/DoyleJ11/auth-system/internal/db"
	"github.com/joho/godotenv"
)

func RootHandler(w http.ResponseWriter, r *http.Request) {
	response := "Server is up!"
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintln(w, response)
}

func main() {
	godotenv.Load()
	db.Connect()

	auth.Init()
	compass.Init()

	http.HandleFunc("/", RootHandler)
	auth.SetupRoutes()
	compass.SetupRoutes()

	fmt.Println("Server listening on port :5050...")
	http.ListenAndServe(":5050", nil)
}