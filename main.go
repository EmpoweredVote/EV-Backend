package main

import (
	"fmt"
	"net/http"

	"github.com/DoyleJ11/auth-system/db"
	"github.com/DoyleJ11/auth-system/routes"
	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load()
	db.Connect()
	routes.SetupRoutes()

	fmt.Println("Server listening on port :5050...")
	http.ListenAndServe(":5050", nil)
}