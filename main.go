package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/EmpoweredVote/EV-Backend/internal/auth"
	"github.com/EmpoweredVote/EV-Backend/internal/compass"
	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/EmpoweredVote/EV-Backend/internal/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/joho/godotenv"
)

func RootHandler(w http.ResponseWriter, r *http.Request) {
	response := "Server is up!"
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintln(w, response)
}

func main() {
	_ = godotenv.Load(".env.local")
	db.Connect()

	port := os.Getenv("PORT")
	if port == "" {
		port = "5050"
	}


	auth.Init()
	compass.Init()
	r := chi.NewRouter()
	r.Use(middleware.CORSMiddleware)
	r.Get("/", RootHandler)
	
	r.Mount("/auth", auth.SetupRoutes())
	r.Mount("/compass", compass.SetupRoutes())

	fmt.Println("Server listening on port :5050...")

	http.ListenAndServe("0.0.0.0:" + port, r)
}