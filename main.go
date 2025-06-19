package main

import (
	"fmt"
	"log"
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
	godotenv.Load()
	db.Connect()

	auth.Init()
	compass.Init()
	r := chi.NewRouter()
	r.Use(middleware.CORSMiddleware)
	r.Get("/", RootHandler)
	
	r.Mount("/auth", auth.SetupRoutes())
	r.Mount("/compass", compass.SetupRoutes())

	fmt.Println("Server listening on port :5050...")
	port := os.Getenv("PORT")
	if port == "" {
		log.Fatal("PORT env not set")
	}
	http.ListenAndServe("0.0.0.0:" + port, r)
}