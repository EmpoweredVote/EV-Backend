package essentials

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

func SetupRoutes() http.Handler {
	r := chi.NewRouter()

	// Public routes
	r.Get("/politicians", GetAllPoliticians)
	r.Get("/politicians/{zip}", GetPoliticiansByZip)
	r.Post("/politicians/search", SearchPoliticians)
	r.Get("/politician/{id}", GetPoliticianByID)

	// Phase B: Candidacy data endpoints
	r.Get("/politician/{id}/endorsements", GetPoliticianEndorsements)
	r.Get("/politician/{id}/stances", GetPoliticianStances)
	r.Get("/politician/{id}/elections", GetPoliticianElections)

	return r
}
