package essentials

import (
	"net/http"

	"github.com/EmpoweredVote/EV-Backend/internal/middleware"
	"github.com/go-chi/chi/v5"
)

func SetupRoutes() http.Handler {
	r := chi.NewRouter()
	sessionFetcher := SessionInfo{}

	// Public routes
	r.Get("/politicians", GetAllPoliticians)
	r.Get("/politicians/{zip}", GetPoliticiansByZip)
	r.Post("/politicians/search", SearchPoliticians)
	r.Get("/cache-status/{zip}", GetCacheStatus)
	r.Get("/politician/{id}", GetPoliticianByID)

	// Phase B: Candidacy data endpoints
	r.Get("/politician/{id}/endorsements", GetPoliticianEndorsements)
	r.Get("/politician/{id}/stances", GetPoliticianStances)
	r.Get("/politician/{id}/elections", GetPoliticianElections)

	// Admin routes - require authentication
	r.Group(func(r chi.Router) {
		r.Use(middleware.SessionMiddleware(sessionFetcher))
		r.Use(middleware.AdminMiddleware(sessionFetcher))

		r.Post("/admin/import", StartBulkImport)
		r.Get("/admin/import/{jobID}", GetImportStatus)
		r.Get("/admin/import", ListImportJobs)
	})

	return r
}
