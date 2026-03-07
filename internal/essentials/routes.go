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
	r.Get("/politician/{id}", GetPoliticianByID)
	r.Get("/candidates/{zip}", GetCandidatesByZip)
	r.Post("/candidates/search", SearchCandidates)

	// Phase B: Candidacy data endpoints
	r.Get("/politician/{id}/endorsements", GetPoliticianEndorsements)
	r.Get("/politician/{id}/stances", GetPoliticianStances)
	r.Get("/politician/{id}/elections", GetPoliticianElections)

	// Phase 55: Legislative data endpoints
	r.Get("/politician/{id}/committees", GetPoliticianCommittees)
	r.Get("/politician/{id}/leadership", GetPoliticianLeadership)

	// Phase 56: Bills, votes, and legislative summary endpoints
	r.Get("/politician/{id}/bills", GetPoliticianBills)
	r.Get("/politician/{id}/votes", GetPoliticianVotes)
	r.Get("/politician/{id}/legislative-summary", GetPoliticianLegislativeSummary)

	// Read & Rank quotes endpoint
	r.Get("/quotes", GetQuotes)

	// Building photos
	r.Get("/cities/{geo_id}/building-photo", GetBuildingPhoto)

	// Admin routes - require authentication
	r.Group(func(r chi.Router) {
		r.Use(middleware.SessionMiddleware(sessionFetcher))
		r.Use(middleware.AdminMiddleware(sessionFetcher))

		r.Post("/admin/import", StartBulkImport)
		r.Get("/admin/import/{jobID}", GetImportStatus)
		r.Get("/admin/import", ListImportJobs)

		// Position descriptions
		r.Get("/admin/position-descriptions", ListPositionDescriptions)
		r.Post("/admin/position-descriptions", UpsertPositionDescription)
		r.Delete("/admin/position-descriptions/{id}", DeletePositionDescription)
	})

	return r
}
