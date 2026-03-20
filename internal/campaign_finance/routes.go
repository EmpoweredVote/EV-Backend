package campaign_finance

import (
	"net/http"

	"github.com/EmpoweredVote/EV-Backend/internal/auth"
	"github.com/EmpoweredVote/EV-Backend/internal/middleware"
	"github.com/go-chi/chi/v5"
)

func Routes(r chi.Router) {
	// Public health check
	r.Get("/campaign-finance/health", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok","schema":"transparent_motivations"}`))
	})

	// Admin-only routes — require valid session + admin role
	sessionFetcher := auth.SessionInfo{}
	r.Group(func(r chi.Router) {
		r.Use(middleware.SessionMiddleware(sessionFetcher))
		r.Use(middleware.AdminMiddleware(sessionFetcher))

		r.Get("/campaign-finance/admin/sources", ListSourcesHandler)
		r.Post("/campaign-finance/admin/sources", CreateSourceHandler)
		r.Put("/campaign-finance/admin/sources/{id}", UpdateSourceHandler)
		r.Delete("/campaign-finance/admin/sources/{id}", DeleteSourceHandler)

		r.Get("/campaign-finance/admin/sources/form", SourcesFormHandler)
		r.Post("/campaign-finance/admin/sources/form", SourcesFormSubmitHandler)
		r.Post("/campaign-finance/admin/sources/form/delete", SourcesFormDeleteHandler)

		r.Post("/campaign-finance/admin/ingest/fec", IngestFECHandler)
	})
}
