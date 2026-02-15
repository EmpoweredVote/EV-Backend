package staging

import (
	"net/http"

	"github.com/EmpoweredVote/EV-Backend/internal/middleware"
	"github.com/go-chi/chi/v5"
)

func SetupRoutes() http.Handler {
	r := chi.NewRouter()
	sessionFetcher := SessionInfo{}

	// All staging routes require authentication
	r.Group(func(r chi.Router) {
		r.Use(middleware.SessionMiddleware(sessionFetcher))

		// Data retrieval
		r.Get("/data", GetAllData)

		// Stance management
		r.Get("/stances", ListStances)
		r.Get("/stances/review-queue", GetReviewQueue)
		r.Post("/stances", CreateStance)
		r.Get("/stances/{id}", GetStance)
		r.Put("/stances/{id}", UpdateStance)

		// Stance workflow
		r.Post("/stances/{id}/submit", SubmitForReview)
		r.Post("/stances/{id}/approve", ApproveStance)
		r.Post("/stances/{id}/reject", RejectStance)
		r.Post("/stances/{id}/edit-resubmit", EditAndResubmit)

		// Locking
		r.Post("/stances/{id}/lock", AcquireLock)
		r.Delete("/stances/{id}/lock", ReleaseLock)

		// Politician management
		r.Get("/politicians", ListPoliticians)
		r.Post("/politicians", CreatePolitician)
		r.Get("/politicians/{id}", GetPolitician)
		r.Put("/politicians/{id}", UpdatePolitician)

		// Admin-only politician approval
		r.Group(func(r chi.Router) {
			r.Use(middleware.AdminMiddleware(sessionFetcher))
			r.Post("/politicians/{id}/approve", ApprovePolitician)
			r.Post("/politicians/{id}/reject", RejectPolitician)
		})
	})

	return r
}
