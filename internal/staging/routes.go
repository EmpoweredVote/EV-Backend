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
		r.Get("/politicians/review-queue", GetPoliticianReviewQueue)
		r.Get("/politicians/{id}", GetPolitician)
		r.Put("/politicians/{id}", UpdatePolitician)

		// Politician review workflow
		r.Post("/politicians/{id}/submit", SubmitPoliticianForReview)
		r.Post("/politicians/{id}/review-approve", ApprovePoliticianReview)
		r.Post("/politicians/{id}/review-reject", RejectPoliticianReview)
		r.Post("/politicians/{id}/edit-resubmit", EditAndResubmitPolitician)
		r.Post("/politicians/{id}/lock", AcquirePoliticianLock)
		r.Delete("/politicians/{id}/lock", ReleasePoliticianLock)

		// Admin-only politician approval (override)
		r.Group(func(r chi.Router) {
			r.Use(middleware.AdminMiddleware(sessionFetcher))
			r.Post("/politicians/{id}/approve", ApprovePolitician)
			r.Post("/politicians/{id}/reject", RejectPolitician)
		})

		// Building photo management
		r.Get("/building-photos/gaps", GetBuildingPhotoGaps)
		r.Get("/building-photos/review-queue", GetBuildingPhotoReviewQueue)
		r.Get("/building-photos", ListBuildingPhotos)
		r.Post("/building-photos", CreateBuildingPhoto)
		r.Get("/building-photos/{id}", GetBuildingPhotoByID)
		r.Put("/building-photos/{id}", UpdateBuildingPhoto)
		r.Post("/building-photos/{id}/submit", SubmitBuildingPhotoForReview)
		r.Post("/building-photos/{id}/review-approve", ApproveBuildingPhotoReview)
		r.Post("/building-photos/{id}/review-reject", RejectBuildingPhotoReview)

		// Admin-only building photo approval (skip peer review)
		r.Group(func(r chi.Router) {
			r.Use(middleware.AdminMiddleware(sessionFetcher))
			r.Post("/building-photos/{id}/approve", AdminApproveBuildingPhoto)
		})
	})

	return r
}
