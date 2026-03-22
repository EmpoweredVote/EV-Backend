package treasury

import (
	"net/http"

	"github.com/EmpoweredVote/EV-Backend/internal/middleware"
	"github.com/go-chi/chi/v5"
)

func SetupRoutes() http.Handler {
	r := chi.NewRouter()
	sessionFetcher := SessionInfo{}

	// Public routes - read-only access to budget data
	r.Get("/municipalities", ListMunicipalities)
	r.Get("/municipalities/{municipality_id}", GetMunicipality)
	r.Get("/budgets", ListBudgets)
	r.Get("/budgets/{budget_id}", GetBudget)
	r.Get("/budgets/{budget_id}/categories", GetBudgetCategories)

	// Admin routes - require authentication
	r.Group(func(r chi.Router) {
		r.Use(middleware.SessionMiddleware(sessionFetcher))
		r.Use(middleware.AdminMiddleware(sessionFetcher))

		r.Post("/municipalities", CreateMunicipality)
		r.Put("/municipalities/{municipality_id}", UpdateMunicipality)
		r.Delete("/municipalities/{municipality_id}", DeleteMunicipality)

		r.Post("/budgets", CreateBudget)
		r.Post("/budgets/import", ImportBudget)
		r.Put("/budgets/{budget_id}", UpdateBudget)
		r.Delete("/budgets/{budget_id}", DeleteBudget)
	})

	return r
}
