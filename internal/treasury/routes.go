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
	r.Get("/cities", ListCities)
	r.Get("/cities/{city_id}", GetCity)
	r.Get("/budgets", ListBudgets)
	r.Get("/budgets/{budget_id}", GetBudget)
	r.Get("/budgets/{budget_id}/categories", GetBudgetCategories)

	// Admin routes - require authentication
	r.Group(func(r chi.Router) {
		r.Use(middleware.SessionMiddleware(sessionFetcher))
		r.Use(middleware.AdminMiddleware(sessionFetcher))

		r.Post("/cities", CreateCity)
		r.Put("/cities/{city_id}", UpdateCity)
		r.Delete("/cities/{city_id}", DeleteCity)

		r.Post("/budgets", CreateBudget)
		r.Post("/budgets/import", ImportBudget)
		r.Put("/budgets/{budget_id}", UpdateBudget)
		r.Delete("/budgets/{budget_id}", DeleteBudget)
	})

	return r
}
