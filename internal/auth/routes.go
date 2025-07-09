package auth

import (
	"net/http"

	"github.com/EmpoweredVote/EV-Backend/internal/middleware"
	"github.com/go-chi/chi/v5"
)

func SetupRoutes() http.Handler {
	r := chi.NewRouter()
	sessionFetcher := SessionInfo{}

	// Public routes
	r.Post("/login", LoginHandler)
	r.Post("/register", RegisterHandler)

	// Protected routes
	r.Group(func(r chi.Router) {
		r.Use(middleware.SessionMiddleware(sessionFetcher))
		r.Get("/me", MeHandler)
		r.Post("/update-password", UpdatePasswordHandler)
		r.Post("/logout", LogoutHandler)
		r.Get("/empowered-accounts", EmpoweredAccountHandler)
		r.Get("/admin", AdminCheckHandler)

		r.With(middleware.AdminMiddleware(sessionFetcher)).Get("/admin", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Admin access granted"))
		})
	})

	return r
}