package webhooks

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

func SetupRoutes() http.Handler {
	r := chi.NewRouter()

	// Public routes
	r.Post("/framer/volunteer", FramerFormWebhook)

	return r
}
