package essentials

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

func SetupRoutes() http.Handler {
	r := chi.NewRouter()

	// Public routes
	r.Get("/officials/{zip}", GetOfficialsByZip)

	return r
}
