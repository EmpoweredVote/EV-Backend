package compass

import (
	"net/http"

	"github.com/DoyleJ11/auth-system/internal/middleware"
	"github.com/go-chi/chi/v5"
)

func SetupRoutes() http.Handler {
	r := chi.NewRouter()
	sessionFetcher := SessionInfo{}

	r.Get("/topics", TopicHandler)
	r.Get("/topics/batch", TopicBatchHandler)

	r.Group(func(r chi.Router) {
		r.Use(middleware.SessionMiddleware(sessionFetcher))
		r.Post("/answers", AnswerHandler)
		r.Get("/answers", AnswerHandler)
		r.Get("/compare", CompareHandler)
	})

	return r
}