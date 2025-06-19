package compass

import (
	"net/http"

	"github.com/EmpoweredVote/EV-Backend/internal/middleware"
	"github.com/go-chi/chi/v5"
)

func SetupRoutes() http.Handler {
	r := chi.NewRouter()
	sessionFetcher := SessionInfo{}

	r.Get("/topics", TopicHandler)
	r.Get("/topics/batch", TopicBatchHandler)
	r.Get("/categories", CategoryHandler)

	r.Group(func(r chi.Router) {
		r.Use(middleware.SessionMiddleware(sessionFetcher))
		r.Post("/answers", AnswerHandler)
		r.Get("/answers", AnswerHandler)
		r.Post("/answers/batch", AnswerBatchHander)
		r.Post("/compare", CompareHandler)
	})

	return r
}