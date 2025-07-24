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
	r.Post("/topics/batch", TopicBatchHandler)
	r.Get("/categories", CategoryHandler)
	r.Get("/context", GetContextHandler)

	r.Group(func(r chi.Router) {
		r.Use(middleware.SessionMiddleware(sessionFetcher))
		r.Post("/answers", AnswerHandler)
		r.Get("/answers", AnswerHandler)
		r.Post("/answers/batch", AnswerBatchHander)
		r.Post("/compare", CompareHandler)
		r.Patch("/topics/update", TopicUpdateHandler)
		r.Post("/topics/create", CreateTopicHandler)
		r.Patch("/stances/update", StancesUpdateHandler)
		r.Patch("/topics/categories/update", UpdateTopicCategoriesHandler)
		r.Post("/context", ContextHandler)
		r.Post("/answers/dummy", PopulateDummyAnswers)
		r.Post("/answers/admin", UpdateAnswerHandler)
	})

	return r
}