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
	r.Get("/politicians/{politician_id}/{topic_id}/context", GetPoliticianContext)
	r.Get("/politicians/{politician_id}/answers", GetPoliticianAnswers)

	r.Group(func(r chi.Router) {
		r.Use(middleware.SessionMiddleware(sessionFetcher))
		r.Post("/answers", UserAnswersHandler)
		r.Get("/answers", UserAnswersHandler)
		r.Post("/answers/batch", UserAnswerBatchHandler)
		r.Get("/selected-topics", SelectedTopicsHandler)
		r.Put("/selected-topics", SelectedTopicsHandler)
		r.Post("/politicians/{politician_id}/answers/batch", PoliticianAnswerBatch)
		r.Post("/compare", CompareHandler)
		r.Group(func(r chi.Router) {
			r.Use(middleware.AdminMiddleware(sessionFetcher))
			r.Patch("/topics/update", TopicUpdateHandler)
			r.Post("/topics/create", CreateTopicHandler)
			r.Patch("/stances/update", StancesUpdateHandler)
			r.Patch("/topics/categories/update", UpdateTopicCategoriesHandler)
			r.Post("/politicians/context", PoliticianContextHandler)
			r.Put("/politicians/{politician_id}/answers", UpsertPoliticianAnswers)
			r.Delete("/topics/delete/{id}", DeleteTopicHandler)
		})
	})

	return r
}
