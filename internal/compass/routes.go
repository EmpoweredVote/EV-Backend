package compass

import (
	"net/http"

	"github.com/DoyleJ11/auth-system/internal/middleware"
)

func SetupRoutes() {
	sessionFetcher := SessionInfo{}

	http.Handle("/topics", middleware.SessionMiddleware(sessionFetcher)(http.HandlerFunc(TopicHandler)))
	http.Handle("/answers", middleware.SessionMiddleware(sessionFetcher)(http.HandlerFunc(AnswerHandler)))
	http.Handle("/compare", middleware.SessionMiddleware(sessionFetcher)(http.HandlerFunc(CompareHandler)))
}