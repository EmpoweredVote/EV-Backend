package meetings

import (
	"net/http"

	"github.com/EmpoweredVote/EV-Backend/internal/middleware"
	"github.com/go-chi/chi/v5"
)

func SetupRoutes() http.Handler {
	r := chi.NewRouter()
	sessionFetcher := SessionInfo{}

	// Public routes - read-only access to meeting data
	r.Get("/", ListMeetings)
	r.Get("/{meeting_id}", GetMeeting)
	r.Get("/{meeting_id}/transcript", GetTranscript)
	r.Get("/{meeting_id}/speakers", GetSpeakers)
	r.Get("/{meeting_id}/summary", GetSummary)
	r.Get("/{meeting_id}/votes", GetVotes)
	r.Get("/search", SearchTranscripts)

	// Admin routes - import and manage meeting data
	r.Group(func(r chi.Router) {
		r.Use(middleware.SessionMiddleware(sessionFetcher))
		r.Use(middleware.AdminMiddleware(sessionFetcher))

		r.Post("/import", ImportMeeting)
		r.Delete("/{meeting_id}", DeleteMeeting)
	})

	return r
}
