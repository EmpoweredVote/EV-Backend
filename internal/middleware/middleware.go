package middleware

import (
	"context"
	"net/http"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/utils"
)

type SessionFetcher interface {
	FindSessionByID(id string) (utils.SessionData, error)
}

func SessionMiddleware(fetcher SessionFetcher) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cookie, err := r.Cookie("session_id")
			if err != nil {
				http.Error(w, "Couldn't find cookie", http.StatusUnauthorized)
				return
			}

			session, err := fetcher.FindSessionByID(cookie.Value)
			if err != nil {
				http.Error(w, "Couldn't find session", http.StatusUnauthorized)
				return
			}

			if session.ExpiresAt.Before(time.Now()) {
				http.Error(w, "Session expired", http.StatusUnauthorized)
				return
			}

			ctx := context.WithValue(r.Context(), utils.ContextUserIDKey, session.UserID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

var allowed = map[string]struct{}{
	"http://localhost:5173":              {},
	"https://empoweredvote.github.io":   {},
	"https://ev-backend-edhm.onrender.com": {},
}

func CORSMiddleware(next http.Handler) (http.Handler) {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")

		// Echo the origin back only if it’s on our allow-list
		if _, ok := allowed[origin]; ok {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin") // important for caches
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Access-Control-Allow-Methods",
				"GET, POST, PUT, PATCH, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers",
				"Content-Type, Authorization")
		}

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}