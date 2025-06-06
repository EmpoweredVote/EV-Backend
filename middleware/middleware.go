package middleware

import (
	"context"
	"net/http"
	"time"

	"github.com/DoyleJ11/auth-system/db"
	"github.com/DoyleJ11/auth-system/models"
)

type contextKey string
const ContextUserIDKey contextKey = "userID"

func SessionMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var session models.Session

		cookie, err := r.Cookie("session_id")
		if err != nil {
			http.Error(w, "Couldn't find cookie", http.StatusUnauthorized)
			return
		}

		if cookie.Value == "" {
			http.Error(w, "Invalid Session", http.StatusUnauthorized)
			return
		}

		err = db.DB.First(&session, "session_id = ?", cookie.Value).Error
		if err != nil {
			http.Error(w, "Couldn't find session", http.StatusUnauthorized)
			return
		} else if session.ExpiresAt.Before(time.Now()) {
			http.Error(w, "Session expired", http.StatusUnauthorized)
			return
		} else {
			ctx := context.WithValue(r.Context(), ContextUserIDKey, session.UserID)

			next.ServeHTTP(w, r.WithContext(ctx))
		}
	})
}