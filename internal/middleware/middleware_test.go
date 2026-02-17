package middleware_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/middleware"
	"github.com/EmpoweredVote/EV-Backend/internal/utils"
)

// mockFetcher implements middleware.SessionFetcher without any database dependency.
type mockFetcher struct {
	session utils.SessionData
	err     error
}

func (m mockFetcher) FindSessionByID(id string) (utils.SessionData, error) {
	return m.session, m.err
}

// callWithCookie wraps a simple 200-OK inner handler in the provided middleware,
// optionally setting one cookie on the request, and returns the recorded response.
func callWithCookie(t *testing.T, mw func(http.Handler) http.Handler, cookieName, cookieValue string) *httptest.ResponseRecorder {
	t.Helper()

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := mw(inner)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	if cookieName != "" {
		req.AddCookie(&http.Cookie{Name: cookieName, Value: cookieValue})
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// TestSessionMiddleware_MissingCookie verifies that a request with no session_id
// cookie receives a 401 response.
func TestSessionMiddleware_MissingCookie(t *testing.T) {
	fetcher := mockFetcher{}
	mw := middleware.SessionMiddleware(fetcher)

	rec := callWithCookie(t, mw, "", "")

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

// TestSessionMiddleware_ExpiredSession verifies that a request with a valid session_id
// cookie but an expired session receives a 401 response containing "Session expired".
func TestSessionMiddleware_ExpiredSession(t *testing.T) {
	fetcher := mockFetcher{
		session: utils.SessionData{
			UserID:    "some-user",
			ExpiresAt: time.Now().Add(-1 * time.Hour), // 1 hour in the past
		},
		err: nil,
	}
	mw := middleware.SessionMiddleware(fetcher)

	rec := callWithCookie(t, mw, "session_id", "expired-session-id")

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "Session expired") {
		t.Errorf("expected body to contain %q, got: %q", "Session expired", body)
	}
}

// TestSessionMiddleware_FetcherError verifies that a fetcher error (e.g. session not found)
// results in a 401 response.
func TestSessionMiddleware_FetcherError(t *testing.T) {
	fetcher := mockFetcher{
		session: utils.SessionData{},
		err:     errors.New("session not found"),
	}
	mw := middleware.SessionMiddleware(fetcher)

	rec := callWithCookie(t, mw, "session_id", "nonexistent-session-id")

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

// TestSessionMiddleware_ValidSession verifies that a request with a valid, non-expired
// session receives a 200 response and that the userID is injected into the context.
func TestSessionMiddleware_ValidSession(t *testing.T) {
	const wantUserID = "test-user-123"

	fetcher := mockFetcher{
		session: utils.SessionData{
			UserID:    wantUserID,
			ExpiresAt: time.Now().Add(1 * time.Hour), // 1 hour in the future
		},
		err: nil,
	}

	// inner handler reads and echoes the userID from context
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUserID, ok := utils.GetUserIDFromContext(r.Context())
		if !ok {
			http.Error(w, "userID not in context", http.StatusInternalServerError)
			return
		}
		if gotUserID != wantUserID {
			http.Error(w, "wrong userID in context: "+gotUserID, http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	mw := middleware.SessionMiddleware(fetcher)
	handler := mw(inner)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.AddCookie(&http.Cookie{Name: "session_id", Value: "valid-session-id"})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

// TestAdminMiddleware_MissingUserID verifies that AdminMiddleware returns 401
// when no userID is present in the request context (i.e. SessionMiddleware did not run
// or injected nothing). This test does not require a database connection.
func TestAdminMiddleware_MissingUserID(t *testing.T) {
	// Pass a zero-value mockFetcher — AdminMiddleware doesn't use the fetcher
	// for the missing-userID path.
	mw := middleware.AdminMiddleware(mockFetcher{})

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := mw(inner)
	req := httptest.NewRequest(http.MethodGet, "/admin", nil)
	// Deliberately no userID in context.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "missing user ID") {
		t.Errorf("expected body to contain %q, got: %q", "missing user ID", body)
	}
}
