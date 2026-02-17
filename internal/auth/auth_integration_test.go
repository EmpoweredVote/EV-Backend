package auth_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/auth"
	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/EmpoweredVote/EV-Backend/internal/middleware"
	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"golang.org/x/crypto/bcrypt"
)

// dbAvailable tracks whether the database connection was established.
var dbAvailable bool

// testServer is the shared httptest server for all integration tests.
var testServer *httptest.Server

func TestMain(m *testing.M) {
	// Load .env.local relative to the EV-Backend root (two directories up from internal/auth/).
	_ = godotenv.Load("../../.env.local")

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		// No database available — skip all integration tests gracefully.
		os.Exit(m.Run())
	}

	// Force local dev cookie mode so cookies work over plain HTTP (httptest uses HTTP).
	// Clearing PORT causes sessionCookie() to use Secure=false, SameSite=Lax.
	os.Setenv("PORT", "")

	db.Connect()
	dbAvailable = true

	// Set up auth tables (idempotent).
	auth.Init()

	// Mount auth routes on a Chi router, matching production setup in main.go.
	r := chi.NewRouter()
	r.Use(chimiddleware.Logger)
	r.Use(middleware.CORSMiddleware)
	r.Mount("/auth", auth.SetupRoutes())

	testServer = httptest.NewServer(r)
	defer testServer.Close()

	os.Exit(m.Run())
}

// createTestUser inserts a unique user into the database and registers a cleanup
// function to remove it. Returns the username and plaintext password.
func createTestUser(t *testing.T) (username, password string) {
	t.Helper()
	if !dbAvailable {
		t.Skip("skipping integration test (requires DATABASE_URL)")
	}

	username = fmt.Sprintf("testuser_%s", uuid.New().String()[:8])
	password = "TestPass123!"
	hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("bcrypt error: %v", err)
	}

	user := auth.User{
		UserID:         uuid.New().String(),
		Username:       username,
		HashedPassword: string(hashed),
	}
	if err := db.DB.Create(&user).Error; err != nil {
		t.Fatalf("failed to create test user: %v", err)
	}

	t.Cleanup(func() {
		db.DB.Where("user_id = ?", user.UserID).Delete(&auth.Session{})
		db.DB.Where("user_id = ?", user.UserID).Delete(&auth.User{})
	})

	return username, password
}

// newClientWithJar returns an http.Client with a fresh cookie jar that automatically
// carries cookies between requests.
func newClientWithJar(t *testing.T) *http.Client {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar.New: %v", err)
	}
	return &http.Client{Jar: jar}
}

// loginUser posts to /auth/login and returns the response. The client's cookie jar
// is populated with the session_id cookie on success.
func loginUser(t *testing.T, client *http.Client, username, password string) *http.Response {
	t.Helper()
	body, _ := json.Marshal(map[string]string{
		"username": username,
		"password": password,
	})
	resp, err := client.Post(testServer.URL+"/auth/login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /auth/login: %v", err)
	}
	return resp
}

// readBody reads and returns the response body as a string, draining and closing it.
func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}

// TestLoginReturnsSessionCookie verifies that POST /auth/login with valid credentials
// returns 200, a Set-Cookie header containing session_id, and a JSON body with user_id
// and username.
func TestLoginReturnsSessionCookie(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test (requires DATABASE_URL)")
	}
	username, password := createTestUser(t)
	client := newClientWithJar(t)

	resp := loginUser(t, client, username, password)
	body := readBody(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", resp.StatusCode, body)
	}

	// Check Set-Cookie header contains session_id.
	setCookie := resp.Header.Get("Set-Cookie")
	if !strings.Contains(setCookie, "session_id") {
		t.Errorf("expected Set-Cookie to contain 'session_id', got: %q", setCookie)
	}

	// Check JSON body contains user_id and username.
	var result map[string]string
	if err := json.Unmarshal([]byte(body), &result); err != nil {
		t.Fatalf("invalid JSON body: %s", body)
	}
	if result["user_id"] == "" {
		t.Error("expected user_id in response body")
	}
	if result["username"] != username {
		t.Errorf("expected username %q, got %q", username, result["username"])
	}
}

// TestSessionPersistsAcrossRequests verifies that after login, GET /auth/me returns 200
// with the correct user data when the same cookie-jar client is used. This confirms
// the session cookie is stored and sent automatically on subsequent requests.
func TestSessionPersistsAcrossRequests(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test (requires DATABASE_URL)")
	}
	username, password := createTestUser(t)
	client := newClientWithJar(t)

	loginResp := loginUser(t, client, username, password)
	loginBody := readBody(t, loginResp)
	if loginResp.StatusCode != http.StatusOK {
		t.Fatalf("login failed: %d %s", loginResp.StatusCode, loginBody)
	}

	// GET /auth/me — cookie jar carries session_id automatically.
	meResp, err := client.Get(testServer.URL + "/auth/me")
	if err != nil {
		t.Fatalf("GET /auth/me: %v", err)
	}
	meBody := readBody(t, meResp)

	if meResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /auth/me, got %d; body: %s", meResp.StatusCode, meBody)
	}

	var me map[string]interface{}
	if err := json.Unmarshal([]byte(meBody), &me); err != nil {
		t.Fatalf("invalid JSON body: %s", meBody)
	}
	if me["username"] != username {
		t.Errorf("expected username %q from /auth/me, got %q", username, me["username"])
	}
}

// TestLogoutClearsSession verifies the full logout flow: login, logout, then /auth/me
// returns 401. This confirms the session is deleted from the database on logout.
func TestLogoutClearsSession(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test (requires DATABASE_URL)")
	}
	username, password := createTestUser(t)
	client := newClientWithJar(t)

	// Login.
	loginResp := loginUser(t, client, username, password)
	loginBody := readBody(t, loginResp)
	if loginResp.StatusCode != http.StatusOK {
		t.Fatalf("login failed: %d %s", loginResp.StatusCode, loginBody)
	}

	// Logout.
	logoutResp, err := client.Post(testServer.URL+"/auth/logout", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /auth/logout: %v", err)
	}
	logoutBody := readBody(t, logoutResp)
	if logoutResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /auth/logout, got %d; body: %s", logoutResp.StatusCode, logoutBody)
	}

	// /auth/me should now return 401.
	meResp, err := client.Get(testServer.URL + "/auth/me")
	if err != nil {
		t.Fatalf("GET /auth/me after logout: %v", err)
	}
	meBody := readBody(t, meResp)

	if meResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 from /auth/me after logout, got %d; body: %s", meResp.StatusCode, meBody)
	}
}

// TestTabReloadPreservesLogin verifies that multiple GET /auth/me calls with the same
// cookie-jar client all return 200. This simulates a browser tab reload and confirms
// the session is stable across multiple requests.
func TestTabReloadPreservesLogin(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test (requires DATABASE_URL)")
	}
	username, password := createTestUser(t)
	client := newClientWithJar(t)

	// Login.
	loginResp := loginUser(t, client, username, password)
	loginBody := readBody(t, loginResp)
	if loginResp.StatusCode != http.StatusOK {
		t.Fatalf("login failed: %d %s", loginResp.StatusCode, loginBody)
	}

	// First /auth/me call (initial load).
	meResp1, err := client.Get(testServer.URL + "/auth/me")
	if err != nil {
		t.Fatalf("GET /auth/me (first): %v", err)
	}
	body1 := readBody(t, meResp1)
	if meResp1.StatusCode != http.StatusOK {
		t.Errorf("expected 200 from first /auth/me, got %d; body: %s", meResp1.StatusCode, body1)
	}

	// Second /auth/me call (simulates tab reload).
	meResp2, err := client.Get(testServer.URL + "/auth/me")
	if err != nil {
		t.Fatalf("GET /auth/me (second): %v", err)
	}
	body2 := readBody(t, meResp2)
	if meResp2.StatusCode != http.StatusOK {
		t.Errorf("expected 200 from second /auth/me, got %d; body: %s", meResp2.StatusCode, body2)
	}
}

// TestExpiredSessionRejected verifies that a session manually expired in the database
// is rejected with 401 and the body contains "Session expired".
func TestExpiredSessionRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test (requires DATABASE_URL)")
	}
	username, password := createTestUser(t)
	client := newClientWithJar(t)

	// Login to get a valid session.
	loginResp := loginUser(t, client, username, password)
	loginBody := readBody(t, loginResp)
	if loginResp.StatusCode != http.StatusOK {
		t.Fatalf("login failed: %d %s", loginResp.StatusCode, loginBody)
	}

	// Retrieve user_id from login body to target the correct session.
	var loginResult map[string]string
	if err := json.Unmarshal([]byte(loginBody), &loginResult); err != nil {
		t.Fatalf("invalid login response JSON: %s", loginBody)
	}
	userID := loginResult["user_id"]

	// Manually expire the session in the database.
	if err := db.DB.Model(&auth.Session{}).
		Where("user_id = ?", userID).
		Update("expires_at", time.Now().Add(-1*time.Hour)).Error; err != nil {
		t.Fatalf("failed to expire session: %v", err)
	}

	// GET /auth/me should now return 401 with "Session expired".
	meResp, err := client.Get(testServer.URL + "/auth/me")
	if err != nil {
		t.Fatalf("GET /auth/me after expiry: %v", err)
	}
	meBody := readBody(t, meResp)

	if meResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 from /auth/me with expired session, got %d; body: %s", meResp.StatusCode, meBody)
	}
	if !strings.Contains(meBody, "Session expired") {
		t.Errorf("expected body to contain %q, got: %q", "Session expired", meBody)
	}
}
