package essentials

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/time/rate"
)

const legiScanMonthlyLimit = 30_000

// LegiScanClient provides rate-limited access to the LegiScan API with a
// monthly query budget enforced via a persisted JSON counter.
//
// Rate limits:
//   - Per-second: 3 burst / 1 sustained (via token bucket limiter)
//   - Monthly:    30,000 queries total (reset on new calendar month)
type LegiScanClient struct {
	apiKey     string
	httpClient *http.Client
	limiter    *rate.Limiter
	counter    *monthlyCounter
}

// monthlyCounter tracks API usage for the current calendar month.
// The counter persists to a JSON file and auto-resets when the month rolls over.
type monthlyCounter struct {
	Month   string `json:"month"`   // "2026-03"
	Queries int    `json:"queries"` // total queries this month
	path    string // file path for persistence — NOT serialized
}

// NewLegiScanClient creates a new LegiScanClient with rate limiting and monthly
// budget enforcement. counterPath specifies where to persist the monthly counter
// JSON file. If empty, it defaults to $HOME/.ev-backend/legiscan_counter.json.
func NewLegiScanClient(apiKey string, counterPath string) (*LegiScanClient, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("LegiScan API key is required")
	}

	if counterPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			home = "."
		}
		counterPath = filepath.Join(home, ".ev-backend", "legiscan_counter.json")
	}

	return &LegiScanClient{
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		limiter:    rate.NewLimiter(rate.Every(time.Second), 3), // 3/sec burst, 1/sec sustained
		counter:    loadMonthlyCounter(counterPath),
	}, nil
}

// loadMonthlyCounter reads the counter JSON file from path. If the file does
// not exist or is corrupted, a fresh counter for the current month is returned.
func loadMonthlyCounter(path string) *monthlyCounter {
	counter := &monthlyCounter{
		Month:   currentMonthKey(),
		Queries: 0,
		path:    path,
	}

	data, err := os.ReadFile(path)
	if err != nil {
		// File does not exist or unreadable — start fresh
		return counter
	}

	if err := json.Unmarshal(data, counter); err != nil {
		// Corrupted JSON — start fresh
		counter.Month = currentMonthKey()
		counter.Queries = 0
	}
	counter.path = path

	// Auto-reset on month rollover
	if counter.Month != currentMonthKey() {
		counter.Month = currentMonthKey()
		counter.Queries = 0
	}

	return counter
}

// save writes the counter to its JSON file atomically (write to temp file, rename).
func (c *monthlyCounter) save() error {
	if c.path == "" {
		return nil // no path configured — skip persistence
	}

	// Ensure the directory exists
	dir := filepath.Dir(c.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("creating counter directory %s: %w", dir, err)
	}

	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling counter: %w", err)
	}

	// Write to temp file then rename for atomicity
	tmpPath := c.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("writing counter temp file: %w", err)
	}
	if err := os.Rename(tmpPath, c.path); err != nil {
		return fmt.Errorf("renaming counter file: %w", err)
	}

	return nil
}

// currentMonthKey returns the current month as "YYYY-MM" string.
func currentMonthKey() string {
	return time.Now().UTC().Format("2006-01")
}

// checkAndIncrementBudget verifies monthly budget is not exhausted, handles
// month rollover, increments the counter, and persists it.
func (c *LegiScanClient) checkAndIncrementBudget() error {
	// Month rollover check
	if c.counter.Month != currentMonthKey() {
		c.counter.Month = currentMonthKey()
		c.counter.Queries = 0
	}

	if c.counter.Queries >= legiScanMonthlyLimit {
		return fmt.Errorf("LegiScan monthly budget exhausted: %d/%d queries used in %s",
			c.counter.Queries, legiScanMonthlyLimit, c.counter.Month)
	}

	c.counter.Queries++
	if err := c.counter.save(); err != nil {
		// Log but don't fail — save errors are non-fatal for the API call
		_ = err
	}

	return nil
}

// Query executes a LegiScan API operation with rate limiting and budget enforcement.
//
// op is the LegiScan operation name (e.g., "getRollCall", "getSessionList").
// params is a map of additional query parameters for the operation.
//
// Returns the raw JSON response body on success. LegiScan wraps all responses
// in {"status": "OK", ...} — this method validates the status field.
func (c *LegiScanClient) Query(ctx context.Context, op string, params map[string]string) (json.RawMessage, error) {
	if err := c.checkAndIncrementBudget(); err != nil {
		return nil, err
	}
	if err := c.limiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("rate limiter: %w", err)
	}

	// Build URL: https://api.legiscan.com/?key=KEY&op=OPERATION&param=value
	u, _ := url.Parse("https://api.legiscan.com/")
	q := u.Query()
	q.Set("key", c.apiKey)
	q.Set("op", op)
	for k, v := range params {
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("LegiScan API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("LegiScan API returned %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading LegiScan response: %w", err)
	}

	// LegiScan wraps all responses in {"status":"OK", ...} — parse and check
	var wrapper struct {
		Status string          `json:"status"`
		Alert  json.RawMessage `json:"alert,omitempty"`
	}
	if err := json.Unmarshal(body, &wrapper); err != nil {
		return nil, fmt.Errorf("parsing LegiScan response: %w", err)
	}
	if wrapper.Status != "OK" {
		return nil, fmt.Errorf("LegiScan API error status: %s", wrapper.Status)
	}

	return body, nil
}

// GetBudgetStatus returns the current month's query count and the monthly limit.
// Phase 56 uses this to check remaining budget before starting imports.
func (c *LegiScanClient) GetBudgetStatus() (used int, limit int) {
	// Check for month rollover first
	if c.counter.Month != currentMonthKey() {
		return 0, legiScanMonthlyLimit
	}
	return c.counter.Queries, legiScanMonthlyLimit
}

// RemainingBudget returns the number of queries remaining for this month.
func (c *LegiScanClient) RemainingBudget() int {
	used, limit := c.GetBudgetStatus()
	remaining := limit - used
	if remaining < 0 {
		return 0
	}
	return remaining
}

// ===== LegiScan Response Types (pre-defined for Phase 56 use) =====

// LegiScanRollCall represents the roll call data returned by LegiScan's getRollCall operation.
type LegiScanRollCall struct {
	RollCallID int            `json:"roll_call_id"`
	BillID     int            `json:"bill_id"`
	Date       string         `json:"date"`
	Desc       string         `json:"desc"`
	Yea        int            `json:"yea"`
	Nay        int            `json:"nay"`
	NV         int            `json:"nv"`
	Absent     int            `json:"absent"`
	Total      int            `json:"total"`
	Passed     int            `json:"passed"`
	Chamber    string         `json:"chamber"`
	ChamberID  int            `json:"chamber_id"`
	Votes      []LegiScanVote `json:"votes"`
}

// LegiScanVote represents an individual member's vote within a roll call.
type LegiScanVote struct {
	PeopleID int    `json:"people_id"`
	VoteID   int    `json:"vote_id"`
	VoteText string `json:"vote_text"` // "Yea", "Nay", "NV", "Absent"
}

// LegiScanPerson represents a legislator record from LegiScan.
// NOTE: LegiScan's getPerson does NOT return bioguide_id. Use the bridge table
// (legislative_politician_id_map) for cross-source identity resolution.
type LegiScanPerson struct {
	PeopleID      int    `json:"people_id"`
	StateID       int    `json:"state_id"`
	Party         string `json:"party"`
	Role          string `json:"role"`
	Name          string `json:"name"`
	FirstName     string `json:"first_name"`
	LastName      string `json:"last_name"`
	District      string `json:"district"`
	VotesmartID   int    `json:"votesmart_id"`
	OpensecretsID string `json:"opensecrets_id"`
	Ballotpedia   string `json:"ballotpedia"`
}
