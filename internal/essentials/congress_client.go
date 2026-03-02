package essentials

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

// CongressClient provides rate-limited access to the Congress.gov API v3.
//
// Rate limits:
//   - Congress.gov allows 5,000 req/hr
//   - We operate at 4,500 req/hr (90%) = 1 token every 800ms with burst 10
//
// All methods accept a context for timeout propagation and cancellation.
type CongressClient struct {
	apiKey     string
	httpClient *http.Client
	limiter    *rate.Limiter
	baseURL    string
}

// NewCongressClient creates a CongressClient with token bucket rate limiting
// enforced at ~4,500 req/hr (one token every 800ms, burst 10).
func NewCongressClient(apiKey string) *CongressClient {
	return &CongressClient{
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		limiter:    rate.NewLimiter(rate.Every(800*time.Millisecond), 10),
		baseURL:    "https://api.congress.gov/v3",
	}
}

// ===== Internal pagination engine =====

// fetchPaginated pages through a Congress.gov endpoint using offset-based pagination.
//
// path is appended to baseURL (e.g., "/member/A000360/sponsored-legislation").
// params holds any additional query parameters; api_key, format, limit, and offset
// are set automatically on every request.
//
// dest is a callback that receives the raw JSON body and returns (n, err) where
// n is the number of items parsed from this page. Pagination stops when n < 250
// (the canonical stop condition — the API silently truncates at 250 items per page
// and the total field was removed, so len(items) < limit is the only safe signal).
func (c *CongressClient) fetchPaginated(
	ctx context.Context,
	path string,
	params url.Values,
	dest func([]byte) (int, error),
) error {
	const limit = 250
	offset := 0

	for {
		// Wait for rate limiter token before every HTTP request
		if err := c.limiter.Wait(ctx); err != nil {
			return fmt.Errorf("congress rate limiter: %w", err)
		}

		// Build full URL with pagination parameters
		reqURL := c.baseURL + path
		q := url.Values{}
		for k, v := range params {
			q[k] = v
		}
		q.Set("api_key", c.apiKey)
		q.Set("format", "json")
		q.Set("limit", strconv.Itoa(limit))
		q.Set("offset", strconv.Itoa(offset))

		req, err := http.NewRequestWithContext(ctx, "GET", reqURL+"?"+q.Encode(), nil)
		if err != nil {
			return fmt.Errorf("creating congress request for %s: %w", path, err)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("Congress.gov request failed for %s: %w", path, err)
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return fmt.Errorf("reading Congress.gov response for %s: %w", path, readErr)
		}

		if resp.StatusCode != http.StatusOK {
			snippet := string(body)
			if len(snippet) > 200 {
				snippet = snippet[:200]
			}
			return fmt.Errorf("Congress.gov returned %d for %s: %s", resp.StatusCode, path, snippet)
		}

		// Pass body to the caller-provided parser; n = number of items on this page
		n, err := dest(body)
		if err != nil {
			return fmt.Errorf("parsing Congress.gov response for %s (offset=%d): %w", path, offset, err)
		}

		// CRITICAL stop condition: stop when fewer items than limit were returned.
		// Never use pagination.next or assume a round number means there are more pages.
		if n < limit {
			break
		}

		offset += limit
	}

	return nil
}

// ===== Congress.gov response types =====

// congressBillItem represents a single bill/resolution in sponsored or cosponsored
// legislation list responses.
type congressBillItem struct {
	Congress       int    `json:"congress"`
	Type           string `json:"type"`           // "HR", "S", "HJRES", "SJRES", "HCONRES", "SCONRES", "HRES", "SRES"
	Number         string `json:"number"`         // numeric string: "1044"
	LatestTitle    string `json:"latestTitle"`
	IntroducedDate string `json:"introducedDate"` // "YYYY-MM-DD"
	LatestAction   struct {
		ActionDate string `json:"actionDate"`
		Text       string `json:"text"`
	} `json:"latestAction"`
	URL string `json:"url"`
}

// houseVoteItem represents a single roll call vote from the House vote list endpoint.
type houseVoteItem struct {
	RollCallNumber int    `json:"rollCallNumber"`
	Congress       int    `json:"congress"`
	SessionNumber  int    `json:"sessionNumber"`
	VoteQuestion   string `json:"voteQuestion"`
	VoteDate       string `json:"voteDate"`
	Result         string `json:"result"`
	YeaTotal       int    `json:"yeaTotal"`
	NayTotal       int    `json:"nayTotal"`
	BillNumber     string `json:"billNumber,omitempty"`
	BillTitle      string `json:"billTitle,omitempty"`
	BillURL        string `json:"billUrl,omitempty"`
}

// houseVoteMemberItem represents a single member's vote within a roll call.
type houseVoteMemberItem struct {
	BioguideID string `json:"bioguideId"`
	VoteCast   string `json:"voteCast"` // "Aye", "Nay", "Present", "Not Voting"
}

// congressSummaryItem represents one CRS summary for a bill.
type congressSummaryItem struct {
	Text       string `json:"text"`       // HTML-encoded CRS summary
	ActionDate string `json:"actionDate"` // for sorting: "YYYY-MM-DD"
}

// ===== Typed convenience methods =====

// GetSponsoredLegislation returns all bills sponsored by a member in the given congress.
//
// bioguideID is the Congress.gov member identifier (e.g., "A000360").
// congress is the congress number (e.g., 118 for 118th Congress).
func (c *CongressClient) GetSponsoredLegislation(ctx context.Context, bioguideID string, congress int) ([]congressBillItem, error) {
	path := fmt.Sprintf("/member/%s/sponsored-legislation", bioguideID)
	params := url.Values{"congress": {strconv.Itoa(congress)}}

	var items []congressBillItem
	err := c.fetchPaginated(ctx, path, params, func(body []byte) (int, error) {
		var page struct {
			SponsoredLegislation []congressBillItem `json:"sponsoredLegislation"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return 0, err
		}
		items = append(items, page.SponsoredLegislation...)
		return len(page.SponsoredLegislation), nil
	})
	if err != nil {
		return nil, err
	}
	return items, nil
}

// GetCosponsoredLegislation returns all bills cosponsored by a member in the given congress.
//
// bioguideID is the Congress.gov member identifier (e.g., "A000360").
// congress is the congress number (e.g., 118 for 118th Congress).
func (c *CongressClient) GetCosponsoredLegislation(ctx context.Context, bioguideID string, congress int) ([]congressBillItem, error) {
	path := fmt.Sprintf("/member/%s/cosponsored-legislation", bioguideID)
	params := url.Values{"congress": {strconv.Itoa(congress)}}

	var items []congressBillItem
	err := c.fetchPaginated(ctx, path, params, func(body []byte) (int, error) {
		var page struct {
			CosponsoredLegislation []congressBillItem `json:"cosponsoredLegislation"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return 0, err
		}
		items = append(items, page.CosponsoredLegislation...)
		return len(page.CosponsoredLegislation), nil
	})
	if err != nil {
		return nil, err
	}
	return items, nil
}

// GetHouseVoteList returns all House roll call votes for a given congress and session.
//
// congress is the congress number (e.g., 118).
// session is the session number (1 or 2 for House).
func (c *CongressClient) GetHouseVoteList(ctx context.Context, congress, session int) ([]houseVoteItem, error) {
	path := fmt.Sprintf("/house-vote/%d/%d", congress, session)

	var items []houseVoteItem
	err := c.fetchPaginated(ctx, path, url.Values{}, func(body []byte) (int, error) {
		var page struct {
			HouseRollCallVotes []houseVoteItem `json:"houseRollCallVotes"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return 0, err
		}
		items = append(items, page.HouseRollCallVotes...)
		return len(page.HouseRollCallVotes), nil
	})
	if err != nil {
		return nil, err
	}
	return items, nil
}

// GetHouseVoteMemberVotes returns individual member votes for a specific roll call.
//
// This is a single-page request (not paginated) — all member votes fit in one response.
// congress, session, and rollCallNumber identify the specific roll call.
func (c *CongressClient) GetHouseVoteMemberVotes(ctx context.Context, congress, session, rollCallNumber int) ([]houseVoteMemberItem, error) {
	// Wait for rate limiter token
	if err := c.limiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("congress rate limiter: %w", err)
	}

	path := fmt.Sprintf("/house-vote/%d/%d/%d/votes", congress, session, rollCallNumber)
	reqURL := c.baseURL + path

	q := url.Values{}
	q.Set("api_key", c.apiKey)
	q.Set("format", "json")

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL+"?"+q.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("creating request for roll call %d: %w", rollCallNumber, err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Congress.gov member votes request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading member votes response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		snippet := string(body)
		if len(snippet) > 200 {
			snippet = snippet[:200]
		}
		return nil, fmt.Errorf("Congress.gov returned %d for roll call %d: %s", resp.StatusCode, rollCallNumber, snippet)
	}

	// Response structure: {"houseRollCallVoteMemberVotes": {"results": [...]}}
	var wrapper struct {
		HouseRollCallVoteMemberVotes struct {
			Results []houseVoteMemberItem `json:"results"`
		} `json:"houseRollCallVoteMemberVotes"`
	}
	if err := json.Unmarshal(body, &wrapper); err != nil {
		return nil, fmt.Errorf("parsing member votes response: %w", err)
	}

	return wrapper.HouseRollCallVoteMemberVotes.Results, nil
}

// GetBillSummary returns the most recent CRS plain-language summary for a bill.
//
// congress is the congress number (e.g., 118).
// billType must be provided in any case — it will be lowercased for the URL path
// (e.g., "HR" or "hr" both work; the API path uses lowercase "hr").
// billNumber is the numeric identifier (e.g., "1044").
//
// Returns empty string if no summaries are available for the bill.
func (c *CongressClient) GetBillSummary(ctx context.Context, congress int, billType, billNumber string) (string, error) {
	// Wait for rate limiter token
	if err := c.limiter.Wait(ctx); err != nil {
		return "", fmt.Errorf("congress rate limiter: %w", err)
	}

	// Bill type must be lowercase in the URL path
	billTypeLower := strings.ToLower(billType)
	path := fmt.Sprintf("/bill/%d/%s/%s/summaries", congress, billTypeLower, billNumber)
	reqURL := c.baseURL + path

	q := url.Values{}
	q.Set("api_key", c.apiKey)
	q.Set("format", "json")

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL+"?"+q.Encode(), nil)
	if err != nil {
		return "", fmt.Errorf("creating bill summary request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("Congress.gov bill summary request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading bill summary response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		snippet := string(body)
		if len(snippet) > 200 {
			snippet = snippet[:200]
		}
		return "", fmt.Errorf("Congress.gov returned %d for bill summary: %s", resp.StatusCode, snippet)
	}

	var page struct {
		Summaries []congressSummaryItem `json:"summaries"`
	}
	if err := json.Unmarshal(body, &page); err != nil {
		return "", fmt.Errorf("parsing bill summaries response: %w", err)
	}

	if len(page.Summaries) == 0 {
		return "", nil
	}

	// Sort by actionDate descending to get the most recent summary
	sort.Slice(page.Summaries, func(i, j int) bool {
		return page.Summaries[i].ActionDate > page.Summaries[j].ActionDate
	})

	return stripHTMLTags(page.Summaries[0].Text), nil
}

// ===== Text processing helpers =====

// stripHTMLTags removes HTML tags from a string and trims surrounding whitespace.
// Used to clean CRS summary text from the Congress.gov summaries endpoint.
func stripHTMLTags(s string) string {
	re := regexp.MustCompile(`<[^>]*>`)
	return strings.TrimSpace(re.ReplaceAllString(s, ""))
}

// ===== Vote normalization helpers (used by import CLIs in Phase 56-03) =====

// normalizeVoteCast maps the various vote cast strings from Congress.gov member
// vote responses to the canonical position values stored in legislative_votes:
// "yea", "nay", "present", "not_voting", or "absent".
func normalizeVoteCast(voteCast string) string {
	switch strings.ToLower(voteCast) {
	case "aye", "yea", "yes":
		return "yea"
	case "nay", "no":
		return "nay"
	case "present":
		return "present"
	case "not voting", "nv":
		return "not_voting"
	case "absent":
		return "absent"
	default:
		return strings.ToLower(voteCast)
	}
}

// normalizeVoteResult maps the result strings from Congress.gov roll call responses
// to the canonical result values stored in legislative_votes: "passed" or "failed".
func normalizeVoteResult(result string) string {
	t := strings.ToLower(result)
	switch {
	case strings.Contains(t, "passed") || strings.Contains(t, "agreed"):
		return "passed"
	case strings.Contains(t, "failed") || strings.Contains(t, "rejected"):
		return "failed"
	default:
		return t
	}
}

// ===== Bill status normalization helper (used by import CLIs in Phase 56-02) =====

// normalizeBillStatus derives a human-readable status from the latest action text
// returned in Congress.gov bill list responses. The returned string is stored in
// the legislative_bills.status column.
func normalizeBillStatus(latestActionText string) string {
	t := strings.ToLower(latestActionText)
	switch {
	case strings.Contains(t, "became public law") || strings.Contains(t, "signed by president"):
		return "Signed"
	case strings.Contains(t, "passed senate") || strings.Contains(t, "passed house"):
		return "Passed"
	case strings.Contains(t, "reported by"):
		return "Reported"
	case strings.Contains(t, "referred to"):
		return "In Committee"
	default:
		return "Introduced"
	}
}
