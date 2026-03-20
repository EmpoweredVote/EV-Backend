package socrata

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

const (
	defaultBaseURL = "https://data.lacity.org/resource/m6g2-gc6c.json"
	pageLimit      = 50000
	maxRetries     = 3
)

// SocrataClient handles all HTTP communication with the LA City Socrata endpoint
// using the SODA v2.0 API.
type SocrataClient struct {
	baseURL    string
	appToken   string
	httpClient *http.Client
}

// NewClient creates a SocrataClient configured for the LA City contributions dataset.
func NewClient(appToken string) *SocrataClient {
	return &SocrataClient{
		baseURL:  defaultBaseURL,
		appToken: appToken,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// FetchContributions retrieves all contribution records for the given committee ID.
// If since is non-nil, only records with con_date after that timestamp are fetched
// (delta-fetch). Otherwise a full fetch is performed.
// Paginates automatically using $limit=50000 and $offset.
func (c *SocrataClient) FetchContributions(ctx context.Context, cmtID string, since *time.Time) ([]map[string]interface{}, error) {
	where := fmt.Sprintf("cmt_id='%s'", cmtID)
	if since != nil {
		where += fmt.Sprintf(" AND con_date>'%s'", since.UTC().Format("2006-01-02T15:04:05.000"))
	}

	var all []map[string]interface{}
	offset := 0

	for {
		page, err := c.fetchPage(ctx, where, offset)
		if err != nil {
			return nil, fmt.Errorf("FetchContributions (offset=%d): %w", offset, err)
		}

		all = append(all, page...)

		if len(page) < pageLimit {
			// Last page — we're done.
			break
		}

		offset += pageLimit
		// Courtesy throttle between pages.
		time.Sleep(100 * time.Millisecond)
	}

	return all, nil
}

// fetchPage retrieves a single page of results with exponential backoff retry.
// Retries on HTTP 429 (rate limit) and 5xx (server error).
// Returns immediately on 4xx errors other than 429.
func (c *SocrataClient) fetchPage(ctx context.Context, where string, offset int) ([]map[string]interface{}, error) {
	backoff := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff[attempt-1]):
			}
		}

		params := url.Values{}
		params.Set("$where", where)
		params.Set("$limit", fmt.Sprintf("%d", pageLimit))
		params.Set("$offset", fmt.Sprintf("%d", offset))

		reqURL := c.baseURL + "?" + params.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, fmt.Errorf("fetchPage: build request: %w", err)
		}

		if c.appToken != "" {
			req.Header.Set("X-App-Token", c.appToken)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("fetchPage: http do: %w", err)
			continue // retry on network error
		}

		if resp.StatusCode == http.StatusOK {
			var records []map[string]interface{}
			if decodeErr := json.NewDecoder(resp.Body).Decode(&records); decodeErr != nil {
				resp.Body.Close()
				return nil, fmt.Errorf("fetchPage: decode response: %w", decodeErr)
			}
			resp.Body.Close()
			return records, nil
		}

		statusCode := resp.StatusCode
		resp.Body.Close()

		// Retry on 429 and 5xx.
		if statusCode == http.StatusTooManyRequests || statusCode >= 500 {
			lastErr = fmt.Errorf("fetchPage: HTTP %d (retryable)", statusCode)
			continue
		}

		// Non-retryable 4xx — fail immediately.
		return nil, fmt.Errorf("fetchPage: HTTP %d (non-retryable)", statusCode)
	}

	return nil, fmt.Errorf("fetchPage: exhausted %d retries: %w", maxRetries, lastErr)
}
