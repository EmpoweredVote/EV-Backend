package cicero

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/essentials/provider"
)

const (
	// BaseURL is the Cicero API endpoint.
	BaseURL = "https://app.cicerodata.com/v3.1/official"

	// PageMax is the maximum number of results per page.
	PageMax = 199
)

// Client is an HTTP client for the Cicero API.
type Client struct {
	apiKey     string
	httpClient *http.Client
}

// NewClient creates a new Cicero API client.
func NewClient(apiKey string) *Client {
	return &Client{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// FetchOfficialsByZip fetches officials for a ZIP code, optionally filtered by district types.
func (c *Client) FetchOfficialsByZip(ctx context.Context, zip string, districtTypes []string) ([]CiceroOfficial, error) {
	params := url.Values{}
	params.Set("search_postal", zip)
	params.Set("search_country", "US")
	params.Set("format", "json")
	params.Set("key", c.apiKey)
	params.Set("max", strconv.Itoa(PageMax))

	for _, dt := range districtTypes {
		params.Add("district_type", dt)
	}

	return c.fetchAllPages(ctx, params)
}

// FetchAllOfficials fetches all officials for a ZIP code (all district types).
func (c *Client) FetchAllOfficials(ctx context.Context, zip string) ([]CiceroOfficial, error) {
	return c.FetchOfficialsByZip(ctx, zip, provider.AllDistrictTypes)
}

// HealthCheck verifies the API key is valid by making a minimal request.
func (c *Client) HealthCheck(ctx context.Context) error {
	params := url.Values{}
	params.Set("search_postal", "20001") // DC ZIP
	params.Set("search_country", "US")
	params.Set("format", "json")
	params.Set("key", c.apiKey)
	params.Set("max", "1")
	params.Add("district_type", "NATIONAL_EXEC")

	fullURL := fmt.Sprintf("%s?%s", BaseURL, params.Encode())

	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("health check request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check failed: status %d", resp.StatusCode)
	}

	return nil
}

// fetchAllPages handles pagination for Cicero API requests.
func (c *Client) fetchAllPages(ctx context.Context, baseParams url.Values) ([]CiceroOfficial, error) {
	var all []CiceroOfficial
	offset := 0

	for {
		params := url.Values{}
		for k, vs := range baseParams {
			for _, v := range vs {
				params.Add(k, v)
			}
		}
		params.Set("offset", strconv.Itoa(offset))

		fullURL := fmt.Sprintf("%s?%s", BaseURL, params.Encode())

		start := time.Now()
		provider.LogRequest("cicero", "GET", BaseURL, map[string]interface{}{
			"offset":        offset,
			"district_type": baseParams["district_type"],
		})

		req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			provider.LogError("cicero", "fetch", err)
			return nil, fmt.Errorf("cicero request: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			err := fmt.Errorf("cicero status %d", resp.StatusCode)
			provider.LogError("cicero", "fetch", err)
			return nil, err
		}

		var page CiceroAPIResponse
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			resp.Body.Close()
			provider.LogError("cicero", "decode", err)
			return nil, fmt.Errorf("decode cicero: %w", err)
		}
		resp.Body.Close()

		// Flatten results from all candidates
		pageCount := 0
		for _, candidate := range page.Response.Results.Candidates {
			all = append(all, candidate.Officials...)
			pageCount += len(candidate.Officials)
		}

		provider.LogResponse("cicero", resp.StatusCode, time.Since(start), pageCount)

		// Stop if this page returned less than max (end of results)
		if pageCount < PageMax {
			break
		}

		offset += pageCount
	}

	return all, nil
}
