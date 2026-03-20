package fec

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"
)

type fecPagination struct {
	PerPage int `json:"per_page"`
	Count   int `json:"count"`
	Pages   int `json:"pages"`
	LastIndexes *struct {
		LastIndex                   string `json:"last_index"`
		LastContributionReceiptDate string `json:"last_contribution_receipt_date"`
	} `json:"last_indexes"`
}

type fecScheduleAResponse struct {
	Pagination fecPagination            `json:"pagination"`
	Results    []map[string]interface{} `json:"results"`
}

// FetchAllPages fetches all Schedule A contribution pages for the given candidate ID
// and election cycle using FEC keyset pagination. The FEC_API_KEY environment variable
// must be set; an error is returned immediately if it is absent.
func FetchAllPages(candidateID, cycle string) ([]map[string]interface{}, int, error) {
	apiKey := os.Getenv("FEC_API_KEY")
	if apiKey == "" {
		return nil, 0, fmt.Errorf("FEC_API_KEY environment variable is not set")
	}

	baseURL := "https://api.open.fec.gov/v1/schedules/schedule_a/"
	httpClient := &http.Client{Timeout: 30 * time.Second}

	var allRecords []map[string]interface{}
	totalExpected := 0
	firstPage := true

	// Keyset pagination state
	var lastIndex string
	var lastContributionReceiptDate string

	for {
		params := url.Values{}
		params.Set("api_key", apiKey)
		params.Set("candidate_id", candidateID)
		params.Set("two_year_transaction_period", cycle)
		params.Set("per_page", "100")
		params.Set("sort", "contribution_receipt_date")

		if !firstPage {
			params.Set("last_index", lastIndex)
			params.Set("last_contribution_receipt_date", lastContributionReceiptDate)
		}

		requestURL := baseURL + "?" + params.Encode()

		resp, err := httpClient.Get(requestURL)
		if err != nil {
			return nil, totalExpected, fmt.Errorf("FEC API request failed: %w", err)
		}

		var page fecScheduleAResponse
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			resp.Body.Close()
			return nil, totalExpected, fmt.Errorf("FEC API response decode failed: %w", err)
		}
		resp.Body.Close()

		if firstPage {
			totalExpected = page.Pagination.Count
			firstPage = false
		}

		allRecords = append(allRecords, page.Results...)

		// Stop when no more results or no next cursor
		if len(page.Results) == 0 || page.Pagination.LastIndexes == nil {
			break
		}

		lastIndex = page.Pagination.LastIndexes.LastIndex
		lastContributionReceiptDate = page.Pagination.LastIndexes.LastContributionReceiptDate

		// Rate limiting: stay safely under 1000 req/hr limit
		time.Sleep(4 * time.Second)
	}

	return allRecords, totalExpected, nil
}
