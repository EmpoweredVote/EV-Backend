package ballotready

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/essentials/provider"
)

// Client is a GraphQL client for the BallotReady/CivicEngine API.
type Client struct {
	apiKey     string
	endpoint   string
	httpClient *http.Client
}

// NewClient creates a new BallotReady API client.
func NewClient(apiKey, endpoint string) *Client {
	return &Client{
		apiKey:   apiKey,
		endpoint: endpoint,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// officeHoldersByZipQuery is the GraphQL query for fetching current officeholders by ZIP code.
// Uses Relay cursor-based pagination (first/after).
const officeHoldersByZipQuery = `
query OfficeHoldersByZip($zip: String!, $first: Int!, $after: String) {
  officeHolders(
    location: { zip: $zip }
    filterBy: { isCurrent: true }
    first: $first
    after: $after
  ) {
    edges {
      cursor
      node {
        id
        databaseId
        isCurrent
        isAppointed
        isVacant
        officeTitle
        startAt
        endAt
        totalYearsInOffice
        person {
          id
          databaseId
          firstName
          middleName
          lastName
          suffix
          nickname
          fullName
          slug
          bioText
          bioguideId
          images {
            url
            type
          }
          contacts {
            email
            phone
            fax
            type
          }
          degrees {
            id
            degree
            major
            school
            gradYear
          }
          experiences {
            id
            title
            organization
            type
            start
            end
          }
          urls {
            url
            type
          }
        }
        parties {
          name
          shortName
        }
        position {
          id
          databaseId
          name
          level
          tier
          state
          judicial
          appointed
          subAreaName
          subAreaValue
          normalizedPosition {
            name
            description
            mtfcc
          }
          electionFrequencies {
            frequency
            referenceYear
          }
        }
        addresses {
          addressLine1
          addressLine2
          city
          state
          zip
          type
        }
        contacts {
          email
          phone
          fax
          type
        }
        urls {
          url
          type
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
`

// officeHoldersByAddressQuery is the GraphQL query for fetching current officeholders by street address.
// BallotReady geocodes the address server-side, giving precise district-level results.
const officeHoldersByAddressQuery = `
query OfficeHoldersByAddress($address: String!, $first: Int!, $after: String) {
  officeHolders(
    location: { address: $address }
    filterBy: { isCurrent: true }
    first: $first
    after: $after
  ) {
    edges {
      cursor
      node {
        id
        databaseId
        isCurrent
        isAppointed
        isVacant
        officeTitle
        startAt
        endAt
        totalYearsInOffice
        person {
          id
          databaseId
          firstName
          middleName
          lastName
          suffix
          nickname
          fullName
          slug
          bioText
          bioguideId
          images {
            url
            type
          }
          contacts {
            email
            phone
            fax
            type
          }
          degrees {
            id
            degree
            major
            school
            gradYear
          }
          experiences {
            id
            title
            organization
            type
            start
            end
          }
          urls {
            url
            type
          }
        }
        parties {
          name
          shortName
        }
        position {
          id
          databaseId
          name
          level
          tier
          state
          judicial
          appointed
          subAreaName
          subAreaValue
          normalizedPosition {
            name
            description
            mtfcc
          }
          electionFrequencies {
            frequency
            referenceYear
          }
        }
        addresses {
          addressLine1
          addressLine2
          city
          state
          zip
          type
        }
        contacts {
          email
          phone
          fax
          type
        }
        urls {
          url
          type
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
`

// pageSize is the number of results per page for pagination.
const pageSize = 100

// FetchOfficeHoldersByZip fetches all current officeholders for a ZIP code,
// handling Relay cursor-based pagination automatically.
func (c *Client) FetchOfficeHoldersByZip(ctx context.Context, zip string) ([]OfficeHolderNode, error) {
	var allNodes []OfficeHolderNode
	var cursor *string

	start := time.Now()
	provider.LogRequest("ballotready", "POST", c.endpoint, map[string]interface{}{
		"zip": zip,
	})

	for {
		variables := map[string]interface{}{
			"zip":   zip,
			"first": pageSize,
		}
		if cursor != nil {
			variables["after"] = *cursor
		}

		reqBody := GraphQLRequest{
			Query:     officeHoldersByZipQuery,
			Variables: variables,
		}

		body, err := json.Marshal(reqBody)
		if err != nil {
			return nil, fmt.Errorf("marshal request: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, "POST", c.endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+c.apiKey)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			provider.LogError("ballotready", "fetch", err)
			return nil, fmt.Errorf("ballotready request: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			err := fmt.Errorf("ballotready status %d", resp.StatusCode)
			provider.LogError("ballotready", "fetch", err)
			return nil, err
		}

		var gqlResp GraphQLResponse
		if err := json.NewDecoder(resp.Body).Decode(&gqlResp); err != nil {
			provider.LogError("ballotready", "decode", err)
			return nil, fmt.Errorf("decode response: %w", err)
		}

		if len(gqlResp.Errors) > 0 {
			err := fmt.Errorf("graphql errors: %s", gqlResp.Errors[0].Message)
			provider.LogError("ballotready", "graphql", err)
			return nil, err
		}

		if gqlResp.Data == nil || gqlResp.Data.OfficeHolders == nil {
			break
		}

		conn := gqlResp.Data.OfficeHolders
		for _, edge := range conn.Edges {
			allNodes = append(allNodes, edge.Node)
		}

		if !conn.PageInfo.HasNextPage || conn.PageInfo.EndCursor == "" {
			break
		}

		cursor = &conn.PageInfo.EndCursor
	}

	provider.LogResponse("ballotready", 200, time.Since(start), len(allNodes))
	return allNodes, nil
}

// FetchOfficeHoldersByAddress fetches all current officeholders for a street address,
// handling Relay cursor-based pagination automatically.
// BallotReady geocodes the address server-side for precise district matching.
func (c *Client) FetchOfficeHoldersByAddress(ctx context.Context, address string) ([]OfficeHolderNode, error) {
	var allNodes []OfficeHolderNode
	var cursor *string

	start := time.Now()
	provider.LogRequest("ballotready", "POST", c.endpoint, map[string]interface{}{
		"address": address,
	})

	for {
		variables := map[string]interface{}{
			"address": address,
			"first":   pageSize,
		}
		if cursor != nil {
			variables["after"] = *cursor
		}

		reqBody := GraphQLRequest{
			Query:     officeHoldersByAddressQuery,
			Variables: variables,
		}

		body, err := json.Marshal(reqBody)
		if err != nil {
			return nil, fmt.Errorf("marshal request: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, "POST", c.endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+c.apiKey)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			provider.LogError("ballotready", "fetch", err)
			return nil, fmt.Errorf("ballotready request: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			err := fmt.Errorf("ballotready status %d", resp.StatusCode)
			provider.LogError("ballotready", "fetch", err)
			return nil, err
		}

		var gqlResp GraphQLResponse
		if err := json.NewDecoder(resp.Body).Decode(&gqlResp); err != nil {
			provider.LogError("ballotready", "decode", err)
			return nil, fmt.Errorf("decode response: %w", err)
		}

		if len(gqlResp.Errors) > 0 {
			err := fmt.Errorf("graphql errors: %s", gqlResp.Errors[0].Message)
			provider.LogError("ballotready", "graphql", err)
			return nil, err
		}

		if gqlResp.Data == nil || gqlResp.Data.OfficeHolders == nil {
			break
		}

		conn := gqlResp.Data.OfficeHolders
		for _, edge := range conn.Edges {
			allNodes = append(allNodes, edge.Node)
		}

		if !conn.PageInfo.HasNextPage || conn.PageInfo.EndCursor == "" {
			break
		}

		cursor = &conn.PageInfo.EndCursor
	}

	provider.LogResponse("ballotready", 200, time.Since(start), len(allNodes))
	return allNodes, nil
}

// HealthCheck verifies the API key is valid.
func (c *Client) HealthCheck(ctx context.Context) error {
	reqBody := GraphQLRequest{
		Query: `query { __typename }`,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

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
