package geocoding

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"
)

// Result holds structured data from a Google Maps geocoding response.
type Result struct {
	Zip       string  `json:"zip"`
	State     string  `json:"state"`      // 2-letter state abbreviation
	County    string  `json:"county"`
	City      string  `json:"city"`
	Formatted string  `json:"formatted"`  // Full formatted address
	Lat       float64 `json:"lat"`
	Lng       float64 `json:"lng"`
}

// Client wraps the Google Maps Geocoding API.
type Client struct {
	apiKey     string
	httpClient *http.Client
}

// NewClient creates a geocoding client from the GOOGLE_MAPS_API_KEY env var.
// Returns nil, nil if the key is not set (graceful degradation).
func NewClient() (*Client, error) {
	key := os.Getenv("GOOGLE_MAPS_API_KEY")
	if key == "" {
		return nil, nil
	}
	return &Client{
		apiKey: key,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}, nil
}

type geocodeResponse struct {
	Results []geocodeResult `json:"results"`
	Status  string          `json:"status"`
}

type geocodeResult struct {
	AddressComponents []addressComponent `json:"address_components"`
	FormattedAddress  string             `json:"formatted_address"`
	Geometry          geometry           `json:"geometry"`
}

type addressComponent struct {
	LongName  string   `json:"long_name"`
	ShortName string   `json:"short_name"`
	Types     []string `json:"types"`
}

type geometry struct {
	Location latLng `json:"location"`
}

type latLng struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

// Geocode converts a free-form address string into structured location data.
func (c *Client) Geocode(ctx context.Context, address string) (*Result, error) {
	u := fmt.Sprintf("https://maps.googleapis.com/maps/api/geocode/json?address=%s&key=%s",
		url.QueryEscape(address), c.apiKey)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("geocoding request: %w", err)
	}
	defer resp.Body.Close()

	// Check HTTP status first
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("geocoding API returned HTTP %d: check that Geocoding API is enabled in Google Cloud Console", resp.StatusCode)
	}

	var geoResp geocodeResponse
	if err := json.NewDecoder(resp.Body).Decode(&geoResp); err != nil {
		return nil, fmt.Errorf("decoding response (HTTP %d): %w — verify Geocoding API is enabled", resp.StatusCode, err)
	}

	if geoResp.Status != "OK" {
		if len(geoResp.Results) == 0 {
			return nil, fmt.Errorf("geocoding failed: status=%s (no results) — check API key permissions", geoResp.Status)
		}
		return nil, fmt.Errorf("geocoding failed: status=%s", geoResp.Status)
	}
	if len(geoResp.Results) == 0 {
		return nil, fmt.Errorf("geocoding returned no results for address")
	}

	result := geoResp.Results[0]
	out := &Result{
		Formatted: result.FormattedAddress,
		Lat:       result.Geometry.Location.Lat,
		Lng:       result.Geometry.Location.Lng,
	}

	for _, comp := range result.AddressComponents {
		for _, t := range comp.Types {
			switch t {
			case "postal_code":
				out.Zip = comp.ShortName
			case "administrative_area_level_1":
				out.State = comp.ShortName
			case "administrative_area_level_2":
				out.County = comp.LongName
			case "locality":
				out.City = comp.LongName
			}
		}
	}

	if out.Zip == "" {
		return nil, fmt.Errorf("no ZIP code found in geocoding result for: %s", address)
	}

	return out, nil
}
