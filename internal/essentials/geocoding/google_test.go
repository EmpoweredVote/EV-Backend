package geocoding

import (
	"context"
	"os"
	"testing"
)

func TestGeocode(t *testing.T) {
	// This test requires GOOGLE_MAPS_API_KEY to be set
	if os.Getenv("GOOGLE_MAPS_API_KEY") == "" {
		t.Skip("GOOGLE_MAPS_API_KEY not set")
	}

	client, err := NewClient()
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	if client == nil {
		t.Fatal("Expected non-nil client when API key is set")
	}

	ctx := context.Background()

	// Try a simple address
	result, err := client.Geocode(ctx, "1600 Pennsylvania Ave NW, Washington, DC")
	if err != nil {
		t.Logf("Geocode error: %v", err)
		t.Logf("This might mean the Google Maps Geocoding API is not enabled for this key.")
		t.Logf("Visit: https://console.cloud.google.com/apis/library/geocoding-backend.googleapis.com")
		t.FailNow()
	}

	t.Logf("✅ Geocoded result: %+v", result)

	if result.Zip != "20500" && result.Zip != "20006" {
		t.Errorf("Expected ZIP 20500 or 20006, got %s", result.Zip)
	}
	if result.State != "DC" {
		t.Errorf("Expected state DC, got %s", result.State)
	}
}
