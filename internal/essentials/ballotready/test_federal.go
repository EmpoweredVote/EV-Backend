package ballotready

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// TestFederalOfficials specifically tests for federal-level officials
func TestFederalOfficials() error {
	ctx := context.Background()
	client := NewClient(os.Getenv("BALLOTREADY_KEY"), "https://bpi.civicengine.com/graphql")

	outputDir := "/Users/chrisandrews/Documents/GitHub/EV-Backend/ballotready_samples"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	// Test regular residential ZIPs
	tests := []struct {
		name string
		zip  string
	}{
		{"federal_indianapolis", "46204"}, // Indianapolis - should have US Senators
		{"federal_bloomington", "47401"},  // Bloomington - should have US Senators + Rep
		{"federal_miami", "33101"},        // Miami - Rubio, Scott
		{"federal_dc", "20001"},           // Regular DC residential ZIP
	}

	for _, test := range tests {
		fmt.Printf("\n=== Testing: %s (ZIP: %s) ===\n", test.name, test.zip)

		holders, err := client.FetchOfficeHoldersByZip(ctx, test.zip)
		if err != nil {
			fmt.Printf("Error fetching: %v\n", err)
			continue
		}

		// Filter to FEDERAL level only
		var federal []OfficeHolderNode
		for _, holder := range holders {
			if holder.Position != nil && holder.Position.Level == "FEDERAL" {
				federal = append(federal, holder)
			}
		}

		fmt.Printf("Found %d federal officials (out of %d total)\n", len(federal), len(holders))

		if len(federal) > 0 {
			// Save to file
			filename := filepath.Join(outputDir, test.name+".json")
			data, err := json.MarshalIndent(federal, "", "  ")
			if err != nil {
				fmt.Printf("Error marshaling: %v\n", err)
				continue
			}

			if err := os.WriteFile(filename, data, 0644); err != nil {
				fmt.Printf("Error writing file: %v\n", err)
				continue
			}

			fmt.Printf("✓ Saved to %s\n", filename)

			// Print officials
			for _, holder := range federal {
				name := "Unknown"
				if holder.Person != nil {
					name = holder.Person.FullName
				}
				position := "Unknown Position"
				if holder.Position != nil {
					position = holder.Position.Name
				}

				bioText := "(empty)"
				if holder.Person != nil && holder.Person.BioText != "" {
					bioText = fmt.Sprintf("(%d chars)", len(holder.Person.BioText))
				}

				fmt.Printf("  - %s | %s | bioText: %s\n", name, position, bioText)
			}
		}
	}

	return nil
}
