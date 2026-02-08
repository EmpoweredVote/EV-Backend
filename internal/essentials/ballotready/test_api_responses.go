package ballotready

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// TestSampleOfficials queries BallotReady for various officials and saves responses to files
func TestSampleOfficials() error {
	ctx := context.Background()
	client := NewClient(os.Getenv("BALLOTREADY_KEY"), "https://bpi.civicengine.com/graphql")

	outputDir := "/Users/chrisandrews/Documents/GitHub/EV-Backend/ballotready_samples"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	// Test queries for different levels of government
	tests := []struct {
		name     string
		zip      string
		search   string // what to look for in the results
		level    string // expected level
	}{
		// Federal Executive
		{
			name:   "01_federal_president",
			zip:    "20500", // White House
			search: "President",
			level:  "FEDERAL",
		},
		// Federal Cabinet (from DC area)
		{
			name:   "02_federal_cabinet",
			zip:    "20500",
			search: "Secretary",
			level:  "FEDERAL",
		},
		// State Executive - Indiana Governor
		{
			name:   "03_state_governor_IN",
			zip:    "46204", // Indianapolis
			search: "Governor",
			level:  "STATE",
		},
		// State Treasurer - Indiana
		{
			name:   "04_state_treasurer_IN",
			zip:    "46204",
			search: "Treasurer",
			level:  "STATE",
		},
		// State Secretary of State
		{
			name:   "05_state_secretary_IN",
			zip:    "46204",
			search: "Secretary of State",
			level:  "STATE",
		},
		// County Executive - Monroe County, IN
		{
			name:   "06_county_officials",
			zip:    "47401", // Bloomington
			search: "County",
			level:  "COUNTY",
		},
		// City/Local Officials
		{
			name:   "07_city_mayor",
			zip:    "47401",
			search: "Mayor",
			level:  "LOCAL",
		},
		// School Board
		{
			name:   "08_school_board",
			zip:    "47401",
			search: "School",
			level:  "SCHOOL",
		},
		// Township Officials
		{
			name:   "09_township_officials",
			zip:    "47401",
			search: "Township",
			level:  "LOCAL",
		},
	}

	for _, test := range tests {
		fmt.Printf("\n=== Testing: %s (ZIP: %s) ===\n", test.name, test.zip)

		// Fetch office holders by ZIP
		holders, err := client.FetchOfficeHoldersByZip(ctx, test.zip)
		if err != nil {
			fmt.Printf("Error fetching %s: %v\n", test.name, err)
			continue
		}

		// Filter to relevant officials
		var relevant []OfficeHolderNode
		for _, holder := range holders {
			// Check if this matches what we're looking for
			if holder.Position != nil {
				matchLevel := holder.Position.Level == test.level
				matchSearch := true
				if test.search != "" {
					matchSearch = false
					// Check position name
					if holder.Position.Name != "" {
						if contains(holder.Position.Name, test.search) {
							matchSearch = true
						}
					}
					// Check office title
					if holder.OfficeTitle != "" {
						if contains(holder.OfficeTitle, test.search) {
							matchSearch = true
						}
					}
				}

				if matchLevel && matchSearch {
					relevant = append(relevant, holder)
				}
			}
		}

		if len(relevant) == 0 {
			fmt.Printf("No officials found matching criteria\n")
			// Save all results for this level anyway
			relevant = filterByLevel(holders, test.level)
			if len(relevant) > 3 {
				relevant = relevant[:3] // Limit to first 3
			}
		} else {
			// Limit to first 2 matching officials
			if len(relevant) > 2 {
				relevant = relevant[:2]
			}
		}

		// Save to file
		filename := filepath.Join(outputDir, test.name+".json")
		data, err := json.MarshalIndent(relevant, "", "  ")
		if err != nil {
			fmt.Printf("Error marshaling JSON: %v\n", err)
			continue
		}

		if err := os.WriteFile(filename, data, 0644); err != nil {
			fmt.Printf("Error writing file: %v\n", err)
			continue
		}

		fmt.Printf("✓ Saved %d officials to %s\n", len(relevant), filename)

		// Print summary
		for _, holder := range relevant {
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

	fmt.Printf("\n=== All samples saved to: %s ===\n", outputDir)
	fmt.Println("\nYou can review the JSON files to see all available fields for each official type.")

	return nil
}

func contains(s, substr string) bool {
	// Case-insensitive contains
	s = toLower(s)
	substr = toLower(substr)
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func toLower(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c = c + 32
		}
		result[i] = c
	}
	return string(result)
}

func filterByLevel(holders []OfficeHolderNode, level string) []OfficeHolderNode {
	var result []OfficeHolderNode
	for _, holder := range holders {
		if holder.Position != nil && holder.Position.Level == level {
			result = append(result, holder)
		}
	}
	return result
}
