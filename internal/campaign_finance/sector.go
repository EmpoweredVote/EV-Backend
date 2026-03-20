package campaign_finance

import (
	"encoding/json"
	"strings"
)

// occupationSectorMap maps lowercase occupation keywords to sector labels.
// Keys are substrings — first match wins. Order matters: more specific terms first.
var occupationSectorMap = map[string]string{
	"attorney":          "Legal",
	"lawyer":            "Legal",
	"counsel":           "Legal",
	"paralegal":         "Legal",
	"software engineer": "Technology",
	"software developer": "Technology",
	"programmer":        "Technology",
	"developer":         "Technology",
	"data scientist":    "Technology",
	"physician":         "Healthcare",
	"doctor":            "Healthcare",
	"surgeon":           "Healthcare",
	"dentist":           "Healthcare",
	"nurse":             "Healthcare",
	"pharmacist":        "Healthcare",
	"therapist":         "Healthcare",
	"professor":         "Education",
	"teacher":           "Education",
	"instructor":        "Education",
	"principal":         "Education",
	"superintendent":    "Education",
	"retired":           "Retired",
	"homemaker":         "Homemaker",
	"housewife":         "Homemaker",
	"banker":            "Finance/Insurance",
	"financial advisor": "Finance/Insurance",
	"financial analyst": "Finance/Insurance",
	"accountant":        "Finance/Insurance",
	"cpa":               "Finance/Insurance",
	"actuary":           "Finance/Insurance",
	"insurance":         "Finance/Insurance",
	"investment":        "Finance/Insurance",
	"broker":            "Finance/Insurance",
	"realtor":           "Real Estate",
	"real estate":       "Real Estate",
	"property manager":  "Real Estate",
	"farmer":            "Agriculture",
	"rancher":           "Agriculture",
	"agronomist":        "Agriculture",
	"executive":         "Business",
	"ceo":               "Business",
	"coo":               "Business",
	"cfo":               "Business",
	"president":         "Business",
	"director":          "Business",
	"manager":           "Business",
	"consultant":        "Business",
	"engineer":          "Engineering",
	"architect":         "Engineering",
	"student":           "Student",
	"self-employed":     "Business",
	"self employed":     "Business",
	"entrepreneur":      "Business",
}

// ClassifySector lowercases and trims the occupation string, then checks
// occupationSectorMap for the first keyword match. Returns "Other/Unclassified"
// if no keyword matches.
func ClassifySector(occupation string) string {
	lower := strings.ToLower(strings.TrimSpace(occupation))
	if lower == "" {
		return "Other/Unclassified"
	}
	for keyword, sector := range occupationSectorMap {
		if strings.Contains(lower, keyword) {
			return sector
		}
	}
	return "Other/Unclassified"
}

// fecRawRecord is the minimal shape of raw_record JSON for FEC contributions.
type fecRawRecord struct {
	EntityType                string `json:"entity_type"`
	ContributorCommitteeID    string `json:"contributor_committee_id"`
	ContributorOccupation     string `json:"contributor_occupation"`
	ContributorEmployer       string `json:"contributor_employer"`
	ContributorName           string `json:"contributor_name"`
}

// parseRawRecord unmarshals raw_record JSONB bytes into fecRawRecord.
// Returns zero-value struct on parse error.
func parseRawRecord(rawRecord []byte) fecRawRecord {
	var rec fecRawRecord
	if len(rawRecord) == 0 {
		return rec
	}
	_ = json.Unmarshal(rawRecord, &rec)
	return rec
}

// ExtractDonorType inspects the entity_type and contributor_committee_id fields
// in raw_record JSONB to determine whether this is an individual or PAC.
// Returns "individual", "pac", or "unknown".
func ExtractDonorType(rawRecord []byte) string {
	rec := parseRawRecord(rawRecord)
	entityType := strings.ToUpper(strings.TrimSpace(rec.EntityType))
	if strings.HasPrefix(entityType, "IND") {
		return "individual"
	}
	if strings.HasPrefix(entityType, "COM") || strings.HasPrefix(entityType, "PAC") {
		return "pac"
	}
	if rec.ContributorCommitteeID != "" {
		return "pac"
	}
	if entityType != "" {
		return "pac"
	}
	return "unknown"
}

// ExtractOccupation returns the contributor_occupation field from raw_record JSONB.
func ExtractOccupation(rawRecord []byte) string {
	return parseRawRecord(rawRecord).ContributorOccupation
}

// ExtractEmployer returns the contributor_employer field from raw_record JSONB.
func ExtractEmployer(rawRecord []byte) string {
	return parseRawRecord(rawRecord).ContributorEmployer
}
