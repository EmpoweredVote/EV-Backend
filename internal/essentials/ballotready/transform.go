package ballotready

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/EmpoweredVote/EV-Backend/internal/essentials/provider"
)

// MapDistrictType converts BallotReady position level, judicial flag, and normalized
// position name to a Cicero-style district type string.
func MapDistrictType(level string, judicial bool, positionName string) string {
	level = strings.ToUpper(strings.TrimSpace(level))
	name := strings.ToLower(positionName)

	switch level {
	case "FEDERAL":
		switch {
		case strings.Contains(name, "president") || strings.Contains(name, "vice president"):
			return "NATIONAL_EXEC"
		case strings.Contains(name, "senator") || strings.Contains(name, "senate"):
			return "NATIONAL_UPPER"
		default:
			// Representative, House, Delegate, Resident Commissioner, etc.
			return "NATIONAL_LOWER"
		}

	case "STATE":
		switch {
		case judicial:
			return "JUDICIAL"
		case strings.Contains(name, "governor") ||
			strings.Contains(name, "secretary of state") ||
			strings.Contains(name, "attorney general") ||
			strings.Contains(name, "treasurer") ||
			strings.Contains(name, "auditor") ||
			strings.Contains(name, "comptroller") ||
			strings.Contains(name, "lieutenant governor"):
			return "STATE_EXEC"
		case strings.Contains(name, "senate") || strings.Contains(name, "senator"):
			return "STATE_UPPER"
		default:
			// House, Representative, Assembly, Delegate, etc.
			return "STATE_LOWER"
		}

	case "COUNTY":
		return "COUNTY"

	case "CITY":
		if strings.Contains(name, "mayor") {
			return "LOCAL_EXEC"
		}
		return "LOCAL"

	case "LOCAL":
		switch {
		case strings.Contains(name, "school"):
			return "SCHOOL"
		case judicial:
			return "JUDICIAL"
		default:
			return "LOCAL"
		}

	case "TOWNSHIP":
		if strings.Contains(name, "mayor") || (strings.Contains(name, "trustee") && strings.Contains(name, "president")) {
			return "LOCAL_EXEC"
		}
		return "LOCAL"

	case "REGIONAL":
		return "COUNTY"
	}

	// Fallback
	return "LOCAL"
}

// TransformToNormalized converts a BallotReady OfficeHolderNode to a NormalizedOfficial.
// Returns nil if the node has no person data.
func TransformToNormalized(node OfficeHolderNode) *provider.NormalizedOfficial {
	if node.Person == nil {
		return nil
	}

	person := node.Person

	// Determine party from first party entry
	party := ""
	if len(node.Parties) > 0 {
		party = node.Parties[0].Name
	}

	// Build URLs list from all url entries
	urls := make([]string, 0, len(node.URLs))
	for _, u := range node.URLs {
		if u.URL != "" {
			urls = append(urls, u.URL)
		}
	}

	// Build identifiers from URLs by type
	identifiers := make([]provider.NormalizedIdentifier, 0)
	for _, u := range node.URLs {
		if u.URL == "" {
			continue
		}
		urlType := strings.ToLower(u.Type)
		switch {
		case strings.Contains(urlType, "twitter") || strings.Contains(urlType, "x.com"):
			identifiers = append(identifiers, provider.NormalizedIdentifier{
				IdentifierType:  "twitter",
				IdentifierValue: u.URL,
			})
		case strings.Contains(urlType, "facebook"):
			identifiers = append(identifiers, provider.NormalizedIdentifier{
				IdentifierType:  "facebook",
				IdentifierValue: u.URL,
			})
		case strings.Contains(urlType, "instagram"):
			identifiers = append(identifiers, provider.NormalizedIdentifier{
				IdentifierType:  "instagram",
				IdentifierValue: u.URL,
			})
		case strings.Contains(urlType, "linkedin"):
			identifiers = append(identifiers, provider.NormalizedIdentifier{
				IdentifierType:  "linkedin",
				IdentifierValue: u.URL,
			})
		}
	}

	// Build email list from contacts
	emails := make([]string, 0)
	for _, c := range node.Contacts {
		if c.Email != "" {
			emails = append(emails, c.Email)
		}
	}

	// Get first phone from contacts for address records
	firstPhone := ""
	for _, c := range node.Contacts {
		if c.Phone != "" {
			firstPhone = c.Phone
			break
		}
	}

	// Transform addresses
	addresses := make([]provider.NormalizedAddress, 0, len(node.Addresses))
	for _, addr := range node.Addresses {
		addresses = append(addresses, provider.NormalizedAddress{
			Address1:   addr.AddressLine1,
			Address2:   addr.AddressLine2,
			Address3:   addr.City,
			State:      addr.State,
			PostalCode: addr.Zip,
			Phone1:     firstPhone,
		})
	}

	// Determine district type and position name for mapping
	districtType := "LOCAL"
	positionName := ""
	positionState := ""
	positionDBID := 0

	if node.Position != nil {
		positionDBID = node.Position.DatabaseID
		positionState = node.Position.State

		// Use normalizedPosition.name if available, fallback to position.name
		if node.Position.NormalizedPosition != nil {
			positionName = node.Position.NormalizedPosition.Name
		} else {
			positionName = node.Position.Name
		}

		districtType = MapDistrictType(node.Position.Level, node.Position.Judicial, positionName)
	}

	// Build election frequency string from electionFrequencies
	electionFrequency := ""
	if node.Position != nil && len(node.Position.ElectionFrequencies) > 0 {
		freq := node.Position.ElectionFrequencies[0].Frequency
		if len(freq) > 0 {
			parts := make([]string, len(freq))
			for i, f := range freq {
				parts[i] = strconv.Itoa(f)
			}
			electionFrequency = strings.Join(parts, ",")
		}
	}

	// Use officeTitle if available, otherwise fall back to position name
	officeTitle := node.OfficeTitle
	if officeTitle == "" && node.Position != nil {
		officeTitle = node.Position.Name
	}

	// Build district info (BallotReady doesn't separate district and chamber)
	district := provider.NormalizedDistrict{
		ExternalID:   positionDBID,
		DistrictType: districtType,
		State:        positionState,
	}

	if node.Position != nil {
		district.Label = node.Position.Name
		if node.Position.SubAreaValue != "" {
			district.DistrictID = node.Position.SubAreaValue
		}
		if node.Position.NormalizedPosition != nil {
			district.MTFCC = node.Position.NormalizedPosition.MTFCC
		}
		if node.Position.SubAreaName != "" {
			district.Subtype = node.Position.SubAreaName
		}
	}

	// Build chamber info
	chamber := provider.NormalizedChamber{
		ExternalID:        positionDBID,
		ElectionFrequency: electionFrequency,
	}
	if node.Position != nil {
		chamber.Name = node.Position.Name
		chamber.Government = provider.NormalizedGovernment{
			Name:  node.Position.Name,
			State: positionState,
		}
	}

	// Extract position description
	positionDescription := ""
	if node.Position != nil && node.Position.NormalizedPosition != nil {
		positionDescription = node.Position.NormalizedPosition.Description
	}

	// Map images
	images := make([]provider.NormalizedImage, 0, len(person.Images))
	for _, img := range person.Images {
		images = append(images, provider.NormalizedImage{
			URL:  img.URL,
			Type: img.Type,
		})
	}

	// Map degrees
	degrees := make([]provider.NormalizedDegree, 0, len(person.Degrees))
	for _, deg := range person.Degrees {
		degrees = append(degrees, provider.NormalizedDegree{
			ExternalID: deg.ID,
			Degree:     deg.Degree,
			Major:      deg.Major,
			School:     deg.School,
			GradYear:   deg.GradYear,
		})
	}

	// Map experiences
	experiences := make([]provider.NormalizedExperience, 0, len(person.Experiences))
	for _, exp := range person.Experiences {
		experiences = append(experiences, provider.NormalizedExperience{
			ExternalID:   exp.ID,
			Title:        exp.Title,
			Organization: exp.Organization,
			Type:         exp.Type,
			Start:        exp.Start,
			End:          exp.End,
		})
	}

	return &provider.NormalizedOfficial{
		ExternalID:         strconv.Itoa(node.DatabaseID),
		FirstName:          person.FirstName,
		MiddleInitial:      extractMiddleInitial(person.MiddleName),
		LastName:           person.LastName,
		PreferredName:      person.Nickname,
		NameSuffix:         person.Suffix,
		Party:              party,
		URLs:               urls,
		EmailAddresses:     emails,
		Addresses:          addresses,
		Identifiers:        identifiers,
		Committees:         []provider.NormalizedCommittee{},
		Source:             "ballotready",
		ValidFrom:          node.StartAt,
		ValidTo:            node.EndAt,
		BioText:            person.BioText,
		BioguideID:         person.BioguideID,
		Slug:               person.Slug,
		TotalYearsInOffice: node.TotalYearsInOffice,
		Images:             images,
		Degrees:            degrees,
		Experiences:        experiences,
		Office: provider.NormalizedOffice{
			Title:             officeTitle,
			RepresentingState: positionState,
			Description:       positionDescription,
			District:          district,
			Chamber:           chamber,
		},
	}
}

// extractMiddleInitial gets the first letter of a middle name.
func extractMiddleInitial(middleName string) string {
	middleName = strings.TrimSpace(middleName)
	if middleName == "" {
		return ""
	}
	return string(middleName[0])
}

// TransformBatch converts a slice of OfficeHolderNodes to NormalizedOfficials.
// Nodes without person data are skipped.
func TransformBatch(nodes []OfficeHolderNode) []provider.NormalizedOfficial {
	result := make([]provider.NormalizedOfficial, 0, len(nodes))
	for _, node := range nodes {
		if norm := TransformToNormalized(node); norm != nil {
			result = append(result, *norm)
		}
	}
	return result
}

// FilterByDistrictTypes filters officeholder nodes to only include those
// matching the given district types.
func FilterByDistrictTypes(nodes []OfficeHolderNode, districtTypes []string) []OfficeHolderNode {
	if len(districtTypes) == 0 {
		return nodes
	}

	typeSet := make(map[string]bool, len(districtTypes))
	for _, dt := range districtTypes {
		typeSet[strings.ToUpper(dt)] = true
	}

	result := make([]OfficeHolderNode, 0)
	for _, node := range nodes {
		dt := districtTypeForNode(node)
		if typeSet[dt] {
			result = append(result, node)
		}
	}

	return result
}

// districtTypeForNode computes the district type for an OfficeHolderNode.
func districtTypeForNode(node OfficeHolderNode) string {
	if node.Position == nil {
		return "LOCAL"
	}

	positionName := ""
	if node.Position.NormalizedPosition != nil {
		positionName = node.Position.NormalizedPosition.Name
	} else {
		positionName = node.Position.Name
	}

	return MapDistrictType(node.Position.Level, node.Position.Judicial, positionName)
}

// FormatElectionFrequency formats an election frequency array as a human-readable string.
func FormatElectionFrequency(freq []int) string {
	if len(freq) == 0 {
		return ""
	}
	parts := make([]string, len(freq))
	for i, f := range freq {
		parts[i] = fmt.Sprintf("%d", f)
	}
	return strings.Join(parts, ",")
}
