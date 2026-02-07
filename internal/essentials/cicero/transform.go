package cicero

import (
	"strconv"

	"github.com/EmpoweredVote/EV-Backend/internal/essentials/provider"
)

// TransformToNormalized converts a CiceroOfficial to a NormalizedOfficial.
func TransformToNormalized(off CiceroOfficial) provider.NormalizedOfficial {
	// Handle nil WebFormURL
	webFormURL := ""
	if off.WebFormURL != nil {
		webFormURL = *off.WebFormURL
	}

	// Transform addresses
	addresses := make([]provider.NormalizedAddress, 0, len(off.Addresses))
	for _, addr := range off.Addresses {
		addresses = append(addresses, provider.NormalizedAddress{
			Address1:   addr.Address1,
			Address2:   addr.Address2,
			Address3:   addr.Address3,
			State:      addr.State,
			PostalCode: addr.PostalCode,
			Phone1:     addr.Phone1,
			Phone2:     addr.Phone2,
		})
	}

	// Transform identifiers
	identifiers := make([]provider.NormalizedIdentifier, 0, len(off.Identifiers))
	for _, ident := range off.Identifiers {
		identifiers = append(identifiers, provider.NormalizedIdentifier{
			IdentifierType:  ident.IdentifierType,
			IdentifierValue: ident.IdentifierValue,
		})
	}

	// Transform committees
	committees := make([]provider.NormalizedCommittee, 0, len(off.Committees))
	for _, comm := range off.Committees {
		committees = append(committees, provider.NormalizedCommittee{
			Name:     comm.Name,
			URLs:     comm.URLs,
			Position: comm.Position,
		})
	}

	return provider.NormalizedOfficial{
		ExternalID:     strconv.Itoa(off.OfficialID),
		FirstName:      off.FirstName,
		MiddleInitial:  off.MiddleInitial,
		LastName:       off.LastName,
		PreferredName:  off.PreferredName,
		NameSuffix:     off.NameSuffix,
		Party:          off.Party,
		WebFormURL:     webFormURL,
		URLs:           off.Urls,
		EmailAddresses: off.EmailAddresses,
		PhotoOriginURL: off.PhotoOriginURL,
		ValidFrom:      off.ValidFrom,
		ValidTo:        off.ValidTo,
		Notes:          off.Notes,
		Addresses:      addresses,
		Identifiers:    identifiers,
		Committees:     committees,
		Source:         "cicero",
		Office: provider.NormalizedOffice{
			Title:             off.Office.Title,
			RepresentingState: off.Office.RepresentingState,
			RepresentingCity:  off.Office.RepresentingCity,
			District: provider.NormalizedDistrict{
				ExternalID:   off.Office.District.SK,
				OCDID:        off.Office.District.OCDID,
				Label:        off.Office.District.Label,
				DistrictType: off.Office.District.Type,
				DistrictID:   off.Office.District.DistrictID,
				Subtype:      off.Office.District.Subtype,
				State:        off.Office.District.State,
				City:         off.Office.District.City,
				NumOfficials: off.Office.District.NumOfficials,
				ValidFrom:    off.Office.District.ValidFrom,
				ValidTo:      off.Office.District.ValidTo,
			},
			Chamber: provider.NormalizedChamber{
				ExternalID:        off.Office.Chamber.ID,
				Name:              off.Office.Chamber.Name,
				NameFormal:        off.Office.Chamber.NameFormal,
				OfficialCount:     off.Office.Chamber.OfficialCount,
				TermLimit:         off.Office.Chamber.TermLimit,
				TermLength:        off.Office.Chamber.TermLength,
				InaugurationRules: off.Office.Chamber.InaugurationRules,
				ElectionFrequency: off.Office.Chamber.ElectionFrequency,
				ElectionRules:     off.Office.Chamber.ElectionRules,
				VacancyRules:      off.Office.Chamber.VacancyRules,
				Remarks:           off.Office.Chamber.Remarks,
				Government: provider.NormalizedGovernment{
					Name:  off.Office.Chamber.Government.Name,
					Type:  off.Office.Chamber.Government.Type,
					State: off.Office.Chamber.Government.State,
					City:  off.Office.Chamber.Government.City,
				},
			},
		},
	}
}

// TransformBatch converts a slice of CiceroOfficials to NormalizedOfficials.
func TransformBatch(officials []CiceroOfficial) []provider.NormalizedOfficial {
	result := make([]provider.NormalizedOfficial, 0, len(officials))
	for _, off := range officials {
		result = append(result, TransformToNormalized(off))
	}
	return result
}
