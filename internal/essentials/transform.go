package essentials

import (
	"reflect"
	"strings"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/google/uuid"
)

type TransformResults struct {
	Politician           *Politician
	District             *District
	Chamber              *Chamber
	Government           *Government
	Committees           []*Committee
	PoliticianCommittees []PoliticianCommittee
}

func TransformCiceroData(official CiceroOfficial) (TransformResults, error) {
	polID := uuid.New()
	distID := uuid.New()
	chamberID := uuid.New()
	officeID := uuid.New()
	govID := uuid.New()

	// Addresses
	var newAddresses []Address
	for _, address := range official.Addresses {
		newAddresses = append(newAddresses, Address{
			Address1:   address.Address1,
			Address2:   address.Address2,
			Address3:   address.Address3,
			State:      address.State,
			PostalCode: address.PostalCode,
			Phone1:     address.Phone1,
			Phone2:     address.Phone2,
		})
	}

	// Identifiers
	var newIdentifiers []Identifier
	seen := make(map[string]struct{})
	for _, ident := range official.Identifiers {
		t := strings.TrimSpace(ident.IdentifierType)
		v := strings.TrimSpace(ident.IdentifierValue)
		if t == "" || v == "" {
			continue
		}
		key := strings.ToLower(t) + "||" + strings.ToLower(v)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		newIdentifiers = append(newIdentifiers, Identifier{
			PoliticianID:    polID,
			IdentifierType:  t,
			IdentifierValue: v,
		})
	}

	// Office/District/Chamber/Gov
	newOffice := &Office{
		ID:                officeID,
		PoliticianID:      polID,
		ChamberID:         chamberID,
		DistrictID:        distID,
		Title:             official.Office.Title,
		RepresentingState: official.Office.RepresentingState,
		RepresentingCity:  official.Office.RepresentingCity,
	}

	newDistrict := &District{
		ID:             distID,
		ExternalID:     official.Office.District.SK,
		OCDID:          official.Office.District.OCDID,
		Label:          official.Office.District.Label,
		DistrictType:   official.Office.District.Type,
		DistrictID:     official.Office.District.DistrictID,
		Subtype:        official.Office.District.Subtype,
		State:          official.Office.District.State,
		City:           official.Office.District.City,
		NumOfficials:   official.Office.District.NumOfficials,
		ValidFrom:      official.Office.District.ValidFrom,
		ValidTo:        official.Office.District.ValidTo,
		LastUpdateDate: official.Office.District.LastUpdateDate,
	}

	newChamber := &Chamber{
		ID:                chamberID,
		ExternalID:        official.Office.Chamber.ID,
		GovernmentID:      govID,
		Name:              official.Office.Chamber.Name,
		NameFormal:        official.Office.Chamber.NameFormal,
		OfficialCount:     official.Office.Chamber.OfficialCount,
		TermLimit:         official.Office.Chamber.TermLimit,
		TermLength:        official.Office.Chamber.TermLength,
		InaugurationRules: official.Office.Chamber.InaugurationRules,
		ElectionFrequency: official.Office.Chamber.ElectionFrequency,
		ElectionRules:     official.Office.Chamber.ElectionRules,
		VacancyRules:      official.Office.Chamber.VacancyRules,
		Remarks:           official.Office.Chamber.Remarks,
	}

	newGovernment := &Government{
		ID:    govID,
		Name:  official.Office.Chamber.Government.Name,
		Type:  official.Office.Chamber.Government.Type,
		State: official.Office.Chamber.Government.State,
		City:  official.Office.Chamber.Government.City,
	}

	// guard against nil
	webForm := ""
	if official.WebFormURL != nil {
		webForm = *official.WebFormURL
	}

	newPolitician := &Politician{
		ID:             polID,
		ExternalID:     official.OfficialID,
		FirstName:      official.FirstName,
		MiddleInitial:  official.MiddleInitial,
		LastName:       official.LastName,
		PreferredName:  official.PreferredName,
		NameSuffix:     official.NameSuffix,
		Party:          official.Party,
		WebFormURL:     webForm,
		URLs:           official.Urls,
		PhotoOriginURL: official.PhotoOriginURL,
		Notes:          official.Notes,
		OfficeID:       newOffice.ID,
		Addresses:      newAddresses,
		ValidFrom:      official.ValidFrom,
		ValidTo:        official.ValidTo,
		EmailAddresses: official.EmailAddresses,
		Identifiers:    newIdentifiers,
	}

	full := strings.TrimSpace(strings.Join([]string{
		official.FirstName, official.MiddleInitial, official.LastName,
	}, " "))
	newPolitician.FullName = strings.Join(strings.Fields(full), " ") // normalize spaces

	// Dedupe by normalized name, keep first non-empty position per name
	norm := func(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

	commByName := make(map[string]*Committee)
	posByName := make(map[string]string)

	for _, c := range official.Committees {
		if strings.TrimSpace(c.Name) == "" {
			continue
		}
		key := norm(c.Name)
		if _, ok := commByName[key]; !ok {
			commByName[key] = &Committee{
				ID:   uuid.New(),
				Name: c.Name,
				URLs: c.URLs,
			}
		}
		// first non-empty position
		if posByName[key] == "" && strings.TrimSpace(c.Position) != "" {
			posByName[key] = c.Position
		}
	}

	// Resolve committees against DB and build joins
	var finalCommittees []*Committee
	var joins []PoliticianCommittee

	for key, committee := range commByName {
		// Try to find existing by name
		var existing Committee
		err := db.DB.Where("LOWER(name) = LOWER(?)", committee.Name).First(&existing).Error

		var committeeID uuid.UUID
		if err == nil {
			// Found something in DB
			if committeeEqual(*committee, existing) {
				// identical = reuse existing, do not create a new one
				committeeID = existing.ID
			} else {
				// differs = return new committee for upsert
				committeeID = committee.ID
				finalCommittees = append(finalCommittees, committee)
			}
		} else {
			// not found = create new
			committeeID = committee.ID
			finalCommittees = append(finalCommittees, committee)
		}

		joins = append(joins, PoliticianCommittee{
			ID:           uuid.New(),
			PoliticianID: polID,
			CommitteeID:  committeeID,
			Position:     posByName[key], // may be ""
		})
	}

	results := TransformResults{
		Politician:           newPolitician,
		District:             newDistrict,
		Chamber:              newChamber,
		Government:           newGovernment,
		Committees:           finalCommittees, // only the ones to insert/update
		PoliticianCommittees: joins,           // always link the politician
	}

	// Omit unchanged entities (politician/district/chamber/government)
	newPolitician, _ = CheckAndOmitIfEqual(newPolitician, "external_id = ?", newPolitician.ExternalID, politicianEqual)
	newDistrict, _ = CheckAndOmitIfEqual(newDistrict, "external_id = ?", newDistrict.ExternalID, districtEqual)
	newChamber, _ = CheckAndOmitIfEqual(newChamber, "external_id = ?", newChamber.ExternalID, chambersEqual)

	// refresh results with possibly nil pointers
	results.Politician = newPolitician
	results.District = newDistrict
	results.Chamber = newChamber
	results.Government = newGovernment

	return results, nil
}

func CheckAndOmitIfEqual[T any](model *T, where string, arg any, equalFn func(a, b T) bool) (*T, error) {
	if model == nil {
		return nil, nil
	}
	var existing T
	err := db.DB.Where(where, arg).First(&existing).Error
	if err == nil && equalFn(*model, existing) {
		return nil, nil
	}
	return model, nil
}

// Equality helpers (ignore ID)

func chambersEqual(a, b Chamber) bool {
	a.ID = uuid.Nil
	b.ID = uuid.Nil
	return reflect.DeepEqual(a, b)
}

func districtEqual(a, b District) bool {
	a.ID = uuid.Nil
	b.ID = uuid.Nil
	return reflect.DeepEqual(a, b)
}

func politicianEqual(a, b Politician) bool {
	a.ID = uuid.Nil
	b.ID = uuid.Nil
	return reflect.DeepEqual(a, b)
}

func committeeEqual(a, b Committee) bool {
	a.ID = uuid.Nil
	b.ID = uuid.Nil
	return reflect.DeepEqual(a, b)
}
