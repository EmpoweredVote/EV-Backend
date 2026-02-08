package essentials

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/EmpoweredVote/EV-Backend/internal/essentials/cicero"
	"github.com/EmpoweredVote/EV-Backend/internal/essentials/provider"
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

// TransformNormalizedToModels converts a NormalizedOfficial to database models.
// This is the provider-agnostic transformation function.
func TransformNormalizedToModels(off provider.NormalizedOfficial) (TransformResults, error) {
	polID := uuid.New()
	distID := uuid.New()
	chamberID := uuid.New()
	officeID := uuid.New()
	govID := uuid.New()

	// Parse external ID - may be numeric string from Cicero or alphanumeric from BallotReady
	externalID := 0
	if id, err := strconv.Atoi(off.ExternalID); err == nil {
		externalID = id
	}

	// Addresses
	var newAddresses []Address
	for _, addr := range off.Addresses {
		newAddresses = append(newAddresses, Address{
			Address1:   addr.Address1,
			Address2:   addr.Address2,
			Address3:   addr.Address3,
			State:      addr.State,
			PostalCode: addr.PostalCode,
			Phone1:     addr.Phone1,
			Phone2:     addr.Phone2,
		})
	}

	// Identifiers (dedupe)
	var newIdentifiers []Identifier
	seen := make(map[string]struct{})
	for _, ident := range off.Identifiers {
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

	// Office/District/Chamber/Government
	newOffice := &Office{
		ID:                   officeID,
		PoliticianID:         polID,
		ChamberID:            chamberID,
		DistrictID:           distID,
		Title:                off.Office.Title,
		RepresentingState:    off.Office.RepresentingState,
		Description:          off.Office.Description,
		RepresentingCity:     off.Office.RepresentingCity,
		Seats:                off.Office.Seats,
		NormalizedPositionName: off.Office.NormalizedPositionName,
		PartisanType:         off.Office.PartisanType,
		Salary:               off.Office.Salary,
		IsAppointedPosition:  off.Office.IsAppointedPosition,
	}

	newDistrict := &District{
		ID:                  distID,
		ExternalID:          off.Office.District.ExternalID,
		OCDID:               off.Office.District.OCDID,
		Label:               off.Office.District.Label,
		DistrictType:        off.Office.District.DistrictType,
		DistrictID:          off.Office.District.DistrictID,
		Subtype:             off.Office.District.Subtype,
		State:               off.Office.District.State,
		City:                off.Office.District.City,
		MTFCC:               off.Office.District.MTFCC,
		NumOfficials:        off.Office.District.NumOfficials,
		ValidFrom:           off.Office.District.ValidFrom,
		ValidTo:             off.Office.District.ValidTo,
		GeoID:               off.Office.District.GeoID,
		IsJudicial:          off.Office.District.IsJudicial,
		HasUnknownBoundaries: off.Office.District.HasUnknownBoundaries,
		Retention:           off.Office.District.Retention,
	}

	newChamber := &Chamber{
		ID:                chamberID,
		ExternalID:        off.Office.Chamber.ExternalID,
		GovernmentID:      govID,
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
		StaggeredTerm:     off.Office.Chamber.StaggeredTerm,
	}

	newGovernment := &Government{
		ID:    govID,
		Name:  off.Office.Chamber.Government.Name,
		Type:  off.Office.Chamber.Government.Type,
		State: off.Office.Chamber.Government.State,
		City:  off.Office.Chamber.Government.City,
	}

	// Map images
	newImages := make([]PoliticianImage, 0, len(off.Images))
	for _, img := range off.Images {
		newImages = append(newImages, PoliticianImage{
			ID:           uuid.New(),
			PoliticianID: polID,
			URL:          img.URL,
			Type:         img.Type,
		})
	}

	// Map degrees
	newDegrees := make([]Degree, 0, len(off.Degrees))
	for _, deg := range off.Degrees {
		newDegrees = append(newDegrees, Degree{
			ID:           uuid.New(),
			PoliticianID: polID,
			ExternalID:   deg.ExternalID,
			Degree:       deg.Degree,
			Major:        deg.Major,
			School:       deg.School,
			GradYear:     deg.GradYear,
		})
	}

	// Map experiences
	newExperiences := make([]Experience, 0, len(off.Experiences))
	for _, exp := range off.Experiences {
		newExperiences = append(newExperiences, Experience{
			ID:           uuid.New(),
			PoliticianID: polID,
			ExternalID:   exp.ExternalID,
			Title:        exp.Title,
			Organization: exp.Organization,
			Type:         exp.Type,
			Start:        exp.Start,
			End:          exp.End,
		})
	}

	// Map contacts (person-level and officeholder-level)
	newContacts := make([]PoliticianContact, 0, len(off.Contacts))
	for _, contact := range off.Contacts {
		newContacts = append(newContacts, PoliticianContact{
			ID:           uuid.New(),
			PoliticianID: polID,
			Source:       contact.Source,
			Email:        contact.Email,
			Phone:        contact.Phone,
			Fax:          contact.Fax,
			ContactType:  contact.ContactType,
		})
	}

	newPolitician := &Politician{
		ID:                 polID,
		ExternalID:         externalID,
		ExternalGlobalID:   off.ExternalGlobalID,
		FirstName:          off.FirstName,
		MiddleInitial:      off.MiddleInitial,
		LastName:           off.LastName,
		PreferredName:      off.PreferredName,
		NameSuffix:         off.NameSuffix,
		Party:              off.Party,
		PartyShortName:     off.PartyShortName,
		WebFormURL:         off.WebFormURL,
		URLs:               off.URLs,
		PhotoOriginURL:     off.PhotoOriginURL,
		Notes:              off.Notes,
		OfficeID:           newOffice.ID,
		Addresses:          newAddresses,
		ValidFrom:          off.ValidFrom,
		ValidTo:            off.ValidTo,
		EmailAddresses:     off.EmailAddresses,
		Identifiers:        newIdentifiers,
		Source:             off.Source,
		BioText:            off.BioText,
		BioguideID:         off.BioguideID,
		Slug:               off.Slug,
		TotalYearsInOffice: off.TotalYearsInOffice,
		IsAppointed:        off.IsAppointed,
		IsVacant:           off.IsVacant,
		IsOffCycle:         off.IsOffCycle,
		Specificity:        off.Specificity,
		Images:             newImages,
		Degrees:            newDegrees,
		Experiences:        newExperiences,
		Contacts:           newContacts,
	}

	// Build full name
	full := strings.TrimSpace(strings.Join([]string{
		off.FirstName, off.MiddleInitial, off.LastName,
	}, " "))
	newPolitician.FullName = strings.Join(strings.Fields(full), " ")

	// Committees - dedupe by normalized name
	norm := func(s string) string { return strings.ToLower(strings.TrimSpace(s)) }
	commByName := make(map[string]*Committee)
	posByName := make(map[string]string)

	for _, c := range off.Committees {
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
		if posByName[key] == "" && strings.TrimSpace(c.Position) != "" {
			posByName[key] = c.Position
		}
	}

	// Resolve committees against DB and build joins
	var finalCommittees []*Committee
	var joins []PoliticianCommittee

	for key, committee := range commByName {
		var existing Committee
		err := db.DB.Where("LOWER(name) = LOWER(?)", committee.Name).First(&existing).Error

		var committeeID uuid.UUID
		if err == nil {
			if committeeEqual(*committee, existing) {
				committeeID = existing.ID
			} else {
				committeeID = committee.ID
				finalCommittees = append(finalCommittees, committee)
			}
		} else {
			committeeID = committee.ID
			finalCommittees = append(finalCommittees, committee)
		}

		joins = append(joins, PoliticianCommittee{
			ID:           uuid.New(),
			PoliticianID: polID,
			CommitteeID:  committeeID,
			Position:     posByName[key],
		})
	}

	results := TransformResults{
		Politician:           newPolitician,
		District:             newDistrict,
		Chamber:              newChamber,
		Government:           newGovernment,
		Committees:           finalCommittees,
		PoliticianCommittees: joins,
	}

	// Omit unchanged entities
	newPolitician, _ = CheckAndOmitIfEqual(newPolitician, "external_id = ?", newPolitician.ExternalID, politicianEqual)
	newDistrict, _ = CheckAndOmitIfEqual(newDistrict, "external_id = ?", newDistrict.ExternalID, districtEqual)
	newChamber, _ = CheckAndOmitIfEqual(newChamber, "external_id = ?", newChamber.ExternalID, chambersEqual)

	results.Politician = newPolitician
	results.District = newDistrict
	results.Chamber = newChamber
	results.Government = newGovernment

	return results, nil
}

// TransformCiceroOfficialToNormalized is a helper that converts a Cicero official
// to normalized format using the cicero package transformer.
func TransformCiceroOfficialToNormalized(off cicero.CiceroOfficial) provider.NormalizedOfficial {
	return cicero.TransformToNormalized(off)
}
