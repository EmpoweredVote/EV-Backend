package cicero

// CiceroAPIResponse is the top-level response from the Cicero API.
type CiceroAPIResponse struct {
	Response struct {
		Results struct {
			Candidates []struct {
				MatchAddress   string `json:"match_addr"`
				MatchCity      string `json:"match_city"`
				MatchSubregion string `json:"match_subregion"`
				MatchRegion    string `json:"match_region"`
				MatchPostal    string `json:"match_postal"`
				Count          struct {
					From  int `json:"from"`
					To    int `json:"to"`
					Total int `json:"total"`
				} `json:"count"`
				Officials []CiceroOfficial `json:"officials"`
			} `json:"candidates"`
		} `json:"results"`
	} `json:"response"`
}

// CiceroOfficial represents an elected official from the Cicero API.
type CiceroOfficial struct {
	OfficialID     int                `json:"id"`
	FirstName      string             `json:"first_name"`
	MiddleInitial  string             `json:"middle_initial"`
	LastName       string             `json:"last_name"`
	PreferredName  string             `json:"preferred_name"`
	NameSuffix     string             `json:"name_suffix"`
	Party          string             `json:"party"`
	WebFormURL     *string            `json:"web_form_url"`
	Urls           []string           `json:"urls"`
	EmailAddresses []string           `json:"email_addresses"`
	PhotoOriginURL string             `json:"photo_origin_url"`
	Notes          []string           `json:"notes"`
	ValidFrom      string             `json:"valid_from"`
	ValidTo        string             `json:"valid_to"`
	Office         CiceroOffice       `json:"office"`
	Addresses      []CiceroAddress    `json:"addresses"`
	Identifiers    []CiceroIdentifier `json:"identifiers"`
	Committees     []CiceroCommittee  `json:"committees"`
}

// CiceroOffice represents an office held by an official.
type CiceroOffice struct {
	Title             string         `json:"title"`
	RepresentingState string         `json:"representing_state"`
	RepresentingCity  string         `json:"representing_city"`
	District          CiceroDistrict `json:"district"`
	Chamber           CiceroChamber  `json:"chamber"`
}

// CiceroDistrict represents an electoral district.
type CiceroDistrict struct {
	Type           string `json:"district_type"`
	DistrictID     string `json:"district_id"`
	SK             int    `json:"sk"`
	OCDID          string `json:"ocd_id"`
	Subtype        string `json:"subtype"`
	Label          string `json:"label"`
	State          string `json:"state"`
	City           string `json:"city"`
	NumOfficials   int    `json:"num_officials"`
	ValidFrom      string `json:"valid_from"`
	ValidTo        string `json:"valid_to"`
	LastUpdateDate string `json:"last_update_date"`
}

// CiceroChamber represents a legislative chamber.
type CiceroChamber struct {
	ID                int              `json:"id"`
	Name              string           `json:"name"`
	NameFormal        string           `json:"name_formal"`
	OfficialCount     int              `json:"official_count"`
	TermLimit         string           `json:"term_limit"`
	TermLength        string           `json:"term_length"`
	InaugurationRules string           `json:"inauguration_rules"`
	ElectionFrequency string           `json:"election_frequency"`
	ElectionRules     string           `json:"election_rules"`
	VacancyRules      string           `json:"vacancy_rules"`
	Remarks           string           `json:"remarks"`
	Government        CiceroGovernment `json:"government"`
}

// CiceroGovernment represents a government entity.
type CiceroGovernment struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	State string `json:"state"`
	City  string `json:"city"`
}

// CiceroAddress represents a contact address.
type CiceroAddress struct {
	Address1   string `json:"address_1"`
	Address2   string `json:"address_2"`
	Address3   string `json:"address_3"`
	State      string `json:"state"`
	PostalCode string `json:"postal_code"`
	Phone1     string `json:"phone_1"`
	Phone2     string `json:"phone_2"`
}

// CiceroIdentifier represents a social/platform identifier.
type CiceroIdentifier struct {
	ID              int    `json:"id"`
	IdentifierType  string `json:"identifier_type"`
	IdentifierValue string `json:"identifier_value"`
}

// CiceroCommittee represents a committee membership.
type CiceroCommittee struct {
	Name                 string             `json:"name"`
	URLs                 []string           `json:"urls"`
	CommitteeIdentifiers []CiceroIdentifier `json:"committee_identifiers"`
	Position             string             `json:"position"`
}
