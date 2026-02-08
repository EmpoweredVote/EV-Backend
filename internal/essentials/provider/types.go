package provider

// NormalizedOfficial represents a politician from any data provider in a common format.
// This is the intermediate representation between provider-specific API responses
// and our database models.
type NormalizedOfficial struct {
	// Unique ID from the source system (stored as string for API flexibility)
	ExternalID string `json:"external_id"`

	// Personal info
	FirstName     string `json:"first_name"`
	MiddleInitial string `json:"middle_initial"`
	LastName      string `json:"last_name"`
	PreferredName string `json:"preferred_name"`
	NameSuffix    string `json:"name_suffix"`
	Party         string `json:"party"`

	// Contact info
	WebFormURL     string   `json:"web_form_url"`
	URLs           []string `json:"urls"`
	EmailAddresses []string `json:"email_addresses"`
	PhotoOriginURL string   `json:"photo_origin_url"`

	// Term info
	ValidFrom string `json:"valid_from"`
	ValidTo   string `json:"valid_to"`
	Notes     []string `json:"notes"`

	// New BallotReady fields
	BioText            string              `json:"bio_text"`
	BioguideID         string              `json:"bioguide_id"`
	Slug               string              `json:"slug"`
	TotalYearsInOffice int                 `json:"total_years_in_office"`
	PartyShortName     string              `json:"party_short_name"`
	IsAppointed        bool                `json:"is_appointed"`
	IsVacant           bool                `json:"is_vacant"`
	IsOffCycle         bool                `json:"is_off_cycle"`
	Specificity        string              `json:"specificity"`
	Images             []NormalizedImage   `json:"images"`
	Degrees            []NormalizedDegree  `json:"degrees"`
	Experiences        []NormalizedExperience `json:"experiences"`
	Contacts           []NormalizedContact    `json:"contacts"`

	// Office and position
	Office NormalizedOffice `json:"office"`

	// Addresses
	Addresses []NormalizedAddress `json:"addresses"`

	// Social/platform identifiers
	Identifiers []NormalizedIdentifier `json:"identifiers"`

	// Committee memberships
	Committees []NormalizedCommittee `json:"committees"`

	// Source tracking
	Source string `json:"source"` // "cicero" or "ballotready"

	// Global ID from provider (BallotReady Relay ID for candidacy queries)
	ExternalGlobalID string `json:"external_global_id"`
}

// NormalizedOffice represents an office held by an official.
type NormalizedOffice struct {
	Title                string `json:"title"`
	RepresentingState    string `json:"representing_state"`
	RepresentingCity     string `json:"representing_city"`
	Description          string `json:"description"` // Position description
	Seats                int    `json:"seats"`
	NormalizedPositionName string `json:"normalized_position_name"`
	PartisanType         string `json:"partisan_type"`
	Salary               string `json:"salary"`
	IsAppointedPosition  bool   `json:"is_appointed_position"`

	District NormalizedDistrict `json:"district"`
	Chamber  NormalizedChamber  `json:"chamber"`
}

// NormalizedDistrict represents an electoral district.
type NormalizedDistrict struct {
	ExternalID          int    `json:"external_id"`
	OCDID               string `json:"ocd_id"`
	Label               string `json:"label"`
	DistrictType        string `json:"district_type"` // NATIONAL_EXEC, NATIONAL_UPPER, etc.
	DistrictID          string `json:"district_id"`
	Subtype             string `json:"subtype"`
	State               string `json:"state"`
	City                string `json:"city"`
	MTFCC               string `json:"mtfcc"`
	NumOfficials        int    `json:"num_officials"`
	ValidFrom           string `json:"valid_from"`
	ValidTo             string `json:"valid_to"`
	GeoID               string `json:"geo_id"`
	IsJudicial          bool   `json:"is_judicial"`
	HasUnknownBoundaries bool   `json:"has_unknown_boundaries"`
	Retention           bool   `json:"retention"`
}

// NormalizedChamber represents a legislative chamber or governing body.
type NormalizedChamber struct {
	ExternalID        int    `json:"external_id"`
	Name              string `json:"name"`
	NameFormal        string `json:"name_formal"`
	OfficialCount     int    `json:"official_count"`
	TermLimit         string `json:"term_limit"`
	TermLength        string `json:"term_length"`
	InaugurationRules string `json:"inauguration_rules"`
	ElectionFrequency string `json:"election_frequency"`
	ElectionRules     string `json:"election_rules"`
	VacancyRules      string `json:"vacancy_rules"`
	Remarks           string `json:"remarks"`
	StaggeredTerm     bool   `json:"staggered_term"`

	Government NormalizedGovernment `json:"government"`
}

// NormalizedGovernment represents a government entity.
type NormalizedGovernment struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	State string `json:"state"`
	City  string `json:"city"`
}

// NormalizedAddress represents a contact address.
type NormalizedAddress struct {
	Address1   string `json:"address_1"`
	Address2   string `json:"address_2"`
	Address3   string `json:"address_3"`
	State      string `json:"state"`
	PostalCode string `json:"postal_code"`
	Phone1     string `json:"phone_1"`
	Phone2     string `json:"phone_2"`
}

// NormalizedIdentifier represents a social/platform identifier.
type NormalizedIdentifier struct {
	IdentifierType  string `json:"identifier_type"` // e.g., "twitter", "facebook"
	IdentifierValue string `json:"identifier_value"`
}

// NormalizedCommittee represents a committee membership.
type NormalizedCommittee struct {
	Name     string   `json:"name"`
	URLs     []string `json:"urls"`
	Position string   `json:"position"` // Role in the committee
}

// NormalizedImage represents a profile photo.
type NormalizedImage struct {
	URL  string `json:"url"`
	Type string `json:"type"` // "default", "thumb"
}

// NormalizedDegree represents an educational degree.
type NormalizedDegree struct {
	ExternalID string `json:"external_id"`
	Degree     string `json:"degree"`
	Major      string `json:"major"`
	School     string `json:"school"`
	GradYear   int    `json:"grad_year"`
}

// NormalizedExperience represents work or office history.
type NormalizedExperience struct {
	ExternalID   string `json:"external_id"`
	Title        string `json:"title"`
	Organization string `json:"organization"`
	Type         string `json:"type"` // "elected_office", "employment", "military"
	Start        string `json:"start"`
	End          string `json:"end"` // Can be "Present" or a year
}

// NormalizedContact represents contact information (email, phone, fax).
type NormalizedContact struct {
	Source      string `json:"source"` // "person" or "officeholder"
	Email       string `json:"email"`
	Phone       string `json:"phone"`
	Fax         string `json:"fax"`
	ContactType string `json:"contact_type"` // "district", "capitol", etc.
}

// Phase B: Candidacy data normalized types

// NormalizedCandidacy represents a single candidacy/race participation.
type NormalizedCandidacy struct {
	CandidacyExternalID string                      `json:"candidacy_external_id"` // BallotReady candidacy ID
	Withdrawn           bool                        `json:"withdrawn"`
	Result              string                      `json:"result"` // "WON", "LOST", "RUNOFF", etc.
	PartyName           string                      `json:"party_name"`
	ElectionName        string                      `json:"election_name"`
	ElectionDate        string                      `json:"election_date"`
	PositionName        string                      `json:"position_name"`
	IsPrimary           bool                        `json:"is_primary"`
	IsRunoff            bool                        `json:"is_runoff"`
	IsUnexpiredTerm     bool                        `json:"is_unexpired_term"`
	Endorsements        []NormalizedEndorsement     `json:"endorsements"`
	Stances             []NormalizedStance          `json:"stances"`
}

// NormalizedEndorsement represents an endorsement from an organization.
type NormalizedEndorsement struct {
	CandidacyExternalID string                          `json:"candidacy_external_id"`
	EndorserString      string                          `json:"endorser_string"` // Raw endorser text
	Recommendation      string                          `json:"recommendation"`  // "PRO", "CON"
	Status              string                          `json:"status"`
	ElectionDate        string                          `json:"election_date"`
	Organization        *NormalizedEndorserOrganization `json:"organization,omitempty"`
}

// NormalizedEndorserOrganization represents an endorsing organization.
type NormalizedEndorserOrganization struct {
	ExternalID  string `json:"external_id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	LogoURL     string `json:"logo_url"`
	IssueName   string `json:"issue_name"`
	State       string `json:"state"`
}

// NormalizedStance represents a politician's stance on an issue.
type NormalizedStance struct {
	CandidacyExternalID string           `json:"candidacy_external_id"`
	Statement           string           `json:"statement"`
	ReferenceURL        string           `json:"reference_url"`
	Locale              string           `json:"locale"`
	ElectionDate        string           `json:"election_date"`
	Issue               *NormalizedIssue `json:"issue"`
}

// NormalizedIssue represents a political issue.
type NormalizedIssue struct {
	ExternalID   string           `json:"external_id"`
	Name         string           `json:"name"`
	Key          string           `json:"key"`
	ExpandedText string           `json:"expanded_text"`
	Parent       *NormalizedIssue `json:"parent,omitempty"`
}
