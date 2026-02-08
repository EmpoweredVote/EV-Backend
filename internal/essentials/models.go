package essentials

import (
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

type Politician struct {
	ID             uuid.UUID             `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	ExternalID     int                   `json:"external_id" gorm:"uniqueIndex"`
	OfficeID       uuid.UUID             `json:"office_id" gorm:"type:uuid"`
	ValidFrom      string                `json:"valid_from"`
	ValidTo        string                `json:"valid_to"`
	LastUpdateDate string                `json:"last_update_date"`
	FirstName      string                `json:"first_name"`
	MiddleInitial  string                `json:"middle_initial"`
	LastName       string                `json:"last_name"`
	FullName       string                `json:"full_name"`
	PreferredName  string                `json:"preferred_name"`
	NameSuffix     string                `json:"name_suffix"`
	Party          string                `json:"party"`
	Addresses      []Address             `json:"addresses" gorm:"foreignKey:PoliticianID"`
	EmailAddresses pq.StringArray        `json:"email_addresses" gorm:"type:text[]"`
	URLs           pq.StringArray        `json:"urls" gorm:"type:text[]"`
	WebFormURL     string                `json:"web_form_url"`
	PhotoOriginURL string                `json:"photo_origin_url"`
	PhotoCustomURL *string               `json:"-"`
	Identifiers    []Identifier          `json:"identifiers" gorm:"foreignKey:PoliticianID"`
	Notes          pq.StringArray        `json:"notes" gorm:"type:text[]"`
	Committees     []PoliticianCommittee `json:"committees" gorm:"foreignKey:PoliticianID"`

	// New BallotReady fields
	BioText            string            `json:"bio_text"`
	BioguideID         string            `json:"bioguide_id"`
	Slug               string            `json:"slug"`
	TotalYearsInOffice int               `json:"total_years_in_office"`
	PartyShortName     string            `json:"party_short_name"`
	IsAppointed        bool              `json:"is_appointed"`
	IsVacant           bool              `json:"is_vacant"`
	IsOffCycle         bool              `json:"is_off_cycle"`
	Specificity        string            `json:"specificity"`
	ExternalGlobalID   string            `json:"external_global_id"` // Relay-style base64 ID for candidacy queries
	Images             []PoliticianImage  `json:"images" gorm:"foreignKey:PoliticianID"`
	Degrees            []Degree           `json:"degrees" gorm:"foreignKey:PoliticianID"`
	Experiences        []Experience       `json:"experiences" gorm:"foreignKey:PoliticianID"`
	Contacts           []PoliticianContact `json:"contacts" gorm:"foreignKey:PoliticianID"`

	// Provenance / Syncing
	Source     string    `json:"source"` // "cicero" or "ballotready"
	LastSynced time.Time `json:"last_synced"`
}

type Office struct {
	ID                   uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID         uuid.UUID `json:"politician_id" gorm:"type:uuid;uniqueIndex"`
	ChamberID            uuid.UUID `json:"chamber_id" gorm:"type:uuid"`
	DistrictID           uuid.UUID `json:"district_id" gorm:"type:uuid"`
	Title                string    `json:"title"`
	RepresentingState    string    `json:"representing_state"`
	RepresentingCity     string    `json:"representing_city"`
	Description          string    `json:"description"` // Position description from BallotReady
	Seats                int       `json:"seats"`
	NormalizedPositionName string    `json:"normalized_position_name"`
	PartisanType         string    `json:"partisan_type"`
	Salary               string    `json:"salary"`
	IsAppointedPosition  bool      `json:"is_appointed_position"`
}

type Chamber struct {
	ID                uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	ExternalID        int       `json:"external_id" gorm:"uniqueIndex"`
	GovernmentID      uuid.UUID `json:"government_id" gorm:"type:uuid"`
	Name              string    `json:"name"`
	NameFormal        string    `json:"name_formal"`
	OfficialCount     int       `json:"official_count"` // Can be null
	TermLimit         string    `json:"term_limit"`
	TermLength        string    `json:"term_length"`
	InaugurationRules string    `json:"inauguration_rules"`
	ElectionFrequency string    `json:"election_frequency"`
	ElectionRules     string    `json:"election_rules"`
	VacancyRules      string    `json:"vacancy_rules"`
	Remarks           string    `json:"remarks"`
	StaggeredTerm     bool      `json:"staggered_term"`
}

type District struct {
	ID                  uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	ExternalID          int       `json:"external_id" gorm:"uniqueIndex"`
	OCDID               string    `json:"ocd_id"`
	Label               string    `json:"label"`
	DistrictType        string    `json:"district_type"`
	DistrictID          string    `json:"district_id"`
	Subtype             string    `json:"subtype"`
	State               string    `json:"state"`
	City                string    `json:"city"`
	MTFCC               string    `json:"mtfcc"`
	NumOfficials        int       `json:"num_officials"`
	ValidFrom           string    `json:"valid_from"`
	ValidTo             string    `json:"valid_to"`
	LastUpdateDate      string    `json:"-"`
	GeoID               string    `json:"geo_id"`
	IsJudicial          bool      `json:"is_judicial"`
	HasUnknownBoundaries bool      `json:"has_unknown_boundaries"`
	Retention           bool      `json:"retention"`
}

type Government struct {
	ID    uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	Name  string    `json:"name"`
	Type  string    `json:"type"`
	State string    `json:"state"`
	City  string    `json:"city"`
}

type Address struct {
	ID           uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID uuid.UUID `json:"politician_id" gorm:"type:uuid"`
	Address1     string    `json:"address_1"`
	Address2     string    `json:"address_2"`
	Address3     string    `json:"address_3"`
	State        string    `json:"state"`
	PostalCode   string    `json:"postal_code"`
	Phone1       string    `json:"phone_1"`
	Phone2       string    `json:"phone_2"`
}

type Identifier struct {
	ID              uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID    uuid.UUID `json:"politician_id" gorm:"type:uuid;index:uniq_pol_ident,unique"`
	IdentifierType  string    `json:"identifier_type" gorm:"index:uniq_pol_ident,unique"`
	IdentifierValue string    `json:"identifier_value" gorm:"index:uniq_pol_ident,unique"`
}

type Committee struct {
	ID      uuid.UUID             `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	Name    string                `json:"name"`
	URLs    pq.StringArray        `json:"urls" gorm:"type:text[]"`
	Members []PoliticianCommittee `json:"members" gorm:"foreignKey:CommitteeID"`
}

type PoliticianCommittee struct {
	ID           uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID uuid.UUID `json:"politician_id" gorm:"type:uuid;index:uniq_pol_comm,unique"`
	CommitteeID  uuid.UUID `json:"committee_id"  gorm:"type:uuid;index:uniq_pol_comm,unique"`
	Position     string    `json:"position"`
}

type FederalCache struct {
	ID          int       `gorm:"primaryKey;autoIncrement:false;default:1" json:"id"` // Single row
	LastFetched time.Time `json:"last_fetched"`
}

type StateCache struct {
	State       string    `gorm:"primaryKey;size:2" json:"state"` // e.g., "WA", "CA"
	LastFetched time.Time `json:"last_fetched"`
}

type ZipCache struct {
	Zip         string    `gorm:"primaryKey;size:10" json:"zip"`
	State       string    `gorm:"size:2" json:"state"` // Store the state for this ZIP
	LastFetched time.Time `json:"last_fetched"`
}

type ZipPolitician struct {
	Zip          string    `gorm:"primaryKey;size:10" json:"zip"`
	PoliticianID uuid.UUID `gorm:"type:uuid;primaryKey" json:"politician_id"`
	LastSeen     time.Time `json:"last_seen"`
	IsContained  *bool     `json:"is_contained,omitempty"` // Nullable: true=fully contains ZIP, false=partial overlap, null=unknown
}

type PoliticianImage struct {
	ID           uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID uuid.UUID `json:"politician_id" gorm:"type:uuid;index"`
	URL          string    `json:"url"`
	Type         string    `json:"type"` // "default", "thumb"
}

type Degree struct {
	ID           uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID uuid.UUID `json:"politician_id" gorm:"type:uuid;index"`
	ExternalID   string    `json:"external_id"` // ID from BallotReady
	Degree       string    `json:"degree"`      // "Bachelor's", "JD", "Master's", etc.
	Major        string    `json:"major"`
	School       string    `json:"school"`
	GradYear     int       `json:"grad_year"`
}

type Experience struct {
	ID           uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID uuid.UUID `json:"politician_id" gorm:"type:uuid;index"`
	ExternalID   string    `json:"external_id"` // ID from BallotReady
	Title        string    `json:"title"`
	Organization string    `json:"organization"`
	Type         string    `json:"type"` // "elected_office", "employment", "military"
	Start        string    `json:"start"`
	End          string    `json:"end"` // Can be "Present" or a year
}

// Phase B: Candidacy data models

type EndorserOrganization struct {
	ID          uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	ExternalID  string    `json:"external_id" gorm:"uniqueIndex"` // BallotReady endorser ID
	Name        string    `json:"name"`
	Description string    `json:"description"`
	LogoURL     string    `json:"logo_url"`
	IssueName   string    `json:"issue_name"` // e.g., "Education", "Environment"
	State       string    `json:"state"`      // State focus, if applicable
}

type Endorsement struct {
	ID                   uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID         uuid.UUID `json:"politician_id" gorm:"type:uuid;uniqueIndex:idx_unique_endorsement"`
	OrganizationID       uuid.UUID `json:"organization_id" gorm:"type:uuid;uniqueIndex:idx_unique_endorsement"`
	EndorserString       string    `json:"endorser_string"` // Raw endorser text if organization not resolved
	Recommendation       string    `json:"recommendation"`  // "PRO", "CON", or empty
	Status               string    `json:"status"`          // "endorsed", "not_endorsed", etc.
	ElectionDate         string    `json:"election_date"`
	CandidacyExternalID  string    `json:"candidacy_external_id" gorm:"uniqueIndex:idx_unique_endorsement"` // Links to specific race
	Organization         *EndorserOrganization `json:"organization,omitempty" gorm:"foreignKey:OrganizationID"`
}

type Issue struct {
	ID           uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	ExternalID   string    `json:"external_id" gorm:"uniqueIndex"` // BallotReady issue ID
	Name         string    `json:"name"`
	Key          string    `json:"key"`           // Slug/identifier
	ExpandedText string    `json:"expanded_text"` // Full description
	ParentID     *uuid.UUID `json:"parent_id" gorm:"type:uuid"` // Self-referential for sub-issues
	Parent       *Issue    `json:"parent,omitempty" gorm:"foreignKey:ParentID"`
}

type PoliticianStance struct {
	ID                  uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID        uuid.UUID `json:"politician_id" gorm:"type:uuid;uniqueIndex:idx_unique_stance"`
	IssueID             uuid.UUID `json:"issue_id" gorm:"type:uuid;uniqueIndex:idx_unique_stance"`
	Statement           string    `json:"statement"`     // The politician's position
	ReferenceURL        string    `json:"reference_url"` // Source link
	Locale              string    `json:"locale"`        // e.g., "en"
	CandidacyExternalID string    `json:"candidacy_external_id" gorm:"uniqueIndex:idx_unique_stance"` // Links to specific race
	ElectionDate        string    `json:"election_date"`
	Issue               *Issue    `json:"issue,omitempty" gorm:"foreignKey:IssueID"`
}

type ElectionRecord struct {
	ID                  uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID        uuid.UUID `json:"politician_id" gorm:"type:uuid;index"`
	CandidacyExternalID string    `json:"candidacy_external_id" gorm:"uniqueIndex"` // BallotReady candidacy ID
	ElectionName        string    `json:"election_name"`
	ElectionDate        string    `json:"election_date"`
	PositionName        string    `json:"position_name"`
	Result              string    `json:"result"`      // "WON", "LOST", "RUNOFF", etc.
	Withdrawn           bool      `json:"withdrawn"`
	PartyName           string    `json:"party_name"`
	IsPrimary           bool      `json:"is_primary"`
	IsRunoff            bool      `json:"is_runoff"`
	IsUnexpiredTerm     bool      `json:"is_unexpired_term"`
}

type PoliticianContact struct {
	ID           uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID uuid.UUID `json:"politician_id" gorm:"type:uuid;index"`
	Source       string    `json:"source"` // "person" or "officeholder"
	Email        string    `json:"email"`
	Phone        string    `json:"phone"`
	Fax          string    `json:"fax"`
	ContactType  string    `json:"contact_type"` // "district", "capitol", etc.
}

func (Politician) TableName() string {
	return "essentials.politicians"
}

func (Office) TableName() string {
	return "essentials.offices"
}

func (Chamber) TableName() string {
	return "essentials.chambers"
}

func (District) TableName() string {
	return "essentials.districts"
}

func (Government) TableName() string {
	return "essentials.governments"
}

func (Address) TableName() string {
	return "essentials.addresses"
}

func (Identifier) TableName() string {
	return "essentials.identifiers"
}

func (Committee) TableName() string {
	return "essentials.committees"
}

func (PoliticianCommittee) TableName() string {
	return "essentials.politician_committees"
}

func (FederalCache) TableName() string {
	return "essentials.federal_cache"
}

func (StateCache) TableName() string {
	return "essentials.state_caches"
}

func (ZipCache) TableName() string {
	return "essentials.zip_caches"
}

func (ZipPolitician) TableName() string {
	return "essentials.zip_politicians"
}

func (PoliticianImage) TableName() string {
	return "essentials.politician_images"
}

func (Degree) TableName() string {
	return "essentials.degrees"
}

func (Experience) TableName() string {
	return "essentials.experiences"
}

func (EndorserOrganization) TableName() string {
	return "essentials.endorser_organizations"
}

func (Endorsement) TableName() string {
	return "essentials.endorsements"
}

func (Issue) TableName() string {
	return "essentials.issues"
}

func (PoliticianStance) TableName() string {
	return "essentials.politician_stances"
}

func (ElectionRecord) TableName() string {
	return "essentials.election_records"
}

func (PoliticianContact) TableName() string {
	return "essentials.politician_contacts"
}
