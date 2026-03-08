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
	Source           string    `json:"source"` // "cicero" or "ballotready"
	LastSynced       time.Time `json:"last_synced"`
	IsActive         bool      `json:"is_active" gorm:"default:true"`    // true = currently serving in their primary seat
	IsIncumbent      bool      `json:"is_incumbent" gorm:"default:true"` // false = candidate-only, excluded from default search
	DataSource       string    `json:"data_source,omitempty"`         // e.g. "ballotready", "scraped", "manual"
	TermDatePrecision string   `json:"term_date_precision,omitempty"` // "year", "month", "day"

	// Legislative data fetching
	LegDataFetchedAt *time.Time `json:"leg_data_fetched_at,omitempty" gorm:"index"`
}

type Office struct {
	ID                   uuid.UUID  `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID         *uuid.UUID `json:"politician_id" gorm:"type:uuid;uniqueIndex"`
	ChamberID            uuid.UUID  `json:"chamber_id" gorm:"type:uuid"`
	DistrictID           uuid.UUID  `json:"district_id" gorm:"type:uuid"`
	Title                string     `json:"title"`
	RepresentingState    string     `json:"representing_state"`
	RepresentingCity     string     `json:"representing_city"`
	Description          string     `json:"description"` // Position description from BallotReady
	Seats                int        `json:"seats"`
	NormalizedPositionName string   `json:"normalized_position_name"`
	PartisanType         string     `json:"partisan_type"`
	Salary               string     `json:"salary"`
	IsAppointedPosition  bool       `json:"is_appointed_position"`
	IsVacant             bool       `json:"is_vacant" gorm:"default:false"`
	VacantSince          *time.Time `json:"vacant_since,omitempty"`
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
	Type         string    `json:"type"`                          // "default", "thumb"
	PhotoLicense string    `json:"photo_license,omitempty"`       // "cc_by_sa", "press_use", "scraped_no_license", etc.
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
	ID                   uuid.UUID  `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID         uuid.UUID  `json:"politician_id" gorm:"type:uuid;uniqueIndex:idx_unique_endorsement"`
	OrganizationID       *uuid.UUID `json:"organization_id,omitempty" gorm:"type:uuid;uniqueIndex:idx_unique_endorsement"`
	EndorserString       string     `json:"endorser_string"` // Raw endorser text if organization not resolved
	Recommendation       string     `json:"recommendation"`  // "PRO", "CON", or empty
	Status               string     `json:"status"`          // "endorsed", "not_endorsed", etc.
	ElectionDate         string     `json:"election_date"`
	CandidacyExternalID  string     `json:"candidacy_external_id" gorm:"uniqueIndex:idx_unique_endorsement"` // Links to specific race
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
	IsActive            bool      `json:"is_active" gorm:"default:false"` // Manually set to true for currently running candidates
}

type PoliticianContact struct {
	ID              uuid.UUID  `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID    uuid.UUID  `json:"politician_id" gorm:"type:uuid;index"`
	Source          string     `json:"source"` // "person", "officeholder", or "scraped"
	Email           string     `json:"email"`
	Phone           string     `json:"phone"`
	Fax             string     `json:"fax"`
	WebsiteURL      string     `json:"website_url,omitempty"`
	ContactType     string     `json:"contact_type"` // "district", "capitol", "city_website", etc.
	ContactSyncedAt *time.Time `json:"contact_synced_at,omitempty"`
}

// PositionDescription provides reusable descriptions for positions by normalized name.
// When an office has no description of its own (e.g., scraped data), the API falls back
// to this table: first matching (normalized_position_name, district_type), then generic
// (normalized_position_name, district_type = '').
type PositionDescription struct {
	ID                     uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	NormalizedPositionName string    `json:"normalized_position_name" gorm:"uniqueIndex:idx_pos_desc_lookup"`
	DistrictType           string    `json:"district_type" gorm:"uniqueIndex:idx_pos_desc_lookup;default:''"` // empty = generic/all
	Description            string    `json:"description"`
	Source                 string    `json:"source"` // "ballotready", "manual"
}

// BuildingPhoto stores city hall / government building photos for place-based display.
// Keyed by Census GEOID (e.g., "0644000" for City of Los Angeles).
type BuildingPhoto struct {
	PlaceGeoid  string    `json:"place_geoid" gorm:"primaryKey;size:20"`
	URL         string    `json:"url"`         // Supabase CDN URL
	SourceURL   string    `json:"source_url"`  // Original Wikimedia URL
	License     string    `json:"license"`     // e.g., "cc_by_sa"
	Attribution string    `json:"attribution"` // Author/uploader credit
	WikiTitle   string    `json:"wiki_title"`  // Wikimedia file title for re-fetch
	FetchedAt   time.Time `json:"fetched_at"`
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

func (PositionDescription) TableName() string {
	return "essentials.position_descriptions"
}

func (BuildingPhoto) TableName() string {
	return "essentials.building_photos"
}

// Quote stores a curated politician quote for the Read & Rank feature.
// Deduplication is handled at import time by matching on (politician_id, topic_key, source_url).
// The same politician can have multiple quotes per topic (e.g., different quotes on the same issue).
type Quote struct {
	ID           uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID uuid.UUID `json:"politician_id" gorm:"type:uuid;not null;index:idx_quotes_politician"`
	TopicKey     string    `json:"topic_key" gorm:"not null;index:idx_quotes_topic_key"`
	QuoteText    string    `json:"quote_text" gorm:"type:text;not null"`
	SourceURL    string    `json:"source_url"`
	SourceName   string    `json:"source_name"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func (Quote) TableName() string {
	return "essentials.quotes"
}

// ===== Phase 54: Legislative Data Foundation =====

// LegislativeSession represents a congressional or legislative session
type LegislativeSession struct {
	ID           uuid.UUID  `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	Jurisdiction string     `json:"jurisdiction"` // "federal", "indiana", "california", "bloomington-in", "la-county-ca"
	Name         string     `json:"name"`         // "119th Congress", "2025 IN Regular Session"
	StartDate    *time.Time `json:"start_date"`
	EndDate      *time.Time `json:"end_date"`
	IsCurrent    bool       `json:"is_current"`
	ExternalID   string     `json:"external_id"` // congress number, LegiScan session ID
	Source       string     `json:"source"`       // "congress-legislators", "legiscan", "manual"
}

func (LegislativeSession) TableName() string { return "essentials.legislative_sessions" }

// LegislativeCommittee represents a committee or subcommittee
type LegislativeCommittee struct {
	ID           uuid.UUID  `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	SessionID    *uuid.UUID `json:"session_id,omitempty" gorm:"type:uuid"`
	ParentID     *uuid.UUID `json:"parent_id,omitempty" gorm:"type:uuid"` // self-referential for subcommittees
	ExternalID   string     `json:"external_id" gorm:"uniqueIndex:idx_committee_ext"`
	Jurisdiction string     `json:"jurisdiction" gorm:"uniqueIndex:idx_committee_ext"`
	Name         string     `json:"name"`
	Type         string     `json:"type"`    // "committee", "subcommittee", "joint"
	Chamber      string     `json:"chamber"` // "house", "senate", "joint", "local"
	IsCurrent    bool       `json:"is_current"`
	Source       string     `json:"source"` // "congress-legislators", "legiscan", "manual"
}

func (LegislativeCommittee) TableName() string { return "essentials.legislative_committees" }

// LegislativeCommitteeMembership links a politician to a committee with role
type LegislativeCommitteeMembership struct {
	ID             uuid.UUID  `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	CommitteeID    uuid.UUID  `json:"committee_id" gorm:"type:uuid;uniqueIndex:idx_cmember"`
	PoliticianID   uuid.UUID  `json:"politician_id" gorm:"type:uuid;uniqueIndex:idx_cmember"`
	CongressNumber int        `json:"congress_number" gorm:"uniqueIndex:idx_cmember"` // 119 for 119th Congress; 0 for non-federal
	Role           string     `json:"role"`      // "member", "chair", "vice_chair", "ranking_member", "ex_officio"
	IsCurrent      bool       `json:"is_current"`
	SessionID      *uuid.UUID `json:"session_id,omitempty" gorm:"type:uuid"`
}

func (LegislativeCommitteeMembership) TableName() string {
	return "essentials.legislative_committee_memberships"
}

// LegislativeLeadershipRole represents leadership positions (Speaker, Majority Leader, etc.)
type LegislativeLeadershipRole struct {
	ID           uuid.UUID  `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID uuid.UUID  `json:"politician_id" gorm:"type:uuid;uniqueIndex:idx_leadership"`
	SessionID    *uuid.UUID `json:"session_id,omitempty" gorm:"type:uuid;uniqueIndex:idx_leadership"`
	Chamber      string     `json:"chamber" gorm:"uniqueIndex:idx_leadership"` // "house", "senate", "local"
	Title        string     `json:"title"` // "Speaker", "Majority Leader", "President Pro Tempore"
	StartDate    *time.Time `json:"start_date"`
	EndDate      *time.Time `json:"end_date"`
	IsCurrent    bool       `json:"is_current"`
	Source       string     `json:"source"`
}

func (LegislativeLeadershipRole) TableName() string {
	return "essentials.legislative_leadership_roles"
}

// LegislativeBill represents a bill, resolution, ordinance, or motion
type LegislativeBill struct {
	ID           uuid.UUID      `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	SessionID    uuid.UUID      `json:"session_id" gorm:"type:uuid"`
	ExternalID   string         `json:"external_id" gorm:"uniqueIndex:idx_bill_ext"`
	Jurisdiction string         `json:"jurisdiction" gorm:"uniqueIndex:idx_bill_ext"`
	Number       string         `json:"number"`      // "HB 1044", "SB 123"
	Title        string         `json:"title"`
	Summary      string         `json:"summary" gorm:"type:text"` // CRS plain-language summary (federal) or empty
	RawStatus    string         `json:"raw_status"`
	StatusLabel  string         `json:"status_label"` // Normalized: "In Committee", "Passed", "Signed"
	SponsorID    *uuid.UUID     `json:"sponsor_id,omitempty" gorm:"type:uuid"`
	IntroducedAt *time.Time     `json:"introduced_at"`
	PassedAt     *time.Time     `json:"passed_at"`
	SignedAt      *time.Time     `json:"signed_at"`
	TopicTags    pq.StringArray `json:"topic_tags" gorm:"type:text[]"`
	URL          string         `json:"url"`
	Source       string         `json:"source"` // "congress", "legiscan", "manual"
}

func (LegislativeBill) TableName() string { return "essentials.legislative_bills" }

// LegislativeBillCosponsor links a politician to a bill as cosponsor
type LegislativeBillCosponsor struct {
	ID           uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	BillID       uuid.UUID `json:"bill_id" gorm:"type:uuid;uniqueIndex:idx_cosponsor"`
	PoliticianID uuid.UUID `json:"politician_id" gorm:"type:uuid;uniqueIndex:idx_cosponsor"`
}

func (LegislativeBillCosponsor) TableName() string {
	return "essentials.legislative_bill_cosponsors"
}

// LegislativeVote represents an individual member vote on a roll call
type LegislativeVote struct {
	ID             uuid.UUID  `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID   uuid.UUID  `json:"politician_id" gorm:"type:uuid;uniqueIndex:idx_leg_vote;index"`
	BillID         *uuid.UUID `json:"bill_id,omitempty" gorm:"type:uuid;uniqueIndex:idx_leg_vote"`
	SessionID      uuid.UUID  `json:"session_id" gorm:"type:uuid;uniqueIndex:idx_leg_vote"`
	ExternalVoteID string     `json:"external_vote_id" gorm:"uniqueIndex:idx_leg_vote"`
	VoteQuestion   string     `json:"vote_question"` // "Passage", "Amendment #3", "Cloture"
	Position       string     `json:"position"`      // "yea", "nay", "abstain", "absent", "not_voting", "present"
	VoteDate       time.Time  `json:"vote_date"`
	Result         string     `json:"result"` // "passed", "failed", "tabled"
	YeaCount       int        `json:"yea_count"`
	NayCount       int        `json:"nay_count"`
	Source         string     `json:"source"` // "congress", "legiscan", "manual"
}

func (LegislativeVote) TableName() string { return "essentials.legislative_votes" }

// LegislativePoliticianIDMap is the cross-source identity bridge table
type LegislativePoliticianIDMap struct {
	ID           uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID uuid.UUID `json:"politician_id" gorm:"type:uuid;not null;uniqueIndex:idx_leg_id_map"`
	IDType       string    `json:"id_type" gorm:"uniqueIndex:idx_leg_id_map"`  // "bioguide", "ocd_person", "legiscan", "legistar", "openstates"
	IDValue      string    `json:"id_value" gorm:"uniqueIndex:idx_leg_id_map"`
	VerifiedAt   time.Time `json:"verified_at"`
	Source       string    `json:"source"` // "congress-legislators-yaml", "manual", "legiscan-lookup"
}

func (LegislativePoliticianIDMap) TableName() string {
	return "essentials.legislative_politician_id_map"
}
