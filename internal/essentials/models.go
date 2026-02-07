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
	Images             []PoliticianImage `json:"images" gorm:"foreignKey:PoliticianID"`
	Degrees            []Degree          `json:"degrees" gorm:"foreignKey:PoliticianID"`
	Experiences        []Experience      `json:"experiences" gorm:"foreignKey:PoliticianID"`

	// Provenance / Syncing
	Source     string    `json:"source"` // "cicero" or "ballotready"
	LastSynced time.Time `json:"last_synced"`
}

type Office struct {
	ID                uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID      uuid.UUID `json:"politician_id" gorm:"type:uuid;uniqueIndex"`
	ChamberID         uuid.UUID `json:"chamber_id" gorm:"type:uuid"`
	DistrictID        uuid.UUID `json:"district_id" gorm:"type:uuid"`
	Title             string    `json:"title"`
	RepresentingState string    `json:"representing_state"`
	RepresentingCity  string    `json:"representing_city"`
	Description       string    `json:"description"` // Position description from BallotReady
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
}

type District struct {
	ID             uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	ExternalID     int       `json:"external_id" gorm:"uniqueIndex"`
	OCDID          string    `json:"ocd_id"`
	Label          string    `json:"label"`
	DistrictType   string    `json:"district_type"`
	DistrictID     string    `json:"district_id"`
	Subtype        string    `json:"subtype"`
	State          string    `json:"state"`
	City           string    `json:"city"`
	MTFCC          string    `json:"mtfcc"`
	NumOfficials   int       `json:"num_officials"`
	ValidFrom      string    `json:"valid_from"`
	ValidTo        string    `json:"valid_to"`
	LastUpdateDate string    `json:"-"`
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
