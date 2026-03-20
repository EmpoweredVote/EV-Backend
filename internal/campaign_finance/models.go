package campaign_finance

import (
	"time"

	"github.com/google/uuid"
)

// PoliticianSource links an essentials.politicians record to a campaign-finance data source.
// The EssentialsPoliticianID is a plain UUID column (no FK constraint) to avoid
// AutoMigrate cross-schema ordering issues.
type PoliticianSource struct {
	ID                     uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	EssentialsPoliticianID uuid.UUID `json:"essentials_politician_id" gorm:"type:uuid;not null;uniqueIndex:idx_politician_source_system;index;comment:soft reference to essentials.politicians.id"`
	SourceSystem           string    `json:"source_system" gorm:"type:varchar(32);not null;uniqueIndex:idx_politician_source_system"`
	ExternalID             string    `json:"external_id" gorm:"type:varchar(128)"`
	ResearchStatus         string    `json:"research_status" gorm:"type:varchar(32);not null;default:'needs_research';check:research_status IN ('needs_research','confirmed','not_applicable','disputed')"`
	Notes                  string    `json:"notes" gorm:"type:text"`
	CreatedAt              time.Time `json:"created_at"`
	UpdatedAt              time.Time `json:"updated_at"`
}

func (PoliticianSource) TableName() string { return "transparent_motivations.politician_sources" }

// Donor represents an individual or organization that made a campaign contribution.
type Donor struct {
	ID             uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	Name           string    `json:"name" gorm:"type:varchar(512);not null"`
	NormalizedName string    `json:"normalized_name" gorm:"type:varchar(512);index"`
	DonorType      string    `json:"donor_type" gorm:"type:varchar(32)"`
	City           string    `json:"city" gorm:"type:varchar(128)"`
	State          string    `json:"state" gorm:"type:varchar(2)"`
	Employer       string    `json:"employer" gorm:"type:varchar(256)"`
	Occupation     string    `json:"occupation" gorm:"type:varchar(256)"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func (Donor) TableName() string { return "transparent_motivations.donors" }

// Committee represents a campaign or political action committee.
type Committee struct {
	ID                uuid.UUID  `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	Name              string     `json:"name" gorm:"type:varchar(512);not null"`
	CommitteeType     string     `json:"committee_type" gorm:"type:varchar(32)"`
	SourceSystem      string     `json:"source_system" gorm:"type:varchar(32);not null"`
	ExternalID        string     `json:"external_id" gorm:"type:varchar(128)"`
	PoliticianSourceID *uuid.UUID `json:"politician_source_id" gorm:"type:uuid;index"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

func (Committee) TableName() string { return "transparent_motivations.committees" }

// Contribution records a single campaign finance contribution event.
// ConfidenceLevel and DataSource are required with CHECK constraints.
type Contribution struct {
	ID                 uuid.UUID  `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	DonorID            *uuid.UUID `json:"donor_id" gorm:"type:uuid;index"`
	CommitteeID        *uuid.UUID `json:"committee_id" gorm:"type:uuid;index"`
	PoliticianSourceID uuid.UUID  `json:"politician_source_id" gorm:"type:uuid;not null;index"`
	Amount             float64    `json:"amount" gorm:"type:decimal(14,2);not null"`
	ContributionDate   *time.Time `json:"contribution_date"`
	ElectionCycle      string     `json:"election_cycle" gorm:"type:varchar(4)"`
	ConfidenceLevel     string     `json:"confidence_level" gorm:"type:varchar(16);not null;check:confidence_level IN ('HIGH','MEDIUM','ESTIMATED')"`
	DataSource          string     `json:"data_source" gorm:"type:varchar(32);not null;check:data_source IN ('fec','indiana','cal_access','la_socrata','community_verified');uniqueIndex:idx_contribution_dedup"`
	SourceTransactionID string     `json:"source_transaction_id" gorm:"type:varchar(128);uniqueIndex:idx_contribution_dedup"`
	RawRecord          []byte     `json:"raw_record" gorm:"type:jsonb"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

func (Contribution) TableName() string { return "transparent_motivations.contributions" }

// DataSourceMetadata tracks sync state for each campaign finance data source.
type DataSourceMetadata struct {
	ID             uint       `json:"id" gorm:"primaryKey;autoIncrement"`
	SourceSystem   string     `json:"source_system" gorm:"type:varchar(32);not null;uniqueIndex"`
	LastSyncAt     *time.Time `json:"last_sync_at"`
	LastSyncStatus string     `json:"last_sync_status" gorm:"type:varchar(32)"`
	LastRecordCount int       `json:"last_record_count"`
	Notes          string     `json:"notes" gorm:"type:text"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

func (DataSourceMetadata) TableName() string {
	return "transparent_motivations.data_source_metadata"
}

// SourceAuditLog records changes to PoliticianSource records for accountability.
type SourceAuditLog struct {
	ID                 uint      `json:"id" gorm:"primaryKey;autoIncrement"`
	PoliticianSourceID uuid.UUID `json:"politician_source_id" gorm:"type:uuid;not null;index"`
	ChangedByUserID    uuid.UUID `json:"changed_by_user_id" gorm:"type:uuid;not null"`
	ChangedByUsername  string    `json:"changed_by_username" gorm:"type:varchar(128)"`
	Action             string    `json:"action" gorm:"type:varchar(16);not null"`
	OldValue           []byte    `json:"old_value" gorm:"type:jsonb"`
	NewValue           []byte    `json:"new_value" gorm:"type:jsonb"`
	ChangedAt          time.Time `json:"changed_at" gorm:"not null;default:now()"`
}

func (SourceAuditLog) TableName() string { return "transparent_motivations.source_audit_log" }

// IngestionRun records the lifecycle and outcome of every adapter execution.
// Status values: running | completed | completed_with_warning | failed | skipped_no_change
type IngestionRun struct {
	ID                 uint       `json:"id" gorm:"primaryKey;autoIncrement"`
	AdapterName        string     `json:"adapter_name" gorm:"type:varchar(64);not null;index"`
	PoliticianSourceID *uuid.UUID `json:"politician_source_id" gorm:"type:uuid;index"`
	ElectionCycle      string     `json:"election_cycle" gorm:"type:varchar(4)"`
	StartedAt          time.Time  `json:"started_at" gorm:"not null"`
	CompletedAt        *time.Time `json:"completed_at"`
	Status             string     `json:"status" gorm:"type:varchar(32);not null;default:'running';check:status IN ('running','completed','completed_with_warning','failed')"`
	RecordsFetched     int        `json:"records_fetched"`
	RecordsInserted    int        `json:"records_inserted"`
	RecordsSkipped     int        `json:"records_skipped"`
	RecordsUnresolved  int        `json:"records_unresolved"`
	ErrorCount         int        `json:"errors" gorm:"column:errors"`
	DurationMs         int64      `json:"duration_ms"`
	Notes              string     `json:"notes" gorm:"type:text"`
	SourceETag         string     `json:"source_etag" gorm:"type:varchar(256)"`
	ZIPDownloadedAt    *time.Time `json:"zip_downloaded_at"`
}

func (IngestionRun) TableName() string { return "transparent_motivations.ingestion_runs" }
