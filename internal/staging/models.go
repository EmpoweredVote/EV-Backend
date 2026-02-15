package staging

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// JSONB wraps json.RawMessage with Scanner/Valuer for GORM JSONB columns.
type JSONB json.RawMessage

func (j JSONB) Value() (driver.Value, error) {
	if len(j) == 0 {
		return "[]", nil
	}
	return string(j), nil
}

func (j *JSONB) Scan(value interface{}) error {
	if value == nil {
		*j = JSONB("[]")
		return nil
	}
	switch v := value.(type) {
	case []byte:
		*j = append((*j)[0:0], v...)
	case string:
		*j = JSONB(v)
	default:
		return fmt.Errorf("unsupported type: %T", value)
	}
	return nil
}

func (j JSONB) MarshalJSON() ([]byte, error) {
	if len(j) == 0 {
		return []byte("[]"), nil
	}
	return json.RawMessage(j).MarshalJSON()
}

func (j *JSONB) UnmarshalJSON(data []byte) error {
	if j == nil {
		return fmt.Errorf("JSONB: UnmarshalJSON on nil pointer")
	}
	*j = append((*j)[0:0], data...)
	return nil
}

// StagingStance holds volunteer-entered stance data before approval
type StagingStance struct {
	ID         uuid.UUID `gorm:"type:uuid;primaryKey;default:uuid_generate_v4()" json:"id"`
	ContextKey string    `gorm:"uniqueIndex;not null" json:"context_key"` // format: {politician_external_id}_{topic_key}

	// Politician reference
	PoliticianExternalID *string `json:"politician_external_id,omitempty"`
	PoliticianName       string  `gorm:"not null" json:"politician_name"`

	// Topic reference
	TopicKey string     `gorm:"not null;index" json:"topic_key"`
	TopicID  *uuid.UUID `gorm:"type:uuid" json:"topic_id,omitempty"` // Links to compass.topics

	// Stance data
	Value     int            `gorm:"not null" json:"value"` // 1-5
	Reasoning string         `json:"reasoning"`
	Sources   pq.StringArray `gorm:"type:text[]" json:"sources"`

	// Workflow state
	Status  string `gorm:"default:'draft';index" json:"status"` // draft, needs_review, approved, rejected
	AddedBy string `gorm:"not null" json:"added_by"`

	// Review tracking
	ReviewCount    int            `gorm:"default:0" json:"review_count"`
	ReviewedBy     pq.StringArray `gorm:"type:text[]" json:"reviewed_by"`
	LastReviewedAt *time.Time     `json:"last_reviewed_at,omitempty"`

	// Locking (10-min auto-expire)
	LockedBy *string    `json:"locked_by,omitempty"`
	LockedAt *time.Time `json:"locked_at,omitempty"`

	// After approval, track where it went
	ApprovedToAnswerID *string    `json:"approved_to_answer_id,omitempty"`
	ApprovedAt         *time.Time `json:"approved_at,omitempty"`

	// Timestamps
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (StagingStance) TableName() string {
	return "staging.stances"
}

// StagingPolitician holds volunteer-entered politician data before approval
type StagingPolitician struct {
	ID         uuid.UUID `gorm:"type:uuid;primaryKey;default:uuid_generate_v4()" json:"id"`
	ExternalID *string   `gorm:"uniqueIndex" json:"external_id,omitempty"` // cicero ID if known
	FullName   string    `gorm:"not null" json:"full_name"`
	Party      string    `json:"party"`
	Office     string    `json:"office"`
	OfficeLevel string   `json:"office_level"` // federal, state, local
	State      string    `json:"state"`
	District   string    `json:"district"`

	// Optional biographical fields (stored as JSONB for flexibility)
	BioText     string `json:"bio_text,omitempty"`
	PhotoURL    string `json:"photo_url,omitempty"`
	Contacts    JSONB  `json:"contacts" gorm:"type:jsonb;default:'[]'"`    // [{type, value, source}]
	Degrees     JSONB  `json:"degrees" gorm:"type:jsonb;default:'[]'"`     // [{degree, major, school, grad_year}]
	Experiences JSONB  `json:"experiences" gorm:"type:jsonb;default:'[]'"` // [{title, organization, type, start, end}]

	// Workflow
	Status     string  `gorm:"default:'pending';index" json:"status"` // pending, approved, rejected, merged
	AddedBy    string  `gorm:"not null" json:"added_by"`
	ReviewedBy *string `json:"reviewed_by,omitempty"`

	// If approved, link to essentials.politicians
	MergedToID *uuid.UUID `gorm:"type:uuid" json:"merged_to_id,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (StagingPolitician) TableName() string {
	return "staging.politicians"
}

// ReviewLog tracks all review actions for audit purposes
type ReviewLog struct {
	ID            uuid.UUID  `gorm:"type:uuid;primaryKey;default:uuid_generate_v4()" json:"id"`
	StanceID      uuid.UUID  `gorm:"type:uuid;not null;index" json:"stance_id"`
	ReviewerName  string     `gorm:"not null" json:"reviewer_name"`
	Action        string     `gorm:"not null" json:"action"` // approved, rejected, edited
	PreviousValue *int       `json:"previous_value,omitempty"`
	NewValue      *int       `json:"new_value,omitempty"`
	Comment       string     `json:"comment,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
}

func (ReviewLog) TableName() string {
	return "staging.review_logs"
}
