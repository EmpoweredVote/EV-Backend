package meetings

import (
	"time"

	"github.com/google/uuid"
)

// Meeting represents a council meeting with transcript data
type Meeting struct {
	ID              uuid.UUID `gorm:"type:uuid;primaryKey;default:uuid_generate_v4()" json:"id"`
	City            string    `gorm:"not null;index:idx_meeting_city_date" json:"city"`
	State           string    `gorm:"not null" json:"state"`
	Date            time.Time `gorm:"not null;index:idx_meeting_city_date" json:"date"`
	MeetingType     string    `gorm:"not null;default:'Regular Session'" json:"meeting_type"`
	DurationSeconds float64   `json:"duration_seconds"`
	VideoURL        string    `json:"video_url,omitempty"`
	AudioSource     string    `json:"audio_source,omitempty"`
	Status          string    `gorm:"not null;default:'processing'" json:"status"` // processing, complete, error
	SegmentCount    int       `json:"segment_count"`
	SpeakerCount    int       `json:"speaker_count"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`

	Speakers  []Speaker        `gorm:"foreignKey:MeetingID" json:"speakers,omitempty"`
	Segments  []Segment        `gorm:"foreignKey:MeetingID" json:"segments,omitempty"`
	Summaries []MeetingSummary `gorm:"foreignKey:MeetingID" json:"summaries,omitempty"`
	Votes     []Vote           `gorm:"foreignKey:MeetingID" json:"votes,omitempty"`
}

func (Meeting) TableName() string {
	return "meetings.meetings"
}

// Speaker represents a speaker identified in a meeting, optionally linked to a politician
type Speaker struct {
	ID           uuid.UUID  `gorm:"type:uuid;primaryKey;default:uuid_generate_v4()" json:"id"`
	MeetingID    uuid.UUID  `gorm:"type:uuid;not null;index:idx_speaker_meeting" json:"meeting_id"`
	Label        string     `gorm:"not null" json:"label"`                      // SPEAKER_00, SPEAKER_01
	DisplayName  string     `json:"display_name"`                               // "Councilmember Adams"
	Confidence   float64    `json:"confidence"`                                 // 0.0-1.0
	IDMethod     string     `json:"id_method,omitempty"`                        // voice_profile, roll_call, llm, human_review
	PoliticianID *uuid.UUID `gorm:"type:uuid;index:idx_speaker_politician" json:"politician_id,omitempty"` // FK to essentials.politicians (app-level, no DB constraint)
	CreatedAt    time.Time  `json:"created_at"`

	Meeting  Meeting   `gorm:"foreignKey:MeetingID" json:"-"`
	Segments []Segment `gorm:"foreignKey:SpeakerID" json:"segments,omitempty"`
}

func (Speaker) TableName() string {
	return "meetings.speakers"
}

// Segment represents a single transcript segment (one speaker utterance)
type Segment struct {
	ID           uuid.UUID `gorm:"type:uuid;primaryKey;default:uuid_generate_v4()" json:"id"`
	MeetingID    uuid.UUID `gorm:"type:uuid;not null;index:idx_segment_meeting" json:"meeting_id"`
	SpeakerID    uuid.UUID `gorm:"type:uuid;not null;index:idx_segment_speaker" json:"speaker_id"`
	SegmentIndex int       `gorm:"not null" json:"segment_index"` // ordering within meeting
	StartTime    float64   `gorm:"not null" json:"start_time"`    // seconds from start
	EndTime      float64   `gorm:"not null" json:"end_time"`
	Text         string    `gorm:"type:text;not null" json:"text"`

	Meeting Meeting `gorm:"foreignKey:MeetingID" json:"-"`
	Speaker Speaker `gorm:"foreignKey:SpeakerID" json:"-"`
}

func (Segment) TableName() string {
	return "meetings.segments"
}

// MeetingSummary represents an LLM-generated summary of a meeting
type MeetingSummary struct {
	ID          uuid.UUID `gorm:"type:uuid;primaryKey;default:uuid_generate_v4()" json:"id"`
	MeetingID   uuid.UUID `gorm:"type:uuid;not null;index" json:"meeting_id"`
	SummaryType string    `gorm:"not null;default:'full'" json:"summary_type"` // full, executive
	Model       string    `json:"model,omitempty"`                             // which LLM generated it
	CreatedAt   time.Time `json:"created_at"`

	Meeting  Meeting          `gorm:"foreignKey:MeetingID" json:"-"`
	Sections []SummarySection `gorm:"foreignKey:SummaryID" json:"sections,omitempty"`
}

func (MeetingSummary) TableName() string {
	return "meetings.meeting_summaries"
}

// SummarySection represents a section within a meeting summary
type SummarySection struct {
	ID          uuid.UUID `gorm:"type:uuid;primaryKey;default:uuid_generate_v4()" json:"id"`
	SummaryID   uuid.UUID `gorm:"type:uuid;not null;index" json:"summary_id"`
	SectionType string    `gorm:"not null" json:"section_type"` // roll_call, consent_agenda, discussion, public_comment, vote, procedural
	Title       string    `gorm:"not null" json:"title"`
	Content     string    `gorm:"type:text;not null" json:"content"` // markdown
	StartTime   *float64  `json:"start_time,omitempty"`              // timestamp link into transcript
	EndTime     *float64  `json:"end_time,omitempty"`
	SortOrder   int       `gorm:"default:0" json:"sort_order"`

	Summary MeetingSummary `gorm:"foreignKey:SummaryID" json:"-"`
}

func (SummarySection) TableName() string {
	return "meetings.summary_sections"
}

// Vote represents a recorded vote on a resolution or ordinance
type Vote struct {
	ID          uuid.UUID `gorm:"type:uuid;primaryKey;default:uuid_generate_v4()" json:"id"`
	MeetingID   uuid.UUID `gorm:"type:uuid;not null;index" json:"meeting_id"`
	Resolution  string    `json:"resolution"`              // "Resolution 26-04", "Ordinance 2026-15"
	Description string    `gorm:"type:text" json:"description"`
	Result      string    `gorm:"not null" json:"result"`  // passed, failed, tabled
	VoteType    string    `json:"vote_type,omitempty"`     // unanimous, roll_call, voice
	Timestamp   *float64  `json:"timestamp,omitempty"`     // time in meeting (seconds)
	CreatedAt   time.Time `json:"created_at"`

	Meeting Meeting      `gorm:"foreignKey:MeetingID" json:"-"`
	Records []VoteRecord `gorm:"foreignKey:VoteID" json:"records,omitempty"`
}

func (Vote) TableName() string {
	return "meetings.votes"
}

// VoteRecord represents an individual council member's vote
type VoteRecord struct {
	ID        uuid.UUID `gorm:"type:uuid;primaryKey;default:uuid_generate_v4()" json:"id"`
	VoteID    uuid.UUID `gorm:"type:uuid;not null;index:idx_vote_record,unique" json:"vote_id"`
	SpeakerID uuid.UUID `gorm:"type:uuid;not null;index:idx_vote_record,unique" json:"speaker_id"`
	Position  string    `gorm:"not null" json:"position"` // yea, nay, abstain, absent

	Vote    Vote    `gorm:"foreignKey:VoteID" json:"-"`
	Speaker Speaker `gorm:"foreignKey:SpeakerID" json:"-"`
}

func (VoteRecord) TableName() string {
	return "meetings.vote_records"
}
