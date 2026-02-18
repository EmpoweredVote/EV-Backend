package compass

import (
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

type Answer struct {
	ID           string    `gorm:"primaryKey" json:"id"`
	PoliticianID uuid.UUID `json:"politician_id" gorm:"index:idx_pol_topic"`
	UserID       string    `json:"user_id"      gorm:"index:idx_user_topic"`
	TopicID      uuid.UUID `json:"topic_id"     gorm:"index:idx_pol_topic;index:idx_user_topic"`
	Value        float64   `gorm:"default: 0" json:"value"`
	WriteInText  string    `json:"write_in_text,omitempty"`
	CreatedAt    time.Time
}

type Stance struct {
	ID      string    `gorm:"primaryKey" json:"id"`
	Value   int       `json:"value"`
	Text    string    `json:"text"`
	TopicID uuid.UUID `json:"topic_id"`
}

type Topic struct {
	ID           uuid.UUID `gorm:"type:uuid;primaryKey" json:"id"`
	TopicKey     string    `gorm:"uniqueIndex;not null" json:"topic_key"`
	Title        string    `json:"title"`
	ShortTitle   string    `gorm:"uniqueIndex" json:"short_title"`
	StartPhrase  string    `json:"start_phrase"`
	QuestionText string    `json:"question_text,omitempty"`
	Level        pq.StringArray `gorm:"type:text[]" json:"level,omitempty"`
	IsActive     bool      `gorm:"default:true" json:"is_active"`

	Stances    []Stance   `gorm:"foreignKey:TopicID" json:"stances"`
	Categories []Category `gorm:"many2many:compass.topic_categories;" json:"categories"`
}

type Category struct {
	ID     uuid.UUID `gorm:"type:uuid;primaryKey" json:"id"`
	Title  string    `gorm:"uniqueIndex" json:"title"`
	Topics []Topic   `gorm:"many2many:compass.topic_categories;" json:"topics"`
}

type Context struct {
	ID           string         `gorm:"primaryKey" json:"id"`
	PoliticianID uuid.UUID      `json:"politician_id"`
	UserID       string         `json:"user_id"`
	TopicID      uuid.UUID      `json:"topic_id"`
	Reasoning    string         `json:"reasoning"`
	Sources      pq.StringArray `gorm:"type:text[]" json:"sources"`
}

func (Answer) TableName() string {
	return "compass.answers"
}

func (Stance) TableName() string {
	return "compass.stances"
}

func (Topic) TableName() string {
	return "compass.topics"
}

func (Category) TableName() string {
	return "compass.categories"
}

func (Context) TableName() string {
	return "compass.contexts"
}

type UserCompass struct {
	UserID    string         `gorm:"primaryKey" json:"user_id"`
	TopicIDs  pq.StringArray `gorm:"type:text[]" json:"topic_ids"`
	UpdatedAt time.Time      `json:"updated_at"`
}

func (UserCompass) TableName() string {
	return "compass.user_compasses"
}
