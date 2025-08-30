package compass

import (
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

type Answer struct {
	ID        string `gorm:"primaryKey" json:"id"`
	UserID    string `json:"user_id"`
	TopicID   string `json:"topic_id"`
	Value     int    `gorm:"default: 0" json:"value"`
	CreatedAt time.Time
}

type Stance struct {
	ID      string    `gorm:"primaryKey" json:"id"`
	Value   int       `json:"value"`
	Text    string    `json:"text"`
	TopicID uuid.UUID `json:"topic_id"`
}

type Topic struct {
	ID         uuid.UUID  `gorm:"type:uuid;primaryKey" json:"id"`
	Title      string     `json:"title"`
	ShortTitle string     `gorm:"uniqueIndex" json:"short_title"`
	Stances    []Stance   `gorm:"foreignKey:TopicID" json:"stances"`
	Categories []Category `gorm:"many2many:compass.topic_categories;" json:"categories"`
}

type Category struct {
	ID     uuid.UUID `gorm:"type:uuid;primaryKey" json:"id"`
	Title  string    `gorm:"uniqueIndex" json:"title"`
	Topics []Topic   `gorm:"many2many:compass.topic_categories;" json:"topics"`
}

type Context struct {
	ID        string         `gorm:"primaryKey" json:"id"`
	UserID    string         `json:"user_id"`
	TopicID   string         `json:"topic_id"`
	Reasoning string         `json:"reasoning"`
	Sources   pq.StringArray `gorm:"type:text[]" json:"sources"`
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
