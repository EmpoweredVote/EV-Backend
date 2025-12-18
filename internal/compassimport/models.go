package compassimport

import "github.com/google/uuid"

type Topic struct {
	ID          uuid.UUID `gorm:"type:uuid;primaryKey;column:id"`
	TopicKey    string    `gorm:"column:topic_key"`
	Title       string    `gorm:"column:title"`
	ShortTitle  string    `gorm:"column:short_title"`
	StartPhrase string    `gorm:"column:start_phrase"`
	IsActive    bool      `gorm:"column:is_active"`
}

func (Topic) TableName() string { return "compass.topics" }

type Stance struct {
	ID      string    `gorm:"primaryKey;column:id"`
	Value   int       `gorm:"column:value"`
	Text    string    `gorm:"column:text"`
	TopicID uuid.UUID `gorm:"type:uuid;column:topic_id"`
}

func (Stance) TableName() string { return "compass.stances" }

type Category struct {
	ID    uuid.UUID `gorm:"type:uuid;primaryKey;column:id"`
	Title string    `gorm:"column:title"`
}

func (Category) TableName() string { return "compass.categories" }

type TopicCategory struct {
	TopicID    uuid.UUID `gorm:"type:uuid;column:topic_id"`
	CategoryID uuid.UUID `gorm:"type:uuid;column:category_id"`
}

func (TopicCategory) TableName() string { return "compass.topic_categories" }
