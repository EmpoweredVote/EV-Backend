package compass

import (
	"time"

	"github.com/google/uuid"
)

type Answer struct {
	ID			string		`gorm:"primaryKey"`
	UserID 		string
	TopicID 	string      `json:"topic_id"`
	Value 		int         `gorm:"default: 0" json:"value"`
	CreatedAt	time.Time
}

type Stance struct {
	ID      string    `gorm:"primaryKey"`
	Value   int
	Text    string
	TopicID uuid.UUID
}

type Topic struct {
	ID		    uuid.UUID	`gorm:"type:uuid;primaryKey"`
	Title	    string
	ShortTitle  string		`gorm:"uniqueIndex"`
	Stances	    []Stance	`gorm:"foreignKey:TopicID" json:"stances"`
	Categories  []Category  `gorm:"many2many:topic_categories;"`
}

type Category struct {
	ID     uuid.UUID    `gorm:"type:uuid;primaryKey"`
	Title  string      `gorm:"uniqueIndex"`
	Topics []Topic     `gorm:"many2many:topic_categories;"`
}
