package models

type Session struct {
	SessionID string `gorm:"primaryKey" json:"-"`
	UserID	  string `gorm:"not null;unique" json:"-"`
}