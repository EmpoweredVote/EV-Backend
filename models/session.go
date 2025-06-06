package models

import "time"

type Session struct {
	SessionID string 	`gorm:"primaryKey" json:"-"`
	UserID	  string 	`gorm:"not null;unique" json:"-"`
	ExpiresAt time.Time `gorm:"not null"`
}