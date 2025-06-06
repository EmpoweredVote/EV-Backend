package utils

import "time"

type SessionData struct {
	UserID    string
	ExpiresAt time.Time
}