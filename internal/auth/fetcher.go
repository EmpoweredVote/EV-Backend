package auth

import (
	"github.com/DoyleJ11/auth-system/internal/db"
	"github.com/DoyleJ11/auth-system/internal/utils"
)


type SessionInfo struct {}

func (si SessionInfo) FindSessionByID(id string) (utils.SessionData, error) {
	var session Session

	err := db.DB.First(&session, "session_id = ?", id).Error
	if err != nil {
		return utils.SessionData{}, err
	}

	return utils.SessionData{
		UserID:		session.UserID,
		ExpiresAt:	session.ExpiresAt,
	}, nil
}