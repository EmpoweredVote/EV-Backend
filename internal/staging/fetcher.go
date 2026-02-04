package staging

import (
	"github.com/EmpoweredVote/EV-Backend/internal/auth"
	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/EmpoweredVote/EV-Backend/internal/utils"
)

type SessionInfo struct{}

func (si SessionInfo) FindSessionByID(id string) (utils.SessionData, error) {
	var session auth.Session

	err := db.DB.First(&session, "session_id = ?", id).Error
	if err != nil {
		return utils.SessionData{}, err
	}

	return utils.SessionData{
		UserID:    session.UserID,
		ExpiresAt: session.ExpiresAt,
	}, nil
}
