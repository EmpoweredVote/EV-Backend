package utils

import (
	"context"
)

type contextKey string
const ContextUserIDKey contextKey = "userID"

func GetUserIDFromContext(ctx context.Context) (string, bool) {
	userID := ctx.Value(ContextUserIDKey)
	userIDStr, ok := userID.(string)
	return userIDStr, ok
}