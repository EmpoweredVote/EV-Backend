package utils

import "github.com/google/uuid"

func GenerateUUID() string {
	myUUID := uuid.New()
	return myUUID.String()
}