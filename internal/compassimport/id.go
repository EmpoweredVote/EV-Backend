package compassimport

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

func v5(ns uuid.UUID, name string) uuid.UUID {
	return uuid.NewSHA1(ns, []byte(name))
}

func TopicID(ns uuid.UUID, topicKey string) uuid.UUID {
	return v5(ns, "topic:"+topicKey)
}

func StanceID(ns uuid.UUID, topicID uuid.UUID, value int) string {
	return v5(ns, fmt.Sprintf("stance:%s:%d", topicID.String(), value)).String()
}

func CategoryID(ns uuid.UUID, title string) uuid.UUID {
	canon := strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(title)), " "))
	return v5(ns, "category:"+canon)
}
