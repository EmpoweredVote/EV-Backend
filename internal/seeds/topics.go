package seeds

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/EmpoweredVote/EV-Backend/internal/compass"
	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func SeedTopics() error {
	var topics []compass.Topic

	file, err := os.ReadFile("internal/compass/data/topics.json")
	if err != nil {
		return fmt.Errorf("could not read topics.json: %w", err)
	}

	if err := json.Unmarshal(file, &topics); err != nil {
		return fmt.Errorf("failed to parse topics.json: %w", err)
	}

	for _, topic := range topics {
		var existing compass.Topic
		err := db.DB.First(&existing, "id = ?", topic.ID).Error

		if err == nil {
			log.Printf("⚠️ Topic exists, skipping: %s", topic.Title)
			continue
		} else if err != gorm.ErrRecordNotFound {
			return fmt.Errorf("DB error on topic %s: %w", topic.Title, err)
		}

		for i := range topic.Stances {
			topic.Stances[i].ID = uuid.NewString()
			topic.Stances[i].TopicID = topic.ID
		}

		if err := db.DB.Create(&topic).Error; err != nil {
			return fmt.Errorf("failed to create topic %s: %w", topic.Title, err)
		}
	}

	log.Printf("✅ Seeded %d topics", len(topics))
	return nil
}