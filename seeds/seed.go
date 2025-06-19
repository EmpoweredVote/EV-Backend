package seeds

// import (
// 	"encoding/json"
// 	"log"
// 	"os"

// 	"github.com/EmpoweredVote/EV-Backend/internal/compass"
// 	"github.com/EmpoweredVote/EV-Backend/internal/db"
// 	"github.com/google/uuid"
// 	"github.com/joho/godotenv"
// 	"gorm.io/gorm"
// )

// func main() {
// 	// Load .env for DB credentials
// 	if err := godotenv.Load(); err != nil {
// 		log.Fatal("Error loading .env file")
// 	}

// 	// Connect to DB
// 	db.Connect()

// 	var topics []compass.Topic

// 	file, err := os.ReadFile("internal/compass/data/topics.json")
// 	if err != nil {
// 		log.Fatal("Could not read topics.json")
// 	}

// 	err = json.Unmarshal(file, &topics)
// 	if err != nil {
// 		log.Fatalf("Failed unmarshaling json: %v", err)
// 	}

// for _, topic := range topics {
// 	var existing compass.Topic
// 	err := db.DB.First(&existing, "id = ?", topic.ID).Error

// 	if err == nil {
// 		log.Printf("Topic already exists, skipping: %s", topic.Title)
// 		continue
// 	} else if err != gorm.ErrRecordNotFound {
// 		log.Fatalf("DB error while checking topic %s: %v", topic.Title, err)
// 	}

// 	// Set TopicID on each Stance before insert
// 	for i := range topic.Stances {
// 		topic.Stances[i].ID = uuid.NewString()
// 		topic.Stances[i].TopicID = topic.ID
// 	}

// 	if err := db.DB.Create(&topic).Error; err != nil {
// 		log.Fatalf("Failed to create topic %s: %v", topic.Title, err)
// 	}
// }

// 	log.Printf("✅ Successfully seeded %d topics", len(topics))
// }