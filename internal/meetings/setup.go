package meetings

import (
	"log"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
)

func Init() {
	if err := db.EnsureSchema(db.DB, "meetings"); err != nil {
		log.Fatal("Failed to ensure schema meetings: ", err)
	}

	if err := db.DB.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`).Error; err != nil {
		log.Fatal("Failed to enable uuid-ossp extension:", err)
	}

	if err := db.DB.AutoMigrate(
		&Meeting{},
		&Speaker{},
		&Segment{},
		&MeetingSummary{},
		&SummarySection{},
		&Vote{},
		&VoteRecord{},
	); err != nil {
		log.Fatal("Failed to auto-migrate meetings tables: ", err)
	}

	// Full-text search on transcript segments
	if err := db.DB.Exec(`
		CREATE INDEX IF NOT EXISTS idx_segment_text_search
		ON meetings.segments USING gin(to_tsvector('english', text));
	`).Error; err != nil {
		log.Fatal("Failed to create segment text search index: ", err)
	}

	// Efficient segment ordering within a meeting
	if err := db.DB.Exec(`
		CREATE INDEX IF NOT EXISTS idx_segment_meeting_order
		ON meetings.segments (meeting_id, segment_index);
	`).Error; err != nil {
		log.Fatal("Failed to create segment ordering index: ", err)
	}

	log.Println("Meetings module initialized")
}
