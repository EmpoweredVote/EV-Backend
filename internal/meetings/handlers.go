package meetings

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// ListMeetings returns all meetings with optional city filter
func ListMeetings(w http.ResponseWriter, r *http.Request) {
	query := db.DB.Model(&Meeting{}).Order("date DESC")

	if city := r.URL.Query().Get("city"); city != "" {
		query = query.Where("city = ?", city)
	}
	if status := r.URL.Query().Get("status"); status != "" {
		query = query.Where("status = ?", status)
	}

	var meetings []Meeting
	if err := query.Find(&meetings).Error; err != nil {
		http.Error(w, "Failed to fetch meetings: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(meetings)
}

// GetMeeting returns a single meeting with speakers and summary
func GetMeeting(w http.ResponseWriter, r *http.Request) {
	meetingID := chi.URLParam(r, "meeting_id")

	var meeting Meeting
	if err := db.DB.
		Preload("Speakers").
		Preload("Summaries.Sections", func(db *gorm.DB) *gorm.DB {
			return db.Order("sort_order ASC")
		}).
		Preload("Votes.Records").
		First(&meeting, "id = ?", meetingID).Error; err != nil {
		http.Error(w, "Meeting not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(meeting)
}

// GetTranscript returns the full transcript segments for a meeting
func GetTranscript(w http.ResponseWriter, r *http.Request) {
	meetingID := chi.URLParam(r, "meeting_id")

	var segments []Segment
	if err := db.DB.
		Where("meeting_id = ?", meetingID).
		Order("segment_index ASC").
		Preload("Speaker").
		Find(&segments).Error; err != nil {
		http.Error(w, "Failed to fetch transcript: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Build response with speaker names inline
	type SegmentOut struct {
		SegmentIndex int     `json:"segment_index"`
		StartTime    float64 `json:"start_time"`
		EndTime      float64 `json:"end_time"`
		SpeakerName  string  `json:"speaker_name"`
		SpeakerLabel string  `json:"speaker_label"`
		Text         string  `json:"text"`
	}

	out := make([]SegmentOut, len(segments))
	for i, s := range segments {
		out[i] = SegmentOut{
			SegmentIndex: s.SegmentIndex,
			StartTime:    s.StartTime,
			EndTime:      s.EndTime,
			SpeakerName:  s.Speaker.DisplayName,
			SpeakerLabel: s.Speaker.Label,
			Text:         s.Text,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// GetSpeakers returns the speakers for a meeting
func GetSpeakers(w http.ResponseWriter, r *http.Request) {
	meetingID := chi.URLParam(r, "meeting_id")

	var speakers []Speaker
	if err := db.DB.Where("meeting_id = ?", meetingID).Find(&speakers).Error; err != nil {
		http.Error(w, "Failed to fetch speakers: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(speakers)
}

// GetSummary returns the meeting summary with sections
func GetSummary(w http.ResponseWriter, r *http.Request) {
	meetingID := chi.URLParam(r, "meeting_id")

	var summary MeetingSummary
	if err := db.DB.
		Where("meeting_id = ?", meetingID).
		Preload("Sections", func(db *gorm.DB) *gorm.DB {
			return db.Order("sort_order ASC")
		}).
		First(&summary).Error; err != nil {
		http.Error(w, "No summary found for this meeting", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(summary)
}

// GetVotes returns the votes for a meeting with individual records
func GetVotes(w http.ResponseWriter, r *http.Request) {
	meetingID := chi.URLParam(r, "meeting_id")

	var votes []Vote
	if err := db.DB.
		Where("meeting_id = ?", meetingID).
		Preload("Records").
		Find(&votes).Error; err != nil {
		http.Error(w, "Failed to fetch votes: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(votes)
}

// SearchTranscripts performs full-text search across transcript segments
func SearchTranscripts(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if q == "" {
		http.Error(w, "Query parameter 'q' is required", http.StatusBadRequest)
		return
	}

	type SearchResult struct {
		SegmentID    uuid.UUID `json:"segment_id"`
		MeetingID    uuid.UUID `json:"meeting_id"`
		MeetingDate  time.Time `json:"meeting_date"`
		MeetingCity  string    `json:"meeting_city"`
		SpeakerName  string    `json:"speaker_name"`
		StartTime    float64   `json:"start_time"`
		Text         string    `json:"text"`
	}

	var results []SearchResult
	if err := db.DB.Raw(`
		SELECT
			s.id AS segment_id,
			m.id AS meeting_id,
			m.date AS meeting_date,
			m.city AS meeting_city,
			sp.display_name AS speaker_name,
			s.start_time,
			s.text
		FROM meetings.segments s
		JOIN meetings.meetings m ON m.id = s.meeting_id
		JOIN meetings.speakers sp ON sp.id = s.speaker_id
		WHERE to_tsvector('english', s.text) @@ plainto_tsquery('english', ?)
		ORDER BY m.date DESC, s.segment_index ASC
		LIMIT 50
	`, q).Scan(&results).Error; err != nil {
		http.Error(w, "Search failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// ImportMeeting imports a full meeting from CouncilScribe JSON output
func ImportMeeting(w http.ResponseWriter, r *http.Request) {
	var req MeetingImport
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.City == "" || req.Date == "" {
		http.Error(w, "city and date are required", http.StatusBadRequest)
		return
	}

	meetingDate, err := time.Parse("2006-01-02", req.Date)
	if err != nil {
		http.Error(w, "Invalid date format (expected YYYY-MM-DD): "+err.Error(), http.StatusBadRequest)
		return
	}

	tx := db.DB.Begin()
	if tx.Error != nil {
		http.Error(w, "Failed to start transaction", http.StatusInternalServerError)
		return
	}

	// Create meeting record
	meeting := Meeting{
		City:            req.City,
		State:           req.State,
		Date:            meetingDate,
		MeetingType:     req.MeetingType,
		DurationSeconds: req.DurationSeconds,
		VideoURL:        req.VideoURL,
		AudioSource:     req.AudioSource,
		Status:          "complete",
		SegmentCount:    len(req.Segments),
		SpeakerCount:    len(req.Speakers),
	}
	if err := tx.Create(&meeting).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Failed to create meeting: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Create speakers and build label -> UUID map
	speakerMap := make(map[string]uuid.UUID) // label -> DB ID
	for label, sp := range req.Speakers {
		speaker := Speaker{
			MeetingID:   meeting.ID,
			Label:       label,
			DisplayName: sp.DisplayName,
			Confidence:  sp.Confidence,
			IDMethod:    sp.IDMethod,
		}
		if err := tx.Create(&speaker).Error; err != nil {
			tx.Rollback()
			http.Error(w, "Failed to create speaker: "+err.Error(), http.StatusInternalServerError)
			return
		}
		speakerMap[label] = speaker.ID
	}

	// Create segments
	for _, seg := range req.Segments {
		speakerID, ok := speakerMap[seg.SpeakerLabel]
		if !ok {
			continue // skip segments with unknown speakers
		}
		segment := Segment{
			MeetingID:    meeting.ID,
			SpeakerID:    speakerID,
			SegmentIndex: seg.SegmentIndex,
			StartTime:    seg.StartTime,
			EndTime:      seg.EndTime,
			Text:         seg.Text,
		}
		if err := tx.Create(&segment).Error; err != nil {
			tx.Rollback()
			http.Error(w, "Failed to create segment: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if err := tx.Commit().Error; err != nil {
		http.Error(w, "Failed to commit transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Imported meeting: %s %s (%s) — %d speakers, %d segments",
		req.City, req.Date, req.MeetingType, len(req.Speakers), len(req.Segments))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     "success",
		"meeting_id": meeting.ID,
		"speakers":   len(req.Speakers),
		"segments":   len(req.Segments),
	})
}

// DeleteMeeting deletes a meeting and all associated data
func DeleteMeeting(w http.ResponseWriter, r *http.Request) {
	meetingID := chi.URLParam(r, "meeting_id")

	tx := db.DB.Begin()

	// Delete in order respecting foreign keys
	if err := tx.Exec("DELETE FROM meetings.vote_records WHERE vote_id IN (SELECT id FROM meetings.votes WHERE meeting_id = ?)", meetingID).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Failed to delete vote records: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := tx.Delete(&Vote{}, "meeting_id = ?", meetingID).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Failed to delete votes: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := tx.Delete(&SummarySection{}, "summary_id IN (SELECT id FROM meetings.meeting_summaries WHERE meeting_id = ?)", meetingID).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Failed to delete summary sections: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := tx.Delete(&MeetingSummary{}, "meeting_id = ?", meetingID).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Failed to delete summaries: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := tx.Delete(&Segment{}, "meeting_id = ?", meetingID).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Failed to delete segments: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := tx.Delete(&Speaker{}, "meeting_id = ?", meetingID).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Failed to delete speakers: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := tx.Delete(&Meeting{}, "id = ?", meetingID).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Failed to delete meeting: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := tx.Commit().Error; err != nil {
		http.Error(w, "Failed to commit: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// --- Import DTOs ---

// MeetingImport matches CouncilScribe's JSON export structure
type MeetingImport struct {
	City            string                      `json:"city"`
	State           string                      `json:"state"`
	Date            string                      `json:"date"`
	MeetingType     string                      `json:"meeting_type"`
	DurationSeconds float64                     `json:"duration_seconds"`
	VideoURL        string                      `json:"video_url"`
	AudioSource     string                      `json:"audio_source"`
	Speakers        map[string]SpeakerImport    `json:"speakers"`
	Segments        []SegmentImport             `json:"segments"`
}

type SpeakerImport struct {
	DisplayName string  `json:"speaker_name"`
	Confidence  float64 `json:"confidence"`
	IDMethod    string  `json:"id_method"`
}

type SegmentImport struct {
	SegmentIndex int     `json:"segment_id"`
	SpeakerLabel string  `json:"speaker_label"`
	StartTime    float64 `json:"start_time"`
	EndTime      float64 `json:"end_time"`
	Text         string  `json:"text"`
}
