package stanceimport

import (
	"fmt"
	"strconv"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"gorm.io/gorm"
)

// Config controls how the stance import runs.
type Config struct {
	CSVPath string
	DryRun  bool
}

// ImportResult summarises a completed import run.
type ImportResult struct {
	Processed int
	Inserted  int
	Updated   int
	Skipped   int
	Errors    []string
}

// ----- lightweight model types for this package -----

type importTopic struct {
	ID       uuid.UUID `gorm:"column:id"`
	TopicKey string    `gorm:"column:topic_key"`
}

func (importTopic) TableName() string { return "compass.topics" }

type importPolitician struct {
	ID         uuid.UUID `gorm:"column:id"`
	FullName   string    `gorm:"column:full_name"`
	ExternalID int       `gorm:"column:external_id"`
}

func (importPolitician) TableName() string { return "essentials.politicians" }

type importAnswer struct {
	ID           string    `gorm:"column:id;primaryKey"`
	PoliticianID uuid.UUID `gorm:"column:politician_id"`
	UserID       string    `gorm:"column:user_id"`
	TopicID      uuid.UUID `gorm:"column:topic_id"`
	Value        float64   `gorm:"column:value"`
	WriteInText  string    `gorm:"column:write_in_text"`
	CreatedAt    time.Time `gorm:"column:created_at"`
}

func (importAnswer) TableName() string { return "compass.answers" }

type importContext struct {
	ID           string         `gorm:"column:id;primaryKey"`
	PoliticianID uuid.UUID      `gorm:"column:politician_id"`
	UserID       string         `gorm:"column:user_id"`
	TopicID      uuid.UUID      `gorm:"column:topic_id"`
	Reasoning    string         `gorm:"column:reasoning"`
	Sources      pq.StringArray `gorm:"column:sources;type:text[]"`
}

func (importContext) TableName() string { return "compass.contexts" }

// ----- main entry point -----

// Run executes the stance import against the global db.DB connection.
// It reads the CSV at cfg.CSVPath, validates each row, resolves politician and
// topic IDs, and upserts records into compass.answers and compass.contexts.
func Run(cfg Config) (*ImportResult, error) {
	result := &ImportResult{}

	// ------------------------------------------------------------------
	// 1. Load all topics → map[topic_key]uuid
	// ------------------------------------------------------------------
	var topics []importTopic
	if err := db.DB.Find(&topics).Error; err != nil {
		return nil, fmt.Errorf("load topics: %w", err)
	}
	topicMap := make(map[string]uuid.UUID, len(topics))
	for _, t := range topics {
		topicMap[t.TopicKey] = t.ID
	}

	// ------------------------------------------------------------------
	// 2. Load all politicians → map[full_name]uuid + map[external_id]uuid
	//    Also detect ambiguous full names (multiple politicians share a name).
	// ------------------------------------------------------------------
	var politicians []importPolitician
	if err := db.DB.Find(&politicians).Error; err != nil {
		return nil, fmt.Errorf("load politicians: %w", err)
	}

	nameCount := make(map[string]int, len(politicians))
	for _, p := range politicians {
		nameCount[p.FullName]++
	}

	ambiguousNames := make(map[string]bool)
	for name, count := range nameCount {
		if count > 1 {
			ambiguousNames[name] = true
		}
	}

	byName := make(map[string]uuid.UUID, len(politicians))
	byExternalID := make(map[int]uuid.UUID, len(politicians))
	for _, p := range politicians {
		byName[p.FullName] = p.ID // last-write wins; ambiguous set is the safety net
		if p.ExternalID != 0 {
			byExternalID[p.ExternalID] = p.ID
		}
	}

	// ------------------------------------------------------------------
	// 3. Parse CSV
	// ------------------------------------------------------------------
	rows, err := ParseCSV(cfg.CSVPath)
	if err != nil {
		return nil, fmt.Errorf("parse csv: %w", err)
	}

	// ------------------------------------------------------------------
	// 4. Process each row
	// ------------------------------------------------------------------
	for rowNum, row := range rows {
		lineNum := rowNum + 2 // 1-based, header is line 1
		result.Processed++

		// a. Validate value range
		if row.Value < 1 || row.Value > 5 {
			msg := fmt.Sprintf("row %d: value %d out of range 1-5", lineNum, row.Value)
			result.Errors = append(result.Errors, msg)
			result.Skipped++
			continue
		}

		// b. Validate topic_key
		topicID, topicOK := topicMap[row.TopicKey]
		if !topicOK {
			msg := fmt.Sprintf("row %d: unknown topic_key '%s'", lineNum, row.TopicKey)
			result.Errors = append(result.Errors, msg)
			result.Skipped++
			continue
		}

		// c. Check for ambiguous politician name
		if ambiguousNames[row.FullName] {
			msg := fmt.Sprintf("row %d: ambiguous politician name '%s' — multiple matches in database", lineNum, row.FullName)
			result.Errors = append(result.Errors, msg)
			result.Skipped++
			continue
		}

		// d. Resolve politician
		politicianID, nameOK := byName[row.FullName]
		if !nameOK {
			if row.ExternalID != "" {
				extID, parseErr := strconv.Atoi(row.ExternalID)
				if parseErr == nil {
					politicianID, nameOK = byExternalID[extID]
				}
			}
			if !nameOK {
				msg := fmt.Sprintf("row %d: politician '%s' not found", lineNum, row.FullName)
				result.Errors = append(result.Errors, msg)
				result.Skipped++
				continue
			}
		}

		// e. Dry-run: skip DB writes
		if cfg.DryRun {
			continue
		}

		// f. Upsert compass.answers
		inserted, err := upsertAnswer(db.DB, politicianID, topicID, float64(row.Value))
		if err != nil {
			msg := fmt.Sprintf("row %d: upsert answer: %v", lineNum, err)
			result.Errors = append(result.Errors, msg)
			result.Skipped++
			continue
		}

		// g. Upsert compass.contexts (source URLs)
		if err := upsertContext(db.DB, politicianID, topicID, row.SourceURLs); err != nil {
			msg := fmt.Sprintf("row %d: upsert context: %v", lineNum, err)
			result.Errors = append(result.Errors, msg)
			// Don't skip the whole row — answer was already written
		}

		// h. Track insert vs update
		if inserted {
			result.Inserted++
		} else {
			result.Updated++
		}
	}

	// ------------------------------------------------------------------
	// 5. Print summary
	// ------------------------------------------------------------------
	fmt.Printf("Processed: %d, Inserted: %d, Updated: %d, Skipped: %d\n",
		result.Processed, result.Inserted, result.Updated, result.Skipped)
	for _, e := range result.Errors {
		fmt.Printf("ERROR: %s\n", e)
	}

	return result, nil
}

// upsertAnswer creates or updates a compass.answers row.
// Returns (true, nil) if a new row was inserted, (false, nil) if updated.
func upsertAnswer(gormDB *gorm.DB, politicianID, topicID uuid.UUID, value float64) (bool, error) {
	var existing importAnswer
	err := gormDB.
		Where("politician_id = ? AND topic_id = ?", politicianID, topicID).
		First(&existing).Error

	if err == gorm.ErrRecordNotFound {
		// Insert
		row := importAnswer{
			ID:           uuid.NewString(),
			PoliticianID: politicianID,
			TopicID:      topicID,
			Value:        value,
			CreatedAt:    time.Now(),
		}
		if err := gormDB.Create(&row).Error; err != nil {
			return false, err
		}
		return true, nil
	}
	if err != nil {
		return false, err
	}

	// Update
	if err := gormDB.Model(&existing).Update("value", value).Error; err != nil {
		return false, err
	}
	return false, nil
}

// upsertContext creates or updates a compass.contexts row.
func upsertContext(gormDB *gorm.DB, politicianID, topicID uuid.UUID, sourceURLs []string) error {
	var existing importContext
	err := gormDB.
		Where("politician_id = ? AND topic_id = ?", politicianID, topicID).
		First(&existing).Error

	if err == gorm.ErrRecordNotFound {
		// Insert
		row := importContext{
			ID:           uuid.NewString(),
			PoliticianID: politicianID,
			TopicID:      topicID,
			Reasoning:    "",
			Sources:      pq.StringArray(sourceURLs),
		}
		return gormDB.Create(&row).Error
	}
	if err != nil {
		return err
	}

	// Update sources
	return gormDB.Model(&existing).Update("sources", pq.StringArray(sourceURLs)).Error
}
