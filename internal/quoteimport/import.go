package quoteimport

import (
	"fmt"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Config controls how the quote import runs.
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
	ID       uuid.UUID `gorm:"column:id"`
	FullName string    `gorm:"column:full_name"`
}

func (importPolitician) TableName() string { return "essentials.politicians" }

type importQuote struct {
	ID           uuid.UUID `gorm:"column:id;primaryKey"`
	PoliticianID uuid.UUID `gorm:"column:politician_id"`
	TopicKey     string    `gorm:"column:topic_key"`
	QuoteText    string    `gorm:"column:quote_text"`
	SourceURL    string    `gorm:"column:source_url"`
	SourceName   string    `gorm:"column:source_name"`
	CreatedAt    time.Time `gorm:"column:created_at"`
	UpdatedAt    time.Time `gorm:"column:updated_at"`
}

func (importQuote) TableName() string { return "essentials.quotes" }

// ----- main entry point -----

// Run executes the quote import against the global db.DB connection.
// It reads the CSV at cfg.CSVPath, validates each row, resolves politician and
// topic IDs, and upserts records into essentials.quotes.
func Run(cfg Config) (*ImportResult, error) {
	result := &ImportResult{}

	// ------------------------------------------------------------------
	// 1. Load all topics → map[topic_key]bool (for validation only)
	// ------------------------------------------------------------------
	var topics []importTopic
	if err := db.DB.Find(&topics).Error; err != nil {
		return nil, fmt.Errorf("load topics: %w", err)
	}
	topicExists := make(map[string]bool, len(topics))
	for _, t := range topics {
		topicExists[t.TopicKey] = true
	}

	// ------------------------------------------------------------------
	// 2. Load all politicians → map[full_name]uuid
	//    Also detect ambiguous full names (multiple politicians share a name).
	// ------------------------------------------------------------------
	var politicians []importPolitician
	if err := db.DB.Find(&politicians).Error; err != nil {
		return nil, fmt.Errorf("load politicians: %w", err)
	}

	// First pass: count occurrences of each name
	nameCount := make(map[string]int, len(politicians))
	for _, p := range politicians {
		nameCount[p.FullName]++
	}

	// Build ambiguous name set: any name appearing more than once
	ambiguousNames := make(map[string]bool)
	for name, count := range nameCount {
		if count > 1 {
			ambiguousNames[name] = true
		}
	}

	// Second pass: build lookup map (last-write wins; ambiguous set is the safety net)
	byName := make(map[string]uuid.UUID, len(politicians))
	for _, p := range politicians {
		byName[p.FullName] = p.ID
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

		// a. Validate topic_key
		if !topicExists[row.TopicKey] {
			msg := fmt.Sprintf("row %d: unknown topic_key '%s'", lineNum, row.TopicKey)
			result.Errors = append(result.Errors, msg)
			result.Skipped++
			continue
		}

		// b. Validate quote_text is non-empty
		if row.QuoteText == "" {
			msg := fmt.Sprintf("row %d: quote_text is empty", lineNum)
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

		// d. Resolve politician by full_name
		politicianID, nameOK := byName[row.FullName]
		if !nameOK {
			msg := fmt.Sprintf("row %d: politician '%s' not found", lineNum, row.FullName)
			result.Errors = append(result.Errors, msg)
			result.Skipped++
			continue
		}

		// e. Dry-run: skip DB writes
		if cfg.DryRun {
			result.Processed++
			continue
		}

		// f. Upsert quote: check for existing row by (politician_id, topic_key, source_url)
		inserted, err := upsertQuote(db.DB, politicianID, row)
		if err != nil {
			msg := fmt.Sprintf("row %d: upsert quote: %v", lineNum, err)
			result.Errors = append(result.Errors, msg)
			result.Skipped++
			continue
		}

		// g. Track insert vs update
		if inserted {
			result.Inserted++
		} else {
			result.Updated++
		}
		result.Processed++
	}

	// ------------------------------------------------------------------
	// 5. Print summary and errors
	// ------------------------------------------------------------------
	fmt.Printf("Processed: %d, Inserted: %d, Updated: %d, Skipped: %d\n",
		result.Processed, result.Inserted, result.Updated, result.Skipped)
	for _, e := range result.Errors {
		fmt.Printf("ERROR: %s\n", e)
	}

	return result, nil
}

// upsertQuote creates or updates an essentials.quotes row.
// Deduplication key: (politician_id, topic_key, source_url).
// Returns (true, nil) if a new row was inserted, (false, nil) if updated.
func upsertQuote(gormDB *gorm.DB, politicianID uuid.UUID, row QuoteRow) (bool, error) {
	var existing importQuote
	err := gormDB.
		Where("politician_id = ? AND topic_key = ? AND source_url = ?",
			politicianID, row.TopicKey, row.SourceURL).
		First(&existing).Error

	if err == gorm.ErrRecordNotFound {
		// Insert new quote
		now := time.Now()
		q := importQuote{
			ID:           uuid.New(),
			PoliticianID: politicianID,
			TopicKey:     row.TopicKey,
			QuoteText:    row.QuoteText,
			SourceURL:    row.SourceURL,
			SourceName:   row.SourceName,
			CreatedAt:    now,
			UpdatedAt:    now,
		}
		if err := gormDB.Create(&q).Error; err != nil {
			return false, err
		}
		return true, nil
	}
	if err != nil {
		return false, err
	}

	// Update quote_text and source_name on existing row
	now := time.Now()
	if err := gormDB.Model(&existing).Updates(map[string]interface{}{
		"quote_text":  row.QuoteText,
		"source_name": row.SourceName,
		"updated_at":  now,
	}).Error; err != nil {
		return false, err
	}
	return false, nil
}
