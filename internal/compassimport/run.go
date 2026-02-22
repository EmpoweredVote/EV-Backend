package compassimport

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func Run(cfg Config) error {
	if !cfg.Wipe {
		return errors.New("refusing to run: set Wipe=true (this importer truncates compass tables)")
	}

	ns, err := uuid.Parse(cfg.Namespace)
	if err != nil {
		return fmt.Errorf("invalid namespace uuid: %w", err)
	}

	rows, err := ParseCSV(cfg.CSVPath)
	if err != nil {
		return err
	}

	db, err := gorm.Open(postgres.Open(cfg.DatabaseURL), &gorm.Config{})
	if err != nil {
		return err
	}

	return db.Transaction(func(tx *gorm.DB) error {
		if err := wipeCompass(tx); err != nil {
			return err
		}

		// Build categories map
		catByTitle := map[string]uuid.UUID{}
		for _, r := range rows {
			for _, c := range r.Categories {
				if _, ok := catByTitle[c]; !ok {
					catByTitle[c] = CategoryID(ns, c)
				}
			}
		}

		// Insert categories
		for title, id := range catByTitle {
			c := Category{ID: id, Title: title}
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "title"}},
				DoUpdates: clause.AssignmentColumns([]string{"id"}), // keep id aligned w/ deterministic
			}).Create(&c).Error; err != nil {
				return fmt.Errorf("insert category %q: %w", title, err)
			}
		}

		// Insert topics + stances + join rows
		var topics []Topic
		var stances []Stance
		var links []TopicCategory

		for _, r := range rows {
			tid := TopicID(ns, r.TopicKey)

			topics = append(topics, Topic{
				ID:         tid,
				TopicKey:   r.TopicKey,
				Title:      r.Title,
				ShortTitle: r.ShortTitle,
				IsActive:   true,
			})

			for i := 1; i <= 5; i++ {
				stances = append(stances, Stance{
					ID:      StanceID(ns, tid, i),
					TopicID: tid,
					Value:   i,
					Text:    r.Stances[i-1],
				})
			}

			for _, ctitle := range r.Categories {
				links = append(links, TopicCategory{
					TopicID:    tid,
					CategoryID: catByTitle[ctitle],
				})
			}
		}

		if err := tx.Create(&topics).Error; err != nil {
			return fmt.Errorf("insert topics: %w", err)
		}
		if err := tx.Create(&stances).Error; err != nil {
			return fmt.Errorf("insert stances: %w", err)
		}
		if len(links) > 0 {
			if err := tx.Create(&links).Error; err != nil {
				return fmt.Errorf("insert topic_categories: %w", err)
			}
		}

		return nil
	})
}

func wipeCompass(tx *gorm.DB) error {
	// This will DELETE answers/contexts too, as you requested.
	sql := `
		TRUNCATE TABLE
			compass.answers,
			compass.contexts,
			compass.topic_categories,
			compass.stances,
			compass.topics,
			compass.categories
		CASCADE;
	`
	return tx.Exec(sql).Error
}
