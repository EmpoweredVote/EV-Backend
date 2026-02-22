package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
)

// CLI flags
var (
	csvPath     = flag.String("csv", "", "Path to the source CSV (required)")
	dsn         = flag.String("dsn", os.Getenv("DATABASE_URL"), "Postgres DSN (default: env DATABASE_URL)")
	dryRun      = flag.Bool("dry-run", false, "Parse + validate only; no DB writes")
	confirm     = flag.Bool("confirm", false, "Required to perform destructive replace")
	advisoryKey = flag.Int64("advisory-lock", 0, "Optional Postgres advisory lock key (e.g., 424242). 0 = disabled")
)

// CSV contract
// title,shortTitle,stance1,stance2,stance3,stance4,stance5,categories
// categories are semicolon-separated without spaces; stance order maps to value 1..5

type TopicCSV struct {
	Title      string
	ShortTitle string
	Stances    [5]string
	Categories []string // titles
}

type Counts struct {
	Topics          int64
	Stances         int64
	TopicCategories int64
	Answers         int64
	Contexts        int64
}

func main() {
	_ = godotenv.Load(".env.local")
	flag.Parse()
	if *csvPath == "" {
		fatalf("--csv is required")
	}
	if *dsn == "" {
		fatalf("--dsn not provided and DATABASE_URL not set")
	}

	rows, err := loadCSV(*csvPath)
	if err != nil {
		fatalf("CSV error: %v", err)
	}

	// Basic validation
	if err := validateRows(rows); err != nil {
		fatalf("CSV validation failed: %v", err)
	}

	fmt.Printf("Loaded %d topics from %s\n", len(rows), *csvPath)

	if *dryRun {
		printPlan(rows)
		fmt.Println("Dry run complete. No changes made.")
		return
	}

	if !*confirm {
		fatalf("Refusing to run without --confirm. Add --dry-run to preview.")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", *dsn)
	if err != nil {
		fatalf("connect: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		fatalf("ping: %v", err)
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		fatalf("begin tx: %v", err)
	}
	defer func() {
		_ = tx.Rollback() // no-op if already committed
	}()

	// Optional advisory lock to avoid concurrent runs
	if *advisoryKey != 0 {
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, *advisoryKey); err != nil {
			fatalf("advisory lock: %v", err)
		}
	}

	before, err := countAll(ctx, tx)
	if err != nil {
		fatalf("pre-count: %v", err)
	}
	fmt.Printf("Before: topics=%d stances=%d topic_categories=%d answers=%d contexts=%d\n",
		before.Topics, before.Stances, before.TopicCategories, before.Answers, before.Contexts)

	// Destructive replace (explicit order; no ON DELETE CASCADE assumed)
	if err := wipeCompassData(ctx, tx); err != nil {
		fatalf("wipe data: %v", err)
	}

	// Upsert categories first, build title->id map
	catIDs, err := upsertAllCategories(ctx, tx, rows)
	if err != nil {
		fatalf("upsert categories: %v", err)
	}
	fmt.Printf("Upserted %d distinct categories\n", len(catIDs))

	// Insert topics + stances + topic_categories
	if err := insertAll(ctx, tx, rows, catIDs); err != nil {
		fatalf("insert data: %v", err)
	}

	after, err := countAll(ctx, tx)
	if err != nil {
		fatalf("post-count: %v", err)
	}
	fmt.Printf("After:  topics=%d stances=%d topic_categories=%d answers=%d contexts=%d\n",
		after.Topics, after.Stances, after.TopicCategories, after.Answers, after.Contexts)

	// sanity: stances == topics * 5
	if after.Stances != after.Topics*5 {
		fatalf("sanity check failed: stances=%d topics=%d (expected stances = topics*5)", after.Stances, after.Topics)
	}

	if err := tx.Commit(); err != nil {
		fatalf("commit: %v", err)
	}
	fmt.Println("Seed complete ✅")
}

func loadCSV(path string) ([]TopicCSV, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Wrap in a buffered reader for safety; csv.Reader handles quotes & commas
	br := bufio.NewReader(f)
	r := csv.NewReader(br)
	r.TrimLeadingSpace = true
	// Do NOT enable LazyQuotes unless you must; prefer proper CSV export

	headers, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	idx := map[string]int{}
	for i, h := range headers {
		idx[strings.TrimSpace(h)] = i
	}
	required := []string{"title", "shortTitle", "stance1", "stance2", "stance3", "stance4", "stance5", "categories"}
	for _, k := range required {
		if _, ok := idx[k]; !ok {
			return nil, fmt.Errorf("missing required column: %s", k)
		}
	}

	var out []TopicCSV
	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("csv read: %w", err)
		}

		row := TopicCSV{
			Title:      strings.TrimSpace(rec[idx["title"]]),
			ShortTitle: strings.TrimSpace(rec[idx["shortTitle"]]),
		}
		row.Stances[0] = strings.TrimSpace(rec[idx["stance1"]])
		row.Stances[1] = strings.TrimSpace(rec[idx["stance2"]])
		row.Stances[2] = strings.TrimSpace(rec[idx["stance3"]])
		row.Stances[3] = strings.TrimSpace(rec[idx["stance4"]])
		row.Stances[4] = strings.TrimSpace(rec[idx["stance5"]])

		cats := strings.TrimSpace(rec[idx["categories"]])
		if cats == "" {
			row.Categories = nil
		} else {
			parts := strings.Split(cats, ";") // semicolon-separated, no spaces expected
			for _, p := range parts {
				c := strings.TrimSpace(p)
				if c != "" {
					row.Categories = append(row.Categories, c)
				}
			}
		}

		out = append(out, row)
	}
	return out, nil
}

func validateRows(rows []TopicCSV) error {
	if len(rows) == 0 {
		return fmt.Errorf("CSV has no data rows")
	}
	seen := make(map[string]struct{}, len(rows))
	for i, r := range rows {
		if r.Title == "" {
			return fmt.Errorf("row %d: title is empty", i+2)
		}
		if r.ShortTitle == "" {
			return fmt.Errorf("row %d: shortTitle is empty", i+2)
		}
		for sIdx, s := range r.Stances {
			if s == "" {
				return fmt.Errorf("row %d: stance%d is empty", i+2, sIdx+1)
			}
		}
		if _, dup := seen[strings.ToLower(r.ShortTitle)]; dup {
			return fmt.Errorf("row %d: duplicate shortTitle '%s'", i+2, r.ShortTitle)
		}
		seen[strings.ToLower(r.ShortTitle)] = struct{}{}
	}
	return nil
}

func printPlan(rows []TopicCSV) {
	distinctCats := map[string]struct{}{}
	for _, r := range rows {
		for _, c := range r.Categories {
			distinctCats[c] = struct{}{}
		}
	}
	fmt.Println("Plan preview:")
	fmt.Printf("  Topics to insert: %d\n", len(rows))
	fmt.Printf("  Stances to insert: %d (5 per topic)\n", len(rows)*5)
	fmt.Printf("  Distinct category titles: %d\n", len(distinctCats))
	fmt.Println("  Tables affected (destructive): compass.answers, compass.contexts, compass.stances, compass.topic_categories, compass.topics")
}

func countAll(ctx context.Context, tx *sql.Tx) (Counts, error) {
	var c Counts
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM compass.topics`).Scan(&c.Topics); err != nil {
		return c, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM compass.stances`).Scan(&c.Stances); err != nil {
		return c, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM compass.topic_categories`).Scan(&c.TopicCategories); err != nil {
		return c, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM compass.answers`).Scan(&c.Answers); err != nil {
		return c, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM compass.contexts`).Scan(&c.Contexts); err != nil {
		return c, err
	}
	return c, nil
}

func wipeCompassData(ctx context.Context, tx *sql.Tx) error {
	tables := []string{
		"compass.answers",
		"compass.contexts",
		"compass.stances",
		"compass.topic_categories",
		"compass.topics",
	}
	for _, t := range tables {
		q := fmt.Sprintf("DELETE FROM %s", t)
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("delete %s: %w", t, err)
		}
	}
	return nil
}

func upsertAllCategories(ctx context.Context, tx *sql.Tx, rows []TopicCSV) (map[string]uuid.UUID, error) {
	distinct := map[string]struct{}{}
	for _, r := range rows {
		for _, c := range r.Categories {
			distinct[c] = struct{}{}
		}
	}
	res := make(map[string]uuid.UUID, len(distinct))
	for title := range distinct {
		id, err := upsertCategory(ctx, tx, title)
		if err != nil {
			return nil, err
		}
		res[title] = id
	}
	return res, nil
}

func upsertCategory(ctx context.Context, tx *sql.Tx, title string) (uuid.UUID, error) {
	var id uuid.UUID
	// Unique on title; return id regardless of insert or update
	q := `INSERT INTO compass.categories (id, title)
	      VALUES ($1, $2)
	      ON CONFLICT (title) DO UPDATE SET title = EXCLUDED.title
	      RETURNING id`
	newID := uuid.New()
	if err := tx.QueryRowContext(ctx, q, newID, title).Scan(&id); err != nil {
		return uuid.Nil, fmt.Errorf("upsert category '%s': %w", title, err)
	}
	return id, nil
}

func insertAll(ctx context.Context, tx *sql.Tx, rows []TopicCSV, catIDs map[string]uuid.UUID) error {
	// prepared statements for speed & safety
	topicStmt, err := tx.PrepareContext(ctx, `INSERT INTO compass.topics (id, title, short_title) VALUES ($1,$2,$3)`)
	if err != nil {
		return err
	}
	defer topicStmt.Close()

	stanceStmt, err := tx.PrepareContext(ctx, `INSERT INTO compass.stances (id, value, text, topic_id) VALUES ($1,$2,$3,$4)`)
	if err != nil {
		return err
	}
	defer stanceStmt.Close()

	joinStmt, err := tx.PrepareContext(ctx, `INSERT INTO compass.topic_categories (topic_id, category_id) VALUES ($1,$2)`)
	if err != nil {
		return err
	}
	defer joinStmt.Close()

	for _, r := range rows {
		topicID := uuid.New()
		if _, err := topicStmt.ExecContext(ctx, topicID, r.Title, r.ShortTitle); err != nil {
			return fmt.Errorf("insert topic '%s': %w", r.ShortTitle, err)
		}
		// stances 1..5 map to value 1..5
		for i := 0; i < 5; i++ {
			stanceID := uuid.New()
			val := i + 1
			text := r.Stances[i]
			if _, err := stanceStmt.ExecContext(ctx, stanceID, val, text, topicID); err != nil {
				return fmt.Errorf("insert stance %d for '%s': %w", val, r.ShortTitle, err)
			}
		}
		// join to categories (dedupe per-topic)
		seen := map[uuid.UUID]struct{}{}
		for _, cTitle := range r.Categories {
			catID, ok := catIDs[cTitle]
			if !ok {
				return fmt.Errorf("category ID not found for title '%s' (internal)", cTitle)
			}
			if _, dup := seen[catID]; dup {
				continue
			}
			seen[catID] = struct{}{}
			if _, err := joinStmt.ExecContext(ctx, topicID, catID); err != nil {
				return fmt.Errorf("insert topic_category for '%s' -> '%s': %w", r.ShortTitle, cTitle, err)
			}
		}
	}
	return nil
}

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}
