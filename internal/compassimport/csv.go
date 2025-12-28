package compassimport

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
)

type Row struct {
	TopicKey    string
	Title       string
	ShortTitle  string
	StartPhrase string
	Stances     [5]string
	Categories  []string
}

var keyRe = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)

func ParseCSV(path string) ([]Row, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(bufio.NewReader(f))
	r.FieldsPerRecord = -1

	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) < 2 {
		return nil, errors.New("csv has no data rows")
	}

	header := records[0]
	// Handle BOM on first header cell
	if len(header) > 0 {
		header[0] = strings.TrimPrefix(header[0], "\ufeff")
	}

	col := map[string]int{}
	for i, h := range header {
		col[strings.TrimSpace(h)] = i
	}

	req := []string{
		"topic_key", "title", "short_title", "start_phrase",
		"stance_1", "stance_2", "stance_3", "stance_4", "stance_5",
		"categories",
	}
	for _, k := range req {
		if _, ok := col[k]; !ok {
			return nil, fmt.Errorf("missing required column: %s", k)
		}
	}

	seenKeys := map[string]bool{}
	var out []Row

	for rowIdx := 1; rowIdx < len(records); rowIdx++ {
		rec := records[rowIdx]
		get := func(name string) string {
			i := col[name]
			if i >= len(rec) {
				return ""
			}
			return strings.TrimSpace(rec[i])
		}

		tk := get("topic_key")
		if tk == "" {
			return nil, fmt.Errorf("row %d: topic_key is required", rowIdx+1)
		}
		if !keyRe.MatchString(tk) {
			return nil, fmt.Errorf("row %d: topic_key must match %s (got %q)", rowIdx+1, keyRe.String(), tk)
		}
		if seenKeys[tk] {
			return nil, fmt.Errorf("row %d: duplicate topic_key %q", rowIdx+1, tk)
		}
		seenKeys[tk] = true

		var st [5]string
		for i := 0; i < 5; i++ {
			st[i] = get(fmt.Sprintf("stance_%d", i+1))
			if st[i] == "" {
				return nil, fmt.Errorf("row %d: stance_%d is blank (not allowed)", rowIdx+1, i+1)
			}
		}

		catsRaw := get("categories")
		var cats []string
		if catsRaw != "" {
			parts := strings.Split(catsRaw, ";")
			for _, p := range parts {
				c := strings.TrimSpace(p)
				if c != "" {
					cats = append(cats, c)
				}
			}
		}

		out = append(out, Row{
			TopicKey:    tk,
			Title:       get("title"),
			ShortTitle:  get("short_title"),
			StartPhrase: get("start_phrase"),
			Stances:     st,
			Categories:  cats,
		})
	}

	return out, nil
}
