package campaign_finance

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

// defaultCompletedCycle returns the most recently completed even-year election cycle.
// "Completed" means prior to the current year: e.g. in 2026, returns "2024".
func defaultCompletedCycle() string {
	year := time.Now().Year()
	// Step back to the most recent even year strictly before current year.
	if year%2 != 0 {
		year--
	} else {
		year -= 2
	}
	return strconv.Itoa(year)
}

// encodeCursor base64-encodes a cursor from a contribution_date + id pair.
func encodeCursor(date time.Time, id uuid.UUID) string {
	raw := date.UTC().Format(time.RFC3339) + "|" + id.String()
	return base64.StdEncoding.EncodeToString([]byte(raw))
}

// decodeCursor decodes a base64 cursor back into a time and UUID.
func decodeCursor(cursor string) (time.Time, uuid.UUID, error) {
	decoded, err := base64.StdEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}
	parts := strings.SplitN(string(decoded), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, uuid.Nil, fmt.Errorf("invalid cursor format")
	}
	t, err := time.Parse(time.RFC3339, parts[0])
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("invalid cursor date: %w", err)
	}
	id, err := uuid.Parse(parts[1])
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("invalid cursor id: %w", err)
	}
	return t, id, nil
}

// validateConfidence maps a lowercase query param to the DB confidence_level value.
// Returns "" (empty) when omitted, or ("", error) when invalid.
func validateConfidence(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	switch strings.ToLower(raw) {
	case "high":
		return "HIGH", nil
	case "medium":
		return "MEDIUM", nil
	case "estimated":
		return "ESTIMATED", nil
	}
	return "", fmt.Errorf("invalid confidence value %q: must be high, medium, or estimated", raw)
}

// ---- response types (no politician_source_id exposed) ----

type sectorEntry struct {
	Sector string  `json:"sector"`
	Total  float64 `json:"total"`
	Count  int     `json:"count"`
}

type topDonorEntry struct {
	Name              string  `json:"name"`
	DonorType         string  `json:"donor_type"`
	Employer          string  `json:"employer"`
	Occupation        string  `json:"occupation"`
	Sector            string  `json:"sector"`
	TotalAmount       float64 `json:"total_amount"`
	ContributionCount int     `json:"contribution_count"`
	ConfidenceLevel   string  `json:"confidence_level"`
}

type summaryResponse struct {
	PoliticianID      string          `json:"politician_id"`
	Cycle             string          `json:"cycle"`
	TotalRaised       float64         `json:"total_raised"`
	ContributionCount int             `json:"contribution_count"`
	ConfidenceLevel   string          `json:"confidence_level"`
	DataSource        string          `json:"data_source"`
	LastSyncAt        *time.Time      `json:"last_sync_at"`
	AvailableCycles   []string        `json:"available_cycles"`
	IndividualTotal   float64         `json:"individual_total"`
	PACTotal          float64         `json:"pac_total"`
	SectorBreakdown   []sectorEntry   `json:"sector_breakdown"`
	TopDonors         []topDonorEntry `json:"top_donors"`
}

type contributionResult struct {
	ID               uuid.UUID `json:"id"`
	DonorName        string    `json:"donor_name"`
	DonorType        string    `json:"donor_type"`
	Employer         string    `json:"employer"`
	Occupation       string    `json:"occupation"`
	Sector           string    `json:"sector"`
	Amount           float64   `json:"amount"`
	ContributionDate string    `json:"contribution_date"`
	ConfidenceLevel  string    `json:"confidence_level"`
	DataSource       string    `json:"data_source"`
}

type pageInfo struct {
	HasNextPage bool   `json:"has_next_page"`
	EndCursor   string `json:"end_cursor"`
	TotalCount  int    `json:"total_count"`
}

type contributionsResponse struct {
	Results  []contributionResult `json:"results"`
	PageInfo pageInfo             `json:"page_info"`
}

// ---- row scan helpers ----

type cycleRow struct {
	ElectionCycle string
}

type totalsRow struct {
	TotalRaised       float64
	ContributionCount int
	ConfidenceLevel   string
	IndividualTotal   float64
	PACTotal          float64
}

type occupationRow struct {
	Occupation string
	Amount     float64
}

type donorRow struct {
	ContributorName   string
	TotalAmount       float64
	ContributionCount int
	ConfidenceLevel   string
	RawRecord         []byte
}

type contribRow struct {
	ID               uuid.UUID
	Amount           float64
	ContributionDate *time.Time
	ElectionCycle    string
	ConfidenceLevel  string
	DataSource       string
	RawRecord        []byte
}

// SummaryHandler handles GET /campaign-finance/politician/{id}/summary
func SummaryHandler(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	politicianID, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, "invalid politician id: must be a UUID", http.StatusBadRequest)
		return
	}

	// Parse optional ?cycle
	cycle := r.URL.Query().Get("cycle")
	if cycle == "" {
		cycle = defaultCompletedCycle()
	}

	// Parse optional ?confidence
	confidenceParam := r.URL.Query().Get("confidence")
	confidence, err := validateConfidence(confidenceParam)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Query available cycles
	availCycleSQL := `
		SELECT DISTINCT c.election_cycle
		FROM transparent_motivations.contributions c
		JOIN transparent_motivations.politician_sources ps ON c.politician_source_id = ps.id
		WHERE ps.essentials_politician_id = ?
		  AND ps.research_status = 'confirmed'
		ORDER BY c.election_cycle DESC`

	rows, err := db.DB.Raw(availCycleSQL, politicianID).Rows()
	if err != nil {
		http.Error(w, "failed to query available cycles", http.StatusInternalServerError)
		return
	}
	var availableCycles []string
	for rows.Next() {
		var row cycleRow
		if scanErr := rows.Scan(&row.ElectionCycle); scanErr == nil {
			availableCycles = append(availableCycles, row.ElectionCycle)
		}
	}
	rows.Close()

	// Return zero-state when no data found — not 404
	if len(availableCycles) == 0 {
		resp := summaryResponse{
			PoliticianID:    politicianID.String(),
			Cycle:           cycle,
			AvailableCycles: []string{},
			SectorBreakdown: []sectorEntry{},
			TopDonors:       []topDonorEntry{},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
		return
	}

	// Build totals query with optional confidence filter
	confidenceFilter := ""
	var totalArgs []interface{}
	totalArgs = append(totalArgs, politicianID, cycle)
	if confidence != "" {
		confidenceFilter = "AND c.confidence_level = ?"
		totalArgs = append(totalArgs, confidence)
	}

	totalsSQL := fmt.Sprintf(`
		SELECT
			COALESCE(SUM(c.amount), 0) AS total_raised,
			COUNT(*) AS contribution_count,
			COALESCE(MIN(CASE c.confidence_level
				WHEN 'HIGH'      THEN 1
				WHEN 'MEDIUM'    THEN 2
				WHEN 'ESTIMATED' THEN 3
				ELSE 4 END), 0) AS confidence_level,
			COALESCE(SUM(CASE WHEN c.raw_record->>'entity_type' LIKE 'IND%%' THEN c.amount ELSE 0 END), 0) AS individual_total,
			COALESCE(SUM(CASE WHEN c.raw_record->>'entity_type' NOT LIKE 'IND%%' OR c.raw_record->>'entity_type' IS NULL THEN c.amount ELSE 0 END), 0) AS pac_total
		FROM transparent_motivations.contributions c
		JOIN transparent_motivations.politician_sources ps ON c.politician_source_id = ps.id
		WHERE ps.essentials_politician_id = ?
		  AND c.election_cycle = ?
		  AND ps.research_status = 'confirmed'
		  %s`, confidenceFilter)

	totalsRow := struct {
		TotalRaised       float64
		ContributionCount int
		ConfidenceLevelN  int
		IndividualTotal   float64
		PACTotal          float64
	}{}
	if err := db.DB.Raw(totalsSQL, totalArgs...).
		Row().Scan(
		&totalsRow.TotalRaised,
		&totalsRow.ContributionCount,
		&totalsRow.ConfidenceLevelN,
		&totalsRow.IndividualTotal,
		&totalsRow.PACTotal,
	); err != nil {
		http.Error(w, "failed to query totals", http.StatusInternalServerError)
		return
	}

	// Map numeric confidence rank back to label
	confidenceLabel := map[int]string{1: "HIGH", 2: "MEDIUM", 3: "ESTIMATED"}
	overallConfidence := confidenceLabel[totalsRow.ConfidenceLevelN]
	if overallConfidence == "" {
		overallConfidence = "HIGH"
	}

	// Query occupations for sector breakdown (Go-side classification)
	occupationSQL := fmt.Sprintf(`
		SELECT
			COALESCE(c.raw_record->>'contributor_occupation', '') AS occupation,
			c.amount
		FROM transparent_motivations.contributions c
		JOIN transparent_motivations.politician_sources ps ON c.politician_source_id = ps.id
		WHERE ps.essentials_politician_id = ?
		  AND c.election_cycle = ?
		  AND ps.research_status = 'confirmed'
		  %s`, confidenceFilter)

	occRows, err := db.DB.Raw(occupationSQL, totalArgs...).Rows()
	if err != nil {
		http.Error(w, "failed to query occupations", http.StatusInternalServerError)
		return
	}
	type sectorAccum struct {
		Total float64
		Count int
	}
	sectorMap := make(map[string]*sectorAccum)
	for occRows.Next() {
		var occ string
		var amt float64
		if scanErr := occRows.Scan(&occ, &amt); scanErr != nil {
			continue
		}
		sector := ClassifySector(occ)
		if sectorMap[sector] == nil {
			sectorMap[sector] = &sectorAccum{}
		}
		sectorMap[sector].Total += amt
		sectorMap[sector].Count++
	}
	occRows.Close()

	// Sort sectors by total descending and take top 10
	sectors := make([]sectorEntry, 0, len(sectorMap))
	for s, a := range sectorMap {
		sectors = append(sectors, sectorEntry{Sector: s, Total: a.Total, Count: a.Count})
	}
	// Simple insertion-sort (≤10 entries typical — avoid import overhead)
	for i := 1; i < len(sectors); i++ {
		for j := i; j > 0 && sectors[j].Total > sectors[j-1].Total; j-- {
			sectors[j], sectors[j-1] = sectors[j-1], sectors[j]
		}
	}
	if len(sectors) > 10 {
		sectors = sectors[:10]
	}

	// Query top donors
	topDonorSQL := fmt.Sprintf(`
		SELECT
			COALESCE(c.raw_record->>'contributor_name', '') AS contributor_name,
			SUM(c.amount)  AS total_amount,
			COUNT(*)       AS contribution_count,
			MIN(CASE c.confidence_level
				WHEN 'HIGH'      THEN 1
				WHEN 'MEDIUM'    THEN 2
				WHEN 'ESTIMATED' THEN 3
				ELSE 4 END) AS confidence_level,
			(array_agg(c.raw_record ORDER BY c.amount DESC))[1] AS raw_record
		FROM transparent_motivations.contributions c
		JOIN transparent_motivations.politician_sources ps ON c.politician_source_id = ps.id
		WHERE ps.essentials_politician_id = ?
		  AND c.election_cycle = ?
		  AND ps.research_status = 'confirmed'
		  %s
		GROUP BY c.raw_record->>'contributor_name'
		ORDER BY total_amount DESC
		LIMIT 20`, confidenceFilter)

	donorRows, err := db.DB.Raw(topDonorSQL, totalArgs...).Rows()
	if err != nil {
		http.Error(w, "failed to query top donors", http.StatusInternalServerError)
		return
	}
	var topDonors []topDonorEntry
	for donorRows.Next() {
		var name string
		var totalAmt float64
		var contribCount int
		var confidenceN int
		var rawRecord []byte
		if scanErr := donorRows.Scan(&name, &totalAmt, &contribCount, &confidenceN, &rawRecord); scanErr != nil {
			continue
		}
		occ := ExtractOccupation(rawRecord)
		topDonors = append(topDonors, topDonorEntry{
			Name:              name,
			DonorType:         ExtractDonorType(rawRecord),
			Employer:          ExtractEmployer(rawRecord),
			Occupation:        occ,
			Sector:            ClassifySector(occ),
			TotalAmount:       totalAmt,
			ContributionCount: contribCount,
			ConfidenceLevel:   confidenceLabel[confidenceN],
		})
	}
	donorRows.Close()
	if topDonors == nil {
		topDonors = []topDonorEntry{}
	}

	// Query last_sync_at for FEC
	var meta DataSourceMetadata
	_ = db.DB.Where("source_system = 'fec'").First(&meta).Error

	// Set freshness header
	if meta.LastSyncAt != nil {
		w.Header().Set("X-Data-Updated-At", meta.LastSyncAt.UTC().Format(time.RFC3339))
	}

	resp := summaryResponse{
		PoliticianID:      politicianID.String(),
		Cycle:             cycle,
		TotalRaised:       totalsRow.TotalRaised,
		ContributionCount: totalsRow.ContributionCount,
		ConfidenceLevel:   overallConfidence,
		DataSource:        "fec",
		LastSyncAt:        meta.LastSyncAt,
		AvailableCycles:   availableCycles,
		IndividualTotal:   totalsRow.IndividualTotal,
		PACTotal:          totalsRow.PACTotal,
		SectorBreakdown:   sectors,
		TopDonors:         topDonors,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ContributionsHandler handles GET /campaign-finance/politician/{id}/contributions
func ContributionsHandler(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	politicianID, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, "invalid politician id: must be a UUID", http.StatusBadRequest)
		return
	}

	// ?cycle is required
	cycle := r.URL.Query().Get("cycle")
	if cycle == "" {
		http.Error(w, "cycle query parameter is required", http.StatusBadRequest)
		return
	}

	// Optional ?confidence
	confidenceParam := r.URL.Query().Get("confidence")
	confidence, err := validateConfidence(confidenceParam)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// ?limit — default 20, max 100
	limit := 20
	if lStr := r.URL.Query().Get("limit"); lStr != "" {
		if l, parseErr := strconv.Atoi(lStr); parseErr == nil {
			if l < 1 {
				l = 1
			}
			if l > 100 {
				l = 100
			}
			limit = l
		}
	}

	// ?cursor — optional base64 encoded
	var cursorDate time.Time
	var cursorID uuid.UUID
	hasCursor := false
	if cStr := r.URL.Query().Get("cursor"); cStr != "" {
		cursorDate, cursorID, err = decodeCursor(cStr)
		if err != nil {
			http.Error(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
			return
		}
		hasCursor = true
	}

	// Build WHERE args
	confidenceFilter := ""
	var queryArgs []interface{}
	queryArgs = append(queryArgs, politicianID, cycle)
	if confidence != "" {
		confidenceFilter = "AND c.confidence_level = ?"
		queryArgs = append(queryArgs, confidence)
	}

	cursorFilter := ""
	var pageArgs []interface{}
	pageArgs = append(pageArgs, queryArgs...)
	if hasCursor {
		cursorFilter = "AND (c.contribution_date, c.id::text) < (?, ?)"
		pageArgs = append(pageArgs, cursorDate, cursorID.String())
	}

	pageSQL := fmt.Sprintf(`
		SELECT
			c.id,
			c.amount,
			c.contribution_date,
			c.election_cycle,
			c.confidence_level,
			c.data_source,
			c.raw_record
		FROM transparent_motivations.contributions c
		JOIN transparent_motivations.politician_sources ps ON c.politician_source_id = ps.id
		WHERE ps.essentials_politician_id = ?
		  AND c.election_cycle = ?
		  AND ps.research_status = 'confirmed'
		  %s
		  %s
		ORDER BY c.contribution_date DESC, c.id DESC
		LIMIT %d`, confidenceFilter, cursorFilter, limit+1)

	pageRows, err := db.DB.Raw(pageSQL, pageArgs...).Rows()
	if err != nil {
		http.Error(w, "failed to query contributions", http.StatusInternalServerError)
		return
	}

	var rawResults []contribRow
	for pageRows.Next() {
		var row contribRow
		if scanErr := pageRows.Scan(
			&row.ID, &row.Amount, &row.ContributionDate, &row.ElectionCycle,
			&row.ConfidenceLevel, &row.DataSource, &row.RawRecord,
		); scanErr != nil {
			continue
		}
		rawResults = append(rawResults, row)
	}
	pageRows.Close()

	hasNextPage := len(rawResults) > limit
	if hasNextPage {
		rawResults = rawResults[:limit]
	}

	// Build response items
	results := make([]contributionResult, 0, len(rawResults))
	for _, row := range rawResults {
		dateStr := ""
		if row.ContributionDate != nil {
			dateStr = row.ContributionDate.Format("2006-01-02")
		}
		occ := ExtractOccupation(row.RawRecord)
		rec := parseRawRecord(row.RawRecord)
		results = append(results, contributionResult{
			ID:               row.ID,
			DonorName:        rec.ContributorName,
			DonorType:        ExtractDonorType(row.RawRecord),
			Employer:         ExtractEmployer(row.RawRecord),
			Occupation:       occ,
			Sector:           ClassifySector(occ),
			Amount:           row.Amount,
			ContributionDate: dateStr,
			ConfidenceLevel:  row.ConfidenceLevel,
			DataSource:       row.DataSource,
		})
	}

	// Build end_cursor from last result
	endCursor := ""
	if hasNextPage && len(rawResults) > 0 {
		last := rawResults[len(rawResults)-1]
		if last.ContributionDate != nil {
			endCursor = encodeCursor(*last.ContributionDate, last.ID)
		}
	}

	// Count total matching rows (no cursor, no limit)
	countSQL := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM transparent_motivations.contributions c
		JOIN transparent_motivations.politician_sources ps ON c.politician_source_id = ps.id
		WHERE ps.essentials_politician_id = ?
		  AND c.election_cycle = ?
		  AND ps.research_status = 'confirmed'
		  %s`, confidenceFilter)

	var totalCount int
	if err := db.DB.Raw(countSQL, queryArgs...).Row().Scan(&totalCount); err != nil {
		totalCount = 0
	}

	// Query last_sync_at for freshness header
	var meta DataSourceMetadata
	_ = db.DB.Where("source_system = 'fec'").First(&meta).Error
	if meta.LastSyncAt != nil {
		w.Header().Set("X-Data-Updated-At", meta.LastSyncAt.UTC().Format(time.RFC3339))
	}
	w.Header().Set("X-Total-Count", strconv.Itoa(totalCount))

	resp := contributionsResponse{
		Results: results,
		PageInfo: pageInfo{
			HasNextPage: hasNextPage,
			EndCursor:   endCursor,
			TotalCount:  totalCount,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
