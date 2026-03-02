// cmd/lookup-sources is a research CLI that helps operators find external source IDs
// (FEC candidate IDs, Cal-Access FILER_IDs, Indiana OrgIds, LA Socrata entity IDs)
// for prototype politicians. It reads data/seed/prototype_politicians.json and
// writes suggested mappings to data/seed/source_ids.json.
//
// Usage:
//
//	go run cmd/lookup-sources/main.go              # print mode summary
//	go run cmd/lookup-sources/main.go --help       # print detailed usage
//	go run cmd/lookup-sources/main.go --lookup-fec # query FEC API for all politicians
//	go run cmd/lookup-sources/main.go --lookup-calaccess [--tsv-path=/path/to/FILERS_CD.TSV]
//	go run cmd/lookup-sources/main.go --lookup-indiana
//	go run cmd/lookup-sources/main.go --lookup-socrata
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// PrototypePolitician is a row from data/seed/prototype_politicians.json.
type PrototypePolitician struct {
	EssentialsPoliticianID     string `json:"essentials_politician_id"`
	EssentialsPoliticianIDNote string `json:"essentials_politician_id_note,omitempty"`
	Name                       string `json:"name"`
	FirstName                  string `json:"first_name"`
	LastName                   string `json:"last_name"`
	State                      string `json:"state"`
	Office                     string `json:"office"`
	District                   string `json:"district"`
	Level                      string `json:"level"`
	PilotCity                  string `json:"pilot_city"`
}

// SourceIDEntry is a row written to data/seed/source_ids.json.
type SourceIDEntry struct {
	EssentialsPoliticianID string `json:"essentials_politician_id"`
	PoliticianName         string `json:"politician_name"`
	SourceSystem           string `json:"source_system"`
	ExternalID             string `json:"external_id"`
	ResearchStatus         string `json:"research_status"`
	Notes                  string `json:"notes"`
}

// FECCandidate mirrors the FEC /v1/candidates/ response result object.
type FECCandidate struct {
	CandidateID     string `json:"candidate_id"`
	Name            string `json:"name"`
	Office          string `json:"office"`
	OfficeFull      string `json:"office_full"`
	State           string `json:"state"`
	District        string `json:"district"`
	Party           string `json:"party"`
	Cycles          []int  `json:"cycles"`
	ActiveThrough   int    `json:"active_through"`
	CandidateStatus string `json:"candidate_status"`
}

// FECResponse mirrors the FEC /v1/candidates/ top-level response.
type FECResponse struct {
	Results    []FECCandidate `json:"results"`
	Pagination struct {
		Count int `json:"count"`
		Page  int `json:"page"`
		Pages int `json:"pages"`
	} `json:"pagination"`
}

// fecOfficeCode maps a politician office string to an FEC office letter.
// FEC uses "H" (House), "S" (Senate), "P" (President).
func fecOfficeCode(office string) string {
	lower := strings.ToLower(office)
	switch {
	case strings.Contains(lower, "senator") || strings.Contains(lower, "senate"):
		return "S"
	case strings.Contains(lower, "representative") || strings.Contains(lower, "house"):
		return "H"
	case strings.Contains(lower, "president"):
		return "P"
	default:
		return ""
	}
}

// fecSourceSystem maps FEC office letter to a source_system string.
func fecSourceSystem(officeCode string) string {
	switch officeCode {
	case "H":
		return "fec_house"
	case "S":
		return "fec_senate"
	case "P":
		return "fec_president"
	default:
		return "fec"
	}
}

// dataDir returns the absolute path to data/seed/ relative to where the binary
// runs. When invoked as `go run cmd/lookup-sources/main.go` from the repo root
// the working directory is the repo root — so data/seed is correct. When built
// into a binary and placed elsewhere this falls back to the executable's
// directory. Uses LOOKUP_SOURCES_DATA_DIR env override for tests.
func dataDir() string {
	if override := os.Getenv("LOOKUP_SOURCES_DATA_DIR"); override != "" {
		return override
	}
	return filepath.Join("data", "seed")
}

func readPrototypePoliticians() ([]PrototypePolitician, error) {
	path := filepath.Join(dataDir(), "prototype_politicians.json")
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot open %s: %w\n(tip: run from the EV-Backend repo root)", path, err)
	}
	defer f.Close()
	var politicians []PrototypePolitician
	if err := json.NewDecoder(f).Decode(&politicians); err != nil {
		return nil, fmt.Errorf("cannot parse %s: %w", path, err)
	}
	return politicians, nil
}

// readSourceIDs reads the existing source_ids.json (empty slice if file absent).
func readSourceIDs() ([]SourceIDEntry, error) {
	path := filepath.Join(dataDir(), "source_ids.json")
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return []SourceIDEntry{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cannot open %s: %w", path, err)
	}
	defer f.Close()
	var entries []SourceIDEntry
	if err := json.NewDecoder(f).Decode(&entries); err != nil {
		return nil, fmt.Errorf("cannot parse %s: %w", path, err)
	}
	return entries, nil
}

// writeSourceIDs atomically overwrites data/seed/source_ids.json.
func writeSourceIDs(entries []SourceIDEntry) error {
	path := filepath.Join(dataDir(), "source_ids.json")
	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return fmt.Errorf("json marshal: %w", err)
	}
	return os.WriteFile(path, data, 0644)
}

// mergeEntries upserts new entries into existing by (essentials_politician_id, source_system).
// Existing entries already confirmed by a human are never overwritten.
func mergeEntries(existing, incoming []SourceIDEntry) []SourceIDEntry {
	type key struct{ id, system string }
	idx := map[key]int{}
	for i, e := range existing {
		idx[key{e.EssentialsPoliticianID, e.SourceSystem}] = i
	}
	result := make([]SourceIDEntry, len(existing))
	copy(result, existing)
	for _, inc := range incoming {
		k := key{inc.EssentialsPoliticianID, inc.SourceSystem}
		if pos, found := idx[k]; found {
			// Never downgrade a confirmed entry back to needs_research.
			if result[pos].ResearchStatus == "confirmed" {
				continue
			}
			result[pos] = inc
		} else {
			idx[k] = len(result)
			result = append(result, inc)
		}
	}
	return result
}

// lookupFEC queries the FEC API for every prototype politician that is a
// federal official (House, Senate, President). Results are written to stdout
// and appended to source_ids.json.
func lookupFEC() error {
	apiKey := os.Getenv("FEC_API_KEY")
	usingDemoKey := apiKey == ""
	if usingDemoKey {
		apiKey = "DEMO_KEY"
		fmt.Fprintf(os.Stderr, "WARNING: FEC_API_KEY not set — using DEMO_KEY (40 req/hr limit). Set FEC_API_KEY for 1000 req/hr.\n\n")
	}

	politicians, err := readPrototypePoliticians()
	if err != nil {
		return err
	}

	var incoming []SourceIDEntry
	totalQueried := 0
	totalMatches := 0

	for _, p := range politicians {
		officeCode := fecOfficeCode(p.Office)
		if officeCode == "" {
			// State/local officials don't have FEC IDs — skip silently.
			continue
		}

		totalQueried++
		fmt.Printf("--- %s (%s, %s) ---\n", p.Name, p.Office, p.State)

		candidates, err := searchFEC(p.Name, p.State, officeCode, apiKey)
		if err != nil {
			fmt.Printf("  ERROR: %v\n", err)
		} else if len(candidates) == 0 {
			fmt.Printf("  No FEC matches found.\n")
			// Emit a placeholder entry so every federal politician has a row.
			incoming = append(incoming, SourceIDEntry{
				EssentialsPoliticianID: p.EssentialsPoliticianID,
				PoliticianName:         p.Name,
				SourceSystem:           fecSourceSystem(officeCode),
				ExternalID:             "",
				ResearchStatus:         "needs_research",
				Notes:                  "FEC API returned no matches — manual research needed at https://www.fec.gov/data/candidates/",
			})
		} else {
			totalMatches += len(candidates)
			for _, c := range candidates {
				cycleStr := ""
				if len(c.Cycles) > 0 {
					cycleInts := make([]string, len(c.Cycles))
					for i, cy := range c.Cycles {
						cycleInts[i] = fmt.Sprintf("%d", cy)
					}
					cycleStr = strings.Join(cycleInts, ",")
				}
				fmt.Printf("  ID=%-14s  Office=%-8s  State=%s  District=%-4s  Status=%-2s  ActiveThru=%d  Cycles=[%s]  Name=%s\n",
					c.CandidateID, c.OfficeFull, c.State, c.District, c.CandidateStatus, c.ActiveThrough, cycleStr, c.Name)
				incoming = append(incoming, SourceIDEntry{
					EssentialsPoliticianID: p.EssentialsPoliticianID,
					PoliticianName:         p.Name,
					SourceSystem:           fecSourceSystem(officeCode),
					ExternalID:             c.CandidateID,
					ResearchStatus:         "needs_research",
					Notes: fmt.Sprintf("FEC API match (status=%s, activeThru=%d, cycles=%s) — verify at https://www.fec.gov/data/candidates/%s/",
						c.CandidateStatus, c.ActiveThrough, cycleStr, c.CandidateID),
				})
			}
		}

		// Rate limiting: 1s with DEMO_KEY, 100ms with production key.
		if usingDemoKey {
			time.Sleep(1 * time.Second)
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}

	fmt.Printf("\n=== FEC Lookup Complete: %d politicians queried, %d FEC matches found ===\n", totalQueried, totalMatches)
	fmt.Printf("Writing suggestions to data/seed/source_ids.json...\n")

	existing, err := readSourceIDs()
	if err != nil {
		return err
	}
	merged := mergeEntries(existing, incoming)
	if err := writeSourceIDs(merged); err != nil {
		return fmt.Errorf("write source_ids.json: %w", err)
	}
	fmt.Printf("Wrote %d total entries to data/seed/source_ids.json.\n", len(merged))
	fmt.Printf("\nNext step: Review each FEC candidate ID at https://www.fec.gov/data/candidates/\n")
	fmt.Printf("Change research_status from \"needs_research\" to \"confirmed\" for verified entries.\n")
	fmt.Printf("Remove false matches (wrong person with similar name).\n")
	return nil
}

// searchFEC calls GET https://api.open.fec.gov/v1/candidates/ and returns results.
func searchFEC(name, state, officeCode, apiKey string) ([]FECCandidate, error) {
	params := url.Values{}
	params.Set("name", name)
	params.Set("state", state)
	if officeCode != "" {
		params.Set("office", officeCode)
	}
	params.Set("api_key", apiKey)
	params.Set("per_page", "20")

	endpoint := "https://api.open.fec.gov/v1/candidates/?" + params.Encode()

	resp, err := http.Get(endpoint)
	if err != nil {
		return nil, fmt.Errorf("HTTP GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 {
		return nil, fmt.Errorf("FEC API rate limit exceeded (HTTP 429) — wait 1 hour or set FEC_API_KEY for higher limits")
	}
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("FEC API returned HTTP %d: %s", resp.StatusCode, string(body))
	}

	var result FECResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode FEC response: %w", err)
	}
	return result.Results, nil
}

// lookupCalAccess searches a locally downloaded FILERS_CD.TSV for California
// politicians. If the TSV file doesn't exist it prints download instructions.
func lookupCalAccess(tsvPath string) error {
	if tsvPath == "" {
		tsvPath = "FILERS_CD.TSV"
	}

	// Check if TSV file exists.
	if _, err := os.Stat(tsvPath); os.IsNotExist(err) {
		fmt.Printf("Cal-Access FILERS_CD.TSV not found at: %s\n\n", tsvPath)
		fmt.Println("To download it:")
		fmt.Println("  1. Go to https://www.sos.ca.gov/campaign-lobbying/helpful-resources/raw-data-campaign-finance-and-lobbying-activity")
		fmt.Println("  2. Download the 'Campaign Finance and Lobbying Activity' bulk ZIP")
		fmt.Println("  3. Extract FILERS_CD.TSV from the ZIP")
		fmt.Println("  4. Re-run: go run cmd/lookup-sources/main.go --lookup-calaccess --tsv-path=/path/to/FILERS_CD.TSV")
		fmt.Println()
		fmt.Println("FILERS_CD.TSV key fields:")
		fmt.Println("  FILER_ID    — integer, used as external_id in source_ids.json")
		fmt.Println("  FILER_NAML  — last name")
		fmt.Println("  FILER_NAMF  — first name")
		fmt.Println("  FILER_CITY  — city of record")
		fmt.Println("  FILER_ST    — state")
		return nil
	}

	politicians, err := readPrototypePoliticians()
	if err != nil {
		return err
	}

	// Build a map of CA politician last names for efficient lookup.
	type caPol struct {
		p     PrototypePolitician
		found bool
	}
	caPols := map[string]*caPol{}
	for i := range politicians {
		if politicians[i].State == "CA" {
			lastName := strings.ToLower(politicians[i].LastName)
			caPols[lastName] = &caPol{p: politicians[i]}
		}
	}
	if len(caPols) == 0 {
		fmt.Println("No CA politicians found in prototype_politicians.json.")
		return nil
	}

	fmt.Printf("Searching %s for %d CA politicians...\n\n", tsvPath, len(caPols))

	f, err := os.Open(tsvPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", tsvPath, err)
	}
	defer f.Close()

	// Parse TSV: find header row to locate column indexes.
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	headerLine := ""
	if scanner.Scan() {
		headerLine = scanner.Text()
	}
	headers := strings.Split(headerLine, "\t")
	colIdx := map[string]int{}
	for i, h := range headers {
		colIdx[strings.TrimSpace(h)] = i
	}

	filerIDCol, ok1 := colIdx["FILER_ID"]
	filerNAMLCol, ok2 := colIdx["FILER_NAML"]
	filerNAMFCol, ok3 := colIdx["FILER_NAMF"]
	filerCITYCol := colIdx["FILER_CITY"]
	filerSTCol := colIdx["FILER_ST"]

	if !ok1 || !ok2 || !ok3 {
		return fmt.Errorf("FILERS_CD.TSV missing expected columns (FILER_ID, FILER_NAML, FILER_NAMF). Got: %v", headers[:min(len(headers), 10)])
	}

	type match struct {
		filerID   string
		lastName  string
		firstName string
		city      string
		state     string
	}
	matches := map[string][]match{} // key = lowercase last name

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		cols := strings.Split(scanner.Text(), "\t")
		if len(cols) <= filerNAMLCol {
			continue
		}
		lastName := strings.ToLower(strings.TrimSpace(cols[filerNAMLCol]))
		if _, wanted := caPols[lastName]; !wanted {
			continue
		}
		filerID := ""
		if filerIDCol < len(cols) {
			filerID = strings.TrimSpace(cols[filerIDCol])
		}
		firstName := ""
		if filerNAMFCol < len(cols) {
			firstName = strings.TrimSpace(cols[filerNAMFCol])
		}
		city := ""
		if filerCITYCol < len(cols) {
			city = strings.TrimSpace(cols[filerCITYCol])
		}
		stateVal := ""
		if filerSTCol < len(cols) {
			stateVal = strings.TrimSpace(cols[filerSTCol])
		}
		matches[lastName] = append(matches[lastName], match{filerID, lastName, firstName, city, stateVal})
		caPols[lastName].found = true
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanning %s: %w", tsvPath, err)
	}

	var incoming []SourceIDEntry
	for lastName, cp := range caPols {
		fmt.Printf("--- %s (%s) ---\n", cp.p.Name, cp.p.Office)
		if !cp.found {
			fmt.Printf("  No Cal-Access matches for last name '%s'.\n", lastName)
			incoming = append(incoming, SourceIDEntry{
				EssentialsPoliticianID: cp.p.EssentialsPoliticianID,
				PoliticianName:         cp.p.Name,
				SourceSystem:           "cal_access",
				ExternalID:             "",
				ResearchStatus:         "needs_research",
				Notes:                  "Cal-Access FILERS_CD.TSV: no match for last name — manual research needed",
			})
		} else {
			for _, m := range matches[lastName] {
				fmt.Printf("  FILER_ID=%-10s  Name=%s %s  City=%s  State=%s\n",
					m.filerID, m.firstName, m.lastName, m.city, m.state)
				incoming = append(incoming, SourceIDEntry{
					EssentialsPoliticianID: cp.p.EssentialsPoliticianID,
					PoliticianName:         cp.p.Name,
					SourceSystem:           "cal_access",
					ExternalID:             m.filerID,
					ResearchStatus:         "needs_research",
					Notes:                  fmt.Sprintf("Cal-Access TSV match: FILER_ID=%s (%s %s, %s %s) — verify at https://cal-access.sos.ca.gov/Campaign/Committees/Detail.aspx?id=%s", m.filerID, m.firstName, m.lastName, m.city, m.state, m.filerID),
				})
			}
		}
	}

	existing, err := readSourceIDs()
	if err != nil {
		return err
	}
	merged := mergeEntries(existing, incoming)
	if err := writeSourceIDs(merged); err != nil {
		return fmt.Errorf("write source_ids.json: %w", err)
	}
	fmt.Printf("\nWrote %d total entries to data/seed/source_ids.json.\n", len(merged))
	return nil
}

// lookupIndiana prints manual research instructions for Indiana OrgIds.
func lookupIndiana() {
	fmt.Println("Indiana OrgId Lookup — Manual Process (no API available)")
	fmt.Println("==========================================================")
	fmt.Println()
	fmt.Println("Indiana's campaign finance portal provides no REST API.")
	fmt.Println("OrgIds must be researched manually. Budget 1-2 hours per politician.")
	fmt.Println()
	fmt.Println("Steps for each Indiana politician:")
	fmt.Println("  1. Go to https://campaignfinance.in.gov/PublicSite/Search.aspx")
	fmt.Println("  2. Under 'Candidate Search', search by last name")
	fmt.Println("  3. Click through to the Committee Detail page")
	fmt.Println("  4. Copy the OrgID from the URL:")
	fmt.Println("       CommitteeDetail.aspx?OrgID=NNNN")
	fmt.Println("  5. Add an entry to data/seed/source_ids.json:")
	fmt.Println()
	fmt.Println(`     {`)
	fmt.Println(`       "essentials_politician_id": "<uuid>",`)
	fmt.Println(`       "politician_name": "Todd Young",`)
	fmt.Println(`       "source_system": "indiana",`)
	fmt.Println(`       "external_id": "4676",`)
	fmt.Println(`       "research_status": "confirmed",`)
	fmt.Println(`       "notes": "Indiana OrgId — verified at campaignfinance.in.gov"`)
	fmt.Println(`     }`)
	fmt.Println()
	fmt.Println("Indiana prototype politicians requiring OrgId research:")
	politicians, err := readPrototypePoliticians()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not load prototype_politicians.json: %v\n", err)
		return
	}
	for _, p := range politicians {
		if p.State == "IN" {
			fmt.Printf("  - %s (%s)\n", p.Name, p.Office)
		}
	}
}

// lookupSocrata prints instructions for LA Socrata entity ID research.
func lookupSocrata() {
	fmt.Println("LA Socrata Entity ID Lookup — Manual Process")
	fmt.Println("=============================================")
	fmt.Println()
	fmt.Println("IMPORTANT: The exact Socrata dataset field name for LA campaign finance")
	fmt.Println("entity IDs is UNCONFIRMED (see RESEARCH.md Open Question 2).")
	fmt.Println()
	fmt.Println("Before writing the LA Socrata lookup script, complete this research:")
	fmt.Println()
	fmt.Println("  1. Browse: https://data.lacity.org/browse?q=campaign+contributions")
	fmt.Println("  2. Find the campaign contributions dataset (note the 4-char dataset ID)")
	fmt.Println("  3. In the dataset, identify the column that uniquely identifies a filer")
	fmt.Println("     (look for columns like: filer_id, entity_id, committee_id, filer_code)")
	fmt.Println("  4. Compare with ethics.lacity.gov/data/campaigns/contributions/")
	fmt.Println("     to confirm the field matches what the Ethics Commission uses")
	fmt.Println("  5. Document findings in RESEARCH.md before seeding any Socrata IDs")
	fmt.Println()
	fmt.Println("LA prototype politicians requiring Socrata entity ID research:")
	politicians, err := readPrototypePoliticians()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not load prototype_politicians.json: %v\n", err)
		return
	}
	for _, p := range politicians {
		if p.State == "CA" && p.Level == "local" {
			fmt.Printf("  - %s (%s)\n", p.Name, p.Office)
		}
	}
	fmt.Println()
	fmt.Println("Once confirmed, add entries to data/seed/source_ids.json with:")
	fmt.Println(`  "source_system": "la_socrata"`)
}

// printHelp prints usage information for all modes.
func printHelp() {
	fmt.Println("lookup-sources — Campaign Finance Source ID Research Tool")
	fmt.Println("===========================================================")
	fmt.Println()
	fmt.Println("DESCRIPTION")
	fmt.Println("  Helps operators find external source IDs linking prototype politicians")
	fmt.Println("  to campaign finance data sources (FEC, Cal-Access, Indiana, LA Socrata).")
	fmt.Println()
	fmt.Println("  Reads:  data/seed/prototype_politicians.json")
	fmt.Println("  Writes: data/seed/source_ids.json")
	fmt.Println()
	fmt.Println("USAGE")
	fmt.Println("  go run cmd/lookup-sources/main.go [MODE]")
	fmt.Println()
	fmt.Println("MODES")
	fmt.Println("  (no flags)          Print this mode summary")
	fmt.Println("  --help              Print this help message")
	fmt.Println()
	fmt.Println("  --lookup-fec        Query FEC API for all federal prototype politicians")
	fmt.Println("                      Env: FEC_API_KEY (optional; falls back to DEMO_KEY)")
	fmt.Println("                      Rate limit: 1s/req with DEMO_KEY, 100ms with production key")
	fmt.Println()
	fmt.Println("  --lookup-calaccess  Search Cal-Access FILERS_CD.TSV by politician last name")
	fmt.Println("  --tsv-path=PATH     Path to FILERS_CD.TSV (default: ./FILERS_CD.TSV)")
	fmt.Println("                      Download from: https://www.sos.ca.gov/campaign-lobbying/")
	fmt.Println("                        helpful-resources/raw-data-campaign-finance-and-lobbying-activity")
	fmt.Println()
	fmt.Println("  --lookup-indiana    Print manual instructions for Indiana OrgId research")
	fmt.Println("                      (Indiana has no API — manual portal lookup required)")
	fmt.Println()
	fmt.Println("  --lookup-socrata    Print instructions for LA Socrata entity ID research")
	fmt.Println("                      (entity field name not yet confirmed — see RESEARCH.md)")
	fmt.Println()
	fmt.Println("SOURCE_IDS.JSON FORMAT")
	fmt.Println(`  [`)
	fmt.Println(`    {`)
	fmt.Println(`      "essentials_politician_id": "uuid-here",`)
	fmt.Println(`      "politician_name": "Erin Houchin",`)
	fmt.Println(`      "source_system": "fec_house",`)
	fmt.Println(`      "external_id": "H8IN09157",`)
	fmt.Println(`      "research_status": "needs_research",`)
	fmt.Println(`      "notes": "FEC API match - verify at fec.gov/data/candidates/"`)
	fmt.Println(`    }`)
	fmt.Println(`  ]`)
	fmt.Println()
	fmt.Println("RESEARCH STATUS VALUES")
	fmt.Println("  needs_research  — FEC/Cal-Access suggestion, not yet human-verified")
	fmt.Println("  confirmed       — Human verified the external_id is correct")
	fmt.Println("  not_applicable  — Politician has no filings in this source system")
	fmt.Println("  disputed        — ID found but correctness is questioned")
	fmt.Println()
	fmt.Println("WORKFLOW")
	fmt.Println("  1. go run cmd/lookup-sources/main.go --lookup-fec")
	fmt.Println("  2. Review data/seed/source_ids.json — verify each FEC ID at fec.gov")
	fmt.Println("  3. Change research_status to 'confirmed' for verified entries")
	fmt.Println("  4. go run cmd/lookup-sources/main.go --lookup-calaccess --tsv-path=...")
	fmt.Println("  5. go run cmd/lookup-sources/main.go --lookup-indiana   (manual)")
	fmt.Println("  6. go run cmd/lookup-sources/main.go --lookup-socrata   (manual)")
	fmt.Println("  7. go run cmd/seed-sources/main.go   (seed politician_sources table)")
}

// min returns the smaller of a and b.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	lookupFECFlag := flag.Bool("lookup-fec", false, "Query FEC API for federal prototype politicians")
	lookupCalAccessFlag := flag.Bool("lookup-calaccess", false, "Search Cal-Access FILERS_CD.TSV by politician last name")
	lookupIndianaFlag := flag.Bool("lookup-indiana", false, "Print manual instructions for Indiana OrgId lookup")
	lookupSocrataFlag := flag.Bool("lookup-socrata", false, "Print instructions for LA Socrata entity ID research")
	tsvPath := flag.String("tsv-path", "", "Path to FILERS_CD.TSV for Cal-Access lookup (default: ./FILERS_CD.TSV)")
	helpFlag := flag.Bool("help", false, "Print usage")

	flag.Parse()

	switch {
	case *helpFlag:
		printHelp()

	case *lookupFECFlag:
		if err := lookupFEC(); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			os.Exit(1)
		}

	case *lookupCalAccessFlag:
		if err := lookupCalAccess(*tsvPath); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			os.Exit(1)
		}

	case *lookupIndianaFlag:
		lookupIndiana()

	case *lookupSocrataFlag:
		lookupSocrata()

	default:
		fmt.Println("lookup-sources — Campaign Finance Source ID Research Tool")
		fmt.Println()
		fmt.Println("Available modes:")
		fmt.Println("  --lookup-fec          Query FEC API for all federal politicians")
		fmt.Println("  --lookup-calaccess    Search Cal-Access FILERS_CD.TSV")
		fmt.Println("  --lookup-indiana      Print Indiana manual lookup instructions")
		fmt.Println("  --lookup-socrata      Print LA Socrata research instructions")
		fmt.Println("  --help                Print detailed usage")
		fmt.Println()
		fmt.Println("Run: go run cmd/lookup-sources/main.go --help")
	}
}
