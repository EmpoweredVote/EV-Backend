# Judicial Record Enrichment Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enrich judge profiles with performance evaluations, metrics (reversal rates, caseload), and disciplinary records — displayed as a scorecard on the profile page with a drill-down detail view.

**Architecture:** Extend the existing `judge_details` table with new columns, add three new tables (`judicial_evaluations`, `judicial_metrics`, `judicial_disciplinary_records`), expose a new `/politician/:id/judicial-record` API endpoint, and render a `JudicialScorecard` component in ev-ui that replaces the legislative summary slot for judicial officials.

**Tech Stack:** Go (GORM, chi), PostgreSQL (Supabase), React (ev-ui shared component library), Vite

**Jurisdictions:** Indiana (Monroe County trial courts, state appellate/supreme) and California (LA County Superior Court)

**Deployment Order:** Changes span 3 repos. Deploy in order: (1) EV-Backend (new tables + endpoint), (2) ev-ui (new components, publish new npm version), (3) essentials (update ev-ui dependency, new pages). The ev-ui version must be published before essentials can import the new components.

---

## File Structure

### Backend (EV-Backend)

| File | Action | Responsibility |
|------|--------|---------------|
| `internal/essentials/models.go` | Modify | Add 3 new model structs + extend `JudgeDetail` |
| `internal/essentials/setup.go` | Modify | Register new models in `AutoMigrate` |
| `internal/essentials/handlers.go` | Modify | Add `GetJudicialRecord` handler + `JudicialRecordOut` DTO |
| `internal/essentials/routes.go` | Modify | Register `/politician/{id}/judicial-record` route |
| `internal/essentials/handlers_test.go` | Create | Tests for judicial record handler |

### Frontend (essentials)

| File | Action | Responsibility |
|------|--------|---------------|
| `src/lib/api.jsx` | Modify | Add `fetchJudicialRecord(id)` function |
| `src/pages/Profile.jsx` | Modify | Fetch judicial record for judicial officials, pass to ev-ui |
| `src/pages/JudicialRecord.jsx` | Create | Full judicial record detail page |
| `src/App.jsx` | Modify | Add route for `/politician/:id/judicial-record` |

### Shared UI (ev-ui)

| File | Action | Responsibility |
|------|--------|---------------|
| `src/JudicialScorecard.jsx` | Create | Scorecard summary component (replaces legislative summary for judges) |
| `src/JudicialRecordDetail.jsx` | Create | Full detail view for judicial record page |
| `src/PoliticianProfile.jsx` | Modify | Render JudicialScorecard when `is_judicial` and data present |
| `src/index.js` | Modify | Export new components |

---

## Chunk 1: Backend Data Model & Migration

### Task 1: Extend JudgeDetail Model

**Files:**
- Modify: `internal/essentials/models.go:308-319` (JudgeDetail struct)

- [ ] **Step 1: Write the failing test**

Create `internal/essentials/handlers_test.go` with a test that verifies the new `JudgeDetail` fields exist on the struct:

```go
package essentials

import (
	"testing"
)

func TestJudgeDetailHasEnrichedFields(t *testing.T) {
	jd := JudgeDetail{}
	// These fields must exist on the struct (compile-time check)
	_ = jd.ElectionType
	_ = jd.AreasOfFocus
	_ = jd.DateSeated
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/chrisandrews/Documents/GitHub/EV-Backend && go test ./internal/essentials/ -run TestJudgeDetailHasEnrichedFields -v`
Expected: FAIL — `jd.ElectionType undefined`

- [ ] **Step 3: Add new fields to JudgeDetail struct**

In `internal/essentials/models.go`, update the `JudgeDetail` struct to add these fields after `CourtRole`:

```go
type JudgeDetail struct {
	ID                       uuid.UUID      `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID             uuid.UUID      `json:"politician_id" gorm:"type:uuid;uniqueIndex"`
	AppointedBy              string         `json:"appointed_by"`
	AppointingPresidentParty string         `json:"appointing_president_party"`
	ConfirmationVote         string         `json:"confirmation_vote"`
	CourtRole                string         `json:"court_role"`
	// Enrichment fields
	ElectionType             string         `json:"election_type"`               // "retention" or "contested"
	AreasOfFocus             pq.StringArray `json:"areas_of_focus" gorm:"type:text[]"` // e.g. ["criminal", "family", "civil"]
	DateSeated               *string        `json:"date_seated,omitempty" gorm:"type:date"` // When first took the bench
	CreatedAt                time.Time      `json:"created_at"`
	UpdatedAt                time.Time      `json:"updated_at"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/chrisandrews/Documents/GitHub/EV-Backend && go test ./internal/essentials/ -run TestJudgeDetailHasEnrichedFields -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/chrisandrews/Documents/GitHub/EV-Backend
git add internal/essentials/models.go internal/essentials/handlers_test.go
git commit -m "feat: extend JudgeDetail with election_type, areas_of_focus, date_seated"
```

### Task 2: Add JudicialEvaluation Model

**Files:**
- Modify: `internal/essentials/models.go` (add after JudgeDetail)

- [ ] **Step 1: Write the failing test**

Append to `internal/essentials/handlers_test.go`:

```go
func TestJudicialEvaluationModel(t *testing.T) {
	ev := JudicialEvaluation{}
	_ = ev.PoliticianID
	_ = ev.Source
	_ = ev.Rating
	_ = ev.RatingDate
	_ = ev.SourceURL
	_ = ev.Details

	// Verify table name
	if ev.TableName() != "essentials.judicial_evaluations" {
		t.Errorf("expected table name essentials.judicial_evaluations, got %s", ev.TableName())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/chrisandrews/Documents/GitHub/EV-Backend && go test ./internal/essentials/ -run TestJudicialEvaluationModel -v`
Expected: FAIL — `JudicialEvaluation` undefined

- [ ] **Step 3: Add JudicialEvaluation struct**

Add to `internal/essentials/models.go` after the `JudgeDetail` struct and its `TableName()`:

```go
// JudicialEvaluation stores bar association and judicial performance evaluation ratings.
// Multiple evaluations per judge (different sources, different years).
type JudicialEvaluation struct {
	ID           uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID uuid.UUID `json:"politician_id" gorm:"type:uuid;not null;uniqueIndex:idx_jeval_dedup"`
	Source       string    `json:"source" gorm:"uniqueIndex:idx_jeval_dedup"`   // "Indiana Judicial Qualifications Commission", "LACBA"
	Rating       string    `json:"rating"`                                       // "Well Qualified", "Meets Performance Standards"
	RatingDate   string    `json:"rating_date" gorm:"uniqueIndex:idx_jeval_dedup"` // "2024-11" or "2024"
	SourceURL    string    `json:"source_url"`                                   // Link to evaluation report
	Details      string    `json:"details,omitempty" gorm:"type:text"`           // Additional notes from source
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func (JudicialEvaluation) TableName() string { return "essentials.judicial_evaluations" }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/chrisandrews/Documents/GitHub/EV-Backend && go test ./internal/essentials/ -run TestJudicialEvaluationModel -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/chrisandrews/Documents/GitHub/EV-Backend
git add internal/essentials/models.go internal/essentials/handlers_test.go
git commit -m "feat: add JudicialEvaluation model for bar/JPE ratings"
```

### Task 3: Add JudicialMetric Model

**Files:**
- Modify: `internal/essentials/models.go` (add after JudicialEvaluation)

- [ ] **Step 1: Write the failing test**

Append to `internal/essentials/handlers_test.go`:

```go
func TestJudicialMetricModel(t *testing.T) {
	m := JudicialMetric{}
	_ = m.PoliticianID
	_ = m.MetricType
	_ = m.Value
	_ = m.ContextLabel
	_ = m.ComparisonBaseline
	_ = m.TimePeriod
	_ = m.DataSource
	_ = m.SourceURL

	if m.TableName() != "essentials.judicial_metrics" {
		t.Errorf("expected table name essentials.judicial_metrics, got %s", m.TableName())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/chrisandrews/Documents/GitHub/EV-Backend && go test ./internal/essentials/ -run TestJudicialMetricModel -v`
Expected: FAIL — `JudicialMetric` undefined

- [ ] **Step 3: Add JudicialMetric struct**

Add to `internal/essentials/models.go` after `JudicialEvaluation`:

```go
// JudicialMetric stores quantitative judicial performance data (reversal rates,
// caseload, disposition times, etc.) with contextual comparison labels.
type JudicialMetric struct {
	ID                 uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID       uuid.UUID `json:"politician_id" gorm:"type:uuid;not null;uniqueIndex:idx_jmetric_dedup"`
	MetricType         string    `json:"metric_type" gorm:"uniqueIndex:idx_jmetric_dedup"`  // "reversal_rate", "caseload_volume", "avg_disposition_days"
	Value              float64   `json:"value"`                // Numeric value (e.g., 8.2 for 8.2% reversal rate)
	ContextLabel       string    `json:"context_label"`        // "below county average of 12%"
	ComparisonBaseline string    `json:"comparison_baseline"`  // "Monroe County average", "Indiana state average"
	TimePeriod         string    `json:"time_period" gorm:"uniqueIndex:idx_jmetric_dedup"`  // "2020-2024"
	DataSource         string    `json:"data_source"`          // "CourtListener", "Indiana Courts", "manual"
	SourceURL          string    `json:"source_url"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

func (JudicialMetric) TableName() string { return "essentials.judicial_metrics" }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/chrisandrews/Documents/GitHub/EV-Backend && go test ./internal/essentials/ -run TestJudicialMetricModel -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/chrisandrews/Documents/GitHub/EV-Backend
git add internal/essentials/models.go internal/essentials/handlers_test.go
git commit -m "feat: add JudicialMetric model for reversal rates, caseload data"
```

### Task 4: Add JudicialDisciplinaryRecord Model

**Files:**
- Modify: `internal/essentials/models.go` (add after JudicialMetric)

- [ ] **Step 1: Write the failing test**

Append to `internal/essentials/handlers_test.go`:

```go
func TestJudicialDisciplinaryRecordModel(t *testing.T) {
	dr := JudicialDisciplinaryRecord{}
	_ = dr.PoliticianID
	_ = dr.RecordType
	_ = dr.RecordDate
	_ = dr.Description
	_ = dr.SourceURL

	if dr.TableName() != "essentials.judicial_disciplinary_records" {
		t.Errorf("expected table name essentials.judicial_disciplinary_records, got %s", dr.TableName())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/chrisandrews/Documents/GitHub/EV-Backend && go test ./internal/essentials/ -run TestJudicialDisciplinaryRecordModel -v`
Expected: FAIL — `JudicialDisciplinaryRecord` undefined

- [ ] **Step 3: Add JudicialDisciplinaryRecord struct**

Add to `internal/essentials/models.go` after `JudicialMetric`:

```go
// JudicialDisciplinaryRecord stores sanctions, reprimands, or complaints
// against a judge. Only populated when records exist.
type JudicialDisciplinaryRecord struct {
	ID           uuid.UUID `json:"id" gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	PoliticianID uuid.UUID `json:"politician_id" gorm:"type:uuid;not null;uniqueIndex:idx_jdisc_dedup"`
	RecordType   string    `json:"record_type" gorm:"uniqueIndex:idx_jdisc_dedup"`   // "reprimand", "censure", "suspension", "complaint"
	RecordDate   string    `json:"record_date" gorm:"uniqueIndex:idx_jdisc_dedup"`   // "2023-06-15"
	Description  string    `json:"description" gorm:"type:text"` // Brief factual summary
	SourceURL    string    `json:"source_url"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func (JudicialDisciplinaryRecord) TableName() string {
	return "essentials.judicial_disciplinary_records"
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/chrisandrews/Documents/GitHub/EV-Backend && go test ./internal/essentials/ -run TestJudicialDisciplinaryRecordModel -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/chrisandrews/Documents/GitHub/EV-Backend
git add internal/essentials/models.go internal/essentials/handlers_test.go
git commit -m "feat: add JudicialDisciplinaryRecord model"
```

### Task 5: Register New Models in AutoMigrate

**Files:**
- Modify: `internal/essentials/setup.go:36-77` (AutoMigrate block)

- [ ] **Step 1: Add new models to AutoMigrate**

In `internal/essentials/setup.go`, add the three new models to the `AutoMigrate` call, after `&JudgeDetail{}`:

```go
		// SCOTUS: Judge-specific metadata
		&JudgeDetail{},
		// Judicial record enrichment
		&JudicialEvaluation{},
		&JudicialMetric{},
		&JudicialDisciplinaryRecord{},
```

- [ ] **Step 2: Verify the app compiles**

Run: `cd /Users/chrisandrews/Documents/GitHub/EV-Backend && go build ./...`
Expected: Build succeeds with no errors

- [ ] **Step 3: Commit**

```bash
cd /Users/chrisandrews/Documents/GitHub/EV-Backend
git add internal/essentials/setup.go
git commit -m "feat: register judicial enrichment models in AutoMigrate"
```

---

## Chunk 2: Backend API Endpoint

### Task 6: Add JudicialRecord Handler and DTO

**Files:**
- Modify: `internal/essentials/handlers.go` (add DTOs and handler function)
- Modify: `internal/essentials/handlers_test.go` (add handler test)

- [ ] **Step 1: Write the failing test**

Append to `internal/essentials/handlers_test.go`:

```go
func TestJudicialRecordOutStructure(t *testing.T) {
	out := JudicialRecordOut{}
	_ = out.JudgeDetail
	_ = out.Evaluations
	_ = out.Metrics
	_ = out.DisciplinaryRecords
}

func TestJudicialEvaluationOutStructure(t *testing.T) {
	out := JudicialEvaluationOut{}
	_ = out.Source
	_ = out.Rating
	_ = out.RatingDate
	_ = out.SourceURL
}

func TestJudicialMetricOutStructure(t *testing.T) {
	out := JudicialMetricOut{}
	_ = out.MetricType
	_ = out.Value
	_ = out.ContextLabel
	_ = out.ComparisonBaseline
	_ = out.TimePeriod
}

func TestJudicialDisciplinaryRecordOutStructure(t *testing.T) {
	out := JudicialDisciplinaryRecordOut{}
	_ = out.RecordType
	_ = out.RecordDate
	_ = out.Description
	_ = out.SourceURL
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/chrisandrews/Documents/GitHub/EV-Backend && go test ./internal/essentials/ -run TestJudicialRecordOutStructure -v`
Expected: FAIL — `JudicialRecordOut` undefined

- [ ] **Step 3: Add DTO structs and handler**

Add to `internal/essentials/handlers.go` after the existing DTO structs (after `ContactOut` around line 76):

```go
// Judicial record DTOs

type JudgeDetailOut struct {
	AppointedBy              string   `json:"appointed_by,omitempty"`
	AppointingPresidentParty string   `json:"appointing_president_party,omitempty"`
	ConfirmationVote         string   `json:"confirmation_vote,omitempty"`
	CourtRole                string   `json:"court_role,omitempty"`
	ElectionType             string   `json:"election_type,omitempty"`
	AreasOfFocus             []string `json:"areas_of_focus,omitempty"`
	DateSeated               string   `json:"date_seated,omitempty"`
}

type JudicialEvaluationOut struct {
	Source     string `json:"source"`
	Rating     string `json:"rating"`
	RatingDate string `json:"rating_date"`
	SourceURL  string `json:"source_url,omitempty"`
}

type JudicialMetricOut struct {
	MetricType         string  `json:"metric_type"`
	Value              float64 `json:"value"`
	ContextLabel       string  `json:"context_label"`
	ComparisonBaseline string  `json:"comparison_baseline,omitempty"`
	TimePeriod         string  `json:"time_period,omitempty"`
}

type JudicialDisciplinaryRecordOut struct {
	RecordType  string `json:"record_type"`
	RecordDate  string `json:"record_date"`
	Description string `json:"description"`
	SourceURL   string `json:"source_url,omitempty"`
}

type JudicialRecordOut struct {
	JudgeDetail         *JudgeDetailOut                 `json:"judge_detail,omitempty"`
	Evaluations         []JudicialEvaluationOut         `json:"evaluations"`
	Metrics             []JudicialMetricOut             `json:"metrics"`
	DisciplinaryRecords []JudicialDisciplinaryRecordOut `json:"disciplinary_records"`
}
```

Then add the handler function (add before the route registration section):

```go
// GetJudicialRecord returns the full judicial record for a judge:
// detail info, evaluations, metrics, and disciplinary records.
func GetJudicialRecord(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	polID, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, "Invalid politician ID", http.StatusBadRequest)
		return
	}

	// Fetch judge detail
	var jd JudgeDetail
	var detail *JudgeDetailOut
	if err := db.DB.Where("politician_id = ?", polID).First(&jd).Error; err == nil {
		areasOfFocus := []string(jd.AreasOfFocus)
		if areasOfFocus == nil {
			areasOfFocus = []string{}
		}
		dateSeated := ""
		if jd.DateSeated != nil {
			dateSeated = *jd.DateSeated
		}
		detail = &JudgeDetailOut{
			AppointedBy:              jd.AppointedBy,
			AppointingPresidentParty: jd.AppointingPresidentParty,
			ConfirmationVote:         jd.ConfirmationVote,
			CourtRole:                jd.CourtRole,
			ElectionType:             jd.ElectionType,
			AreasOfFocus:             areasOfFocus,
			DateSeated:               dateSeated,
		}
	}

	// Fetch evaluations
	var evals []JudicialEvaluation
	if err := db.DB.Where("politician_id = ?", polID).Order("rating_date DESC").Find(&evals).Error; err != nil {
		log.Printf("[essentials] WARNING: failed to fetch judicial evaluations for %s: %v", polID, err)
	}
	evalOuts := make([]JudicialEvaluationOut, len(evals))
	for i, e := range evals {
		evalOuts[i] = JudicialEvaluationOut{
			Source:     e.Source,
			Rating:     e.Rating,
			RatingDate: e.RatingDate,
			SourceURL:  e.SourceURL,
		}
	}

	// Fetch metrics
	var metrics []JudicialMetric
	if err := db.DB.Where("politician_id = ?", polID).Find(&metrics).Error; err != nil {
		log.Printf("[essentials] WARNING: failed to fetch judicial metrics for %s: %v", polID, err)
	}
	metricOuts := make([]JudicialMetricOut, len(metrics))
	for i, m := range metrics {
		metricOuts[i] = JudicialMetricOut{
			MetricType:         m.MetricType,
			Value:              m.Value,
			ContextLabel:       m.ContextLabel,
			ComparisonBaseline: m.ComparisonBaseline,
			TimePeriod:         m.TimePeriod,
		}
	}

	// Fetch disciplinary records
	var discs []JudicialDisciplinaryRecord
	if err := db.DB.Where("politician_id = ?", polID).Order("record_date DESC").Find(&discs).Error; err != nil {
		log.Printf("[essentials] WARNING: failed to fetch judicial disciplinary records for %s: %v", polID, err)
	}
	discOuts := make([]JudicialDisciplinaryRecordOut, len(discs))
	for i, d := range discs {
		discOuts[i] = JudicialDisciplinaryRecordOut{
			RecordType:  d.RecordType,
			RecordDate:  d.RecordDate,
			Description: d.Description,
			SourceURL:   d.SourceURL,
		}
	}

	writeJSON(w, JudicialRecordOut{
		JudgeDetail:         detail,
		Evaluations:         evalOuts,
		Metrics:             metricOuts,
		DisciplinaryRecords: discOuts,
	})
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/chrisandrews/Documents/GitHub/EV-Backend && go test ./internal/essentials/ -run "TestJudicial.*OutStructure" -v`
Expected: PASS (all 4 struct tests)

- [ ] **Step 5: Commit**

```bash
cd /Users/chrisandrews/Documents/GitHub/EV-Backend
git add internal/essentials/handlers.go internal/essentials/handlers_test.go
git commit -m "feat: add GetJudicialRecord handler with DTOs"
```

### Task 7: Register the Route

**Files:**
- Modify: `internal/essentials/routes.go:33-34` (after legislative summary route)

- [ ] **Step 1: Add the route**

In `internal/essentials/routes.go`, add after the legislative summary endpoint (line 34):

```go
	// Judicial record endpoint
	r.Get("/politician/{id}/judicial-record", GetJudicialRecord)
```

- [ ] **Step 2: Verify the app compiles**

Run: `cd /Users/chrisandrews/Documents/GitHub/EV-Backend && go build ./...`
Expected: Build succeeds

- [ ] **Step 3: Commit**

```bash
cd /Users/chrisandrews/Documents/GitHub/EV-Backend
git add internal/essentials/routes.go
git commit -m "feat: register /politician/{id}/judicial-record route"
```

---

## Chunk 3: Frontend API + Profile Integration

### Task 8: Add fetchJudicialRecord API Function

**Files:**
- Modify: `src/lib/api.jsx` (essentials repo)

- [ ] **Step 1: Add the API function**

Append to `/Users/chrisandrews/Documents/GitHub/essentials/src/lib/api.jsx`:

```javascript
export async function fetchJudicialRecord(id) {
  try {
    const res = await fetch(`${API}/essentials/politician/${id}/judicial-record`, {
      credentials: "include",
    });
    if (!res.ok) return { evaluations: [], metrics: [], disciplinary_records: [] };
    return res.json();
  } catch (error) {
    console.error("Error fetching judicial record:", error);
    return { evaluations: [], metrics: [], disciplinary_records: [] };
  }
}
```

- [ ] **Step 2: Commit**

```bash
cd /Users/chrisandrews/Documents/GitHub/essentials
git add src/lib/api.jsx
git commit -m "feat: add fetchJudicialRecord API function"
```

### Task 9: Fetch Judicial Record in Profile Page

**Files:**
- Modify: `src/pages/Profile.jsx` (essentials repo)

- [ ] **Step 1: Update imports**

In `/Users/chrisandrews/Documents/GitHub/essentials/src/pages/Profile.jsx`, update the import on line 3:

```javascript
import { fetchPolitician, fetchLegislativeSummary, fetchJudicialRecord } from '../lib/api';
```

- [ ] **Step 2: Add judicial record state and fetching**

Add state after `const [activeElection, setActiveElection] = useState(null);` (line 25):

```javascript
  const [judicialRecord, setJudicialRecord] = useState(null);
```

Then update the `Promise.all` block (lines 46-51) to fetch judicial record in parallel (the endpoint gracefully returns empty data for non-judicial officials, so it's safe to always call):

```javascript
        const [result, legSummary, elections, jRecord] = await Promise.all([
          fetchPolitician(id),
          fetchLegislativeSummary(id),
          fetchElections(),
          fetchJudicialRecord(id),
        ]);
        setPol(result);
        setLegislativeSummary(legSummary);
        const active = (elections || []).find((e) => e.is_active && !e.withdrawn) || null;
        setActiveElection(active);
        if (result.is_judicial) {
          setJudicialRecord(jRecord);
        }
```

- [ ] **Step 3: Pass judicialRecord to PoliticianProfile**

Update the `<PoliticianProfile>` component props (around line 74) to include:

```jsx
          <PoliticianProfile
            politician={pol}
            onBack={() => { /* existing code unchanged */ }}
            backLabel={/* existing code unchanged */}
            banner={/* existing code unchanged */}
            legislativeSummary={legislativeSummary}
            judicialRecord={judicialRecord}
            politicianId={id}
            onNavigateToRecord={(href) => navigate(href)}
          />
```

- [ ] **Step 4: Commit**

```bash
cd /Users/chrisandrews/Documents/GitHub/essentials
git add src/pages/Profile.jsx
git commit -m "feat: fetch and pass judicial record to PoliticianProfile"
```

### Task 10: Add Judicial Record Detail Page Route

**Files:**
- Create: `src/pages/JudicialRecord.jsx` (essentials repo)
- Modify: `src/App.jsx` (essentials repo)

- [ ] **Step 1: Create the JudicialRecord page**

Create `/Users/chrisandrews/Documents/GitHub/essentials/src/pages/JudicialRecord.jsx`:

```jsx
import { useParams, useNavigate } from 'react-router-dom';
import { useEffect, useState } from 'react';
import { fetchPolitician, fetchJudicialRecord } from '../lib/api';
import { JudicialRecordDetail } from '@chrisandrewsedu/ev-ui';
import { Layout } from '../components/Layout';

function JudicialRecord() {
  const { id } = useParams();
  const navigate = useNavigate();

  const [pol, setPol] = useState({});
  const [judicialRecord, setJudicialRecord] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!id) return;
    setLoading(true);

    (async () => {
      try {
        const [result, jRecord] = await Promise.all([
          fetchPolitician(id),
          fetchJudicialRecord(id),
        ]);
        setPol(result);
        setJudicialRecord(jRecord);
      } catch (err) {
        console.error(err);
      } finally {
        setLoading(false);
      }
    })();
  }, [id]);

  return (
    <Layout>
      <div className="min-h-screen bg-[var(--ev-bg-light)]">
        <main className="container mx-auto px-4 sm:px-6 py-4 sm:py-8 max-w-6xl">
          {loading ? (
            <div className="flex justify-center items-center py-24">
              <div className="animate-spin rounded-full h-10 w-10 border-4 border-[var(--ev-teal)] border-t-transparent" />
            </div>
          ) : (
            <JudicialRecordDetail
              politician={pol}
              judicialRecord={judicialRecord}
              onBack={() => navigate(`/politician/${id}`)}
            />
          )}
        </main>
      </div>
    </Layout>
  );
}

export default JudicialRecord;
```

- [ ] **Step 2: Add route to App.jsx**

In `/Users/chrisandrews/Documents/GitHub/essentials/src/App.jsx`, add the import and route. Add the import near other page imports:

```javascript
import JudicialRecord from './pages/JudicialRecord';
```

Add the route near the existing `/politician/:id` route (check App.jsx for exact pattern — likely inside a `<Routes>` block):

```jsx
<Route path="/politician/:id/judicial-record" element={<JudicialRecord />} />
```

- [ ] **Step 3: Commit**

```bash
cd /Users/chrisandrews/Documents/GitHub/essentials
git add src/pages/JudicialRecord.jsx src/App.jsx
git commit -m "feat: add judicial record detail page with route"
```

---

## Chunk 4: ev-ui Shared Components

> **Dependency note:** Chunk 4 must be completed and a new ev-ui npm version published BEFORE Chunk 3 Task 10 (`JudicialRecord.jsx` page) can work, since it imports `JudicialRecordDetail` from `@chrisandrewsedu/ev-ui`. Execute Chunks 1-2 (backend), then Chunk 4 (ev-ui), then Chunk 3 (essentials frontend).

### Task 11: Create Shared Judicial Utilities

**Files:**
- Create: `src/judicialUtils.js` (ev-ui repo)

- [ ] **Step 1: Create the utility file**

Create `/Users/chrisandrews/Documents/GitHub/ev-ui/src/judicialUtils.js`:

```javascript
export const METRIC_LABELS = {
  reversal_rate: 'Reversal Rate',
  caseload_volume: 'Cases Handled',
  avg_disposition_days: 'Avg. Days to Resolve',
  retention_vote_pct: 'Last Retention Vote',
};

export function formatMetricValue(type, value) {
  if (type === 'reversal_rate' || type === 'retention_vote_pct') return `${value}%`;
  if (type === 'avg_disposition_days') return `${Math.round(value)} days`;
  if (type === 'caseload_volume') return Math.round(value).toLocaleString();
  return String(value);
}
```

- [ ] **Step 2: Commit**

```bash
cd /Users/chrisandrews/Documents/GitHub/ev-ui
git add src/judicialUtils.js
git commit -m "feat: add shared judicial metric utilities"
```

### Task 12: Create JudicialScorecard Component

**Files:**
- Create: `src/JudicialScorecard.jsx` (ev-ui repo)

This component renders in the same slot as `LegislativeInlineSummary` on the profile page. It shows a compact scorecard with key metrics and a "View Full Judicial Record" link.

- [ ] **Step 1: Create the component**

Create `/Users/chrisandrews/Documents/GitHub/ev-ui/src/JudicialScorecard.jsx`:

```jsx
import React from 'react';
import { METRIC_LABELS, formatMetricValue } from './judicialUtils.js';

const sectionStyle = {
  fontFamily: "'Manrope', sans-serif",
  borderTop: '1px solid #e2e8f0',
  paddingTop: '24px',
  marginTop: '24px',
};

const headingStyle = {
  fontSize: '16px',
  fontWeight: 700,
  color: '#2d3748',
  marginBottom: '16px',
};

const cardGridStyle = {
  display: 'grid',
  gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
  gap: '12px',
  marginBottom: '16px',
};

const metricCardStyle = {
  backgroundColor: '#f7fafc',
  borderRadius: '8px',
  padding: '14px 16px',
  border: '1px solid #e2e8f0',
};

const metricValueStyle = {
  fontSize: '22px',
  fontWeight: 700,
  color: '#2d3748',
  lineHeight: 1.2,
};

const metricLabelStyle = {
  fontSize: '12px',
  fontWeight: 600,
  color: '#718096',
  textTransform: 'uppercase',
  letterSpacing: '0.5px',
  marginBottom: '4px',
};

const metricContextStyle = {
  fontSize: '12px',
  color: '#a0aec0',
  marginTop: '4px',
};

const evalChipStyle = {
  display: 'inline-flex',
  alignItems: 'center',
  gap: '6px',
  backgroundColor: '#ebf8ff',
  color: '#2b6cb0',
  borderRadius: '6px',
  padding: '6px 12px',
  fontSize: '13px',
  fontWeight: 600,
  marginRight: '8px',
  marginBottom: '8px',
};

const disciplinaryStyle = {
  borderLeft: '3px solid #fc8181',
  backgroundColor: '#fff5f5',
  borderRadius: '0 6px 6px 0',
  padding: '10px 14px',
  marginBottom: '8px',
  fontSize: '13px',
  color: '#742a2a',
};

const linkStyle = {
  display: 'inline-flex',
  alignItems: 'center',
  gap: '4px',
  color: '#319795',
  fontSize: '14px',
  fontWeight: 600,
  textDecoration: 'none',
  marginTop: '8px',
  cursor: 'pointer',
};

/**
 * JudicialScorecard — compact scorecard for judge profiles.
 * Renders in the same slot as LegislativeInlineSummary for legislators.
 *
 * Props:
 *   judicialRecord: { judge_detail, evaluations[], metrics[], disciplinary_records[] }
 *   politicianId: string (for building the detail link)
 *   onNavigateToRecord: (href) => void
 */
export default function JudicialScorecard({ judicialRecord, politicianId, onNavigateToRecord }) {
  if (!judicialRecord) return null;

  const { evaluations = [], metrics = [], disciplinary_records: disciplinary = [] } = judicialRecord;

  // Don't render if there's no data at all
  if (evaluations.length === 0 && metrics.length === 0 && disciplinary.length === 0) {
    return null;
  }

  const handleNavigate = (e) => {
    e.preventDefault();
    if (onNavigateToRecord) {
      onNavigateToRecord(`/politician/${politicianId}/judicial-record`);
    }
  };

  return (
    <section style={sectionStyle}>
      <h3 style={headingStyle}>Judicial Record</h3>

      {/* Evaluation chips */}
      {evaluations.length > 0 && (
        <div style={{ marginBottom: '16px' }}>
          {evaluations.slice(0, 3).map((ev, i) => (
            <span key={i} style={evalChipStyle}>
              {ev.rating}
              <span style={{ fontWeight: 400, color: '#4a90a4', fontSize: '11px' }}>
                — {ev.source}
              </span>
            </span>
          ))}
        </div>
      )}

      {/* Metric cards */}
      {metrics.length > 0 && (
        <div style={cardGridStyle}>
          {metrics.slice(0, 4).map((m, i) => (
            <div key={i} style={metricCardStyle}>
              <div style={metricLabelStyle}>
                {METRIC_LABELS[m.metric_type] || m.metric_type.replace(/_/g, ' ')}
              </div>
              <div style={metricValueStyle}>
                {formatMetricValue(m.metric_type, m.value)}
              </div>
              {m.context_label && (
                <div style={metricContextStyle}>{m.context_label}</div>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Disciplinary flag (if any) */}
      {disciplinary.length > 0 && (
        <div style={{ marginBottom: '12px' }}>
          {disciplinary.slice(0, 2).map((d, i) => (
            <div key={i} style={disciplinaryStyle}>
              <strong>{d.record_type}</strong> — {d.record_date}
              {d.description && <div style={{ marginTop: '4px' }}>{d.description}</div>}
            </div>
          ))}
        </div>
      )}

      {/* Link to full record */}
      <a href={`/politician/${politicianId}/judicial-record`} onClick={handleNavigate} style={linkStyle}>
        View Full Judicial Record →
      </a>
    </section>
  );
}
```

- [ ] **Step 2: Commit**

```bash
cd /Users/chrisandrews/Documents/GitHub/ev-ui
git add src/JudicialScorecard.jsx
git commit -m "feat: add JudicialScorecard component for judge profiles"
```

### Task 13: Create JudicialRecordDetail Component

**Files:**
- Create: `src/JudicialRecordDetail.jsx` (ev-ui repo)

This is the full detail view — accessed via "View Full Judicial Record".

- [ ] **Step 1: Create the component**

Create `/Users/chrisandrews/Documents/GitHub/ev-ui/src/JudicialRecordDetail.jsx`:

```jsx
import React from 'react';
import { METRIC_LABELS, formatMetricValue } from './judicialUtils.js';

const containerStyle = {
  fontFamily: "'Manrope', sans-serif",
  maxWidth: '800px',
};

const backBtnStyle = {
  display: 'inline-flex',
  alignItems: 'center',
  gap: '4px',
  color: '#718096',
  fontSize: '14px',
  fontWeight: 600,
  cursor: 'pointer',
  background: 'none',
  border: 'none',
  padding: '8px 0',
  marginBottom: '16px',
};

const nameStyle = {
  fontSize: '24px',
  fontWeight: 700,
  color: '#2d3748',
  marginBottom: '4px',
};

const roleStyle = {
  fontSize: '15px',
  color: '#718096',
  marginBottom: '24px',
};

const sectionHeadingStyle = {
  fontSize: '18px',
  fontWeight: 700,
  color: '#2d3748',
  marginTop: '32px',
  marginBottom: '12px',
  paddingBottom: '8px',
  borderBottom: '2px solid #e2e8f0',
};

const tableStyle = {
  width: '100%',
  borderCollapse: 'collapse',
  fontSize: '14px',
};

const thStyle = {
  textAlign: 'left',
  padding: '8px 12px',
  fontWeight: 600,
  color: '#718096',
  borderBottom: '1px solid #e2e8f0',
  fontSize: '12px',
  textTransform: 'uppercase',
  letterSpacing: '0.5px',
};

const tdStyle = {
  padding: '10px 12px',
  borderBottom: '1px solid #f7fafc',
  color: '#2d3748',
};

const detailRowStyle = {
  display: 'flex',
  gap: '8px',
  padding: '8px 0',
  borderBottom: '1px solid #f7fafc',
};

const detailLabelStyle = {
  fontWeight: 600,
  color: '#718096',
  minWidth: '160px',
  fontSize: '14px',
};

const detailValueStyle = {
  color: '#2d3748',
  fontSize: '14px',
};

const emptyStateStyle = {
  padding: '24px',
  backgroundColor: '#f7fafc',
  borderRadius: '8px',
  color: '#a0aec0',
  fontSize: '14px',
  textAlign: 'center',
};

/**
 * JudicialRecordDetail — full-page judicial record view.
 *
 * Props:
 *   politician: { full_name, office_title, ... }
 *   judicialRecord: { judge_detail, evaluations[], metrics[], disciplinary_records[] }
 *   onBack: () => void
 */
export function JudicialRecordDetail({ politician = {}, judicialRecord, onBack }) {
  if (!judicialRecord) {
    return (
      <div style={containerStyle}>
        <button style={backBtnStyle} onClick={onBack}>← Back to Profile</button>
        <div style={emptyStateStyle}>No judicial record data available.</div>
      </div>
    );
  }

  const {
    judge_detail: detail,
    evaluations = [],
    metrics = [],
    disciplinary_records: disciplinary = [],
  } = judicialRecord;

  const displayName = politician.full_name || `${politician.first_name || ''} ${politician.last_name || ''}`.trim();

  return (
    <div style={containerStyle}>
      <button style={backBtnStyle} onClick={onBack}>← Back to Profile</button>

      <h1 style={nameStyle}>{displayName}</h1>
      <p style={roleStyle}>
        {detail?.court_role || politician.office_title}
        {detail?.date_seated && ` · Seated ${detail.date_seated}`}
      </p>

      {/* Judge Background */}
      {detail && (
        <>
          <h2 style={sectionHeadingStyle}>Background</h2>
          {detail.appointed_by && (
            <div style={detailRowStyle}>
              <span style={detailLabelStyle}>Appointed by</span>
              <span style={detailValueStyle}>{detail.appointed_by} ({detail.appointing_president_party})</span>
            </div>
          )}
          {detail.confirmation_vote && (
            <div style={detailRowStyle}>
              <span style={detailLabelStyle}>Confirmation Vote</span>
              <span style={detailValueStyle}>{detail.confirmation_vote}</span>
            </div>
          )}
          {detail.election_type && (
            <div style={detailRowStyle}>
              <span style={detailLabelStyle}>Election Type</span>
              <span style={detailValueStyle}>{detail.election_type === 'retention' ? 'Retention (Yes/No)' : 'Contested Election'}</span>
            </div>
          )}
          {detail.areas_of_focus?.length > 0 && (
            <div style={detailRowStyle}>
              <span style={detailLabelStyle}>Areas of Focus</span>
              <span style={detailValueStyle}>{detail.areas_of_focus.join(', ')}</span>
            </div>
          )}
        </>
      )}

      {/* Evaluations */}
      <h2 style={sectionHeadingStyle}>Performance Evaluations</h2>
      {evaluations.length === 0 ? (
        <div style={emptyStateStyle}>No evaluation data available yet.</div>
      ) : (
        <table style={tableStyle}>
          <thead>
            <tr>
              <th style={thStyle}>Source</th>
              <th style={thStyle}>Rating</th>
              <th style={thStyle}>Date</th>
            </tr>
          </thead>
          <tbody>
            {evaluations.map((ev, i) => (
              <tr key={i}>
                <td style={tdStyle}>
                  {ev.source_url ? (
                    <a href={ev.source_url} target="_blank" rel="noopener noreferrer" style={{ color: '#319795' }}>
                      {ev.source}
                    </a>
                  ) : ev.source}
                </td>
                <td style={{ ...tdStyle, fontWeight: 600 }}>{ev.rating}</td>
                <td style={tdStyle}>{ev.rating_date}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {/* Metrics */}
      <h2 style={sectionHeadingStyle}>Performance Metrics</h2>
      {metrics.length === 0 ? (
        <div style={emptyStateStyle}>No performance metric data available yet.</div>
      ) : (
        <table style={tableStyle}>
          <thead>
            <tr>
              <th style={thStyle}>Metric</th>
              <th style={thStyle}>Value</th>
              <th style={thStyle}>Context</th>
              <th style={thStyle}>Period</th>
            </tr>
          </thead>
          <tbody>
            {metrics.map((m, i) => (
              <tr key={i}>
                <td style={{ ...tdStyle, fontWeight: 600 }}>
                  {METRIC_LABELS[m.metric_type] || m.metric_type.replace(/_/g, ' ')}
                </td>
                <td style={tdStyle}>{formatMetricValue(m.metric_type, m.value)}</td>
                <td style={{ ...tdStyle, color: '#718096', fontSize: '13px' }}>{m.context_label}</td>
                <td style={tdStyle}>{m.time_period}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {/* Disciplinary Records */}
      {disciplinary.length > 0 && (
        <>
          <h2 style={sectionHeadingStyle}>Disciplinary History</h2>
          {disciplinary.map((d, i) => (
            <div key={i} style={{
              borderLeft: '3px solid #fc8181',
              backgroundColor: '#fff5f5',
              borderRadius: '0 6px 6px 0',
              padding: '12px 16px',
              marginBottom: '10px',
            }}>
              <div style={{ fontWeight: 700, color: '#742a2a', fontSize: '14px' }}>
                {d.record_type} — {d.record_date}
              </div>
              {d.description && (
                <div style={{ color: '#9b2c2c', fontSize: '13px', marginTop: '4px' }}>{d.description}</div>
              )}
              {d.source_url && (
                <a href={d.source_url} target="_blank" rel="noopener noreferrer"
                  style={{ color: '#c53030', fontSize: '12px', marginTop: '4px', display: 'inline-block' }}>
                  View source →
                </a>
              )}
            </div>
          ))}
        </>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
cd /Users/chrisandrews/Documents/GitHub/ev-ui
git add src/JudicialRecordDetail.jsx
git commit -m "feat: add JudicialRecordDetail component for full judicial record page"
```

### Task 14: Integrate JudicialScorecard into PoliticianProfile

**Files:**
- Modify: `src/PoliticianProfile.jsx` (ev-ui repo)
- Modify: `src/index.js` (ev-ui repo)

- [ ] **Step 1: Add import to PoliticianProfile.jsx**

At the top of `/Users/chrisandrews/Documents/GitHub/ev-ui/src/PoliticianProfile.jsx`, add:

```javascript
import JudicialScorecard from './JudicialScorecard.jsx';
```

- [ ] **Step 2: Add judicialRecord to destructured props**

Find the props destructuring (around line 230) and add `judicialRecord`:

```javascript
// Before:
{ politician = {}, onBack, backLabel, children, banner, style = {}, legislativeSummary, politicianId, onNavigateToRecord }

// After:
{ politician = {}, onBack, backLabel, children, banner, style = {}, legislativeSummary, judicialRecord, politicianId, onNavigateToRecord }
```

- [ ] **Step 3: Conditionally render JudicialScorecard vs LegislativeInlineSummary**

Find where `<LegislativeInlineSummary>` is rendered (around line 823). Wrap it in a conditional:

```jsx
{politician.is_judicial ? (
  <JudicialScorecard
    judicialRecord={judicialRecord}
    politicianId={politicianId}
    onNavigateToRecord={onNavigateToRecord}
  />
) : (
  <LegislativeInlineSummary
    summary={legislativeSummary}
    politicianId={politicianId}
    onNavigateToRecord={onNavigateToRecord}
  />
)}
```

- [ ] **Step 4: Export new components from index.js**

In `/Users/chrisandrews/Documents/GitHub/ev-ui/src/index.js`, add exports:

```javascript
export { default as JudicialScorecard } from './JudicialScorecard.jsx';
export { JudicialRecordDetail } from './JudicialRecordDetail.jsx';
```

- [ ] **Step 5: Commit**

```bash
cd /Users/chrisandrews/Documents/GitHub/ev-ui
git add src/PoliticianProfile.jsx src/JudicialScorecard.jsx src/JudicialRecordDetail.jsx src/index.js
git commit -m "feat: integrate JudicialScorecard into PoliticianProfile for judicial officials"
```

---

## Chunk 5: Data Sourcing Notes (Reference — Not Code Tasks)

This section documents where to source judicial data for the two target jurisdictions. This is for the data population phase that follows implementation.

### Indiana

| Data Type | Source | URL / Notes |
|-----------|--------|-------------|
| Judicial evaluations | Indiana Judicial Qualifications Commission | Published biennially before retention elections. PDF reports available on courts.in.gov |
| Retention ballot data | Indiana Secretary of State | Election results archives |
| Disciplinary records | Indiana Commission on Judicial Qualifications | Public orders on courts.in.gov |
| Court opinions (for reversal rates) | CourtListener API | Free API, covers Indiana appellate + supreme court |
| Caseload data | Indiana Office of Court Services | Annual statistical reports (PDF) |

### California / LA County

| Data Type | Source | URL / Notes |
|-----------|--------|-------------|
| Judicial evaluations | LA County Bar Association (LACBA) | Publishes ratings before elections at lacba.org/judicial-evaluations |
| Judicial evaluations | California State Bar | Publishes "qualified/not qualified" ratings |
| Court opinions (for reversal rates) | CourtListener API | Covers CA appellate courts |
| Disciplinary records | California Commission on Judicial Performance | Public decisions at cjp.ca.gov |
| Election type | Varies by court level | Superior Court = contested elections; Appellate/Supreme = retention |

### CourtListener API (Primary Automated Source)

- **Base URL:** `https://www.courtlistener.com/api/rest/v3/`
- **Key endpoints:** `/opinions/`, `/clusters/`, `/judges/`, `/positions/`
- **Rate limit:** 5,000 requests/hour with API key (free)
- **Coverage:** Good for appellate courts; limited for trial courts
- **Useful for:** Reversal rates, opinion counts, judge biographical data
- **Limitation:** Trial court (Monroe County Circuit, LA Superior) data is sparse — these will need manual supplementation

### Automation Priority

1. **CourtListener API** — automate first for appellate/supreme court judges (reversal rates, opinion data)
2. **Indiana JQC PDFs** — semi-automated scraping of evaluation reports
3. **LACBA evaluations** — semi-automated scraping of ratings pages
4. **Manual entry** — trial court data, caseload statistics from PDF reports
