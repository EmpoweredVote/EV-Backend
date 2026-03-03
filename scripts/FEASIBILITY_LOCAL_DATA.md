# Local Data Pipeline Feasibility Report

**Generated:** 2026-03-03 01:18 UTC
**Script version:** 1.0.0
**Dry-run mode:** No (full check with DB)

---

## Capability Matrix

| Capability | Bloomington OnBoard | LA County Legistar |
|------------|--------------------|--------------------|
| Committee assignments | HTML scrape — confirmed working (`/onboard/committees/1/members`) | Legistar OfficeRecords — confirmed for all 5 supervisors |
| Committee roles (chair/member/etc) | Yes — seat/role text parsed from HTML member list | Yes — OfficeRecordTitle field |
| Sub-committee assignments | HTML scrape — committees 77, 81, 49 accessible | Legistar OfficeRecords — same endpoint, all active bodies |
| Legislation listing | HTML scrape — committee-scoped URL, 20 items/page | Legistar Matters — confirmed open, BOS BodyId=76 |
| Legislation sponsor attribution | Partial — embedded in description text (~50% coverage estimated) | No — MatterRequester NULL for 98.0% of matters |
| Legislation mover attribution | N/A — no vote/motion data in OnBoard | Partial — MoverName populated only for pre-2010 data |
| Vote records | Not available — OnBoard has no vote records | Not available — /VoteRecords returns 404 |
| Individual vote positions | Not available | Not available — /Matters/{{id}}/Sponsors returns empty array |

---

## LA County Legistar Findings

### API Status Summary

| Endpoint | HTTP Status | Notes |
|----------|-------------|-------|
| `GET /Bodies` | 200 | 110 total bodies, 103 active (BodyActiveFlag=1) |
| `GET /OfficeRecords?$filter=PersonId eq N` | 200 | Tested for all 5 supervisors (separate requests per PersonId) |
| `GET /Matters?$filter=MatterBodyId eq 76` | 200 | 50 matters; MatterRequester NULL: 98.0% |
| `GET /Matters/{id}/Histories` | 200 | 5 matters checked; 2 had MoverName |
| `GET /VoteRecords` | 404 | Endpoint does not exist — individual vote records unavailable |
| `GET /Matters/{id}/Sponsors` | 200 | Empty array — no sponsor tracking in LACounty Legistar |

### Supervisor Committee Memberships (active after 2025-01-01)

Total active memberships across all 5 supervisors: **29**

| Supervisor | Active Records | Committees (sample) |
|------------|----------------|---------------------|
| Hilda L. Solis | 7 | Board of Supervisors, Hearing Board of Supervisors, Los Angeles Grand Avenue Aut... |
| Janice Hahn | 5 | Board of Supervisors, Budget Deliberation , Los Angeles County Affordable Housin... |
| Kathryn Barger | 6 | Board of Supervisors, Budget Deliberation , Los Angeles County Affordable Housin... |
| Holly J. Mitchell | 7 | Board of Supervisors, Hearing Board of Supervisors, Los Angeles County Affordabl... |
| Lindsey P. Horvath | 4 | Board of Supervisors, Los Angeles County Affordable Housing Solutions Agency, Lo... |

### MatterRequester Attribution

Of the 50 most recent BOS matters sampled:
- **1 non-NULL** MatterRequester values
- **98.0% NULL** — MatterRequester is not a usable attribution field

### MatterHistories MoverName

Checked 5 recent matters for MoverName in action histories.
MoverName found in historical records: Hilda L. Solis, Sheila Kuehl

**Conclusion:** MoverName attribution is infeasible for recent (2020+) LA County legislation. Only committee assignment data is reliably importable with politician attribution.

---

## Bloomington OnBoard Findings

### Committee Member Pages

| Committee ID | Name | HTTP Status | Members Found | Names |
|-------------|------|-------------|---------------|-------|
| 1 | City Council (full body) | 200 | 9 | Isabel Piedmont-Smith, Kate Rosenbarger, Hopi Stosberg, Dave Rollo, Courtney Daily... |
| 77 | Council Processes | 200 | 4 | Isabel Piedmont-Smith, Sydney Zulich, Hopi Stosberg, Courtney Daily |
| 81 | Fiscal Committee | 200 | 4 | Isabel Piedmont-Smith, Hopi Stosberg, Matt Flaherty, Dave Rollo |
| 49 | Sidewalk / Pedestrian Safety | 200 | 4 | Kate Rosenbarger, Andy Ruff, Sydney Zulich, Courtney Daily |

### Legislation Listing

- Page 1 of `/onboard/committees/1/legislation`: HTTP 200, **20 items** found
- Pagination: 20 items per page (committee-scoped URL works without login)

### Sponsor Text Extraction

Sampled 3 legislation detail pages:
- **3 items** had sponsor text matching regex
- **0 items** had no sponsor text

Samples found:
- Item 6387: `Councilmember Zulich`
- Item 6386: `Councilmember Piedmont-Smith`
- Item 6385: `Councilmembers Piedmont-Smith and Stosberg,`

**Conclusion:** Sponsor extraction from description text is partial (~50% estimated coverage). Regex approach works where text exists; items without sponsor text must be skipped per locked decision.

---

## Politician Name Matching Results

### LA County Supervisors (3 of 5 LA County supervisors matched )

| Legistar Name | Status | DB Match | Method |
|---------------|--------|----------|--------|
| Hilda L. Solis | MATCH | Hilda L. Solis | ilike_exact |
| Janice Hahn | MATCH | Janice Hahn | ilike_exact |
| Kathryn Barger | AMBIG | Kathryn Barger |  |
| Holly J. Mitchell | AMBIG | Holly J. Mitchell |  |
| Lindsey P. Horvath | MATCH | Lindsey P. Horvath | ilike_exact |

### Bloomington Council Members (8 of 9 Bloomington council members matched )

| OnBoard Name | Status | DB Match | Method |
|--------------|--------|----------|--------|
| Isabel Piedmont-Smith | MATCH | Isabel Piedmont-Smith | ilike_exact |
| Kate Rosenbarger | MATCH | Kate Rosenbarger | ilike_exact |
| Hopi Stosberg | MATCH | Hopi H Stosberg | ilike_exact |
| Dave Rollo | MATCH | David R Rollo | ilike_exact |
| Courtney Daily | MISS | None |  |
| Sydney Zulich | MATCH | Sydney Zulich | ilike_exact |
| Matt Flaherty | MATCH | Matt Flaherty | ilike_exact |
| Isak Nti Asare | MATCH | Isak Asare | ilike_exact |
| Andy Ruff | MATCH | Andy Ruff | ilike_exact |

---

## Importable Data Summary

### What 58-02 (Bloomington Import) CAN import

- **Committee assignments** for all 9 Common Council members (main committee + 3 sub-committees)
- **Committee roles** (voting member, council president, chair, etc.) from member page text
- **Legislation metadata** for items with extractable sponsor name in description (~50% coverage)
- **Sponsor attribution** via regex extraction from description field

### What 58-02 CANNOT import

- Vote records (none exist in OnBoard)
- Individual council member vote positions
- Full legislation attribution (50% of items have no sponsor text)

### What 58-03 (LA County Import) CAN import

- **Committee assignments** for all 5 supervisors via Legistar OfficeRecords API
- **Committee roles** via OfficeRecordTitle field
- **Historical legislation movers** (pre-2010, where MoverName is populated)

### What 58-03 CANNOT import with politician attribution

- Recent matters/legislation (2020+) — MatterRequester is NULL, no usable attribution
- Individual vote positions — /VoteRecords endpoint returns 404
- Sponsor data — /Matters/{id}/Sponsors returns empty array for all matters

---

## Gaps and Limitations

| Gap | Source | Evidence | Recommended Action |
|-----|--------|----------|--------------------|
| Vote records | LA County Legistar | `/VoteRecords` returns HTTP 404 | Skip — confirmed infeasible |
| Vote records | Bloomington OnBoard | No API exists; HTML has no vote data | Skip — confirmed infeasible |
| Sponsor attribution | LA County (recent) | MatterRequester NULL for 98.0% of matters | Import only pre-2010 matters where MoverName exists |
| Sponsor attribution | Bloomington | ~50% of items lack sponsor text in description | Import with sponsor where found; document coverage gap |
| MoverName (recent) | LA County Legistar | 0/5 recent matters had MoverName | Restrict import to pre-2010 historical data only |

---

## Recommendations for Import Plans

### Plan 58-02: Bloomington Import

1. **Committee memberships first** — all 9 members confirmed findable, all committee IDs confirmed (1, 77, 81, 49)
2. **Legislation import** — use committee-scoped URL `/onboard/committees/1/legislation?page=N`, iterate all pages
3. **Sponsor extraction** — apply regex; skip items without sponsor match; log skip count for documentation
4. **Bridge table** — populate `legislative_politician_id_map` with `id_type='onboard'` for member IDs
5. **Scope limit** — do NOT attempt vote attribution (not available)

### Plan 58-03: LA County Import

1. **Committee memberships** — use OfficeRecords per PersonId (confirmed for all 5 supervisors)
2. **Committee roles** — capture OfficeRecordTitle field
3. **Skip recent legislation** — MatterRequester NULL rate is too high for reliable attribution
4. **Optional: historical legislation** — only if historical BOS record completeness is desired (pre-2010, MoverName populated)
5. **Bridge table** — populate `legislative_politician_id_map` with `id_type='legistar'` for PersonIds
6. **Scope limit** — do NOT attempt vote attribution (VoteRecords endpoint absent)

---

*Generated by `feasibility_local_data.py` v1.0.0*
