---
name: amperity-explainability-core
description: >
  Identity resolution explainability for any Amperity tenant. Investigate why
  records merged, compute confidence scores, and build explainability tools.
  Use this skill whenever the user mentions identity explainability, merge
  reasoning, "why did these records merge", cluster investigation, Stitch
  debugging, pairwise scores, Unified_Coalesced, Unified_Scores, or identity
  resolution quality. Also trigger for investigating specific amperity_ids,
  diagnosing false positives/negatives, or building apps that explain
  Amperity's Stitch output. Even "look into this cluster" or "why did these
  people merge" should trigger this skill.
---

# Amperity Identity Explainability (Core)

This skill covers three capabilities:

1. **Ad-hoc cluster investigation** -- Query any Amperity tenant to analyze
   specific clusters, understand merge reasoning, and assess identity
   resolution quality.

2. **Building explainability tools** -- Flask apps or scripts that make
   identity resolution transparent and auditable, with confidence scoring,
   variation detection, and plain-language narrative.

3. **Multi-region identity engines** -- Multi-tenant Flask apps with region
   switching, cross-region analytics, COTM Value View dashboards, and drift
   monitoring.

The native Amperity Stitch dashboard shows *what* merged. This skill focuses on
the **why**.

---

## Part 1: Complete Setup Guide (Any Tenant)

You need 6 values in a `.env` file to make this work. Here's how to get each.

### Step 1: Create OAuth2 API Credentials

Go to the Amperity tenant UI:
1. Click **Settings** (gear icon, bottom-left)
2. Click **API keys** in the left nav
3. Click **Create API key**
4. Name it (e.g., "Identity Engine" or "Explainability Tool")
5. Set permissions: **Read-only** is sufficient. Specifically needs:
   `amp-query:run` (run queries), `amp-query:read` (read results),
   `amp-segment:read` (access segments).
6. Click **Generate**
7. **COPY THE CLIENT SECRET NOW** -- it's only shown once. You'll get:
   - `Client ID` (looks like: `amp-abcdef123456`)
   - `Client Secret` (long string, only shown at creation)

```
AMPERITY_CLIENT_ID=amp-abcdef123456
AMPERITY_CLIENT_SECRET=<the long secret string>
```

### Step 2: Get the Tenant Name

The tenant name is the subdomain of your Amperity URL. If you log in at
`https://acme.amperity.com`, the tenant name is `acme`.

```
AMPERITY_TENANT=acme
AMPERITY_TOKEN_URL=https://acme.amperity.com/api/v0/oauth2/token
```

The token URL always follows `https://{tenant}.amperity.com/api/v0/oauth2/token`.

### Step 3: Find the Database ID

Go to **Customer 360** in the Amperity UI. The database ID is in the URL:
`https://acme.amperity.com/customer360/db-XXXXXXXXXXXX`

Or use MCP: `database_list` returns all databases with their IDs.

```
AMPERITY_DATABASE_ID=db-XXXXXXXXXXXX
```

### Step 4: Create a Draft SQL Segment

The Transit+JSON query API requires a segment ID to route queries through.
Create a permanent draft segment -- it does NOT need to be activated.

**Using MCP:**
```
segment_create(
    name="Explainability Query Segment",
    database_id="db-XXXXXXXXXXXX",
    query="SELECT 1"
)
```
Returns the segment ID. Do NOT activate it -- drafts can run interactive queries.

**Using the UI:**
1. Go to **Segments** > **Create segment**
2. Write any SQL (e.g., `SELECT 1`)
3. Save but do NOT activate
4. Copy the segment ID from the URL

```
AMPERITY_QUERY_SEGMENT_ID=seg-XXXXXXXXXXXX
```

### Step 5: Discover the Dataset ID

This is the trickiest one. The dataset ID is NOT visible in the UI -- you
have to capture it from the web app's network traffic.

1. Open the Amperity UI in Chrome
2. Open Chrome DevTools > Network tab
3. Go to **Segments** and open any SQL segment in the editor
4. Run the segment's query (click the play button)
5. In the Network tab, filter for `run-interactive-query`
6. Click the request and look at the Request Payload
7. Find the value for `"~:amperity.query.dataset/id"` -- that's the dataset ID

```
AMPERITY_DATASET_ID=qd-XXXXXXXXXXXXXXXXXXXX
```

### Step 6: Discover Table/Column Names

Every tenant's C360 database has different table names and column conventions.
Before building queries, run this discovery:

```sql
-- Find the unified customer table (could be Merged_Customers,
-- merged_customer, Customer_360, etc.):
SELECT * FROM Merged_Customers LIMIT 1
SELECT * FROM Unified_Coalesced LIMIT 1
SELECT * FROM Unified_Scores LIMIT 1
```

Common column name variations across tenants:

| Concept | Standard | Other possible names |
|---|---|---|
| Amperity ID | amperity_id | AMPID, amp_id |
| First name | given_name | Fname, first_name |
| Last name | surname | Lname, last_name |
| Email | email | Email, email_address |
| Phone | phone | Phone, phone_number |

`Unified_Coalesced` and `Unified_Scores` almost always use standard Amperity
column names regardless of tenant.

### Step 7: Build the Source Name Map

```sql
SELECT datasource, COUNT(*) as record_count
FROM Unified_Coalesced
GROUP BY datasource
ORDER BY record_count DESC
```

Map each raw datasource name to a human-readable label in `explainability.py`.

### Complete .env Template

```bash
# === Tenant Identity ===
AMPERITY_TENANT=acme
AMPERITY_TOKEN_URL=https://acme.amperity.com/api/v0/oauth2/token

# === OAuth2 Credentials (from Settings > API Keys) ===
AMPERITY_CLIENT_ID=amp-xxxxxxxxxxxxxxxx
AMPERITY_CLIENT_SECRET=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# === Query Routing (from UI or MCP) ===
AMPERITY_DATABASE_ID=db-xxxxxxxxxxxxxxx
AMPERITY_QUERY_SEGMENT_ID=seg-xxxxxxxxxxxxxxx
AMPERITY_DATASET_ID=qd-xxxxxxxxxxxxxxxxxxxxxxx
```

---

## Part 2: Ad-hoc Cluster Investigation

### The Query Pattern

Identity investigation always starts with queries against the tenant's
Customer 360 database. The target should be the database ID (e.g.,
`db-XXXXXXXXXXXX`), NOT "Domain tables" or "Stitch tables".

**Step 1: Find the customer**

```sql
-- Name search (adapt column names per tenant)
SELECT DISTINCT amperity_id, given_name, surname, email, phone, city, state
FROM Merged_Customers
WHERE UPPER(given_name) LIKE '%JESSICA%' AND UPPER(surname) LIKE '%SHARPE%'
LIMIT 20
```

For email searches -- JOIN Unified_Coalesced (the merged table only has the
"winning" email, not all emails):
```sql
SELECT DISTINCT mc.amperity_id, mc.given_name, mc.surname, mc.email
FROM Unified_Coalesced uc
JOIN Merged_Customers mc ON uc.amperity_id = mc.amperity_id
WHERE UPPER(uc.email) LIKE '%SOMEONE@GMAIL.COM%'
LIMIT 20
```

**Step 2: Pull all source records for the cluster**

```sql
SELECT amperity_id, pk, datasource, given_name, surname, email, phone,
       address, city, state, postal, loyalty_id, gender, birthdate
FROM Unified_Coalesced
WHERE amperity_id = '{ampid}'
ORDER BY datasource
```

**Step 3: Pull pairwise scores**

```sql
SELECT pk1, pk2, source1, source2, score, match_category, match_type
FROM Unified_Scores
WHERE amperity_id = '{ampid}'
ORDER BY score DESC
```

### Computing Merge Reasoning

Once you have the records and scores:

1. **Extract distinct values** for each field across all source records.
2. **Identify merge signals** -- shared PII values that caused records to link.
3. **Detect variations** -- surname changes, given name typos, multi-state, etc.
4. **Analyze pairwise scores** -- classify direct vs transitive links, compute
   average/min/max, detect transitive-heavy clusters.
5. **Compute 4-component confidence score** -- see Part 3.
6. **Generate narrative** -- natural-language explanation paragraph.

---

## Part 3: Confidence Scoring Algorithm

A **4-component composite (0-100)** that directly incorporates actual pairwise
score values from Unified_Scores.

### Pairwise Score Analysis (prerequisite)

```python
match_types = {}
cross_source_pairs = set()
for s in scores:
    mt = (s.get("match_type") or "scored").lower()
    match_types[mt] = match_types.get(mt, 0) + 1
    s1, s2 = s.get("source1", ""), s.get("source2", "")
    if s1 and s2 and s1 != s2:
        cross_source_pairs.add(tuple(sorted([s1, s2])))

transitive_count = match_types.get("scored_transitive", 0)
direct_count = match_types.get("scored", 0) + match_types.get("trivial_duplicate", 0)
transitive_ratio = transitive_count / max(len(scores), 1) if scores else 0
```

### Component 1: Signal Strength (0-40 pts)

Hard identity anchors -- shared PII across multiple sources.

```python
strong_signals = [s for s in signals if s["strength"] == "strong"]    # 3+ sources
moderate_signals = [s for s in signals if s["strength"] == "moderate"] # 2 sources
signal_pts = min(40, len(strong_signals) * 20 + len(moderate_signals) * 5)
```

### Component 2: Pairwise Match Quality (0-30 pts)

Uses actual `score` values from Unified_Scores. Amperity scores are typically
0-5 with threshold at 3.0.

```python
score_pts = 0
if score_values:
    avg_contribution = min(avg_score / 5.0, 1.0) * 20   # 0-20 pts
    min_contribution = min(min_score / 5.0, 1.0) * 10   # 0-10 pts
    score_pts = round(avg_contribution + min_contribution)
elif len(strong_signals) >= 1:
    score_pts = 10  # no scores but identity anchors exist
```

### Component 3: Data Consistency (0-20 pts)

Starts at 20, penalized by variation severity.

```python
consistency_pts = 20
if surname_variations:
    consistency_pts -= min(10, n_surnames * 4)  # 2 surnames = -8, 3+ = -10 cap
if multi_state:
    consistency_pts -= 4
if given_name_variations:
    consistency_pts -= 2
consistency_pts = max(0, consistency_pts)
```

### Component 4: Cross-Source Corroboration (0-10 pts)

```python
cross_pts = min(10, len(cross_source_pairs) * 3) if len(sources) >= 2 else 0
```

### Penalties

| Condition | Effect | Trigger |
|---|---|---|
| Overclustering | Cap at 25 | 20+ records AND 3+ surnames |
| Large cluster | -10 pts | 50+ records |
| No anchors | Cap at 30 | No strong signals AND no scores |
| Transitive-heavy | -8 pts | >70% of links are scored_transitive |
| Weak minimum link | -5 pts | Min score < 3.0 AND no strong signals |

### Confidence Labels

| Score Range | Label |
|---|---|
| 85-100 | Very High |
| 70-84 | High |
| 50-69 | Moderate |
| 0-49 | Low |

### Return Structure

```python
{
    "confidence_score": 78,
    "confidence_label": "High",
    "confidence_breakdown": {
        "score": 78,
        "components": {
            "signal_strength": {"points": 40, "max": 40, "detail": "..."},
            "match_quality": {"points": 22, "max": 30, "detail": "..."},
            "data_consistency": {"points": 12, "max": 20, "detail": "..."},
            "cross_source": {"points": 9, "max": 10, "detail": "..."},
        },
        "penalties_applied": [],
    },
    "narrative": "This cluster resolves to ...",
    "signals": [...],
    "variations": [...],
    "score_stats": {
        "avg": 3.85, "min": 2.10, "max": 4.92, "count": 11,
        "direct_pairs": 8, "transitive_pairs": 3, "transitive_ratio": 0.27,
        "by_category": {"EXACT": 2, "EXCELLENT": 4, ...}
    },
    "completeness": {"email": 0.85, "phone": 0.62, ...},
    "source_records": [...],
}
```

---

## Part 4: The Amperity Query API (Transit+JSON)

The Amperity public REST API has NO ad-hoc SQL query endpoint. The web app
uses an internal interactive query flow that works with OAuth2 API credentials.

### Authentication

OAuth2 client_credentials flow:
```
POST https://{tenant}.amperity.com/api/v0/oauth2/token
Content-Type: application/x-www-form-urlencoded

grant_type=client_credentials&client_id={id}&client_secret={secret}
```
Returns `{"access_token": "...", "expires_in": 3600}`.

### The 3-Step Query Flow

**Step 1: POST run-interactive-query**

Transit+JSON body (Cognitect Transit format -- Clojure serialization).
Transit maps are JSON arrays: `["^ ", "key1", val1, "key2", val2, ...]`

```
POST /api/v0/segments/{seg_id}/run-interactive-query
Content-Type: application/transit+json
Accept: application/transit+json

["^ ",
 "~:amperity.query.dataset/id", "{dataset_id}",
 "~:amperity.query.exec/statement", "{sql}\n",
 "~:amperity.query.exec/description", "Explainability Query",
 "~:amperity.query.cache/skip?", false,
 "~:amperity.query.exec/options", ["~#set", []],
 "~:database/id", "{database_id}",
 "~:amperity.query.engine/id", "athena"]
```

Response is Transit+JSON. Extract `qex_id` from `"~:amperity.query.exec/id"`.

**Step 2: Poll execution status**

```
GET /api/v0/query/executions/{qex_id}
Accept: application/transit+json
```

Check `"~:state"` for `"~:succeeded"` or `"~:failed"`. Poll every 1.5 seconds.

**Step 3: Fetch results**

```
GET /api/v0/segment-query-results/{qex_id}?limit=100
Accept: application/transit+json
```

Columns at `"~:columns"` (list of Transit maps with `"~:name"`).
Data rows at `"~:data"` (list of string arrays matching column order).

### Transit Parsing (Minimal -- No Library Needed)

```python
def _transit_get(data, key):
    """Get value from parsed Transit map (list)."""
    if not isinstance(data, list) or len(data) < 2 or data[0] != "^ ":
        return None
    for i in range(1, len(data) - 1, 2):
        if data[i] == key:
            return data[i + 1]
    return None
```

### Key Gotchas

1. **SQL goes directly in the POST body** -- no separate PUT step needed.
2. **Content-Type MUST be `application/transit+json`** -- regular JSON fails.
3. **All responses are Transit** -- even with `Accept: application/json`.
4. **Data values are always strings** -- even integers come as `"5"` not `5`.
5. **Draft segments work** -- no activation needed.
6. **qex- regex fallback**: `re.search(r'"(qex-[A-Za-z0-9_-]+)"', text)`.

---

## Part 5: Flask App Architecture (Single-Tenant)

### Minimal Structure

```
explainability-engine/
├── app.py              # Flask server (port 5080)
├── amperity_api.py     # OAuth2 + Transit+JSON query client
├── explainability.py   # Merge reasoning + confidence scoring
├── requirements.txt    # flask, flask-cors, python-dotenv, requests
├── .env                # Tenant configuration
└── static/
    └── index.html      # Search UI
```

### Core Routes

| Method | Route | Purpose |
|---|---|---|
| GET | `/` | Search UI |
| GET | `/api/search?q={query}` | Smart search (auto-detects name/email/phone/AMPID) |
| GET | `/api/explain/{ampid}` | Full cluster explainability with confidence scoring |
| GET | `/api/health` | Health check with tenant/credential status |

### Key Functions in amperity_api.py

- `get_access_token()` -- OAuth2 client_credentials flow
- `run_query(sql)` -- The full 3-step Transit+JSON query flow
- `search_customer(q)` -- Smart search (name/email/phone/AMPID detection)
- `get_full_cluster(ampid)` -- UC records + Unified_Scores for a cluster

### Startup

```bash
pip install flask flask-cors python-dotenv requests
python app.py
```

### Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Auth failed` on startup | Bad client ID/secret | Regenerate in Settings > API Keys |
| `403 Forbidden` on query | Insufficient permissions | Need `amp-query:run`, `amp-query:read` |
| `No records found` | Wrong database ID or table name | Verify with `database_list_tables` MCP tool |
| `database/id (nil)` | Missing database_id in Transit body | Check AMPERITY_DATABASE_ID in .env |
| Query hangs forever | Wrong dataset_id | Re-capture from browser DevTools |
| Empty search results | Column name mismatch | Check column names with `SELECT * LIMIT 1` |

---

## Part 6: Multi-Region Engine (GM POC)

The GM Identity Engine extends the single-tenant architecture to support 3
simultaneous Amperity regions (US, MEO, Mexico) with a region switcher,
cross-region analytics, and a COTM Value View dashboard.

### Structure

```
gm-identity-engine/
├── app.py              # Flask server (port 5080, threaded=True)
├── amperity_api.py     # Multi-region OAuth2 + Transit+JSON client
├── explainability.py   # Merge reasoning + confidence scoring
├── drift_store.py      # SQLite-backed drift monitoring
├── .env                # Multi-region credentials (REGION_{PREFIX}_{FIELD})
├── .env.example        # Template with all region fields
├── requirements.txt    # flask, flask-cors, python-dotenv, requests
├── launch.sh           # One-click launcher
├── data/               # SQLite databases for drift
└── static/
    ├── index.html          # Identity search/explain (single-cluster)
    ├── dashboard.html      # v1: Technical data quality dashboard
    └── dashboard-cotm.html # v2: COTM Value View (client-facing)
```

### Multi-Region .env Pattern

```bash
REGION_{PREFIX}_{FIELD}
# PREFIX = US, MEO, MX (becomes the region ID in the app)
# FIELD = NAME, TENANT, TOKEN_URL, CLIENT_ID, CLIENT_SECRET,
#         DATABASE_ID, SEGMENT_ID, DATASET_ID
```

`load_regions()` in `amperity_api.py` auto-discovers all `REGION_*` blocks.
Each region gets its own `AmperityAPI` instance. The app loads whichever
regions it finds -- you can run with 1, 2, or all 3.

### Three Views

| Route | File | Purpose |
|---|---|---|
| `/` | `index.html` | Identity search + cluster explainability |
| `/tools` | `dashboard.html` | Technical data quality (11 tabs) |
| `/cotm` | `dashboard-cotm.html` | Client-facing COTM Value View |

### Dashboard Tabs (v1 /tools)

1. Cross-Region Pulse, 2. Dedup Scorecard, 3. Source Scorecard,
4. Source Overlap, 5. Name Variants, 6. Crosswalk Explorer,
7. Cluster Quality, 8. Fleet Scanner, 9. Stitch Stats,
10. Stitch Score Distribution, 11. Drift Monitor

### COTM Value View (v2 /cotm)

Client-facing dashboard organized by Amperity's 3 Value Drivers:

**VD1 -- Trusted Data**: Dedup Before/After card, Source completeness heatmap,
Name Variants (probabilistic matching), Crosswalk (golden record), Cluster
Quality (expandable)

**VD2 -- Operate Faster**: Fleet fragmentation Before/After, Stitch performance
with Chart.js, Drift monitoring with alerts

**VD3 -- Acquire/Grow/Retain**: 5 smart segments with live SQL queries:
High-Value Multi-Source, Complete Contact Profiles, Email-Only Gap,
Life Event Indicators, Single-Source Power Users

**Design**: Amperity brand system (DUSK #004B57, TEAL #54D3DE, OCEAN #00A0B2,
ICE #ABF4F7, AMP_YELLOW #EAFF5F). Montserrat font. Amperity SVG logo from
S3. Differentiator badges (Unique/Comparative/Holistic). Per-region mantras
in hero stat cards. Collapsible hero.

**Language**: Client-facing, GM-centric. No "Amperity" in body text.
Before/After labels use "Before: Fragmented / Now: Resolved" not
"Before Amperity / After Amperity". Badges say "Unique Capability" not
"Unique to Amperity".

### Key API Endpoints

| Method | Route | Purpose |
|---|---|---|
| GET | `/api/regions` | List regions with active flag |
| POST | `/api/regions/{id}/activate` | Switch active region |
| GET | `/api/cross-region-pulse` | All regions' summary stats simultaneously |
| GET | `/api/dedup-scorecard` | Per-source dedup rates (active region) |
| GET | `/api/source-scorecard` | Per-source completeness (active region) |
| GET | `/api/source-overlap` | Cross-source shared identity counts |
| GET | `/api/name-variants` | Customers with name variations |
| GET | `/api/crosswalk-sample` | Multi-source golden record sample |
| GET | `/api/cluster-health/*` | Oversized, multi-surname, distribution |
| GET | `/api/fleet-fragmentation` | B2B account fragmentation |
| GET | `/api/stitch-stats` | Global identity resolution metrics |
| GET | `/api/stitch-score-distribution` | Match score histogram |
| GET | `/api/stitch-records-per-source` | Records by source system |
| POST | `/api/segment-preview` | Run ad-hoc SQL, return sample + total |
| POST | `/api/segment-demographics` | State + source distribution for segment |
| POST | `/api/drift/snapshot` | Take drift measurement |
| GET | `/api/drift/alerts` | Active drift alerts |
| GET | `/api/drift/history` | Drift snapshot history |

### Critical Implementation Details

**Amperity API returns numbers as strings.** All numeric values from queries
come back as strings (e.g., `"85.3"` not `85.3`). Always wrap with `Number()`
in JavaScript and `float()`/`int()` in Python.

- `pct()` must use `(Number(n)||0).toFixed(1)` not `(n||0).toFixed(1)`
- `reduce()` calls must wrap: `Number(d.source_records)||0`
- Truthy check `"85.3" || 0` stays `"85.3"` (string), not a number

**Flask must run threaded.** `app.run(threaded=True)` -- the dashboard fires
5-10 parallel `fetch()` calls. Without threading, each request queues behind
the previous one (each can poll for up to 60s).

**Global JSON error handlers are required.** Without them, routes that fail
return Flask's default HTML 500 page. The frontend tries to `.json()` parse
HTML and gets `Unexpected token '<'`. Add:

```python
@app.errorhandler(Exception)
def handle_exception(e):
    code = getattr(e, 'code', 500)
    return jsonify({"error": str(e)}), code if isinstance(code, int) else 500
```

**`apiFetch()` wrapper** in all dashboards: checks `Content-Type` before
`.json()` parse, surfaces `data.error` messages, prevents cryptic failures.

**Region tracking**: `currentRegion` variable must be set by `loadRegions()`
from the active region's `id`. Used by dedup card to pull the correct
`total_profiles` from the cross-region pulse. The pulse returns ALL regions;
use `pulse[currentRegion]` not `.find()` on entries.

**Number consistency**: Hero sums `total_profiles` across all regions.
Per-region cards must use the same region's `total_profiles` from the pulse,
NOT the per-source sum of `unique_ids` (which double-counts customers who
appear in multiple sources).

**Back navigation**: Cluster explain pages use `?from=tabname` URL parameter
to return to the correct dashboard tab. `index.html` reads this param and
redirects to `/tools#tabname` or `/cotm` accordingly.

---

## Part 7: Stitch Configuration Reference

- **Match categories** (descending): EXACT > EXCELLENT > HIGH > MODERATE > WEAK > NON-MATCH
- **Match types**: `scored` (direct), `trivial_duplicate` (exact dupe), `scored_transitive` (indirect)
- **Threshold**: Typically 3.0
- **stable_id**: When enabled, amperity_ids persist across Stitch runs

---

## Part 8: Amperity Brand Design

### COTM Brand System (for Value View dashboards)

```css
--dusk:       #004B57;   /* Primary dark, headers */
--teal:       #54D3DE;   /* Primary accent */
--ocean:      #00A0B2;   /* Secondary accent, links */
--ice:        #ABF4F7;   /* Light accent, hero text */
--amp-yellow: #EAFF5F;   /* CTA, active states, mantras */
--sat-red:    #FF504A;   /* Errors, alerts, "before" state */
--cloud:      #F2F4F4;   /* Page background */
--cement:     #E8EDEF;   /* Borders, dividers */
--anvil:      #282B2E;   /* Body text */
--offblack:   #0C0C0C;   /* Deep black */
```

Font: Montserrat (400/500/600/700/800).
Logo: SVG from `https://amperity-static-assets.s3-us-west-2.amazonaws.com/resources/img/amp_logo_white.svg`
Top bar: DUSK background, logo left, nav links right.
Active nav: AMP_YELLOW background with DUSK text.
Hero: DUSK background, TEAL left accent bar, stat cards with glass effect.
Before/After cards: red-tinted "before", green-tinted "after".
Differentiator badges: DUSK bg for Unique, OCEAN bg for Comparative, teal for Holistic.

### Classic Amperity Brand (for technical dashboards)

```css
--amp-black:   #0F1419;   /* Header/nav */
--amp-dark:    #1C2127;   /* Secondary dark surfaces */
--amp-teal:    #00C2B7;   /* Primary accent */
--amp-teal-d:  #009E95;   /* Darker teal for text on light bg */
--amp-lime:    #C5E600;   /* The "&" dot color */
--amp-cloud:   #F3F4F6;   /* Light background */
--amp-white:   #FFFFFF;   /* Card/content background */
```

Header: near-black (#0F1419), NOT dark teal. Logo: "Amperity" white, "&" teal.
Font: Inter.
