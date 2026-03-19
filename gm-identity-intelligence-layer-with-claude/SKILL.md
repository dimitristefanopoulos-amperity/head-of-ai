---
name: amperity-identity-explainability
description: >
  Build identity resolution explainability tools and perform ad-hoc cluster
  investigation on Amperity tenants. Use this skill whenever the user mentions
  identity explainability, merge reasoning, "why did these records merge",
  cluster investigation, Stitch debugging, pairwise scores, Unified_Coalesced,
  Unified_Scores, identity resolution quality, or wants to build tools/apps
  that explain Amperity's Stitch output. Also trigger for requests to build
  Flask apps or dashboards on top of Amperity identity data, investigate
  specific amperity_ids, diagnose false positives/negatives in identity
  resolution, or understand data quality across source systems. Even casual
  requests like "look into this cluster" or "why did these people merge"
  should trigger this skill.
---

# Amperity Identity Explainability Engine — Complete Blueprint

This is a **portable rebuild blueprint**. It contains everything needed to
reconstruct the full Identity Explainability Engine from scratch for any
Amperity tenant. The account-specific context (table names, column mappings,
source name maps, tenant credentials) comes from the conversation context —
this skill provides the architecture, code patterns, and intelligence layer.

## What This Builds

A Flask POC that queries Amperity tenant(s) and provides:

1. **Identity Search & Explainability** (`/`) — Search by name/email/phone/amperity_id, get cluster explainability with confidence scoring, merge reasoning narrative, and pairwise score analysis
2. **Data Quality Dashboard** (`/tools`) — 11-tab technical view with Cross-Region Pulse, Dedup Scorecard, Source Scorecard, Source Overlap, Name Variants, Crosswalk Explorer, Cluster Quality, Fleet Scanner, Stitch Stats, Score Distribution, Drift Monitor
3. **COTM Value View** (`/cotm`) — Client-facing dashboard organized by Amperity's 3 Value Drivers with Before/After cards, differentiator badges, per-region mantras, smart segments, and GM-centric language
4. **Internal Demo** (`/demo`) — Dark theme presentation with live data, scroll-reveal sections, segment save/copy/open-in-Amperity actions. NOT FOR DISTRIBUTION.
5. **Client Presentation** (`/presentation`) — External-facing dark theme presentation with GM-centric framing, business outcome focus, segment actions, and polished co-branded footer
6. **Agentic Marketing** (`/agentic`) — GM US agentic marketing capabilities presentation, demonstrating how AI agents leverage unified identity data
2. **Data Quality Dashboard** (`/tools`) — 11-tab technical view: Cross-Region Pulse, Dedup Scorecard, Source Scorecard, Source Overlap, Name Variants, Crosswalk Explorer, Cluster Quality, Fleet Scanner, Stitch Stats, Score Distribution, Drift Monitor
3. **COTM Value View** (`/cotm`) — Client-facing dashboard organized by Amperity's 3 Value Drivers with Before/After cards, differentiator badges, smart segments, and client-centric language
4. **Internal Demo** (`/demo`) — Dark theme presentation with live data, scroll-reveal sections, segment actions. NOT FOR DISTRIBUTION.
5. **Client Presentation** (`/presentation`) — External-facing dark theme presentation with client-centric framing, business outcome focus, and co-branded footer. Safe to present to clients.

All views have inter-navigation links and (for multi-region) a region switcher.

---

## Part 1: Architecture

```
identity-engine/
├── app.py              # Flask server (port 5080, threaded)
│                       #   Routes: /, /tools, /cotm, /demo, /presentation,
│                       #            /agentic + 25 API endpoints
├── amperity_api.py     # OAuth2 + Transit+JSON query client
│                       #   Single-tenant OR multi-region (auto-discovered from .env)
│                       #   Segment creation with multi-URL fallback + activation tracking
├── explainability.py   # 4-component confidence scoring + merge narrative
├── drift_store.py      # SQLite-backed drift monitoring with alerting
├── .env                # Credentials and IDs (never commit)
├── .env.example        # Template showing required values
├── requirements.txt    # flask, flask-cors, python-dotenv, requests
├── launch.sh           # One-click launcher
└── static/
    ├── index.html              # Identity search/explain UI
    ├── dashboard.html          # v1: Technical data quality (11 tabs)
    ├── dashboard-cotm.html     # v2: COTM Value View (client-facing)
    ├── dashboard-demo.html     # v3: Internal demo (dark theme theme)
    ├── dashboard-external.html # v4: Client presentation (dark theme, external-safe)
    └── dashboard-agentic.html  # v5: Agentic marketing capabilities
```

### requirements.txt

```
flask
flask-cors
python-dotenv
requests
```

### launch.sh

```bash
#!/bin/bash
cd "$(dirname "$0")"
if [ ! -f .env ]; then
    echo "  ⚠  No .env file found. Copy .env.example to .env and fill in credentials."
    exit 1
fi
pip install -q -r requirements.txt --break-system-packages 2>/dev/null
python3 app.py
```

---

## Part 2: Tenant Configuration

### Single-Tenant .env (Most Accounts)

```bash
AMPERITY_TENANT=acme
AMPERITY_TOKEN_URL=https://acme.amperity.com/api/v0/oauth2/token
AMPERITY_CLIENT_ID=amp-xxxxxxxxxxxxxxxx
AMPERITY_CLIENT_SECRET=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
AMPERITY_DATABASE_ID=db-xxxxxxxxxxxxxxx
AMPERITY_QUERY_SEGMENT_ID=seg-xxxxxxxxxxxxxxx
AMPERITY_DATASET_ID=qd-xxxxxxxxxxxxxxxxxxxxxxx
```

### Multi-Region .env (e.g., GM US/MEO/Mexico)

Each region needs 8 values with `REGION_{PREFIX}_` naming:

```bash
REGION_{PREFIX}_NAME          # Display name (e.g., "US", "Middle East")
REGION_{PREFIX}_TENANT        # Amperity subdomain (e.g., "gm", "gmmeo")
REGION_{PREFIX}_TOKEN_URL     # https://{tenant}.amperity.com/api/v0/oauth2/token
REGION_{PREFIX}_CLIENT_ID     # OAuth2 client ID
REGION_{PREFIX}_CLIENT_SECRET # OAuth2 client secret (only shown at creation!)
REGION_{PREFIX}_DATABASE_ID   # C360 database ID
REGION_{PREFIX}_SEGMENT_ID    # Draft SQL segment ID
REGION_{PREFIX}_DATASET_ID    # Captured from browser DevTools
```

The app loads whichever `REGION_*` blocks it finds — run with 1 or many.

### How to Get Each Value

1. **OAuth2 Credentials**: Settings > API Keys > Create API key. Needs
   `amp-query:run`, `amp-query:read`, `amp-segment:read`. **COPY SECRET NOW** —
   only shown once.

2. **Tenant Name**: Subdomain of URL (`https://macys.amperity.com` → `macys`)

3. **Database ID**: From C360 URL (`/customer360/db-XXXX`) or `database_list` MCP

4. **Draft SQL Segment**: Create via MCP or UI with `SELECT 1`. Save as draft.
   Does NOT need activation — drafts run interactive queries.

5. **Dataset ID** (trickiest — not in UI): Chrome DevTools > Network tab > run
   any SQL query > filter `run-interactive-query` > find
   `"~:amperity.query.dataset/id"` in payload.

### Table/Column Name Discovery

Every tenant has different table/column conventions. The **conversation context**
should provide these mappings. If not known, discover with:

```sql
SELECT * FROM Merged_Customers LIMIT 1   -- or merged_customer, Customer_360
SELECT * FROM Unified_Coalesced LIMIT 1  -- standard columns across ALL tenants
SELECT * FROM Unified_Scores LIMIT 1     -- standard columns across ALL tenants
```

Common variations:

| Concept | Standard | Macy's | Other |
|---|---|---|---|
| Amperity ID | amperity_id | AMPID | amp_id |
| First name | given_name | Fname | first_name |
| Last name | surname | Lname | last_name |
| Email | email | Email | email_address |

**CRITICAL**: `Unified_Coalesced` and `Unified_Scores` almost always use standard
Amperity column names regardless of tenant. Only the merged/customer table varies.

---

## Part 3: The Amperity Query API (Transit+JSON) — CRITICAL

The Amperity public REST API has NO ad-hoc SQL query endpoint. We use the web
app's internal interactive query flow with OAuth2 API credentials.

### Authentication

```
POST https://{tenant}.amperity.com/api/v0/oauth2/token
Content-Type: application/x-www-form-urlencoded

grant_type=client_credentials&client_id={id}&client_secret={secret}
```

Include `X-Amperity-Tenant: {tenant}` header. Returns `{"access_token": "...", "expires_in": 3600}`.

### The 3-Step Query Flow

**Step 1: POST run-interactive-query** (Transit+JSON body)

Transit maps are JSON arrays: `["^ ", "key1", val1, "key2", val2, ...]`
Keys use Transit keyword encoding: `"~:namespace/field-name"`

```
POST /api/v0/segments/{seg_id}/run-interactive-query
Content-Type: application/transit+json
Accept: application/transit+json

["^ ",
 "~:amperity.query.dataset/id", "{dataset_id}",
 "~:amperity.query.exec/statement", "{sql}\n",
 "~:amperity.query.exec/description", "Identity Engine Query",
 "~:amperity.query.cache/skip?", false,
 "~:amperity.query.exec/options", ["~#set", []],
 "~:database/id", "{database_id}",
 "~:amperity.query.engine/id", "prestosql"]
```

Extract `qex_id` from response key `"~:amperity.query.exec/id"`.

**Step 2: Poll execution status**

```
GET /api/v0/query/executions/{qex_id}
Accept: application/transit+json
```

Check `"~:state"` for `"~:succeeded"` or `"~:failed"`. Poll every 1.5 seconds.

**Step 3: Fetch results**

```
GET /api/v0/segment-query-results/{qex_id}?limit=500
Accept: application/json
```

This endpoint returns standard JSON (not Transit). Columns at `"columns"`,
data rows at `"rows"` (or `"data"`).

### Transit Parsing (Minimal — No Library Needed)

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

1. **SQL goes directly in POST body** — no separate PUT step.
2. **Content-Type MUST be `application/transit+json`** for Step 1 — regular JSON fails.
3. **Steps 1-2 responses are Transit**, Step 3 response is standard JSON.
4. **Data values are always strings** — even integers come as `"5"` not `5`.
   Always wrap with `Number()` in JS, `float()`/`int()` in Python.
5. **Draft segments work** — no activation needed for query routing.
6. **qex- regex fallback**: `re.search(r'"(qex-[A-Za-z0-9_-]+)"', text)` if
   Transit parsing misses the ID.

---

## Part 4: Complete Python Backend

### amperity_api.py — Full Implementation

```python
"""
Amperity Transit+JSON Query Layer — Multi-Region Support
Handles OAuth2 auth, 3-step interactive query flow, and all tool queries.
"""
import os, json, time, re, requests
from dotenv import dotenv_values

# ── Region Configuration ──────────────────────────────────────────────────────

class RegionConfig:
    """Holds credentials and IDs for one Amperity tenant/region."""
    def __init__(self, name, tenant, token_url, client_id, client_secret,
                 database_id, segment_id, dataset_id):
        self.name = name
        self.tenant = tenant
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.database_id = database_id
        self.segment_id = segment_id
        self.dataset_id = dataset_id
        self.base_url = f"https://{tenant}.amperity.com"
        self.token = None
        self.token_expiry = 0

def load_regions():
    """Load all regions from .env. Auto-discovers REGION_{PREFIX}_* blocks."""
    env = dotenv_values(".env")
    regions = {}
    prefixes = set()
    for k in env:
        if k.startswith("REGION_"):
            parts = k.split("_", 2)
            if len(parts) >= 3:
                prefixes.add(parts[1])

    for prefix in sorted(prefixes):
        p = f"REGION_{prefix}_"
        tenant = env.get(f"{p}TENANT", "")
        if not tenant:
            continue
        regions[prefix] = RegionConfig(
            name=env.get(f"{p}NAME", prefix),
            tenant=tenant,
            token_url=env.get(f"{p}TOKEN_URL", f"https://{tenant}.amperity.com/api/v0/oauth2/token"),
            client_id=env.get(f"{p}CLIENT_ID", ""),
            client_secret=env.get(f"{p}CLIENT_SECRET", ""),
            database_id=env.get(f"{p}DATABASE_ID", ""),
            segment_id=env.get(f"{p}SEGMENT_ID", ""),
            dataset_id=env.get(f"{p}DATASET_ID", ""),
        )
    return regions


# ── Transit+JSON Helpers ──────────────────────────────────────────────────────

def _transit_get(data, key):
    """Extract value from a parsed Transit map (JSON array: ['^ ', k1, v1, ...])."""
    if not isinstance(data, list) or len(data) < 2 or data[0] != "^ ":
        return None
    for i in range(1, len(data) - 1, 2):
        if data[i] == key:
            return data[i + 1]
    return None


# ── API Client ────────────────────────────────────────────────────────────────

class AmperityAPI:
    """Query client for a single Amperity region."""

    def __init__(self, region: RegionConfig):
        self.region = region
        self.session = requests.Session()

    def _ensure_token(self):
        if self.region.token and time.time() < self.region.token_expiry - 60:
            return
        resp = self.session.post(self.region.token_url, data={
            "grant_type": "client_credentials",
            "client_id": self.region.client_id,
            "client_secret": self.region.client_secret,
        }, headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "X-Amperity-Tenant": self.region.tenant,
        })
        resp.raise_for_status()
        body = resp.json()
        self.region.token = body["access_token"]
        self.region.token_expiry = time.time() + body.get("expires_in", 3600)

    def _auth_headers(self):
        self._ensure_token()
        return {
            "Authorization": f"Bearer {self.region.token}",
            "X-Amperity-Tenant": self.region.tenant,
            "api-version": "2024-04-01",
        }

    def _transit_headers(self):
        h = self._auth_headers()
        h["Content-Type"] = "application/transit+json"
        h["Accept"] = "application/transit+json"
        return h

    def run_query(self, sql, limit=500, timeout=60):
        """Execute SQL via the 3-step interactive query flow."""
        self._ensure_token()
        r = self.region

        # Step 1: POST run-interactive-query (Transit+JSON)
        transit_body = json.dumps([
            "^ ",
            "~:amperity.query.dataset/id", r.dataset_id,
            "~:amperity.query.exec/statement", sql + "\n",
            "~:amperity.query.exec/description", "Identity Engine Query",
            "~:amperity.query.cache/skip?", False,
            "~:amperity.query.exec/options", ["~#set", []],
            "~:database/id", r.database_id,
            "~:amperity.query.engine/id", "prestosql"
        ])

        url = f"{r.base_url}/api/v0/segments/{r.segment_id}/run-interactive-query"
        resp = self.session.post(url, data=transit_body, headers=self._transit_headers())
        resp.raise_for_status()

        # Extract qex_id
        data = json.loads(resp.text)
        qex_id = _transit_get(data, "~:amperity.query.exec/id")
        if not qex_id:
            m = re.search(r'"(qex-[A-Za-z0-9_-]+)"', resp.text)
            qex_id = m.group(1) if m else None
        if not qex_id:
            raise RuntimeError(f"No qex_id in response: {resp.text[:300]}")

        # Step 2: Poll until done
        poll_url = f"{r.base_url}/api/v0/query/executions/{qex_id}"
        poll_headers = self._auth_headers()
        poll_headers["Accept"] = "application/transit+json"
        deadline = time.time() + timeout
        while time.time() < deadline:
            time.sleep(1.5)
            poll = self.session.get(poll_url, headers=poll_headers)
            poll.raise_for_status()
            pdata = json.loads(poll.text)
            state = _transit_get(pdata, "~:state") or ""
            if "succeeded" in state:
                break
            if "failed" in state:
                err = _transit_get(pdata, "~:error") or "unknown"
                raise RuntimeError(f"Query failed: {err}")
        else:
            raise TimeoutError(f"Query timed out after {timeout}s")

        # Step 3: Fetch results (this endpoint returns JSON, not Transit)
        res_url = f"{r.base_url}/api/v0/segment-query-results/{qex_id}?limit={limit}"
        res_headers = self._auth_headers()
        res_headers["Accept"] = "application/json"
        res = self.session.get(res_url, headers=res_headers)
        res.raise_for_status()
        return self._parse_results(json.loads(res.text))

    def _parse_results(self, rdata):
        """Parse query results from JSON or Transit response into list of dicts."""
        if isinstance(rdata, dict):
            rows = rdata.get("rows") or rdata.get("data") or rdata.get("results") or []
            columns = rdata.get("columns") or rdata.get("headers") or []
            if rows and isinstance(rows[0], list) and columns:
                col_names = [c["name"] if isinstance(c, dict) else c for c in columns]
                return [dict(zip(col_names, row)) for row in rows]
            if rows and isinstance(rows[0], dict):
                return rows
            return rows
        if isinstance(rdata, list) and rdata and rdata[0] == "^ ":
            columns_raw = _transit_get(rdata, "~:columns") or []
            rows_raw = _transit_get(rdata, "~:data") or _transit_get(rdata, "~:rows") or []
            col_names = []
            for c in columns_raw:
                if isinstance(c, list) and c and c[0] == "^ ":
                    col_names.append(_transit_get(c, "~:name") or str(c))
                elif isinstance(c, dict):
                    col_names.append(c.get("name", str(c)))
                else:
                    col_names.append(str(c))
            return [dict(zip(col_names, row)) if isinstance(row, list) else row for row in rows_raw]
        return []

    def has_token(self):
        try:
            self._ensure_token()
            return True
        except Exception:
            return False

    # ── Search ────────────────────────────────────────────────────────────
    # NOTE: search_customer queries Unified_Coalesced which uses STANDARD
    # column names on all tenants. For tenants with non-standard merged
    # customer tables (e.g., Macy's merged_customer with AMPID/Fname/Lname),
    # adapt the search SQL or add a secondary search against that table.

    def search_customer(self, q):
        """Smart search: auto-detect AMPID, email, phone, or name."""
        q = q.strip()
        if re.match(r'^[0-9a-f]{8}-', q, re.I):
            return self.run_query(f"""
                SELECT DISTINCT amperity_id, given_name, surname, email, phone, city, state
                FROM Unified_Coalesced WHERE amperity_id = '{q}' LIMIT 20
            """)
        elif '@' in q:
            return self.run_query(f"""
                SELECT DISTINCT amperity_id, given_name, surname, email, phone, city, state
                FROM Unified_Coalesced WHERE UPPER(email) LIKE '%{q.upper()}%' LIMIT 20
            """)
        elif q.replace('-','').replace('+','').replace(' ','').isdigit():
            clean = q.replace('-','').replace('+','').replace(' ','').replace('(','').replace(')','')
            return self.run_query(f"""
                SELECT DISTINCT amperity_id, given_name, surname, email, phone, city, state
                FROM Unified_Coalesced
                WHERE REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(phone,'-',''),'+',''),' ',''),'(',''),')','') LIKE '%{clean}%'
                LIMIT 20
            """)
        else:
            parts = q.split()
            if len(parts) >= 2:
                fn, ln = parts[0], parts[-1]
                return self.run_query(f"""
                    SELECT DISTINCT amperity_id, given_name, surname, email, phone, city, state
                    FROM Unified_Coalesced
                    WHERE UPPER(given_name) LIKE '%{fn.upper()}%'
                      AND UPPER(surname) LIKE '%{ln.upper()}%'
                    LIMIT 20
                """)
            else:
                return self.run_query(f"""
                    SELECT DISTINCT amperity_id, given_name, surname, email, phone, city, state
                    FROM Unified_Coalesced
                    WHERE UPPER(given_name) LIKE '%{q.upper()}%'
                       OR UPPER(surname) LIKE '%{q.upper()}%'
                    LIMIT 20
                """)

    def get_full_cluster(self, ampid):
        records = self.run_query(f"""
            SELECT amperity_id, pk, datasource, given_name, surname, email,
                   phone, address, city, state, postal, gender, birthdate
            FROM Unified_Coalesced WHERE amperity_id = '{ampid}' ORDER BY datasource
        """)
        scores = self.run_query(f"""
            SELECT pk1, pk2, source1, source2, score, match_category, match_type
            FROM Unified_Scores WHERE amperity_id = '{ampid}' ORDER BY score DESC
        """)
        return records, scores

    # ── Cluster Health ────────────────────────────────────────────────────

    def get_oversized_clusters(self, min_records=20, limit=50):
        return self.run_query(f"""
            SELECT amperity_id, COUNT(*) as record_count,
                   COUNT(DISTINCT datasource) as source_count,
                   COUNT(DISTINCT surname) as surname_count
            FROM Unified_Coalesced GROUP BY amperity_id
            HAVING COUNT(*) >= {min_records}
            ORDER BY COUNT(*) DESC LIMIT {limit}
        """)

    def get_multi_surname_clusters(self, min_surnames=3, limit=50):
        return self.run_query(f"""
            SELECT amperity_id, COUNT(*) as record_count,
                   COUNT(DISTINCT surname) as surname_count,
                   COUNT(DISTINCT datasource) as source_count
            FROM Unified_Coalesced WHERE surname IS NOT NULL AND surname != ''
            GROUP BY amperity_id HAVING COUNT(DISTINCT surname) >= {min_surnames}
            ORDER BY COUNT(DISTINCT surname) DESC LIMIT {limit}
        """)

    def get_single_source_heavy_clusters(self, min_records=30, limit=50):
        return self.run_query(f"""
            SELECT amperity_id, datasource, COUNT(*) as record_count
            FROM Unified_Coalesced GROUP BY amperity_id, datasource
            HAVING COUNT(*) >= {min_records}
            ORDER BY COUNT(*) DESC LIMIT {limit}
        """)

    def get_cluster_size_distribution(self):
        return self.run_query("""
            SELECT cluster_size, COUNT(*) as cluster_count FROM (
                SELECT amperity_id, COUNT(*) as cluster_size
                FROM Unified_Coalesced GROUP BY amperity_id
            ) t GROUP BY cluster_size ORDER BY cluster_size LIMIT 100
        """)

    # ── Source Scorecard ──────────────────────────────────────────────────

    def get_source_scorecard(self):
        return self.run_query("""
            SELECT datasource, COUNT(*) as total_records,
                   COUNT(email) as has_email, COUNT(phone) as has_phone,
                   COUNT(address) as has_address, COUNT(given_name) as has_given_name,
                   COUNT(surname) as has_surname, COUNT(postal) as has_postal
            FROM Unified_Coalesced GROUP BY datasource ORDER BY COUNT(*) DESC
        """)

    def get_source_overlap(self):
        return self.run_query("""
            SELECT a.datasource AS source_a, b.datasource AS source_b,
                   COUNT(DISTINCT a.amperity_id) AS shared_ids
            FROM Unified_Coalesced a
            JOIN Unified_Coalesced b ON a.amperity_id = b.amperity_id AND a.datasource < b.datasource
            GROUP BY a.datasource, b.datasource ORDER BY shared_ids DESC LIMIT 100
        """)

    def get_source_dedup_rates(self):
        return self.run_query("""
            SELECT datasource, COUNT(*) AS source_records,
                   COUNT(DISTINCT amperity_id) AS unique_ids,
                   ROUND(100.0 * (1.0 - CAST(COUNT(DISTINCT amperity_id) AS DOUBLE) / NULLIF(COUNT(*), 0)), 1) AS dedup_rate_pct,
                   COUNT(*) - COUNT(DISTINCT amperity_id) AS duplicates_removed
            FROM Unified_Coalesced GROUP BY datasource ORDER BY dedup_rate_pct DESC
        """)

    # ── Stitch Stats ──────────────────────────────────────────────────────

    def get_current_stitch_stats(self):
        return self.run_query("""
            SELECT COUNT(DISTINCT amperity_id) AS total_clusters,
                   COUNT(*) AS total_records,
                   ROUND(CAST(COUNT(*) AS DOUBLE) / NULLIF(COUNT(DISTINCT amperity_id), 0), 2) AS avg_cluster_size,
                   MAX(cluster_size) AS max_cluster_size
            FROM (SELECT amperity_id, COUNT(*) AS cluster_size FROM Unified_Coalesced GROUP BY amperity_id) t
        """)

    def get_stitch_score_distribution(self):
        return self.run_query("""
            SELECT match_category, COUNT(*) AS pair_count
            FROM Unified_Scores GROUP BY match_category ORDER BY pair_count DESC
        """)

    def get_records_per_source(self):
        return self.run_query("""
            SELECT datasource, COUNT(*) AS record_count, COUNT(DISTINCT amperity_id) AS unique_ids
            FROM Unified_Coalesced GROUP BY datasource ORDER BY record_count DESC
        """)

    # ── Segment Preview & Creation ────────────────────────────────────────

    def preview_segment(self, sql, limit=100):
        count_sql = f"SELECT COUNT(*) AS cnt FROM ({sql}) t"
        count_result = self.run_query(count_sql)
        total = int(count_result[0]["cnt"]) if count_result else 0
        sample = self.run_query(f"{sql}\nLIMIT {limit}")
        return {"total": total, "sample": sample}

    def preview_segment_demographics(self, sql):
        state_dist = self.run_query(f"""
            SELECT state, COUNT(*) AS cnt FROM ({sql}) t
            WHERE state IS NOT NULL GROUP BY state ORDER BY cnt DESC LIMIT 20
        """)
        source_dist = self.run_query(f"""
            SELECT datasource, COUNT(*) AS cnt FROM ({sql}) t
            GROUP BY datasource ORDER BY cnt DESC LIMIT 20
        """)
        return {"state_distribution": state_dist, "source_distribution": source_dist}

    def create_segment(self, name, sql):
        """Create a SQL segment. Tries multiple API URL patterns.
        Returns: id, name, url (if activated), draft_url, list_url, activated bool."""
        self._ensure_token()
        r = self.region
        headers = self._auth_headers()
        headers["Content-Type"] = "application/json"
        body = {"name": name, "database_id": r.database_id, "sql": sql}

        url_patterns = [
            f"{r.base_url}/api/v0/segments",
            f"{r.base_url}/api/segments",
            f"{r.base_url}/api/v1/segments",
        ]

        resp = None
        last_error = None
        for url in url_patterns:
            try:
                resp = self.session.post(url, json=body, headers=headers)
                if resp.status_code != 405:
                    resp.raise_for_status()
                    break
                last_error = f"{resp.status_code} at {url}"
                resp = None
            except requests.exceptions.HTTPError as e:
                if resp and resp.status_code == 405:
                    last_error = f"{resp.status_code} at {url}"
                    resp = None
                    continue
                raise

        if resp is None:
            raise RuntimeError(f"Segment creation failed. Last: {last_error}. Use 'Copy SQL' instead.")

        seg = resp.json()
        seg_id = seg.get("id", "")

        # Attempt activation (may not be available on all tenants)
        activated = False
        if seg_id:
            working_base = resp.url.rsplit("/segments", 1)[0]
            try:
                act_resp = self.session.put(f"{working_base}/segments/{seg_id}/activate", headers=headers)
                act_resp.raise_for_status()
                activated = True
            except Exception:
                activated = False

        return {
            "id": seg_id, "name": name,
            "url": f"{r.base_url}/#/segments/{seg_id}" if (seg_id and activated) else None,
            "draft_url": f"{r.base_url}/#/segments/{seg_id}/draft" if (seg_id and not activated) else None,
            "list_url": f"{r.base_url}/#/segments",
            "tenant": r.tenant, "region": r.name, "activated": activated,
        }

    # ── Name Variants & Fleet Fragmentation ───────────────────────────────

    def get_name_variant_clusters(self, min_name_variants=3, limit=50):
        return self.run_query(f"""
            SELECT amperity_id, COUNT(*) AS record_count,
                   COUNT(DISTINCT datasource) AS source_count,
                   COUNT(DISTINCT COALESCE(given_name, '')) AS given_name_variants,
                   COUNT(DISTINCT COALESCE(surname, '')) AS surname_variants,
                   COUNT(DISTINCT COALESCE(given_name, '') || ' ' || COALESCE(surname, '')) AS full_name_variants,
                   COUNT(DISTINCT COALESCE(email, '')) AS email_count
            FROM Unified_Coalesced GROUP BY amperity_id
            HAVING COUNT(DISTINCT COALESCE(given_name, '') || ' ' || COALESCE(surname, '')) >= {min_name_variants}
            ORDER BY full_name_variants DESC LIMIT {limit}
        """)

    def get_name_variant_count(self, min_name_variants=3):
        """True total count (no LIMIT)."""
        rows = self.run_query(f"""
            SELECT COUNT(*) AS total FROM (
                SELECT amperity_id FROM Unified_Coalesced GROUP BY amperity_id
                HAVING COUNT(DISTINCT COALESCE(given_name, '') || ' ' || COALESCE(surname, '')) >= {min_name_variants}
            )
        """)
        return rows[0]["total"] if rows else 0

    def get_fleet_fragmentation(self, min_records=10, limit=50):
        return self.run_query(f"""
            SELECT amperity_id, COUNT(*) AS record_count,
                   COUNT(DISTINCT datasource) AS source_count,
                   COUNT(DISTINCT COALESCE(surname, '')) AS surname_variants,
                   COUNT(DISTINCT COALESCE(given_name, '')) AS name_variants,
                   COUNT(DISTINCT COALESCE(address, '')) AS address_variants,
                   COUNT(DISTINCT COALESCE(email, '')) AS email_variants,
                   COUNT(DISTINCT COALESCE(phone, '')) AS phone_variants
            FROM Unified_Coalesced GROUP BY amperity_id
            HAVING COUNT(*) >= {min_records} AND COUNT(DISTINCT COALESCE(address, '')) >= 5
            ORDER BY COUNT(*) DESC LIMIT {limit}
        """)

    def get_fleet_fragmentation_count(self, min_records=10):
        """True total count (no LIMIT)."""
        rows = self.run_query(f"""
            SELECT COUNT(*) AS total FROM (
                SELECT amperity_id FROM Unified_Coalesced GROUP BY amperity_id
                HAVING COUNT(*) >= {min_records} AND COUNT(DISTINCT COALESCE(address, '')) >= 5
            )
        """)
        return rows[0]["total"] if rows else 0

    # ── Crosswalk Explorer ────────────────────────────────────────────────

    def get_crosswalk(self, ampid):
        return self.run_query(f"""
            SELECT amperity_id, pk, datasource, given_name, surname, email, phone,
                   address, city, state, postal
            FROM Unified_Coalesced WHERE amperity_id = '{ampid}' ORDER BY datasource, pk
        """)

    def get_crosswalk_sample(self, limit=20):
        return self.run_query(f"""
            SELECT amperity_id, COUNT(*) AS record_count,
                   COUNT(DISTINCT datasource) AS source_count,
                   COUNT(DISTINCT pk) AS pk_count,
                   ARBITRARY(given_name) AS sample_name,
                   ARBITRARY(surname) AS sample_surname
            FROM Unified_Coalesced GROUP BY amperity_id
            HAVING COUNT(DISTINCT datasource) >= 2
            ORDER BY COUNT(DISTINCT datasource) DESC, COUNT(*) DESC LIMIT {limit}
        """)

    # ── Cross-Region Summary ──────────────────────────────────────────────

    def get_region_summary(self):
        stats = self.run_query("""
            SELECT COUNT(DISTINCT amperity_id) AS total_profiles,
                   COUNT(*) AS total_records,
                   COUNT(DISTINCT datasource) AS source_count,
                   ROUND(CAST(COUNT(*) AS DOUBLE) / NULLIF(COUNT(DISTINCT amperity_id), 0), 2) AS avg_records_per_id,
                   ROUND(100.0 * (1.0 - CAST(COUNT(DISTINCT amperity_id) AS DOUBLE) / NULLIF(COUNT(*), 0)), 1) AS overall_dedup_rate
            FROM Unified_Coalesced
        """)
        return stats[0] if stats else {}

    # ── Cluster Diff ──────────────────────────────────────────────────────

    def diff_clusters(self, ampid_a, ampid_b):
        rec_a, sc_a = self.get_full_cluster(ampid_a)
        rec_b, sc_b = self.get_full_cluster(ampid_b)
        return {
            "cluster_a": {"ampid": ampid_a, "records": rec_a, "scores": sc_a},
            "cluster_b": {"ampid": ampid_b, "records": rec_b, "scores": sc_b},
        }
```

### explainability.py — Full Implementation

```python
"""
Merge Reasoning Engine & Confidence Scoring
"""

# ── Source Name Map ───────────────────────────────────────────────────────────
# CUSTOMIZE THIS PER TENANT. Maps raw datasource identifiers to human labels.
# Discover sources with: SELECT datasource, COUNT(*) FROM Unified_Coalesced GROUP BY datasource
SOURCE_NAMES = {}  # Will be populated per-tenant

def friendly_source(raw):
    """Strip CDT_/MEO_ prefixes and _stitch/_Stitch suffix for readability."""
    if raw in SOURCE_NAMES:
        return SOURCE_NAMES[raw]
    s = raw
    for prefix in ("CDT_", "MEO_225931_", "MEO_"):
        if s.startswith(prefix):
            s = s[len(prefix):]
    if s.endswith("_Stitch") or s.endswith("_stitch"):
        s = s[:-7]
    return s.replace("_", " ").title()


def _extract_fields(records):
    fields = {}
    for r in records:
        for k in ("given_name", "surname", "email", "phone", "address",
                   "city", "state", "postal", "gender", "birthdate"):
            v = r.get(k)
            if v and str(v).strip():
                fields.setdefault(k, set()).add(str(v).strip())
    return {k: sorted(v) for k, v in fields.items()}


def _detect_signals(records, fields):
    sources_by_field = {}
    for r in records:
        ds = r.get("datasource", "unknown")
        for f in ("email", "phone", "surname", "given_name", "address", "postal"):
            v = r.get(f)
            if v and str(v).strip():
                key = (f, str(v).strip().upper())
                sources_by_field.setdefault(key, set()).add(ds)

    signals = []
    seen = set()
    for (field, value), srcs in sorted(sources_by_field.items(), key=lambda x: -len(x[1])):
        if len(srcs) >= 2 and (field, value) not in seen:
            seen.add((field, value))
            signals.append({
                "field": field, "value": value[:60],
                "sources": sorted(srcs), "source_count": len(srcs),
                "strength": "strong" if len(srcs) >= 3 else "moderate",
            })
    return sorted(signals, key=lambda s: (-s["source_count"], s["field"]))


def _detect_variations(fields):
    variations = []
    if len(fields.get("surname", [])) > 1:
        variations.append({"type": "surname_variation", "severity": "high",
            "detail": f"Multiple surnames: {', '.join(fields['surname'][:5])}", "values": fields["surname"][:5]})
    if len(fields.get("given_name", [])) > 1:
        variations.append({"type": "given_name_variation", "severity": "low",
            "detail": f"Name variants: {', '.join(fields['given_name'][:5])}", "values": fields["given_name"][:5]})
    if len(fields.get("state", [])) > 1:
        variations.append({"type": "multi_state", "severity": "medium",
            "detail": f"Multiple states: {', '.join(fields['state'][:5])}", "values": fields["state"][:5]})
    if len(fields.get("email", [])) > 2:
        variations.append({"type": "email_variation", "severity": "medium",
            "detail": f"{len(fields['email'])} distinct emails", "values": fields["email"][:5]})
    return variations


def _analyze_scores(scores):
    if not scores:
        return {"avg": 0, "min": 0, "max": 0, "count": 0,
                "direct_pairs": 0, "transitive_pairs": 0, "transitive_ratio": 0, "by_category": {}, "cross_source_pairs": 0}
    score_vals, match_types, cross_source, by_category = [], {}, set(), {}
    for s in scores:
        sv = s.get("score")
        if sv is not None:
            try: score_vals.append(float(sv))
            except: pass
        mt = (s.get("match_type") or "scored").lower()
        match_types[mt] = match_types.get(mt, 0) + 1
        cat = s.get("match_category", "UNKNOWN")
        by_category[cat] = by_category.get(cat, 0) + 1
        s1, s2 = s.get("source1", ""), s.get("source2", "")
        if s1 and s2 and s1 != s2:
            cross_source.add(tuple(sorted([s1, s2])))
    trans = match_types.get("scored_transitive", 0)
    direct = match_types.get("scored", 0) + match_types.get("trivial_duplicate", 0)
    return {
        "avg": round(sum(score_vals) / len(score_vals), 2) if score_vals else 0,
        "min": round(min(score_vals), 2) if score_vals else 0,
        "max": round(max(score_vals), 2) if score_vals else 0,
        "count": len(scores), "direct_pairs": direct, "transitive_pairs": trans,
        "transitive_ratio": round(trans / max(trans + direct, 1), 2),
        "by_category": by_category, "cross_source_pairs": len(cross_source),
    }


def _compute_confidence(signals, score_stats, fields, variations, n_records, n_sources):
    strong = [s for s in signals if s["strength"] == "strong"]
    moderate = [s for s in signals if s["strength"] == "moderate"]

    signal_pts = min(40, len(strong) * 20 + len(moderate) * 5)
    score_pts = 0
    if score_stats["count"] > 0 and score_stats["avg"] > 0:
        score_pts = round(min(score_stats["avg"] / 5.0, 1.0) * 20 + min(max(score_stats["min"], 0) / 5.0, 1.0) * 10)
    elif len(strong) >= 1:
        score_pts = 10

    consistency_pts = 20
    if any(v["type"] == "surname_variation" for v in variations):
        consistency_pts -= min(10, len(fields.get("surname", [])) * 4)
    if any(v["type"] == "multi_state" for v in variations):
        consistency_pts -= 4
    if any(v["type"] == "given_name_variation" for v in variations):
        consistency_pts -= 2
    consistency_pts = max(0, consistency_pts)

    cross_pts = min(10, score_stats.get("cross_source_pairs", 0) * 3) if n_sources >= 2 else 0
    raw = signal_pts + score_pts + consistency_pts + cross_pts

    penalties = []
    n_surnames = len(fields.get("surname", []))
    if n_records >= 20 and n_surnames >= 3:
        raw = min(raw, 25); penalties.append(f"Overclustering: {n_records} records, {n_surnames} surnames")
    if n_records >= 50:
        raw -= 10; penalties.append(f"Large cluster: {n_records} records")
    if not strong and score_stats["count"] == 0:
        raw = min(raw, 30); penalties.append("No strong anchors or pairwise scores")
    if score_stats["transitive_ratio"] > 0.7 and score_stats["count"] > 3:
        raw -= 8; penalties.append(f"Transitive-heavy: {int(score_stats['transitive_ratio']*100)}%")
    if score_stats["min"] < 3.0 and score_stats["count"] > 0 and not strong:
        raw -= 5; penalties.append(f"Weak min link: {score_stats['min']}")

    final = max(0, min(100, raw))
    label = "Very High" if final >= 85 else "High" if final >= 70 else "Moderate" if final >= 50 else "Low"
    return {"score": final, "label": label, "components": {
        "signal_strength": {"points": signal_pts, "max": 40},
        "match_quality": {"points": score_pts, "max": 30},
        "data_consistency": {"points": consistency_pts, "max": 20},
        "cross_source": {"points": cross_pts, "max": 10},
    }, "penalties_applied": penalties}


def _generate_narrative(records, fields, signals, variations, confidence, score_stats):
    n = len(records)
    sources = set(r.get("datasource", "") for r in records)
    source_labels = [friendly_source(s) for s in sorted(sources) if s]
    parts = [f"This cluster contains {n} records from {len(sources)} system{'s' if len(sources) != 1 else ''} ({', '.join(source_labels[:6])})."]
    strong = [s for s in signals if s["strength"] == "strong"]
    if strong:
        parts.append(f"Strong anchors: {'; '.join(f'{s[\"field\"]} across {s[\"source_count\"]} sources' for s in strong[:3])}.")
    if score_stats["count"] > 0:
        parts.append(f"{score_stats['direct_pairs']} direct + {score_stats['transitive_pairs']} transitive links, avg score {score_stats['avg']}.")
    for v in variations:
        if v["severity"] == "high": parts.append(f"⚠ {v['detail']}.")
    parts.append(f"Confidence: {confidence['score']}/100 ({confidence['label']}).")
    return " ".join(parts)


def explain_cluster(records, scores):
    if not records: return {"error": "No records found"}
    fields = _extract_fields(records)
    signals = _detect_signals(records, fields)
    variations = _detect_variations(fields)
    score_stats = _analyze_scores(scores)
    sources = set(r.get("datasource", "") for r in records)
    confidence = _compute_confidence(signals, score_stats, fields, variations, len(records), len(sources))
    narrative = _generate_narrative(records, fields, signals, variations, confidence, score_stats)
    completeness = {}
    for f in ("email", "phone", "address", "city", "state", "postal", "gender", "birthdate"):
        filled = sum(1 for r in records if r.get(f) and str(r.get(f)).strip())
        completeness[f] = round(filled / len(records) * 100, 1)
    return {
        "amperity_id": records[0].get("amperity_id"), "record_count": len(records),
        "source_count": len(sources), "sources": {s: friendly_source(s) for s in sorted(sources) if s},
        "fields": {k: v[:10] for k, v in fields.items()}, "signals": signals[:15],
        "variations": variations, "score_stats": score_stats, "confidence_breakdown": confidence,
        "field_completeness": completeness, "narrative": narrative,
        "records": records[:100], "scores": scores[:100],
    }
```

### drift_store.py — Full Implementation

```python
"""Stitch Drift Monitoring — SQLite persistence."""
import sqlite3, json, time, os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "drift_history.db")

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c

def init_db():
    c = _conn()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS stitch_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT, region TEXT NOT NULL, taken_at TEXT NOT NULL,
            total_clusters INTEGER, total_records INTEGER, avg_cluster_size REAL, max_cluster_size INTEGER
        );
        CREATE TABLE IF NOT EXISTS score_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_id INTEGER REFERENCES stitch_snapshots(id),
            match_category TEXT, pair_count INTEGER
        );
        CREATE TABLE IF NOT EXISTS source_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_id INTEGER REFERENCES stitch_snapshots(id),
            datasource TEXT, record_count INTEGER, unique_ids INTEGER
        );
        CREATE TABLE IF NOT EXISTS alert_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT, region TEXT NOT NULL, created_at TEXT NOT NULL,
            severity TEXT NOT NULL, metric TEXT NOT NULL, message TEXT NOT NULL,
            prev_value REAL, curr_value REAL, pct_change REAL, acknowledged INTEGER DEFAULT 0
        );
    """)
    c.close()

def save_snapshot(region, stats, score_dist=None, source_dist=None):
    c = _conn()
    now = datetime.utcnow().isoformat() + "Z"
    s = stats[0] if isinstance(stats, list) and stats else stats
    cur = c.execute("""INSERT INTO stitch_snapshots (region, taken_at, total_clusters, total_records, avg_cluster_size, max_cluster_size)
        VALUES (?, ?, ?, ?, ?, ?)""",
        (region, now, int(s.get("total_clusters", 0)), int(s.get("total_records", 0)),
         float(s.get("avg_cluster_size", 0)), int(s.get("max_cluster_size", 0))))
    snap_id = cur.lastrowid
    if score_dist:
        for row in score_dist:
            c.execute("INSERT INTO score_snapshots (snapshot_id, match_category, pair_count) VALUES (?, ?, ?)",
                      (snap_id, row.get("match_category"), int(row.get("pair_count", 0))))
    if source_dist:
        for row in source_dist:
            c.execute("INSERT INTO source_snapshots (snapshot_id, datasource, record_count, unique_ids) VALUES (?, ?, ?, ?)",
                      (snap_id, row.get("datasource"), int(row.get("record_count", 0)), int(row.get("unique_ids", 0))))
    c.commit(); c.close()
    return snap_id

def get_history(region, limit=30):
    c = _conn()
    rows = c.execute("SELECT * FROM stitch_snapshots WHERE region = ? ORDER BY taken_at ASC LIMIT ?", (region, limit)).fetchall()
    c.close()
    return [dict(r) for r in rows]

def get_scores_history(region, limit=30):
    c = _conn()
    snaps = c.execute("SELECT id, taken_at FROM stitch_snapshots WHERE region = ? ORDER BY taken_at DESC LIMIT ?", (region, limit)).fetchall()
    result = []
    for snap in reversed(snaps):
        scores = c.execute("SELECT match_category, pair_count FROM score_snapshots WHERE snapshot_id = ?", (snap["id"],)).fetchall()
        result.append({"taken_at": snap["taken_at"], "scores": [dict(s) for s in scores]})
    c.close()
    return result

def get_source_history(region, limit=30):
    c = _conn()
    snaps = c.execute("SELECT id, taken_at FROM stitch_snapshots WHERE region = ? ORDER BY taken_at DESC LIMIT ?", (region, limit)).fetchall()
    result = []
    for snap in reversed(snaps):
        sources = c.execute("SELECT datasource, record_count, unique_ids FROM source_snapshots WHERE snapshot_id = ?", (snap["id"],)).fetchall()
        result.append({"taken_at": snap["taken_at"], "sources": [dict(s) for s in sources]})
    c.close()
    return result

def compute_drift(region, current_stats):
    c = _conn()
    prev = c.execute("SELECT * FROM stitch_snapshots WHERE region = ? ORDER BY taken_at DESC LIMIT 1", (region,)).fetchone()
    c.close()
    if not prev: return []
    s = current_stats[0] if isinstance(current_stats, list) and current_stats else current_stats
    alerts = []
    now = datetime.utcnow().isoformat() + "Z"
    for metric, col in [("total_clusters", "total_clusters"), ("total_records", "total_records"), ("max_cluster_size", "max_cluster_size")]:
        prev_val, curr_val = prev[col], int(s.get(metric, 0))
        if prev_val and prev_val > 0:
            pct = abs(curr_val - prev_val) / prev_val
            if pct >= 0.20: sev = "critical"
            elif pct >= 0.05: sev = "warning"
            else: continue
            alerts.append({"region": region, "severity": sev, "metric": metric,
                "message": f"{metric} changed by {pct*100:.1f}% ({prev_val:,} → {curr_val:,})",
                "prev_value": prev_val, "curr_value": curr_val, "pct_change": round(pct * 100, 1)})
    if alerts:
        c = _conn()
        for a in alerts:
            c.execute("""INSERT INTO alert_log (region, created_at, severity, metric, message, prev_value, curr_value, pct_change)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (a["region"], now, a["severity"], a["metric"], a["message"], a["prev_value"], a["curr_value"], a["pct_change"]))
        c.commit(); c.close()
    return alerts

def get_alerts(region=None, limit=50, unacknowledged_only=False):
    c = _conn()
    sql, params, clauses = "SELECT * FROM alert_log", [], []
    if region: clauses.append("region = ?"); params.append(region)
    if unacknowledged_only: clauses.append("acknowledged = 0")
    if clauses: sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY created_at DESC LIMIT ?"; params.append(limit)
    rows = c.execute(sql, params).fetchall(); c.close()
    return [dict(r) for r in rows]

def acknowledge_alert(alert_id):
    c = _conn()
    c.execute("UPDATE alert_log SET acknowledged = 1 WHERE id = ?", (alert_id,))
    c.commit(); c.close()

init_db()
```

### app.py — Flask Server (Complete)

```python
"""Identity Engine — Flask Server (single or multi-region)"""
import os, sys, socket, json
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from amperity_api import load_regions, AmperityAPI
from explainability import explain_cluster
import drift_store

app = Flask(__name__, static_folder="static")
CORS(app)

# ── Global Error Handlers (always return JSON) ────────────────────────────────
@app.errorhandler(Exception)
def handle_exception(e):
    code = getattr(e, 'code', 500)
    return jsonify({"error": str(e)}), code if isinstance(code, int) else 500

@app.errorhandler(500)
def handle_500(e): return jsonify({"error": str(e)}), 500

@app.errorhandler(404)
def handle_404(e): return jsonify({"error": "Not found"}), 404

# ── Multi-Region Setup ────────────────────────────────────────────────────────
REGIONS, API_CLIENTS, ACTIVE_REGION = {}, {}, None

def init_regions():
    global REGIONS, API_CLIENTS, ACTIVE_REGION
    REGIONS = load_regions()
    for prefix, cfg in REGIONS.items():
        API_CLIENTS[prefix] = AmperityAPI(cfg)
    if REGIONS:
        ACTIVE_REGION = sorted(REGIONS.keys())[0]

def get_api():
    if ACTIVE_REGION and ACTIVE_REGION in API_CLIENTS:
        return API_CLIENTS[ACTIVE_REGION]
    raise RuntimeError("No active region configured")

# ── Region Switching ──────────────────────────────────────────────────────────
@app.route("/api/regions")
def list_regions():
    regions = [{"id": p, "name": c.name, "tenant": c.tenant, "active": p == ACTIVE_REGION,
                "has_token": API_CLIENTS[p].has_token()} for p, c in sorted(REGIONS.items())]
    return jsonify({"regions": regions, "active": ACTIVE_REGION})

@app.route("/api/regions/<region_id>/activate", methods=["POST"])
def activate_region(region_id):
    global ACTIVE_REGION
    if region_id not in REGIONS: return jsonify({"error": f"Unknown region: {region_id}"}), 404
    ACTIVE_REGION = region_id
    return jsonify({"active": ACTIVE_REGION, "name": REGIONS[region_id].name})

# ── Static Pages ──────────────────────────────────────────────────────────────
@app.route("/")
def index(): return send_from_directory("static", "index.html")

@app.route("/tools")
def tools(): return send_from_directory("static", "dashboard.html")

@app.route("/cotm")
def cotm(): return send_from_directory("static", "dashboard-cotm.html")

@app.route("/demo")
def demo(): return send_from_directory("static", "dashboard-demo.html")

@app.route("/presentation")
def presentation(): return send_from_directory("static", "dashboard-external.html")

# ── Identity ──────────────────────────────────────────────────────────────────
@app.route("/api/health")
def health():
    api = get_api(); cfg = api.region
    return jsonify({"status": "ok", "region": ACTIVE_REGION, "region_name": cfg.name,
                    "tenant": cfg.tenant, "database_id": cfg.database_id, "has_token": api.has_token()})

@app.route("/api/search")
def search():
    q = request.args.get("q", "").strip()
    if not q: return jsonify({"error": "Missing 'q'"}), 400
    try:
        results = get_api().search_customer(q)
        return jsonify({"results": results, "count": len(results), "region": ACTIVE_REGION})
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/explain/<ampid>")
def explain(ampid):
    try:
        records, scores = get_api().get_full_cluster(ampid)
        result = explain_cluster(records, scores)
        result["region"] = ACTIVE_REGION
        return jsonify(result)
    except Exception as e: return jsonify({"error": str(e)}), 500

# ── Cluster Health ────────────────────────────────────────────────────────────
@app.route("/api/cluster-health/oversized")
def oversized(): return jsonify(get_api().get_oversized_clusters(int(request.args.get("min", 20)), int(request.args.get("limit", 50))))

@app.route("/api/cluster-health/multi-surname")
def multi_surname(): return jsonify(get_api().get_multi_surname_clusters(int(request.args.get("min", 3)), int(request.args.get("limit", 50))))

@app.route("/api/cluster-health/single-source")
def single_source(): return jsonify(get_api().get_single_source_heavy_clusters(int(request.args.get("min", 30)), int(request.args.get("limit", 50))))

@app.route("/api/cluster-health/distribution")
def distribution(): return jsonify(get_api().get_cluster_size_distribution())

# ── Source & Stitch ───────────────────────────────────────────────────────────
@app.route("/api/source-scorecard")
def source_scorecard(): return jsonify(get_api().get_source_scorecard())

@app.route("/api/source-overlap")
def source_overlap(): return jsonify(get_api().get_source_overlap())

@app.route("/api/dedup-scorecard")
def dedup_scorecard(): return jsonify(get_api().get_source_dedup_rates())

@app.route("/api/stitch-stats")
def stitch_stats(): return jsonify(get_api().get_current_stitch_stats())

@app.route("/api/stitch-score-distribution")
def stitch_scores(): return jsonify(get_api().get_stitch_score_distribution())

@app.route("/api/stitch-records-per-source")
def stitch_sources(): return jsonify(get_api().get_records_per_source())

# ── Name Variants & Fleet ────────────────────────────────────────────────────
@app.route("/api/name-variants")
def name_variants(): return jsonify(get_api().get_name_variant_clusters(int(request.args.get("min", 3)), int(request.args.get("limit", 50))))

@app.route("/api/name-variants-count")
def name_variants_count(): return jsonify({"total": get_api().get_name_variant_count(int(request.args.get("min", 3)))})

@app.route("/api/fleet-fragmentation")
def fleet_fragmentation(): return jsonify(get_api().get_fleet_fragmentation(int(request.args.get("min", 10)), int(request.args.get("limit", 50))))

@app.route("/api/fleet-fragmentation-count")
def fleet_fragmentation_count(): return jsonify({"total": get_api().get_fleet_fragmentation_count(int(request.args.get("min", 10)))})

# ── Crosswalk ─────────────────────────────────────────────────────────────────
@app.route("/api/crosswalk/<ampid>")
def crosswalk(ampid):
    try: return jsonify(get_api().get_crosswalk(ampid))
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/crosswalk-sample")
def crosswalk_sample(): return jsonify(get_api().get_crosswalk_sample(int(request.args.get("limit", 20))))

# ── Segments ──────────────────────────────────────────────────────────────────
@app.route("/api/segment-preview", methods=["POST"])
def segment_preview():
    sql = request.get_json(force=True).get("sql", "").strip()
    if not sql: return jsonify({"error": "Missing 'sql'"}), 400
    try: return jsonify(get_api().preview_segment(sql))
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/save-segment", methods=["POST"])
def save_segment():
    body = request.get_json(force=True)
    name, sql = body.get("name", "").strip(), body.get("sql", "").strip()
    if not name or not sql: return jsonify({"error": "Missing 'name' or 'sql'"}), 400
    try: return jsonify(get_api().create_segment(name, sql))
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/segment-demographics", methods=["POST"])
def segment_demographics():
    sql = request.get_json(force=True).get("sql", "").strip()
    if not sql: return jsonify({"error": "Missing 'sql'"}), 400
    try: return jsonify(get_api().preview_segment_demographics(sql))
    except Exception as e: return jsonify({"error": str(e)}), 500

# ── Cross-Region Pulse ────────────────────────────────────────────────────────
@app.route("/api/cross-region-pulse")
def cross_region_pulse():
    results = {}
    for prefix, api in API_CLIENTS.items():
        try:
            if api.has_token():
                results[prefix] = {"name": REGIONS[prefix].name, "summary": api.get_region_summary()}
            else:
                results[prefix] = {"name": REGIONS[prefix].name, "error": "not connected"}
        except Exception as e:
            results[prefix] = {"name": REGIONS[prefix].name, "error": str(e)}
    return jsonify(results)

# ── Cluster Diff ──────────────────────────────────────────────────────────────
@app.route("/api/cluster-diff")
def cluster_diff():
    a, b = request.args.get("a", ""), request.args.get("b", "")
    if not a or not b: return jsonify({"error": "Need ?a=ampid&b=ampid"}), 400
    try: return jsonify(get_api().diff_clusters(a, b))
    except Exception as e: return jsonify({"error": str(e)}), 500

# ── Drift Monitoring ──────────────────────────────────────────────────────────
@app.route("/api/drift/snapshot", methods=["POST"])
def drift_snapshot():
    api = get_api()
    stats, score_dist, source_dist = api.get_current_stitch_stats(), api.get_stitch_score_distribution(), api.get_records_per_source()
    alerts = drift_store.compute_drift(ACTIVE_REGION, stats)
    snap_id = drift_store.save_snapshot(ACTIVE_REGION, stats, score_dist, source_dist)
    return jsonify({"snapshot_id": snap_id, "alerts": alerts, "stats": stats})

@app.route("/api/drift/history")
def drift_history(): return jsonify(drift_store.get_history(ACTIVE_REGION, int(request.args.get("limit", 30))))

@app.route("/api/drift/scores-history")
def drift_scores(): return jsonify(drift_store.get_scores_history(ACTIVE_REGION, int(request.args.get("limit", 30))))

@app.route("/api/drift/source-history")
def drift_sources(): return jsonify(drift_store.get_source_history(ACTIVE_REGION, int(request.args.get("limit", 30))))

@app.route("/api/drift/alerts")
def drift_alerts(): return jsonify(drift_store.get_alerts(ACTIVE_REGION, int(request.args.get("limit", 50)),
    request.args.get("unacknowledged", "false").lower() == "true"))

@app.route("/api/drift/alerts/<int:alert_id>/acknowledge", methods=["POST"])
def drift_ack(alert_id):
    drift_store.acknowledge_alert(alert_id)
    return jsonify({"acknowledged": True})

# ── Startup ───────────────────────────────────────────────────────────────────
def find_open_port(preferred=5080):
    for port in [preferred] + list(range(5080, 5100)):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("127.0.0.1", port))
                return port
        except OSError: continue
    raise RuntimeError("No open port")

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    print("\n" + "═" * 60 + "\n  IDENTITY ENGINE\n" + "═" * 60)
    init_regions()
    if not REGIONS:
        print("  ⚠  No regions configured. Copy .env.example to .env.")
        sys.exit(1)
    for prefix, cfg in sorted(REGIONS.items()):
        status = "✓ connected" if API_CLIENTS[prefix].has_token() else "✗ auth failed"
        active = " ← active" if prefix == ACTIVE_REGION else ""
        print(f"  [{prefix}] {cfg.name} ({cfg.tenant}) — {status}{active}")
    port = find_open_port()
    print(f"\n  http://127.0.0.1:{port}/\n" + "═" * 60 + "\n")
    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)
```

---

## Part 5: Frontend Design Patterns

### Segment Action Buttons (Demo & Presentation)

Each discovered audience has 3 action buttons:

```javascript
// Global state needed
let currentRegion = '';
let regionTenants = {};  // populated by loadRegions()

// In loadRegions():
d.regions.forEach(r => { regionTenants[r.id] = r.tenant; });

// Save as Segment
async function saveSegment(name, sql, btn) {
  btn.textContent = 'Saving...'; btn.disabled = true;
  try {
    const r = await fetch('/api/save-segment', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({name, sql})
    }).then(r => r.json());
    if (r.error) { btn.textContent = '✗ ' + r.error; return; }
    const isDraft = !r.activated;
    const linkUrl = isDraft ? r.list_url : (r.url || r.list_url);
    btn.textContent = isDraft ? '✓ Saved as Draft' : '✓ Saved & Activated';
    btn.style.background = '#22c55e';
    // If draft, show: "Find {name} in Queries, then click Activate"
    // Link to list_url (Queries page), NOT draft_url (causes spinner)
  } catch(e) { btn.textContent = '✗ Error'; }
}

// Open in Amperity (copies SQL + opens Queries page)
async function openInAmperity(sql, btn) {
  try { await navigator.clipboard.writeText(sql); } catch(e) {
    const ta = document.createElement('textarea'); ta.value = sql;
    ta.style.cssText = 'position:fixed;left:-9999px';
    document.body.appendChild(ta); ta.select(); document.execCommand('copy');
    document.body.removeChild(ta);
  }
  const tenant = regionTenants[currentRegion] || Object.values(regionTenants)[0] || '';
  if (tenant) window.open(`https://${tenant}.amperity.com/#/segments`, '_blank');
  btn.textContent = '✓ SQL Copied — Paste in New Query';
  btn.style.background = '#22c55e';
  setTimeout(() => { btn.textContent = '↗ Open in Amperity'; btn.style.background = ''; }, 3000);
}

// Copy SQL
function copySQL(sql, btn) {
  navigator.clipboard.writeText(sql);
  btn.textContent = '✓ Copied';
  setTimeout(() => { btn.textContent = '📋 Copy SQL'; }, 2000);
}
```

### apiFetch Wrapper (All Dashboards)

```javascript
async function apiFetch(url) {
  const resp = await fetch(url);
  const ct = resp.headers.get('content-type') || '';
  if (!ct.includes('application/json')) {
    throw new Error(`Expected JSON, got ${ct}`);
  }
  const data = await resp.json();
  if (data.error) throw new Error(data.error);
  return data;
}
```

### COTM Value View Critical Patterns

```javascript
// Stitch stats — API returns total_clusters NOT unique_ids
const s = stitchData[0];
const uniqueCustomers = s.total_clusters || s.unique_ids || 0;
const totalRecs = s.total_records || 0;
const compressionRate = totalRecs > 0
  ? (100 * (1 - Number(uniqueCustomers) / Number(totalRecs)))
  : 0;

// Fleet fragmentation — fetch true count in parallel
const [data, countData] = await Promise.all([
  apiFetch('/api/fleet-fragmentation'),
  apiFetch('/api/fleet-fragmentation-count').catch(() => ({ total: 0 })),
]);
const total = countData.total || data.length;  // true total, not LIMIT
```

---

## Part 6: Dark Theme Design System

Dark-first design shared by `/demo` and `/presentation`:

```css
:root {
  --bg: #0C0C0C;
  --panel: #1a1a1a;
  --brand: #54d3de;
  --accent: #EAFF5F;
  --text: #e0e0e0;
  --text-dim: #888;
}
body { background: var(--bg); color: var(--text); font-family: 'Montserrat', sans-serif; }
.card { background: var(--panel); border: 1px solid rgba(84,211,222,0.15);
        border-radius: 16px; padding: 2rem; backdrop-filter: blur(10px); }
```

Effects: glassmorphic cards, scroll-reveal with IntersectionObserver, full-width sections.

**Demo vs Presentation differences**:

| Feature | Demo | Presentation |
|---|---|---|
| Badge | "INTERNAL DEMO" | None |
| Footer | "NOT FOR DISTRIBUTION" | "Amperity x CLIENT" |
| Business Outcomes | Not present | 3 cards |
| Language | "Live from Amperity" | "Live Production Data" |

---

## Part 7: COTM Value View Framework

Organizes technical metrics by Amperity's 3 Value Drivers:

**Value Driver 1 — Trusted Data**: Dedup Before/After, Source Completeness,
Name Variants (with true count), Crosswalk (golden record), Cluster Quality

**Value Driver 2 — Operate Faster**: Fleet Fragmentation (true count),
Stitch Performance with Chart.js, Always-On Drift Monitoring

**Value Driver 3 — Acquire/Grow/Retain**: 5 activation-ready smart segments
with live SQL, demographic breakdowns, per-segment rendering

Brand: DUSK (#004B57), TEAL (#54D3DE), OCEAN (#00A0B2), ICE (#ABF4F7),
AMP_YELLOW (#EAFF5F). Montserrat font.

Client-facing language: No "Amperity" in body text. Before/After uses
"Before: Fragmented / Now: Resolved". Badges say "Unique Capability".
Per-region mantras in hero stat cards. Collapsible hero section.

---

## Part 8: Known Implementation Details (Critical)

1. **Numbers as strings** — API returns all values as strings. Always `Number()` in JS, `float()`/`int()` in Python.
2. **Stitch stats field** — API returns `total_clusters`, NOT `unique_ids`. COTM must handle both.
3. **Flask threaded** — `app.run(threaded=True)` is required. Dashboards fire 5-10 parallel fetches.
4. **JSON error handlers** — Without global handlers, failed routes return HTML 500 that breaks `fetch().json()`.
5. **Amperity deep links** — Use `/#/segments` hash routing (SPA). Draft links (`/#/segments/{id}/draft`) only work from within the loaded app, not external navigation.
6. **Segment creation** — `/api/v0/segments` works. `/api/segments` returns 405. Try patterns in order.
7. **Activation may fail** — Some tenants don't allow API activation. Track `activated` bool.
8. **True count pattern** — Wrap HAVING clause in `COUNT(*)` without LIMIT for true totals.
9. **Number consistency** — Hero and dedup card use `total_profiles` from cross-region pulse, not per-source `unique_ids` sum (which double-counts).
10. **Transit+JSON 3-step** — POST (transit+json) → Poll status (transit+json) → GET results (json).
11. **regionTenants global** — JS map of region ID → tenant name, set by `loadRegions()`, used for deep links.

---

## Part 9: Adapting for a New Tenant

### What Changes Per Tenant

| Component | Changes? | What to customize |
|---|---|---|
| `.env` | Yes | All credential values |
| Search SQL (amperity_api.py) | Maybe | If tenant has non-standard merged customer table with different column names |
| SOURCE_NAMES (explainability.py) | Yes | Map raw datasource names to human labels |
| Smart segments SQL (dashboard-cotm.html) | Yes | Adapt WHERE clauses to available columns |
| Branding text (all dashboards) | Yes | Client name, region names, mantras |

### What's Universal (Never Changes)

| Component | Why |
|---|---|
| Transit+JSON query flow | Same for all tenants |
| Confidence scoring algorithm | Uses standard UC/US columns |
| Drift monitoring | SQLite schema is universal |
| All Flask routes | All routes are universal |
| Unified_Coalesced queries | Standard column names on all tenants |
| Unified_Scores queries | Standard column names on all tenants |

### Quick Checklist

1. Get OAuth2 credentials (Settings > API Keys)
2. Create draft SQL segment (leave unactivated)
3. Capture dataset ID from browser DevTools
4. Get database ID from C360 URL
5. Discover table/column names with `SELECT * FROM ... LIMIT 1`
6. Build SOURCE_NAMES map from `SELECT datasource, COUNT(*) FROM Unified_Coalesced GROUP BY datasource`
7. Populate `.env`, run `./launch.sh`, verify `http://127.0.0.1:5080/api/health`
