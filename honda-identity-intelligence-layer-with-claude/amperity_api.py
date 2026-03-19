"""
Amperity Transit+JSON Query Layer — Macy's Identity Engine
Handles OAuth2 auth, 3-step interactive query flow, and all tool queries.
"""
import os, json, time, re, requests
from dotenv import dotenv_values

# ── Region Configuration ──────────────────────────────────────────────────────

class RegionConfig:
    """Holds credentials and IDs for one Amperity tenant/region."""
    def __init__(self, name, tenant, token_url, client_id, client_secret,
                 database_id, segment_id, dataset_id, tenant_id=None):
        self.name = name
        self.tenant = tenant
        self.tenant_id = tenant_id or tenant  # X-Amperity-Tenant header value
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
    """Load all regions from .env. Supports REGION_*_ prefixes."""
    env = dotenv_values(".env")
    regions = {}
    # Discover region prefixes
    prefixes = set()
    for k in env:
        if k.startswith("REGION_"):
            parts = k.split("_", 2)  # REGION_US_TENANT -> ['REGION','US','TENANT']
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
            tenant_id=env.get(f"{p}TENANT_ID", ""),
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
            "X-Amperity-Tenant": self.region.tenant_id,
        })
        resp.raise_for_status()
        body = resp.json()
        self.region.token = body["access_token"]
        self.region.token_expiry = time.time() + body.get("expires_in", 3600)

    def _auth_headers(self):
        """Base auth headers for all requests."""
        self._ensure_token()
        return {
            "Authorization": f"Bearer {self.region.token}",
            "X-Amperity-Tenant": self.region.tenant_id,
            "api-version": "2024-04-01",
        }

    def _transit_headers(self):
        """Headers for Transit+JSON POST requests (step 1)."""
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
            "~:amperity.query.exec/description", "Macys Identity Engine Query",
            "~:amperity.query.cache/skip?", False,
            "~:amperity.query.exec/options", ["~#set", []],
            "~:database/id", r.database_id,
            "~:amperity.query.engine/id", "athena"
        ])

        url = f"{r.base_url}/api/v0/segments/{r.segment_id}/run-interactive-query"
        resp = self.session.post(url, data=transit_body, headers=self._transit_headers())
        if resp.status_code >= 400:
            print(f"[QUERY ERROR] HTTP {resp.status_code} from {url}")
            print(f"[QUERY ERROR] Response: {resp.text[:500]}")
            print(f"[QUERY ERROR] SQL: {sql[:200]}")
        resp.raise_for_status()

        # Extract qex_id
        data = json.loads(resp.text)
        qex_id = _transit_get(data, "~:amperity.query.exec/id")
        if not qex_id:
            m = re.search(r'"(qex-[A-Za-z0-9_-]+)"', resp.text)
            qex_id = m.group(1) if m else None
        if not qex_id:
            raise RuntimeError(f"No qex_id in response: {resp.text[:300]}")

        # Step 2: Poll until done (Transit response)
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

        # Step 3: Fetch results — this endpoint returns JSON, not Transit
        res_url = f"{r.base_url}/api/v0/segment-query-results/{qex_id}?limit={limit}"
        res_headers = self._auth_headers()
        res_headers["Accept"] = "application/json"
        res = self.session.get(res_url, headers=res_headers)
        res.raise_for_status()
        rdata = json.loads(res.text)

        return self._parse_results(rdata)

    def _parse_results(self, rdata):
        """Parse query results from JSON or Transit response into list of dicts."""
        # Standard JSON dict response: {"rows": [...], "columns": [...]}
        if isinstance(rdata, dict):
            rows = rdata.get("rows") or rdata.get("data") or rdata.get("results") or []
            columns = rdata.get("columns") or rdata.get("headers") or []
            if rows and isinstance(rows[0], list) and columns:
                col_names = [c["name"] if isinstance(c, dict) else c for c in columns]
                return [dict(zip(col_names, row)) for row in rows]
            if rows and isinstance(rows[0], dict):
                return rows
            return rows

        # Transit response: ["^ ", key, val, ...]
        if isinstance(rdata, list) and rdata and rdata[0] == "^ ":
            columns_raw = _transit_get(rdata, "~:columns") or []
            rows_raw = _transit_get(rdata, "~:data") or _transit_get(rdata, "~:rows") or []
            col_names = []
            for c in columns_raw:
                if isinstance(c, list) and c and c[0] == "^ ":
                    col_names.append(_transit_get(c, "~:name") or str(c))
                elif isinstance(c, dict):
                    col_names.append(c.get("name", str(c)))
                elif isinstance(c, str):
                    col_names.append(c)
                else:
                    col_names.append(str(c))
            results = []
            for row in rows_raw:
                if isinstance(row, dict):
                    results.append(row)
                elif isinstance(row, list):
                    results.append(dict(zip(col_names, row)))
            return results

        return []

    def has_token(self):
        try:
            self._ensure_token()
            return True
        except Exception:
            return False

    # ── Search ────────────────────────────────────────────────────────────

    def search_customer(self, q):
        """Smart search: auto-detect AMPID, email, phone, or name.
        Macy's Merged_Customers uses: AMPID, Fname, Lname, Email, Phone, City, State.
        Email search joins through Unified_Coalesced since Merged_Customers only keeps one email."""
        q = q.strip()
        q_upper = q.upper().replace("'", "''")
        if re.match(r'^[0-9a-f]{8}-', q, re.I):
            # AMPID
            return self.run_query(f"""
                SELECT DISTINCT amperity_id, given_name, surname,
                       email, phone, city, state
                FROM Merged_Customers
                WHERE amperity_id = '{q}'
                LIMIT 20
            """)
        elif '@' in q:
            # Email search — must check Unified_Coalesced because Merged_Customers
            # only keeps one "winning" email per AMPID. UC has ALL emails.
            return self.run_query(f"""
                SELECT DISTINCT mc.amperity_id AS amperity_id, mc.given_name AS given_name,
                       mc.surname AS surname, mc.email AS email, mc.phone AS phone,
                       mc.city AS city, mc.state AS state
                FROM Unified_Coalesced uc
                JOIN Merged_Customers mc ON uc.amperity_id = mc.amperity_id
                WHERE UPPER(uc.email) LIKE '%{q_upper}%'
                LIMIT 20
            """)
        elif q.replace('-','').replace('+','').replace(' ','').isdigit():
            clean = q.replace('-','').replace('+','').replace(' ','').replace('(','').replace(')','')
            return self.run_query(f"""
                SELECT DISTINCT amperity_id, given_name, surname,
                       email, phone, city, state
                FROM Merged_Customers
                WHERE COALESCE(Phone, '') LIKE '%{clean}%'
                LIMIT 20
            """)
        else:
            parts = q.split()
            if len(parts) >= 2:
                fn, ln = parts[0].upper(), parts[-1].upper()
                return self.run_query(f"""
                    SELECT DISTINCT amperity_id, given_name, surname,
                           email, phone, city, state
                    FROM Merged_Customers
                    WHERE UPPER(given_name) LIKE '%{fn}%' AND UPPER(surname) LIKE '%{ln}%'
                    LIMIT 20
                """)
            else:
                return self.run_query(f"""
                    SELECT DISTINCT amperity_id, given_name, surname,
                           email, phone, city, state
                    FROM Merged_Customers
                    WHERE UPPER(given_name) LIKE '%{q_upper}%'
                       OR UPPER(surname) LIKE '%{q_upper}%'
                       OR COALESCE(Phone, '') LIKE '%{q_upper}%'
                       OR amperity_id = '{q}'
                    LIMIT 20
                """)

    def get_full_cluster(self, ampid):
        """Get all UC records + pairwise scores for a cluster."""
        records = self.run_query(f"""
            SELECT amperity_id, pk, datasource, given_name, surname, email,
                   phone, address, city, state, postal, gender, birthdate
            FROM Unified_Coalesced
            WHERE amperity_id = '{ampid}'
            ORDER BY datasource
        """)
        scores = self.run_query(f"""
            SELECT pk1, pk2, source1, source2, score, match_category, match_type
            FROM Unified_Scores
            WHERE amperity_id = '{ampid}'
            ORDER BY score DESC
        """)
        return records, scores

    # ── Cluster Health ────────────────────────────────────────────────────

    def get_oversized_clusters(self, min_records=20, limit=50):
        return self.run_query(f"""
            SELECT amperity_id, COUNT(*) as record_count,
                   COUNT(DISTINCT datasource) as source_count,
                   COUNT(DISTINCT surname) as surname_count
            FROM Unified_Coalesced
            GROUP BY amperity_id
            HAVING COUNT(*) >= {min_records}
            ORDER BY COUNT(*) DESC
            LIMIT {limit}
        """)

    def get_multi_surname_clusters(self, min_surnames=3, limit=50):
        return self.run_query(f"""
            SELECT amperity_id, COUNT(*) as record_count,
                   COUNT(DISTINCT surname) as surname_count,
                   COUNT(DISTINCT datasource) as source_count
            FROM Unified_Coalesced
            WHERE surname IS NOT NULL AND surname != ''
            GROUP BY amperity_id
            HAVING COUNT(DISTINCT surname) >= {min_surnames}
            ORDER BY COUNT(DISTINCT surname) DESC
            LIMIT {limit}
        """)

    def get_single_source_heavy_clusters(self, min_records=30, limit=50):
        return self.run_query(f"""
            SELECT amperity_id, datasource, COUNT(*) as record_count
            FROM Unified_Coalesced
            GROUP BY amperity_id, datasource
            HAVING COUNT(*) >= {min_records}
            ORDER BY COUNT(*) DESC
            LIMIT {limit}
        """)

    def get_cluster_size_distribution(self):
        return self.run_query("""
            SELECT cluster_size, COUNT(*) as cluster_count FROM (
                SELECT amperity_id, COUNT(*) as cluster_size
                FROM Unified_Coalesced
                GROUP BY amperity_id
            ) t
            GROUP BY cluster_size
            ORDER BY cluster_size
            LIMIT 100
        """)

    # ── Source Scorecard ──────────────────────────────────────────────────

    def get_source_scorecard(self):
        return self.run_query("""
            SELECT datasource,
                   COUNT(*) as total_records,
                   COUNT(email) as has_email,
                   COUNT(phone) as has_phone,
                   COUNT(address) as has_address,
                   COUNT(given_name) as has_given_name,
                   COUNT(surname) as has_surname,
                   COUNT(postal) as has_postal
            FROM Unified_Coalesced
            GROUP BY datasource
            ORDER BY COUNT(*) DESC
        """)

    def get_source_overlap(self):
        return self.run_query("""
            WITH multi AS (
                SELECT amperity_id, datasource
                FROM Unified_Coalesced
                WHERE amperity_id IN (
                    SELECT amperity_id FROM Unified_Coalesced
                    GROUP BY amperity_id HAVING COUNT(DISTINCT datasource) >= 2
                    LIMIT 500000
                )
            )
            SELECT a.datasource AS source_a, b.datasource AS source_b,
                   COUNT(DISTINCT a.amperity_id) AS shared_ids
            FROM multi a
            JOIN multi b
              ON a.amperity_id = b.amperity_id AND a.datasource < b.datasource
            GROUP BY a.datasource, b.datasource
            ORDER BY shared_ids DESC
            LIMIT 50
        """, timeout=90)

    # ── Stitch Stats ──────────────────────────────────────────────────────

    def get_current_stitch_stats(self):
        """Get total clusters, total records, avg cluster size, max cluster size.
        NOTE: The subquery groups by amperity_id, so COUNT(*) on it = unique IDs.
        Must use SUM(cluster_size) to get true total records."""
        return self.run_query("""
            SELECT COUNT(*) AS total_clusters,
                   SUM(cluster_size) AS total_records,
                   ROUND(CAST(SUM(cluster_size) AS DOUBLE) / NULLIF(COUNT(*), 0), 2) AS avg_cluster_size,
                   MAX(cluster_size) AS max_cluster_size
            FROM (
                SELECT amperity_id, COUNT(*) AS cluster_size
                FROM Unified_Coalesced
                GROUP BY amperity_id
            ) t
        """)

    def get_stitch_score_distribution(self):
        return self.run_query("""
            SELECT match_category, COUNT(*) AS pair_count
            FROM Unified_Scores
            GROUP BY match_category
            ORDER BY pair_count DESC
        """)

    def get_records_per_source(self):
        return self.run_query("""
            SELECT datasource, COUNT(*) AS record_count, COUNT(DISTINCT amperity_id) AS unique_ids
            FROM Unified_Coalesced
            GROUP BY datasource
            ORDER BY record_count DESC
        """)

    # ── Segment Preview ───────────────────────────────────────────────────

    def preview_segment(self, sql, limit=100):
        # Strip existing LIMIT clause — Athena rejects LIMIT inside subqueries
        # and double-LIMIT is a parse error
        clean = re.sub(r'\bLIMIT\s+\d+\s*$', '', sql, flags=re.IGNORECASE).strip()
        count_sql = f"SELECT COUNT(*) AS cnt FROM ({clean}) t"
        count_result = self.run_query(count_sql)
        total = int(count_result[0]["cnt"]) if count_result else 0
        sample = self.run_query(f"{clean}\nLIMIT {limit}")
        return {"total": total, "sample": sample}

    def preview_segment_demographics(self, sql):
        """Get geographic and source distribution for a segment.
        Joins segment amperity_ids back to Unified_Coalesced for demographics,
        so it works even when the segment SQL doesn't include state/datasource columns."""
        # Strip ORDER BY and LIMIT for subquery compatibility (multiline-safe)
        clean_sql = re.sub(r'\bORDER\s+BY\b.*$', '', sql, flags=re.IGNORECASE | re.DOTALL).strip()
        clean_sql = re.sub(r'\bLIMIT\s+\d+\s*$', '', clean_sql, flags=re.IGNORECASE | re.DOTALL).strip()
        if clean_sql.endswith(','):
            clean_sql = clean_sql[:-1]

        # Wrap to extract just DISTINCT amperity_ids with a safety LIMIT to prevent timeout
        # DISTINCT is critical: segment SQL without GROUP BY returns one row per record,
        # which can be millions. We only need unique IDs for the IN clause.
        id_subquery = f"SELECT DISTINCT t.amperity_id FROM ({clean_sql}) t LIMIT 50000"

        print(f"[DEMOGRAPHICS] clean_sql preview: {clean_sql[:150]}...")

        try:
            state_dist = self.run_query(f"""
                SELECT uc.state, COUNT(DISTINCT uc.amperity_id) AS cnt
                FROM Unified_Coalesced uc
                WHERE uc.amperity_id IN ({id_subquery})
                  AND uc.state IS NOT NULL
                GROUP BY uc.state ORDER BY cnt DESC LIMIT 20
            """, timeout=90)
        except Exception as e:
            print(f"[DEMOGRAPHICS] State query failed: {e}")
            state_dist = []

        try:
            source_dist = self.run_query(f"""
                SELECT uc.datasource, COUNT(DISTINCT uc.amperity_id) AS cnt
                FROM Unified_Coalesced uc
                WHERE uc.amperity_id IN ({id_subquery})
                GROUP BY uc.datasource ORDER BY cnt DESC LIMIT 20
            """, timeout=90)
        except Exception as e:
            print(f"[DEMOGRAPHICS] Source query failed: {e}")
            source_dist = []

        return {"state_distribution": state_dist, "source_distribution": source_dist}

    # ── Segment Management ──────────────────────────────────────────────

    def create_segment(self, name, sql):
        """Create a SQL segment in Amperity and activate it. Returns segment metadata.
        Tries multiple API URL patterns since Amperity versions differ."""
        self._ensure_token()
        r = self.region
        headers = self._auth_headers()
        headers["Content-Type"] = "application/json"

        body = {
            "name": name,
            "database_id": r.database_id,
            "sql": sql,
        }

        # Try API URL patterns in order of likelihood
        url_patterns = [
            f"{r.base_url}/api/v0/segments",       # v0 prefix (matches other endpoints)
            f"{r.base_url}/api/segments",           # no version prefix
            f"{r.base_url}/api/v1/segments",        # v1 prefix
        ]

        resp = None
        last_error = None
        for url in url_patterns:
            try:
                resp = self.session.post(url, json=body, headers=headers)
                if resp.status_code != 405:  # 405 = wrong URL, try next
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
            raise RuntimeError(
                f"Segment creation failed on all API patterns. Last: {last_error}. "
                f"Use 'Copy SQL' to create the segment manually in Amperity."
            )

        seg = resp.json()
        seg_id = seg.get("id", "")

        # Attempt activation (may fail if tenant doesn't allow API activation)
        activated = False
        if seg_id:
            working_base = resp.url.rsplit("/segments", 1)[0]
            activate_url = f"{working_base}/segments/{seg_id}/activate"
            try:
                act_resp = self.session.put(activate_url, headers=headers)
                act_resp.raise_for_status()
                activated = True
            except Exception:
                activated = False  # Created as draft — user can activate in UI

        return {
            "id": seg_id,
            "name": name,
            # Link to segment editor: /draft for drafts, direct for activated
            "url": f"{r.base_url}/#/segments/{seg_id}" if (seg_id and activated) else None,
            "draft_url": f"{r.base_url}/#/segments/{seg_id}/draft" if (seg_id and not activated) else None,
            "list_url": f"{r.base_url}/#/segments",
            "tenant": r.tenant,
            "region": r.name,
            "activated": activated,
        }

    # ── Cluster Diff ──────────────────────────────────────────────────────

    # ── Source Dedup Scorecard (MEO killer feature) ─────────────────────

    def get_source_dedup_rates(self):
        """Per-source dedup rates — the exact data that made Anoop say 'alarm bells'."""
        return self.run_query("""
            SELECT datasource,
                   COUNT(*) AS source_records,
                   COUNT(DISTINCT amperity_id) AS unique_ids,
                   ROUND(100.0 * (1.0 - CAST(COUNT(DISTINCT amperity_id) AS DOUBLE) / NULLIF(COUNT(*), 0)), 1) AS dedup_rate_pct,
                   COUNT(*) - COUNT(DISTINCT amperity_id) AS duplicates_removed
            FROM Unified_Coalesced
            GROUP BY datasource
            ORDER BY dedup_rate_pct DESC
        """)

    # ── Household Bloat Scanner (Retail: family member merge-ins) ────

    def get_household_bloat(self, min_names=3, limit=50):
        """Find clusters where multiple household members merged into one identity.
        Retail signal: many distinct given_names sharing an address, multiple loyalty IDs,
        multiple emails — likely a family (spouse, parent/child) incorrectly collapsed."""
        return self.run_query(f"""
            SELECT amperity_id,
                   COUNT(*) AS record_count,
                   COUNT(DISTINCT datasource) AS source_count,
                   COUNT(DISTINCT COALESCE(given_name, '')) AS given_name_variants,
                   COUNT(DISTINCT COALESCE(surname, '')) AS surname_variants,
                   COUNT(DISTINCT COALESCE(email, '')) AS email_variants,
                   COUNT(DISTINCT COALESCE(phone, '')) AS phone_variants,
                   COUNT(DISTINCT COALESCE(loyalty_id, '')) AS loyalty_id_count
            FROM Unified_Coalesced
            GROUP BY amperity_id
            HAVING COUNT(DISTINCT COALESCE(given_name, '')) >= {min_names}
               AND COUNT(DISTINCT COALESCE(surname, '')) <= 2
               AND COUNT(*) >= 4
            ORDER BY COUNT(DISTINCT COALESCE(given_name, '')) DESC
            LIMIT {limit}
        """)

    def get_household_bloat_count(self, min_names=3):
        """Count total household-bloated clusters."""
        rows = self.run_query(f"""
            SELECT COUNT(*) AS total FROM (
                SELECT amperity_id
                FROM Unified_Coalesced
                GROUP BY amperity_id
                HAVING COUNT(DISTINCT COALESCE(given_name, '')) >= {min_names}
                   AND COUNT(DISTINCT COALESCE(surname, '')) <= 2
                   AND COUNT(*) >= 4
            )
        """)
        return rows[0]["total"] if rows else 0

    # ── Name Variant / Transliteration Detector ──────────────────────

    def get_name_variant_clusters(self, min_name_variants=3, limit=50):
        """Find clusters with high name variation — catches Arabic/English (MEO)
        and Spanish/English (Mexico) transliteration gaps.
        Filters out fleet/dealer accounts (high record counts, many emails/addresses)."""
        return self.run_query(f"""
            SELECT amperity_id,
                   COUNT(*) AS record_count,
                   COUNT(DISTINCT datasource) AS source_count,
                   COUNT(DISTINCT COALESCE(given_name, '')) AS given_name_variants,
                   COUNT(DISTINCT COALESCE(surname, '')) AS surname_variants,
                   COUNT(DISTINCT COALESCE(given_name, '') || ' ' || COALESCE(surname, '')) AS full_name_variants,
                   COUNT(DISTINCT COALESCE(email, '')) AS email_count
            FROM Unified_Coalesced
            GROUP BY amperity_id
            HAVING COUNT(DISTINCT COALESCE(given_name, '') || ' ' || COALESCE(surname, '')) >= {min_name_variants}
               AND COUNT(*) <= 150
               AND COUNT(DISTINCT COALESCE(email, '')) <= 15
            ORDER BY full_name_variants DESC
            LIMIT {limit}
        """)

    def get_name_variant_count(self, min_name_variants=3):
        """Count total customers with significant name variation (no LIMIT).
        Applies same fleet/dealer filters as get_name_variant_clusters."""
        rows = self.run_query(f"""
            SELECT COUNT(*) AS total FROM (
                SELECT amperity_id
                FROM Unified_Coalesced
                GROUP BY amperity_id
                HAVING COUNT(DISTINCT COALESCE(given_name, '') || ' ' || COALESCE(surname, '')) >= {min_name_variants}
                   AND COUNT(*) <= 150
                   AND COUNT(DISTINCT COALESCE(email, '')) <= 15
            )
        """)
        return rows[0]["total"] if rows else 0

    # ── Golden Crosswalk Explorer (Mexico: proof of the deliverable) ──

    def get_crosswalk(self, ampid):
        """Get all source PKs mapped to a unified AmpID — the golden crosswalk view."""
        return self.run_query(f"""
            SELECT amperity_id, pk, datasource,
                   given_name, surname, email, phone, address, city, state, postal
            FROM Unified_Coalesced
            WHERE amperity_id = '{ampid}'
            ORDER BY datasource, pk
        """)

    def get_crosswalk_sample(self, limit=20):
        """Get a sample of multi-source crosswalk entries — shows the crosswalk working."""
        return self.run_query(f"""
            SELECT amperity_id, COUNT(*) AS record_count,
                   COUNT(DISTINCT datasource) AS source_count,
                   COUNT(DISTINCT pk) AS pk_count,
                   ARBITRARY(given_name) AS sample_name,
                   ARBITRARY(surname) AS sample_surname
            FROM Unified_Coalesced
            GROUP BY amperity_id
            HAVING COUNT(DISTINCT datasource) >= 2
            ORDER BY COUNT(DISTINCT datasource) DESC, COUNT(*) DESC
            LIMIT {limit}
        """)

    # ── Cross-Region summary stats (for pulse view) ──────────────────

    def get_region_summary(self):
        """Quick summary stats for this region."""
        stats = self.run_query("""
            SELECT COUNT(DISTINCT amperity_id) AS total_profiles,
                   COUNT(*) AS total_records,
                   COUNT(DISTINCT datasource) AS source_count,
                   ROUND(CAST(COUNT(*) AS DOUBLE) / NULLIF(COUNT(DISTINCT amperity_id), 0), 2) AS avg_records_per_id,
                   ROUND(100.0 * (1.0 - CAST(COUNT(DISTINCT amperity_id) AS DOUBLE) / NULLIF(COUNT(*), 0)), 1) AS overall_dedup_rate
            FROM Unified_Coalesced
        """)
        return stats[0] if stats else {}

    # ── Cluster Diff ──────────────────────────────────────────────────

    def diff_clusters(self, ampid_a, ampid_b):
        rec_a, sc_a = self.get_full_cluster(ampid_a)
        rec_b, sc_b = self.get_full_cluster(ampid_b)
        return {
            "cluster_a": {"ampid": ampid_a, "records": rec_a, "scores": sc_a},
            "cluster_b": {"ampid": ampid_b, "records": rec_b, "scores": sc_b},
        }

    # ── Activation Pipeline (Orchestrations & Workflows) ──────────────

    def get_orchestrations(self, limit=200):
        """Fetch orchestrations via Amperity REST API."""
        r = self.region
        self._ensure_token()
        headers = self._auth_headers()
        headers["Accept"] = "application/json"
        # Try multiple API path patterns
        for path in [f"/api/v0/orchestrations", f"/api/orchestrations"]:
            url = f"{r.base_url}{path}"
            resp = self.session.get(url, headers=headers, params={"limit": limit})
            if resp.status_code != 404:
                resp.raise_for_status()
                return resp.json()
        return {"orchestrations": []}

    def get_workflows(self, limit=20, states=None):
        """Fetch recent workflows via Amperity REST API."""
        r = self.region
        self._ensure_token()
        headers = self._auth_headers()
        headers["Accept"] = "application/json"
        params = {"limit": limit}
        if states:
            params["state"] = ",".join(states) if isinstance(states, list) else states
        for path in [f"/api/v0/workflows", f"/api/workflows"]:
            url = f"{r.base_url}{path}"
            resp = self.session.get(url, headers=headers, params=params)
            if resp.status_code != 404:
                resp.raise_for_status()
                return resp.json()
        return {"workflows": []}
