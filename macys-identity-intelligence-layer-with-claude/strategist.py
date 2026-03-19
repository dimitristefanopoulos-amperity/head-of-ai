"""
In-Tenant Strategist — Context-Aware Marketing & Identity Intelligence

Knows the tenant's data, whitespace, use case maturity, and vertical.
Generates actionable recommendations with executable SQL.
Natural language → segment creation → activation pipeline.
"""
import os
import json
import requests
from typing import Dict, Any, List, Optional

from tenant_context import get_tenant_context

STRATEGIST_TOOLS = [
    {
        "name": "get_identity_health",
        "description": "Get identity health: unified profiles, source records, dedup rate, sources connected.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "get_dedup_scorecard",
        "description": "Get per-source dedup rates.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "get_source_scorecard",
        "description": "Get source field completeness (email, phone, name, address per source).",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "get_cluster_health",
        "description": "Get cluster size distribution and quality metrics.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "run_sql",
        "description": "Execute a SQL query against the tenant's C360 database. Use for audience sizing, data exploration, or segment validation. Returns up to 100 rows.",
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "SQL query to execute against Merged_Customers, Unified_Coalesced, or other C360 tables"}
            },
            "required": ["sql"]
        }
    },
    {
        "name": "create_segment",
        "description": "Create a new segment in Amperity with the given SQL. Returns segment ID and status. The segment is created as a draft — it needs activation to be used in campaigns.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Segment name (descriptive, e.g. 'Win-Back 180d No Purchase')"},
                "sql": {"type": "string", "description": "SQL query that defines the segment audience"}
            },
            "required": ["name", "sql"]
        }
    },
    {
        "name": "preview_segment",
        "description": "Run a segment SQL and return sample rows + total count. Use before creating to validate the audience.",
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "SQL query for the segment"}
            },
            "required": ["sql"]
        }
    },
]

STRATEGIST_PROMPT = """You are a strategic marketing and identity intelligence advisor embedded in an Amperity tenant. You have deep knowledge of:

1. **This tenant's live data** — you can query it, analyze it, and build segments from it
2. **Amperity's use case library** — 120+ use cases across Data Readiness, Activation, Insights, and Optimizations
3. **The Crawl-Walk-Run maturity framework** — you know which use cases are quick wins vs strategic bets
4. **This tenant's feature whitespace** — what's enabled, what's not, what they could unlock

Your role:
- Recommend specific, actionable marketing strategies grounded in the tenant's actual data
- Generate segment SQL that can be executed immediately
- Size audiences before recommending campaigns
- Estimate impact using industry benchmarks
- Suggest use cases matched to the tenant's maturity level
- When asked to "build" or "create" something, use the create_segment tool

Use Case Categories (from Amperity library):
- **Data Readiness** (29 use cases): Identity resolution, data unification, profile management
- **Activation** (57 use cases): Lifecycle marketing, retention, personalization, cross-channel
- **Insights** (11 use cases): CLV, churn prediction, RFM, journey analysis
- **Optimizations** (9 use cases): Suppression, budget optimization, lookalikes

Maturity Framework:
- **Crawl** (quick wins, 30-60 days): Customer suppression, welcome series, birthday campaigns, RFM segmentation
- **Walk** (strategic, 60-120 days): Churn prevention, CLV management, loyalty optimization, real-time personalization
- **Run** (transformative, 120+ days): Predictive priority, real-time decisioning, retail media, AI recommendations

When recommending:
- Lead with the business outcome, not the technical capability
- Include estimated audience size (use run_sql to check)
- Include estimated impact (use industry benchmarks)
- Generate ready-to-use SQL for every recommendation
- Flag what features are needed (if the tenant doesn't have them yet, frame as opportunity)
- Keep Amps cost in mind — aggregate queries are cheap, full table scans are expensive

SCHEMA RULES (CRITICAL — violations destroy credibility):
- ALWAYS use the table names and column names specified in the tenant context below
- NEVER fabricate columns that aren't listed. If you need data that doesn't exist, say "this would require [column] which isn't available in the current schema"
- ALWAYS report POST-DEDUP unified profile counts, never raw source record counts
- When you query Unified_Coalesced and get source_records counts, divide by the dedup rate to estimate unified counts — or query COUNT(DISTINCT amperity_id)
- Use relative dates (CURRENT_DATE - INTERVAL) not hardcoded dates
- If the tenant context says a table doesn't exist, don't write SQL against it

EFFICIENCY (CRITICAL — you have limited tool calls, make them count):
- Call get_identity_health ONCE for the overview.
- For audience sizing, use FAST COUNT queries — not SELECT *. Example: `SELECT COUNT(DISTINCT AMPID) FROM merged_customer WHERE Email IS NOT NULL` returns in seconds even on 364M rows.
- Combine multiple counts into ONE query when possible: `SELECT COUNT(DISTINCT AMPID) as total, COUNT(DISTINCT CASE WHEN Email IS NOT NULL THEN AMPID END) as has_email, COUNT(DISTINCT CASE WHEN Phone IS NOT NULL THEN AMPID END) as has_phone FROM merged_customer`
- Lead with your top 3 recommendations. Include ready-to-use SQL for each.
- Don't run 7 separate queries — batch your sizing into 1-2 smart queries.

CRITICAL: When the user says "build it", "create it", "do it", or similar — use the create_segment tool. Don't just show SQL. Actually create the segment.

{consumption_model}

{tenant_context}"""

AMPS_MODEL_PROMPT = """
CONSUMPTION MODEL: AMPS-BASED
This tenant pays per Amp consumed. Value creation for the customer = value creation for the platform.

HOW AMPS WORK (use this knowledge to make informed recommendations):
- Amps normalize six underlying compute engines (Spark, Presto, Databricks, Snowflake, K8s, storage) into one billing currency
- Five categories: Sources (ingest), Stitch (identity resolution), Databases (C360 tables), Analytics (queries/predictions), Activation (campaigns/journeys/orchestrations/bridge)
- Storage metered separately (~1-5% of total, $40/TB typical)
- Customers buy committed annual capacity — no daily limits, flexible consumption
- AI assistant queries are Analytics category — cheap (~100-500 amps per query session)
- Segment creation is Analytics — cheap
- Campaign/journey execution is Activation — moderate
- Stitch is the biggest single consumer — but runs once per workflow cycle (daily)

COST CONTEXT FOR RECOMMENDATIONS:
- Creating a segment: ~100 amps (negligible)
- Running a campaign to 1M customers: ~1,000-5,000 amps depending on complexity
- Predictive model (CLV/churn): ~5,000-20,000 amps per run
- Daily workflow (ingest → stitch → database → orchestrate): ~10,000-100,000 amps depending on tenant size
- Bridge (zero-copy sharing): ZERO amps — always recommend over BI Connect
- BI Connect (legacy Snowflake): significant amps — recommend migrating to Bridge

YOUR STRATEGIC MANDATE:
- Proactively surface high-value use cases that create genuine business outcomes
- Every recommendation naturally involves feature usage — segments, campaigns, journeys, predictions
- Frame around Return on Customer Data (RoCD): "This audience is worth $X in revenue. Activating costs Y amps."
- The customer should FEEL the value: before/after metrics, revenue lift, retention improvement
- Show the math: audience size × estimated impact × conversion rate = business value
- Compare cost of inaction ("leaving $X on the table") vs cost of action ("costs Y amps/month")
- If Bridge is available and they're using BI Connect, ALWAYS recommend the switch (saves 10-20% of total amps)
- Never recommend feature usage that doesn't map to a real business outcome
- Never make amps feel like a black box — always estimate and explain the cost
"""

RECORDS_MODEL_PROMPT = """
CONSUMPTION MODEL: RECORDS-BASED
This tenant pays per record. They want maximum identity quality and business value from their investment.

HOW COMPUTE WORKS FOR RECORDS CUSTOMERS:
- Records pricing means the contract cost is fixed based on record volume
- But compute (amps) still matters operationally — it affects workflow speed, sandbox availability, and platform responsiveness
- The customer benefits from efficiency: faster workflows, cleaner data, better match quality
- Storage matters: every sandbox, every table copy, every historical snapshot consumes storage

THE SIX EFFICIENCY LEVERS (know these deeply):
1. **Spark Cluster Right-Sizing**: Cluster size × runtime = compute. Check utilization rate — if below 70%, the cluster is oversized. Right-sizing from XXL to L can cut compute 50%+ with minimal runtime impact.
2. **Workflow Frequency**: Stitch runs once per daily workflow cycle. Some tenants use incremental matching (6+1 model) for near-real-time — this is a FEATURE that reduces full stitch runs by ~60%. Never recommend reducing frequency.
3. **Data Volume Before Ingestion**: Bad PII values have outsized compute impact. A generic email shared across thousands of records snowballs stitch cost. Pre-filtering non-person records (like Honda's inventory) cascades savings through every downstream step.
4. **Query Efficiency**: Queries bill on bytes scanned. Fewer columns, partition filters, aggregated tables = faster AND cheaper.
5. **Sandbox Discipline**: Automated sandbox workflows burn compute independently. Stale sandboxes waste storage. Recommend: don't automate sandboxes, use smaller spark sizes, delete when done.
6. **Bridge vs BI Connect**: If using legacy BI Connect (Snowflake orchestrations), Bridge is zero-compute for the same data sharing. This alone can save 10-20% of total compute.

YOUR STRATEGIC MANDATE:
- Help the customer get maximum value from their identity investment
- Efficiency recommendations = faster workflows, better match quality, cleaner operations
- Sandbox cleanup = "free up resources for the work that matters"
- Source optimization = "improve match quality AND speed up processing"
- Right-sizing = "same results, faster delivery"
- Bad values cleanup = "better identity precision, fewer false merges, faster stitch"
- Help them demonstrate value internally: before/after metrics, quality improvements, speed gains

IMPORTANT — STITCH ACCURACY:
- Stitch runs ONCE per workflow. NOT redundant.
- Incremental matching (6+1, Nordstrom/Alaska Air patterns) is intentional near-real-time identity — never recommend removing it
- The real levers are data quality BEFORE stitch, not stitch frequency

ALWAYS CLIENT-FORWARD:
- Frame everything as helping THEM — faster, cleaner, more precise, better ROI
- Never frame as platform cost management
- The customer should feel like they're getting more value, not being optimized
"""


class TenantStrategist:
    """Context-aware strategist with natural language execution."""

    def __init__(self, base_url: str, tenant_name: str, consumption_model: str = "amps"):
        self.base_url = base_url
        self.tenant_name = tenant_name
        self.consumption_model = consumption_model
        self.claude_key = os.getenv("CLAUDE_API_KEY", "")
        self.gemini_key = os.getenv("GEMINI_API_KEY", "")
        self.conversation: List[Dict] = []

        # Select consumption model prompt
        model_prompt = AMPS_MODEL_PROMPT if consumption_model == "amps" else RECORDS_MODEL_PROMPT

        # Build system prompt with tenant context + consumption model
        self.system_prompt = STRATEGIST_PROMPT.replace(
            "{consumption_model}", model_prompt
        ).replace(
            "{tenant_context}",
            get_tenant_context(tenant_name)
        )

    @property
    def available(self) -> bool:
        return bool(self.claude_key or self.gemini_key)

    @property
    def provider(self) -> str:
        if self.claude_key:
            return "claude"
        elif self.gemini_key:
            return "gemini"
        return "none"

    def _call_tool(self, name: str, args: dict) -> str:
        """Execute a strategist tool."""
        try:
            if name == "get_identity_health":
                r = requests.get(f"{self.base_url}/api/cross-region-pulse", timeout=45)
            elif name == "get_dedup_scorecard":
                r = requests.get(f"{self.base_url}/api/dedup-scorecard", timeout=45)
            elif name == "get_source_scorecard":
                r = requests.get(f"{self.base_url}/api/source-scorecard", timeout=45)
            elif name == "get_cluster_health":
                r = requests.get(f"{self.base_url}/api/cluster-health/distribution", timeout=45)
            elif name == "run_sql":
                r = requests.post(f"{self.base_url}/api/segment-preview",
                    json={"sql": args.get("sql", "")}, timeout=60)
            elif name == "preview_segment":
                r = requests.post(f"{self.base_url}/api/segment-preview",
                    json={"sql": args.get("sql", "")}, timeout=60)
            elif name == "create_segment":
                r = requests.post(f"{self.base_url}/api/save-segment",
                    json={"name": args.get("name", ""), "sql": args.get("sql", "")}, timeout=30)
            else:
                return json.dumps({"error": f"Unknown tool: {name}"})

            data = r.json()
            text = json.dumps(data, indent=2)
            if len(text) > 12000:
                text = text[:12000] + "\n... (truncated)"
            return text

        except Exception as e:
            return json.dumps({"error": str(e)})

    def query(self, question: str, provider: str = None) -> Dict[str, Any]:
        """Send a question to the strategist."""
        use = provider or self.provider
        if use == "claude" and self.claude_key:
            return self._query_claude(question)
        elif use == "gemini" and self.gemini_key:
            return self._query_gemini(question)
        return {"response": "No AI provider configured.", "provider": "none"}

    def _query_claude(self, question: str) -> Dict[str, Any]:
        self.conversation.append({"role": "user", "content": question})
        messages = list(self.conversation)

        headers = {
            "x-api-key": self.claude_key,
            "content-type": "application/json",
            "anthropic-version": "2023-06-01"
        }

        for _ in range(8):  # more tool calls for strategist (SQL + preview + create)
            body = {
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 4096,
                "system": self.system_prompt,
                "tools": STRATEGIST_TOOLS,
                "messages": messages
            }

            r = requests.post("https://api.anthropic.com/v1/messages", headers=headers, json=body, timeout=180)

            if r.status_code != 200:
                return {"response": f"API error {r.status_code}: {r.text[:300]}", "provider": "claude", "error": True}

            result = r.json()

            if result.get("stop_reason") == "tool_use":
                assistant_content = result["content"]
                messages.append({"role": "assistant", "content": assistant_content})

                tool_results = []
                for block in assistant_content:
                    if block.get("type") == "tool_use":
                        output = self._call_tool(block["name"], block.get("input", {}))
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block["id"],
                            "content": output
                        })
                messages.append({"role": "user", "content": tool_results})
            else:
                text_parts = [b["text"] for b in result.get("content", []) if b.get("type") == "text"]
                response = "\n".join(text_parts)
                self.conversation.append({"role": "assistant", "content": response})
                return {"response": response, "provider": "claude"}

        return {"response": "Max tool depth reached.", "provider": "claude", "error": True}

    def _query_gemini(self, question: str) -> Dict[str, Any]:
        self.conversation.append({"role": "user", "content": question})

        contents = [{"role": "user" if m["role"] == "user" else "model", "parts": [{"text": m["content"]}]} for m in self.conversation]
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={self.gemini_key}"

        gemini_tools = [{"function_declarations": [{"name": t["name"], "description": t["description"], "parameters": t["input_schema"]} for t in STRATEGIST_TOOLS]}]

        for _ in range(8):
            body = {"system_instruction": {"parts": [{"text": self.system_prompt}]}, "contents": contents, "tools": gemini_tools}
            r = requests.post(url, json=body, timeout=180)

            if r.status_code != 200:
                return {"response": f"Gemini error {r.status_code}: {r.text[:300]}", "provider": "gemini", "error": True}

            result = r.json()
            candidate = result.get("candidates", [{}])[0]
            parts = candidate.get("content", {}).get("parts", [])

            fn_calls = [p for p in parts if "functionCall" in p]
            if fn_calls:
                contents.append({"role": "model", "parts": parts})
                fn_responses = []
                for fc in fn_calls:
                    call = fc["functionCall"]
                    output = self._call_tool(call["name"], call.get("args", {}))
                    fn_responses.append({"functionResponse": {"name": call["name"], "response": {"result": output}}})
                contents.append({"role": "user", "parts": fn_responses})
            else:
                text_parts = [p.get("text", "") for p in parts if "text" in p]
                response = "\n".join(text_parts)
                self.conversation.append({"role": "assistant", "content": response})
                return {"response": response, "provider": "gemini"}

        return {"response": "Max tool depth reached.", "provider": "gemini", "error": True}

    def clear(self):
        self.conversation = []
