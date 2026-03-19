"""
AI Engine — Claude/Gemini-powered assistant with live data access.

The assistant can call internal API endpoints to ground its responses
in real tenant data. It understands Amperity's identity resolution
architecture, Stitch output tables, and the intelligence layer.
"""
import os
import json
import requests
from typing import Optional, Dict, Any, List
from tenant_context import get_tenant_context

# ── Tool Definitions (internal API calls the AI can make) ─────────────────

TOOLS_CLAUDE = [
    {
        "name": "get_identity_health",
        "description": "Get identity health summary: unified profiles, source records, dedup rate, data sources connected.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "get_dedup_scorecard",
        "description": "Get per-source deduplication rates showing how many duplicate records each source contributes.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "get_source_scorecard",
        "description": "Get source field completeness — which fields (email, phone, name, address) are populated per source.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "get_stitch_stats",
        "description": "Get Stitch identity resolution statistics: cluster counts, score distributions, records per source.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "get_cluster_health",
        "description": "Get cluster quality metrics: oversized clusters, multi-surname clusters, size distribution.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "get_drift_alerts",
        "description": "Get active drift monitoring alerts — changes in identity metrics between Stitch runs.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "get_drift_history",
        "description": "Get drift monitoring history — historical snapshots of identity metrics over time.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "search_customer",
        "description": "Search for a customer by name, email, phone, or Amperity ID. Returns matching records.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Name, email, phone, or Amperity ID to search for"}
            },
            "required": ["query"]
        }
    },
    {
        "name": "explain_cluster",
        "description": "Get full explainability for an Amperity ID cluster: why records merged, confidence score, signal breakdown, pairwise scores.",
        "input_schema": {
            "type": "object",
            "properties": {
                "amperity_id": {"type": "string", "description": "The Amperity ID (UUID format) to explain"}
            },
            "required": ["amperity_id"]
        }
    },
    {
        "name": "get_name_variants",
        "description": "Get name variation analysis — customers matched across different name spellings via probabilistic matching.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "get_source_overlap",
        "description": "Get cross-source identity overlap — how many customers appear in multiple data sources.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
]

# Gemini tool format
TOOLS_GEMINI = [
    {
        "function_declarations": [
            {
                "name": t["name"],
                "description": t["description"],
                "parameters": t["input_schema"]
            }
            for t in TOOLS_CLAUDE
        ]
    }
]

SYSTEM_PROMPT = """You are an identity resolution analyst embedded in Amperity's Identity Intelligence Layer. You have direct access to live production data from the tenant you're connected to.

Your role:
- Help users understand their identity data: cluster quality, dedup rates, source coverage, drift patterns
- Explain why Stitch made specific merge decisions using explainability data
- Identify data quality issues and suggest actions
- Surface segment opportunities from the identity data
- Translate technical metrics into business impact

Key concepts you understand deeply:
- **Stitch** is Amperity's identity resolution engine. It processes source records through blocking, scoring, and clustering to produce unified customer profiles (Amperity IDs).
- **Dedup rate** = (source records - unified profiles) / source records. Higher means more duplicates resolved.
- **Clusters** are groups of source records that Stitch determined belong to the same person. Each cluster gets one Amperity ID.
- **Confidence score** (0-100) has 4 components: Signal Strength (shared PII), Match Quality (pairwise scores), Data Consistency (variation penalties), Cross-Source Corroboration.
- **Drift** means identity metrics changed between Stitch runs — could indicate data quality issues, new sources, or configuration changes.
- **Supersized clusters** (1000+ records) may indicate bad value contamination or FK chain issues.
- **Singleton clusters** (1 record) mean no matches found — could be genuinely unique or missing linkage.

When answering:
- Use the tools to pull live data before making claims. Don't guess.
- Lead with the key insight, then supporting data.
- When you see potential issues, flag them clearly with what action to take.
- Format numbers with commas for readability.
- Use markdown tables for comparative data.
- Be direct and specific — this is for data practitioners, not executives.
- When discussing expansion opportunities or next steps, reference the tenant context below for what features are enabled vs available, and which use cases are most relevant.
- IMPORTANT: The tenant context includes features the customer hasn't enabled yet. When those features are relevant to a question, mention what becomes possible — frame it as opportunity, not as a gap. Never make the customer feel like they're missing something. The core identity analysis always comes first. Opportunities are a natural "and here's what you could unlock next" — never a "you should be doing this."

{tenant_context}"""


class AIEngine:
    """AI assistant with tool-use access to live identity data."""

    def __init__(self, base_url: str = "http://127.0.0.1:5080"):
        self.base_url = base_url
        self.claude_key = os.getenv("CLAUDE_API_KEY", "")
        self.gemini_key = os.getenv("GEMINI_API_KEY", "")
        self.provider = self._detect_provider()
        self.conversation: List[Dict] = []

        # Resolve ALL tenant contexts for cross-tenant view
        tenant_names = []
        for k, v in os.environ.items():
            if k.startswith("REGION_") and k.endswith("_NAME") and v:
                tenant_names.append(v)
        all_context = "\n".join(get_tenant_context(t) for t in tenant_names if get_tenant_context(t))
        cross_tenant_intro = f"""
CROSS-TENANT INTELLIGENCE MODE
You are analyzing a portfolio of {len(tenant_names)} Amperity tenants simultaneously: {', '.join(tenant_names)}.
Your tools return data across ALL connected tenants at once. Compare and contrast across tenants.
When you see differences in dedup rates, source counts, or cluster health, explain what's driving the difference and what each tenant can learn from the others.
"""
        self.system_prompt = SYSTEM_PROMPT.replace("{tenant_context}", cross_tenant_intro + all_context)

    def _detect_provider(self) -> str:
        if self.claude_key:
            return "claude"
        elif self.gemini_key:
            return "gemini"
        return "none"

    @property
    def available(self) -> bool:
        return self.provider != "none"

    @property
    def available_providers(self) -> list:
        providers = []
        if self.claude_key:
            providers.append("claude")
        if self.gemini_key:
            providers.append("gemini")
        return providers

    def _call_tool(self, name: str, args: dict) -> str:
        """Execute a tool by calling the app's own API endpoints."""
        try:
            if name == "get_identity_health":
                r = requests.get(f"{self.base_url}/api/portfolio-pulse", timeout=90)
            elif name == "get_dedup_scorecard":
                r = requests.get(f"{self.base_url}/api/portfolio-dedup", timeout=90)
            elif name == "get_source_scorecard":
                r = requests.get(f"{self.base_url}/api/portfolio-dedup", timeout=90)
            elif name == "get_stitch_stats":
                r = requests.get(f"{self.base_url}/api/portfolio-pulse", timeout=90)
            elif name == "get_cluster_health":
                r = requests.get(f"{self.base_url}/api/portfolio-clusters", timeout=60)
            elif name == "get_drift_alerts":
                return json.dumps({"info": "Drift monitoring not available in cross-tenant view. Check individual tenant dashboards."})
            elif name == "get_drift_history":
                return json.dumps({"info": "Drift history not available in cross-tenant view. Check individual tenant dashboards."})
            elif name == "search_customer":
                return json.dumps({"info": "Customer search not available in cross-tenant view. Use individual tenant dashboards."})
            elif name == "explain_cluster":
                return json.dumps({"info": "Cluster explainability not available in cross-tenant view. Use individual tenant dashboards."})
            elif name == "get_name_variants":
                return json.dumps({"info": "Name variants not available in cross-tenant view. Use individual tenant dashboards."})
            elif name == "get_source_overlap":
                return json.dumps({"info": "Source overlap not available in cross-tenant view. Use individual tenant dashboards."})
            else:
                return json.dumps({"error": f"Unknown tool: {name}"})

            if r.status_code == 200:
                data = r.json()
                # Truncate very large responses
                text = json.dumps(data, indent=2)
                if len(text) > 15000:
                    text = text[:15000] + "\n... (truncated)"
                return text
            else:
                return json.dumps({"error": f"API returned {r.status_code}", "body": r.text[:500]})

        except Exception as e:
            return json.dumps({"error": str(e)})

    def query(self, question: str, provider: str = None) -> Dict[str, Any]:
        """Send a question and get a response with tool use."""
        use = provider or self.provider
        if use == "claude" and self.claude_key:
            return self._query_claude(question)
        elif use == "gemini" and self.gemini_key:
            return self._query_gemini(question)
        elif self.provider != "none":
            # Fall back to whatever is available
            if self.provider == "claude":
                return self._query_claude(question)
            return self._query_gemini(question)
        else:
            return {"response": "No AI provider configured. Set CLAUDE_API_KEY or GEMINI_API_KEY in .env", "provider": "none"}

    def _query_claude(self, question: str) -> Dict[str, Any]:
        """Query Claude with tool use."""
        self.conversation.append({"role": "user", "content": question})

        messages = list(self.conversation)
        headers = {
            "x-api-key": self.claude_key,
            "content-type": "application/json",
            "anthropic-version": "2023-06-01"
        }

        # Tool use loop
        for _ in range(5):  # max 5 tool calls per question
            body = {
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 4096,
                "system": self.system_prompt,
                "tools": TOOLS_CLAUDE,
                "messages": messages
            }

            r = requests.post("https://api.anthropic.com/v1/messages", headers=headers, json=body, timeout=180)

            if r.status_code != 200:
                error_msg = f"Claude API error {r.status_code}: {r.text[:500]}"
                return {"response": error_msg, "provider": "claude", "error": True}

            result = r.json()
            stop_reason = result.get("stop_reason")

            if stop_reason == "tool_use":
                # Process tool calls
                assistant_content = result["content"]
                messages.append({"role": "assistant", "content": assistant_content})

                tool_results = []
                for block in assistant_content:
                    if block.get("type") == "tool_use":
                        tool_output = self._call_tool(block["name"], block.get("input", {}))
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block["id"],
                            "content": tool_output
                        })

                messages.append({"role": "user", "content": tool_results})

            else:
                # Final text response
                text_parts = [b["text"] for b in result.get("content", []) if b.get("type") == "text"]
                response_text = "\n".join(text_parts)
                self.conversation.append({"role": "assistant", "content": response_text})
                return {"response": response_text, "provider": "claude"}

        return {"response": "Reached maximum tool call depth.", "provider": "claude", "error": True}

    def _query_gemini(self, question: str) -> Dict[str, Any]:
        """Query Gemini with function calling."""
        self.conversation.append({"role": "user", "content": question})

        contents = []
        for msg in self.conversation:
            role = "user" if msg["role"] == "user" else "model"
            contents.append({"role": role, "parts": [{"text": msg["content"]}]})

        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={self.gemini_key}"

        for _ in range(5):
            body = {
                "system_instruction": {"parts": [{"text": self.system_prompt}]},
                "contents": contents,
                "tools": TOOLS_GEMINI
            }

            r = requests.post(url, json=body, timeout=180)

            if r.status_code != 200:
                error_msg = f"Gemini API error {r.status_code}: {r.text[:500]}"
                return {"response": error_msg, "provider": "gemini", "error": True}

            result = r.json()
            candidate = result.get("candidates", [{}])[0]
            content = candidate.get("content", {})
            parts = content.get("parts", [])

            # Check for function calls
            fn_calls = [p for p in parts if "functionCall" in p]
            if fn_calls:
                contents.append({"role": "model", "parts": parts})

                fn_responses = []
                for fc in fn_calls:
                    call = fc["functionCall"]
                    tool_output = self._call_tool(call["name"], call.get("args", {}))
                    fn_responses.append({
                        "functionResponse": {
                            "name": call["name"],
                            "response": {"result": tool_output}
                        }
                    })
                contents.append({"role": "user", "parts": fn_responses})
            else:
                # Final text
                text_parts = [p.get("text", "") for p in parts if "text" in p]
                response_text = "\n".join(text_parts)
                self.conversation.append({"role": "assistant", "content": response_text})
                return {"response": response_text, "provider": "gemini"}

        return {"response": "Reached maximum tool call depth.", "provider": "gemini", "error": True}

    def clear_conversation(self):
        """Reset conversation history."""
        self.conversation = []
