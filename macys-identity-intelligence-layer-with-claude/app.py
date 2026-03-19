"""
Macys Identity Intelligence — Single-Region Flask Server
Single-tenant identity resolution explainability tool for Macy's.
"""
import os, sys, socket, json, traceback
from dotenv import load_dotenv
load_dotenv()
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS

from amperity_api import load_regions, AmperityAPI
from explainability import explain_cluster
import drift_store
from cortex_agent import CortexAgent
from ai_engine import AIEngine
from in_tenant import InTenantAgent
from pipeline import ClientPolicy
from strategist import TenantStrategist

app = Flask(__name__, static_folder="static")
CORS(app)

# ── Global Error Handlers (always return JSON, never HTML) ───────────────────
@app.errorhandler(Exception)
def handle_exception(e):
    code = getattr(e, 'code', 500)
    return jsonify({"error": str(e)}), code if isinstance(code, int) else 500

@app.errorhandler(500)
def handle_500(e):
    return jsonify({"error": str(e)}), 500

@app.errorhandler(404)
def handle_404(e):
    return jsonify({"error": "Not found"}), 404

# ── Multi-Region Setup ────────────────────────────────────────────────────────

REGIONS = {}      # {prefix: RegionConfig}
API_CLIENTS = {}  # {prefix: AmperityAPI}
ACTIVE_REGION = None  # current region prefix
CORTEX_AGENT = None  # CortexAgent instance
AI_ENGINE = None     # Claude/Gemini AI engine
TENANT_AGENT = None
STRATEGIST = None

def init_regions():
    global REGIONS, API_CLIENTS, ACTIVE_REGION
    REGIONS = load_regions()
    for prefix, cfg in REGIONS.items():
        API_CLIENTS[prefix] = AmperityAPI(cfg)
    if REGIONS:
        ACTIVE_REGION = sorted(REGIONS.keys())[0]

def get_api() -> AmperityAPI:
    if ACTIVE_REGION and ACTIVE_REGION in API_CLIENTS:
        return API_CLIENTS[ACTIVE_REGION]
    raise RuntimeError("No active region configured")


# ── Region Switching ──────────────────────────────────────────────────────────

@app.route("/api/regions")
def list_regions():
    regions = []
    for prefix, cfg in sorted(REGIONS.items()):
        regions.append({
            "id": prefix,
            "name": cfg.name,
            "tenant": cfg.tenant,
            "active": prefix == ACTIVE_REGION,
            "has_token": API_CLIENTS[prefix].has_token() if prefix in API_CLIENTS else False,
        })
    return jsonify({"regions": regions, "active": ACTIVE_REGION})

@app.route("/api/regions/<region_id>/activate", methods=["POST"])
def activate_region(region_id):
    global ACTIVE_REGION
    if region_id not in REGIONS:
        return jsonify({"error": f"Unknown region: {region_id}"}), 404
    ACTIVE_REGION = region_id
    return jsonify({"active": ACTIVE_REGION, "name": REGIONS[region_id].name})


# ── Health ────────────────────────────────────────────────────────────────────

@app.route("/api/health")
def health():
    api = get_api()
    cfg = api.region
    return jsonify({
        "status": "ok",
        "region": ACTIVE_REGION,
        "region_name": cfg.name,
        "tenant": cfg.tenant,
        "database_id": cfg.database_id,
        "has_token": api.has_token(),
    })


# ── Identity Intelligence ───────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/tools")
def tools():
    return send_from_directory("static", "dashboard.html")

@app.route("/cotm")
def cotm():
    return send_from_directory("static", "dashboard-cotm.html")

@app.route("/demo")
def demo():
    return send_from_directory("static", "dashboard-demo.html")

@app.route("/presentation")
def presentation():
    return send_from_directory("static", "dashboard-external.html")

@app.route("/deck")
def deck():
    return send_from_directory("static", "dashboard-deck.html")

@app.route("/ai")
def ai_assistant():
    return send_from_directory("static", "ai-assistant.html")

@app.route("/api/search")
def search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "Missing query parameter 'q'"}), 400
    try:
        results = get_api().search_customer(q)
        return jsonify({"results": results, "count": len(results), "region": ACTIVE_REGION})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/explain/<ampid>")
def explain(ampid):
    try:
        records, scores = get_api().get_full_cluster(ampid)
        result = explain_cluster(records, scores)
        result["region"] = ACTIVE_REGION
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Cluster Health ────────────────────────────────────────────────────────────

@app.route("/api/cluster-health/oversized")
def oversized():
    mn = int(request.args.get("min", 20))
    lim = int(request.args.get("limit", 50))
    return jsonify(get_api().get_oversized_clusters(mn, lim))

@app.route("/api/cluster-health/multi-surname")
def multi_surname():
    mn = int(request.args.get("min", 3))
    lim = int(request.args.get("limit", 50))
    return jsonify(get_api().get_multi_surname_clusters(mn, lim))

@app.route("/api/cluster-health/single-source")
def single_source():
    mn = int(request.args.get("min", 30))
    lim = int(request.args.get("limit", 50))
    return jsonify(get_api().get_single_source_heavy_clusters(mn, lim))

@app.route("/api/cluster-health/distribution")
def distribution():
    return jsonify(get_api().get_cluster_size_distribution())


# ── Source Scorecard ──────────────────────────────────────────────────────────

@app.route("/api/source-scorecard")
def source_scorecard():
    return jsonify(get_api().get_source_scorecard())

@app.route("/api/source-overlap")
def source_overlap():
    return jsonify(get_api().get_source_overlap())


# ── Stitch Stats ──────────────────────────────────────────────────────────────

@app.route("/api/stitch-stats")
def stitch_stats():
    return jsonify(get_api().get_current_stitch_stats())

@app.route("/api/stitch-score-distribution")
def stitch_scores():
    return jsonify(get_api().get_stitch_score_distribution())

@app.route("/api/stitch-records-per-source")
def stitch_sources():
    return jsonify(get_api().get_records_per_source())


# ── Segment Preview ───────────────────────────────────────────────────────────

@app.route("/api/segment-preview", methods=["POST"])
def segment_preview():
    body = request.get_json(force=True)
    sql = body.get("sql", "").strip()
    if not sql:
        return jsonify({"error": "Missing 'sql' in body"}), 400
    try:
        return jsonify(get_api().preview_segment(sql))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/save-segment", methods=["POST"])
def save_segment():
    body = request.get_json(force=True)
    name = body.get("name", "").strip()
    sql = body.get("sql", "").strip()
    if not name or not sql:
        return jsonify({"error": "Missing 'name' or 'sql' in body"}), 400
    try:
        result = get_api().create_segment(name, sql)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/segment-demographics", methods=["POST"])
def segment_demographics():
    body = request.get_json(force=True)
    sql = body.get("sql", "").strip()
    if not sql:
        return jsonify({"error": "Missing 'sql' in body"}), 400
    try:
        return jsonify(get_api().preview_segment_demographics(sql))
    except Exception as e:
        print(f"[DEMOGRAPHICS ERROR] {type(e).__name__}: {e}")
        print(f"[DEMOGRAPHICS SQL] {sql[:200]}...")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ── Cross-Region Pulse ────────────────────────────────────────────────────────

@app.route("/api/cross-region-pulse")
def cross_region_pulse():
    """Get summary stats from ALL regions at once — the exec view."""
    results = {}
    for prefix, api in API_CLIENTS.items():
        try:
            if api.has_token():
                summary = api.get_region_summary()
                # Keep "summary" key for Value View (dashboard-cotm.html)
                # AND flatten aliases for Demo page (dashboard-demo.html)
                results[prefix] = {
                    "name": REGIONS[prefix].name,
                    "tenant": REGIONS[prefix].name,
                    "summary": summary,
                    "unified_customers": summary.get("total_profiles", 0),
                    "source_records": summary.get("total_records", 0),
                    "source_count": summary.get("source_count", 0),
                    "avg_cluster_size": summary.get("avg_records_per_id", 0),
                    "dedup_rate": summary.get("overall_dedup_rate", 0),
                    **summary,
                }
            else:
                results[prefix] = {"name": REGIONS[prefix].name, "error": "not connected"}
        except Exception as e:
            results[prefix] = {"name": REGIONS[prefix].name, "error": str(e)}
    return jsonify(results)


# ── Source Dedup Scorecard ────────────────────────────────────────────────────

@app.route("/api/dedup-scorecard")
def dedup_scorecard():
    return jsonify(get_api().get_source_dedup_rates())


# ── Household Bloat Scanner ──────────────────────────────────────────────────

@app.route("/api/fleet-fragmentation")
def household_bloat():
    mn = int(request.args.get("min", 3))
    lim = int(request.args.get("limit", 50))
    return jsonify(get_api().get_household_bloat(mn, lim))


# ── Name Variant / Transliteration Detector ──────────────────────────────────

@app.route("/api/name-variants")
def name_variants():
    mn = int(request.args.get("min", 3))
    lim = int(request.args.get("limit", 50))
    return jsonify(get_api().get_name_variant_clusters(mn, lim))

@app.route("/api/name-variants-count")
def name_variants_count():
    mn = int(request.args.get("min", 3))
    return jsonify({"total": get_api().get_name_variant_count(mn)})

@app.route("/api/fleet-fragmentation-count")
def household_bloat_count():
    mn = int(request.args.get("min", 3))
    return jsonify({"total": get_api().get_household_bloat_count(mn)})


# ── Golden Crosswalk Explorer ────────────────────────────────────────────────

@app.route("/api/crosswalk/<ampid>")
def crosswalk(ampid):
    try:
        return jsonify(get_api().get_crosswalk(ampid))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/crosswalk-sample")
def crosswalk_sample():
    lim = int(request.args.get("limit", 20))
    return jsonify(get_api().get_crosswalk_sample(lim))


# ── Activation Pipeline ───────────────────────────────────────────────────────

@app.route("/api/activation-pipeline")
def activation_pipeline():
    """Get orchestration overview and running workflows from MCP-cached data."""
    import re as _re
    try:
        cache_path = os.path.join(os.path.dirname(__file__), "data", "activation_cache.json")
        with open(cache_path, "r") as f:
            cache = json.load(f)

        orchestrations = cache.get("orchestrations", [])
        orch_groups = cache.get("orchestration_groups", [])
        workflows = cache.get("workflows", [])

        # Classify orchestrations by naming pattern
        categories = {
            "delta_loads": 0,
            "partitioned": 0,
            "xref": 0,
            "metrics": 0,
            "ccpa": 0,
            "other": 0
        }

        for orch in orchestrations:
            name = orch.get("name", "")
            nl = name.lower()
            is_partitioned = bool(_re.search(r'_p\d+', nl) or _re.search(r'ampid_[0-9a-f]', nl))
            if "ccpa" in nl or "compliance" in nl:
                categories["ccpa"] += 1
            elif "metric" in nl or "[raw count]" in nl or "score" in nl:
                categories["metrics"] += 1
            elif "xref" in nl:
                if is_partitioned:
                    categories["partitioned"] += 1
                else:
                    categories["xref"] += 1
            elif "delta" in nl:
                if is_partitioned:
                    categories["partitioned"] += 1
                else:
                    categories["delta_loads"] += 1
            elif is_partitioned:
                categories["partitioned"] += 1
            else:
                categories["other"] += 1

        # CCPA runs as an orchestration group (Daily CCPA Reports) plus
        # individual compliance orchestrations. Ensure group is counted.
        ccpa_groups = [g for g in orch_groups if "ccpa" in g.get("name", "").lower()
                       or "compliance" in g.get("name", "").lower()]
        categories["ccpa"] += len(ccpa_groups)

        # Running orchestrations (from orchestration state)
        running_orchs = [o for o in orchestrations if o.get("state") == "running"]

        # Recent workflows (last 7 days)
        recent_workflows = workflows[:10]

        # Active orchestration groups
        enabled_groups = [g for g in orch_groups if g.get("enabled")]

        return jsonify({
            "total_orchestrations": len(orchestrations),
            "has_more": cache.get("has_more_orchestrations", False),
            "active_groups": len(enabled_groups),
            "categories": categories,
            "running_orchestrations": running_orchs,
            "running_count": len(running_orchs),
            "recent_workflows": recent_workflows,
            "all_workflows_succeeded": all(w.get("state") == "succeeded" for w in workflows),
            "fetched_at": cache.get("fetched_at")
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Cluster Diff ──────────────────────────────────────────────────────────────

@app.route("/api/cluster-diff")
def cluster_diff():
    a = request.args.get("a", "")
    b = request.args.get("b", "")
    if not a or not b:
        return jsonify({"error": "Need ?a=ampid&b=ampid"}), 400
    try:
        return jsonify(get_api().diff_clusters(a, b))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Drift Monitoring ──────────────────────────────────────────────────────────

@app.route("/api/drift/snapshot", methods=["POST"])
def drift_snapshot():
    api = get_api()
    stats = api.get_current_stitch_stats()
    score_dist = api.get_stitch_score_distribution()
    source_dist = api.get_records_per_source()
    alerts = drift_store.compute_drift(ACTIVE_REGION, stats)
    snap_id = drift_store.save_snapshot(ACTIVE_REGION, stats, score_dist, source_dist)
    return jsonify({"snapshot_id": snap_id, "alerts": alerts, "stats": stats})

@app.route("/api/drift/history")
def drift_history():
    lim = int(request.args.get("limit", 30))
    return jsonify(drift_store.get_history(ACTIVE_REGION, lim))

@app.route("/api/drift/scores-history")
def drift_scores():
    lim = int(request.args.get("limit", 30))
    return jsonify(drift_store.get_scores_history(ACTIVE_REGION, lim))

@app.route("/api/drift/source-history")
def drift_sources():
    lim = int(request.args.get("limit", 30))
    return jsonify(drift_store.get_source_history(ACTIVE_REGION, lim))

@app.route("/api/drift/alerts")
def drift_alerts():
    lim = int(request.args.get("limit", 50))
    unack = request.args.get("unacknowledged", "false").lower() == "true"
    return jsonify(drift_store.get_alerts(ACTIVE_REGION, lim, unack))

@app.route("/api/drift/alerts/<int:alert_id>/acknowledge", methods=["POST"])
def drift_ack(alert_id):
    drift_store.acknowledge_alert(alert_id)
    return jsonify({"acknowledged": True})


# ── AI Assistant (Cortex Agent / Nemo) ───────────────────────────────────────

@app.route("/api/ask-ai", methods=["POST"])
def ask_ai():
    """Query Cortex Agent (Nemo/NemoSupport/NemoClientRelations) directly."""
    global CORTEX_AGENT

    if CORTEX_AGENT is None:
        return jsonify({"error": "Cortex Agent not configured. Check Snowflake credentials."}), 503

    body = request.get_json(force=True)
    question = body.get("question", "").strip()
    agent = body.get("agent", "nemo")
    include_context = body.get("include_context", True)

    if not question:
        return jsonify({"error": "Missing 'question' in request body"}), 400

    # Build context from current state
    context = None
    if include_context:
        api = get_api()
        cfg = api.region
        context = {
            "region": ACTIVE_REGION,
            "region_name": cfg.name,
            "tenant": cfg.tenant,
            "database_id": cfg.database_id,
        }

    try:
        result = CORTEX_AGENT.query(question, agent=agent, context=context)
        result["region"] = ACTIVE_REGION
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/list-agents")
def list_agents():
    """List available Cortex Agents."""
    if CORTEX_AGENT is None:
        return jsonify({"error": "Cortex Agent not configured"}), 503
    return jsonify({"agents": CORTEX_AGENT.list_agents()})


# ── Claude/Gemini AI Assistant ────────────────────────────────────────────

@app.route("/api/ask-assistant", methods=["POST"])
def ask_assistant():
    """Query Claude or Gemini with tool access to live tenant data."""
    global AI_ENGINE

    if AI_ENGINE is None or not AI_ENGINE.available:
        return jsonify({"error": "No AI provider configured. Set CLAUDE_API_KEY or GEMINI_API_KEY in .env"}), 503

    body = request.get_json(force=True)
    question = body.get("question", "").strip()

    if not question:
        return jsonify({"error": "Missing 'question' in request body"}), 400

    provider = body.get("provider")

    try:
        result = AI_ENGINE.query(question, provider=provider)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/assistant-clear", methods=["POST"])
def assistant_clear():
    """Clear conversation history."""
    global AI_ENGINE
    if AI_ENGINE:
        AI_ENGINE.clear_conversation()
    return jsonify({"cleared": True})

@app.route("/api/assistant-status")
def assistant_status():
    """Check AI assistant availability."""
    if AI_ENGINE and AI_ENGINE.available:
        return jsonify({"available": True, "provider": AI_ENGINE.provider, "providers": AI_ENGINE.available_providers})
    return jsonify({"available": False, "provider": "none", "providers": []})


# ── Dynamic Port Selection ────────────────────────────────────────────────────

def find_open_port(preferred=5080, range_start=5080, range_end=5099):
    """Try preferred port first, then scan range for an open one."""
    for port in [preferred] + list(range(range_start, range_end + 1)):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("127.0.0.1", port))
                return port
        except OSError:
            continue
    raise RuntimeError(f"No open port found in range {range_start}-{range_end}")


# ── Agentic + Strategist ──────────────────────────────────────────────────────

@app.route("/agentic")
def agentic_view():
    return send_from_directory("static", "agentic.html") if os.path.exists("static/agentic.html") else ("Agentic view not available", 404)

@app.route("/strategist")
def strategist_view():
    return send_from_directory("static", "strategist.html")

@app.route("/policy")
def policy_view():
    return send_from_directory("static", "policy.html")

@app.route("/api/strategist/quick", methods=["POST"])
def strategist_quick():
    """Fast-path: answer from context only, NO tool calls. Instant."""
    claude_key = os.getenv("CLAUDE_API_KEY", "")
    if not claude_key:
        return jsonify({"error": "No API key"}), 503
    body = request.get_json(force=True)
    question = body.get("question", "").strip()
    if not question:
        return jsonify({"error": "Missing question"}), 400
    try:
        import requests as req
        from tenant_context import get_tenant_context
        tenant_name = list(REGIONS.values())[0].name if REGIONS else ""
        ctx = get_tenant_context(tenant_name)
        r = req.post("https://api.anthropic.com/v1/messages", headers={
            "x-api-key": claude_key,
            "content-type": "application/json",
            "anthropic-version": "2023-06-01"
        }, json={
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 1024,
            "system": f"""You are a marketing strategist for an Amperity tenant. Answer in 3-5 concise, actionable sentences.

RULES:
- Synthesize insights, don't regurgitate raw context
- NEVER mention internal details like stitch escalations, specific employee names, enrichment vendor names, or implementation status notes
- NEVER reference "passthrough architecture" or "C360 build" directly — instead say "as your unified customer view matures" or "once golden records are in place"
- Frame everything as business opportunity, not technical gap
- Use real numbers where you know them (364M profiles, 69% dedup, 10 sources)
- Be the strategist the CMO would hire, not the engineer debugging the platform

{ctx}""",
            "messages": [{"role": "user", "content": question}]
        }, timeout=30)
        if r.status_code == 200:
            result = r.json()
            text = " ".join(b["text"] for b in result.get("content", []) if b.get("type") == "text")
            return jsonify({"response": text, "provider": "quick insight"})
        return jsonify({"error": f"API {r.status_code}"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/strategist/ask", methods=["POST"])
def strategist_ask():
    global STRATEGIST
    if STRATEGIST is None or not STRATEGIST.available:
        return jsonify({"error": "Strategist not configured. Set CLAUDE_API_KEY or GEMINI_API_KEY."}), 503
    body = request.get_json(force=True)
    question = body.get("question", "").strip()
    provider = body.get("provider")
    clear_first = body.get("clear", False)
    if not question:
        return jsonify({"error": "Missing question"}), 400
    try:
        if clear_first:
            STRATEGIST.clear()
        return jsonify(STRATEGIST.query(question, provider=provider))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/strategist/clear", methods=["POST"])
def strategist_clear():
    if STRATEGIST:
        STRATEGIST.clear()
    return jsonify({"cleared": True})

@app.route("/api/strategist/status")
def strategist_status():
    if STRATEGIST and STRATEGIST.available:
        return jsonify({"available": True, "provider": STRATEGIST.provider})
    return jsonify({"available": False})

@app.route("/api/agentic/scan", methods=["POST"])
def agentic_scan():
    if TENANT_AGENT is None:
        return jsonify({"error": "Agentic layer not initialized"}), 503
    return jsonify(TENANT_AGENT.run_cycle())

@app.route("/api/agentic/approve", methods=["POST"])
def agentic_approve():
    body = request.get_json(force=True)
    result = TENANT_AGENT.approve(body.get("proposal_id", ""), body.get("user", "operator"))
    return jsonify({"status": "approved"}) if result else (jsonify({"error": "Not found"}), 404)

@app.route("/api/agentic/reject", methods=["POST"])
def agentic_reject():
    body = request.get_json(force=True)
    result = TENANT_AGENT.reject(body.get("proposal_id", ""), body.get("user", "operator"), body.get("reason", ""))
    return jsonify({"status": "rejected"}) if result else (jsonify({"error": "Not found"}), 404)

@app.route("/api/agentic/execute", methods=["POST"])
def agentic_execute():
    body = request.get_json(force=True)
    result = TENANT_AGENT.execute(body.get("proposal_id", ""))
    if result:
        import dataclasses
        return jsonify(dataclasses.asdict(result))
    return jsonify({"error": "Not found or not approved"}), 404

@app.route("/api/agentic/status")
def agentic_agent_status():
    return jsonify(TENANT_AGENT.get_status()) if TENANT_AGENT else (jsonify({"error": "Not initialized"}), 503)

@app.route("/api/agentic/audit")
def agentic_audit():
    if TENANT_AGENT:
        return jsonify({"entries": [e.__dict__ if hasattr(e, '__dict__') else e for e in TENANT_AGENT.audit_log]})
    return jsonify({"entries": []})

@app.route("/api/agentic/policy")
def agentic_policy():
    return jsonify(TENANT_AGENT.policy.to_dict()) if TENANT_AGENT else jsonify({})

@app.route("/api/agentic/policy", methods=["POST"])
def agentic_update_policy():
    body = request.get_json(force=True)
    TENANT_AGENT.policy = ClientPolicy(body)
    return jsonify({"status": "updated"})


# ── Startup ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    print("\n" + "═" * 60)
    print("  MACYS IDENTITY INTELLIGENCE")
    print("═" * 60)

    init_regions()

    # Initialize Cortex Agent (optional)
    try:
        CORTEX_AGENT = CortexAgent()
        print("  ✓ Cortex Agent enabled (Nemo/NemoSupport/NemoClientRelations)")
    except Exception as e:
        print(f"  ⚠ Cortex Agent disabled: {e}")
        CORTEX_AGENT = None

    # Initialize AI Engine (Claude/Gemini)
    port_for_ai = find_open_port()  # peek at port for AI engine base URL
    AI_ENGINE = AIEngine(base_url=f"http://127.0.0.1:{port_for_ai}")
    if AI_ENGINE.available:
        print(f"  ✓ AI Assistant enabled ({AI_ENGINE.provider.title()})")
    else:
        print("  ⚠ AI Assistant disabled: set CLAUDE_API_KEY or GEMINI_API_KEY in .env")

    if not REGIONS:
        print("\n  ⚠  No regions configured in .env")
        print("  Copy .env.example to .env and fill in credentials.")
        print("  See README for per-region setup instructions.\n")
        sys.exit(1)

    for prefix, cfg in sorted(REGIONS.items()):
        api = API_CLIENTS[prefix]
        token_ok = api.has_token()
        status = "✓ connected" if token_ok else "✗ auth failed"
        active = " ← active" if prefix == ACTIVE_REGION else ""
        print(f"  [{prefix}] {cfg.name} ({cfg.tenant}) — {status}{active}")
        print(f"       database: {cfg.database_id}")

    port = port_for_ai  # use the same port we peeked at
    AI_ENGINE.base_url = f"http://127.0.0.1:{port}"  # finalize base URL

    # Initialize Agentic + Strategist
    tenant_name = list(REGIONS.values())[0].name if REGIONS else "Unknown"
    TENANT_AGENT = InTenantAgent(base_url=f"http://127.0.0.1:{port}", tenant_name=tenant_name)
    print(f"  ✓ Agentic layer enabled for {tenant_name}")
    STRATEGIST = TenantStrategist(base_url=f"http://127.0.0.1:{port}", tenant_name=tenant_name, consumption_model=os.getenv("CONSUMPTION_MODEL", "amps"))
    if STRATEGIST.available:
        print(f"  ✓ Strategist enabled ({STRATEGIST.provider.title()})")
    print(f"\n  Identity Intelligence:  http://127.0.0.1:{port}/")
    print(f"  Data Quality:     http://127.0.0.1:{port}/tools")
    print(f"  Value View:       http://127.0.0.1:{port}/cotm")
    print(f"  Internal Demo:    http://127.0.0.1:{port}/demo")
    print(f"  Client Presentation:  http://127.0.0.1:{port}/presentation")
    print(f"  AI Assistant:     http://127.0.0.1:{port}/ai")
    print(f"  Strategist:      http://127.0.0.1:{port}/strategist")
    print(f"  Policy:          http://127.0.0.1:{port}/policy")
    print(f"  API Health:       http://127.0.0.1:{port}/api/health")
    print("═" * 60 + "\n")

    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)
