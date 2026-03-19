"""
Cross-Tenant Intelligence Dashboard
Queries multiple Amperity tenants simultaneously. One view across your portfolio.
"""
import os, sys, socket, json, traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
load_dotenv()
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS

from amperity_api import load_regions, AmperityAPI
from ai_engine import AIEngine

app = Flask(__name__, static_folder="static")
CORS(app)

@app.errorhandler(Exception)
def handle_exception(e):
    code = getattr(e, 'code', 500)
    return jsonify({"error": str(e)}), code if isinstance(code, int) else 500

# ── Multi-Tenant Setup ────────────────────────────────────────────────────────

REGIONS = {}
API_CLIENTS = {}
AI_ENGINE = None

def init_regions():
    global REGIONS, API_CLIENTS
    REGIONS = load_regions()
    for prefix, cfg in REGIONS.items():
        API_CLIENTS[prefix] = AmperityAPI(cfg)

def _query_tenant(prefix, fn):
    """Run a function against a tenant, return (prefix, result) or (prefix, error)."""
    try:
        api = API_CLIENTS[prefix]
        result = fn(api)
        return prefix, result, None
    except Exception as e:
        return prefix, None, str(e)

def query_all_tenants(fn, timeout=30):
    """Query all connected tenants in parallel. Returns {prefix: result}."""
    results = {}
    errors = {}
    with ThreadPoolExecutor(max_workers=len(API_CLIENTS)) as pool:
        futures = {pool.submit(_query_tenant, p, fn): p for p in API_CLIENTS}
        for f in as_completed(futures, timeout=timeout):
            prefix, result, error = f.result()
            if error:
                errors[prefix] = error
            else:
                results[prefix] = result
    return results, errors


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/api/tenants")
def list_tenants():
    tenants = []
    for prefix, cfg in sorted(REGIONS.items()):
        api = API_CLIENTS[prefix]
        tenants.append({
            "id": prefix,
            "name": cfg.name,
            "tenant": cfg.tenant,
            "connected": api.has_token(),
        })
    return jsonify({"tenants": tenants})

@app.route("/api/portfolio-pulse")
def portfolio_pulse():
    """Identity health across all tenants — the money shot."""
    def get_pulse(api):
        rows = api.run_query("""
            SELECT
                COUNT(DISTINCT amperity_id) AS total_profiles,
                COUNT(*) AS total_records
            FROM Unified_Coalesced
        """, limit=1)
        r = rows[0] if rows else {}
        profiles = int(float(r.get("total_profiles", 0)))
        records = int(float(r.get("total_records", 0)))
        dedup = round((records - profiles) / records * 100, 1) if records else 0

        # Source count
        src_rows = api.run_query("""
            SELECT COUNT(DISTINCT datasource) AS cnt FROM Unified_Coalesced
        """, limit=1)
        sources = int(float(src_rows[0].get("cnt", 0))) if src_rows else 0

        return {
            "total_profiles": profiles,
            "total_records": records,
            "dedup_rate": dedup,
            "source_count": sources,
        }

    results, errors = query_all_tenants(get_pulse, timeout=45)

    # Attach tenant metadata
    for prefix in results:
        cfg = REGIONS[prefix]
        results[prefix]["name"] = cfg.name
        results[prefix]["tenant"] = cfg.tenant

    # Portfolio totals
    total_profiles = sum(r["total_profiles"] for r in results.values())
    total_records = sum(r["total_records"] for r in results.values())
    total_sources = sum(r["source_count"] for r in results.values())
    avg_dedup = round(sum(r["dedup_rate"] for r in results.values()) / max(len(results), 1), 1)

    return jsonify({
        "tenants": results,
        "errors": errors,
        "portfolio": {
            "total_profiles": total_profiles,
            "total_records": total_records,
            "total_sources": total_sources,
            "avg_dedup_rate": avg_dedup,
            "tenant_count": len(results),
        }
    })

@app.route("/api/portfolio-dedup")
def portfolio_dedup():
    """Per-source dedup rates across all tenants."""
    def get_dedup(api):
        return api.run_query("""
            SELECT datasource,
                   COUNT(*) AS source_records,
                   COUNT(DISTINCT amperity_id) AS unique_ids
            FROM Unified_Coalesced
            GROUP BY datasource
            ORDER BY source_records DESC
        """, limit=50)

    results, errors = query_all_tenants(get_dedup, timeout=45)

    for prefix in results:
        for row in results[prefix]:
            sr = int(float(row.get("source_records", 0)))
            ui = int(float(row.get("unique_ids", 0)))
            row["dedup_rate"] = round((sr - ui) / sr * 100, 1) if sr else 0
            row["source_records"] = sr
            row["unique_ids"] = ui

    return jsonify({"tenants": results, "errors": errors})

@app.route("/api/portfolio-clusters")
def portfolio_clusters():
    """Cluster size distribution across all tenants."""
    def get_clusters(api):
        return api.run_query("""
            SELECT
                CASE
                    WHEN cnt = 1 THEN '1 (Singleton)'
                    WHEN cnt = 2 THEN '2'
                    WHEN cnt BETWEEN 3 AND 5 THEN '3-5'
                    WHEN cnt BETWEEN 6 AND 10 THEN '6-10'
                    WHEN cnt BETWEEN 11 AND 50 THEN '11-50'
                    WHEN cnt BETWEEN 51 AND 100 THEN '51-100'
                    WHEN cnt BETWEEN 101 AND 1000 THEN '101-1000'
                    ELSE '1000+'
                END AS bucket,
                COUNT(*) AS cluster_count
            FROM (SELECT amperity_id, COUNT(*) AS cnt FROM Unified_Coalesced GROUP BY amperity_id)
            GROUP BY 1 ORDER BY MIN(cnt)
        """, limit=20)

    results, errors = query_all_tenants(get_clusters, timeout=60)
    return jsonify({"tenants": results, "errors": errors})


# ── AI Assistant ──────────────────────────────────────────────────────────────

@app.route("/api/ask-assistant", methods=["POST"])
def ask_assistant():
    global AI_ENGINE
    if AI_ENGINE is None or not AI_ENGINE.available:
        return jsonify({"error": "Set CLAUDE_API_KEY or GEMINI_API_KEY in .env"}), 503
    body = request.get_json(force=True)
    question = body.get("question", "").strip()
    provider = body.get("provider")
    if not question:
        return jsonify({"error": "Missing question"}), 400
    try:
        return jsonify(AI_ENGINE.query(question, provider=provider))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/assistant-clear", methods=["POST"])
def assistant_clear():
    if AI_ENGINE:
        AI_ENGINE.clear_conversation()
    return jsonify({"cleared": True})

@app.route("/api/assistant-status")
def assistant_status():
    if AI_ENGINE and AI_ENGINE.available:
        return jsonify({"available": True, "provider": AI_ENGINE.provider, "providers": AI_ENGINE.available_providers})
    return jsonify({"available": False, "provider": "none", "providers": []})


# ── Startup ───────────────────────────────────────────────────────────────────

def find_open_port(preferred=5090, range_start=5090, range_end=5099):
    for port in [preferred] + list(range(range_start, range_end + 1)):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("127.0.0.1", port))
                return port
        except OSError:
            continue
    raise RuntimeError("No open port found")

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    print("\n" + "═" * 60)
    print("  CROSS-TENANT INTELLIGENCE")
    print("═" * 60)

    init_regions()

    port = find_open_port()
    AI_ENGINE = AIEngine(base_url=f"http://127.0.0.1:{port}")
    if AI_ENGINE.available:
        print(f"  ✓ AI Assistant enabled ({AI_ENGINE.provider.title()})")
    else:
        print("  ⚠ AI disabled: set CLAUDE_API_KEY or GEMINI_API_KEY")

    if not REGIONS:
        print("\n  ⚠  No tenants configured in .env")
        sys.exit(1)

    connected = 0
    for prefix, cfg in sorted(REGIONS.items()):
        api = API_CLIENTS[prefix]
        ok = api.has_token()
        if ok: connected += 1
        status = "✓" if ok else "✗"
        print(f"  {status} {cfg.name} ({cfg.tenant})")

    print(f"\n  {connected}/{len(REGIONS)} tenants connected")
    print(f"\n  Dashboard:  http://127.0.0.1:{port}/")
    print("═" * 60 + "\n")

    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)
