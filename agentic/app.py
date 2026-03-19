"""
Agentic Intelligence — Operations Dashboard
Runs the OBSERVE → EVALUATE → PROPOSE → GATE pipeline across tenants.
Human-in-the-loop approval for proposals that don't meet auto-approve criteria.
"""
import os, sys, socket, json
from dotenv import load_dotenv
load_dotenv()
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

from pipeline import AgenticPipeline, ClientPolicy

app = Flask(__name__, static_folder="static")
CORS(app)

# ── State ─────────────────────────────────────────────────────────────────────
pipeline = AgenticPipeline()
tenant_snapshots = {}  # {tenant_name: last_known_data}
TENANT_PORTS = {}  # discovered at startup

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/api/scan", methods=["POST"])
def scan_tenants():
    """Run OBSERVE → EVALUATE → PROPOSE → GATE across all running tenants."""
    global tenant_snapshots

    results = {}
    for name, port in TENANT_PORTS.items():
        try:
            r = requests.get(f"http://127.0.0.1:{port}/api/cross-region-pulse", timeout=60)
            data = r.json()
            # Extract first region's data
            regions = data.get("regions", data)
            if isinstance(regions, dict):
                tenant_data = list(regions.values())[0] if regions else {}
            else:
                tenant_data = {}

            tenant_data["name"] = name

            # Run pipeline
            previous = tenant_snapshots.get(name)
            result = pipeline.run(tenant_data, previous)
            results[name] = result

            # Save snapshot for next comparison
            tenant_snapshots[name] = tenant_data

        except Exception as e:
            results[name] = {"error": str(e), "tenant": name}

    return jsonify({
        "results": results,
        "cost_summary": pipeline.get_cost_summary(),
        "pending_count": len(pipeline.get_pending()),
    })

@app.route("/api/pending")
def get_pending():
    """Get proposals awaiting human review."""
    return jsonify({"proposals": pipeline.get_pending()})

@app.route("/api/approve", methods=["POST"])
def approve_proposal():
    """Human approves a proposal."""
    body = request.get_json(force=True)
    prop_id = body.get("proposal_id", "")
    user = body.get("user", "operator")
    result = pipeline.approve(prop_id, user)
    if result:
        return jsonify({"status": "approved", "proposal": json.loads(json.dumps(result, default=str))})
    return jsonify({"error": "Proposal not found or not pending"}), 404

@app.route("/api/reject", methods=["POST"])
def reject_proposal():
    """Human rejects a proposal."""
    body = request.get_json(force=True)
    prop_id = body.get("proposal_id", "")
    user = body.get("user", "operator")
    reason = body.get("reason", "")
    result = pipeline.reject(prop_id, user, reason)
    if result:
        return jsonify({"status": "rejected", "proposal": json.loads(json.dumps(result, default=str))})
    return jsonify({"error": "Proposal not found or not pending"}), 404

@app.route("/api/audit")
def get_audit():
    """Get full audit trail."""
    return jsonify({"entries": pipeline.get_audit_log()})

@app.route("/api/costs")
def get_costs():
    """Get cost summary."""
    return jsonify(pipeline.get_cost_summary())

@app.route("/api/policy")
def get_policy():
    """Get current client policy."""
    return jsonify(pipeline.policy.to_dict())

@app.route("/api/policy", methods=["POST"])
def update_policy():
    """Update client policy."""
    body = request.get_json(force=True)
    pipeline.policy = ClientPolicy(body)
    return jsonify({"status": "updated", "policy": pipeline.policy.to_dict()})

@app.route("/api/tenants")
def list_tenants():
    """List discovered tenant apps."""
    return jsonify({"tenants": TENANT_PORTS})


# ── Startup ───────────────────────────────────────────────────────────────────

def discover_tenants():
    """Find running tenant apps by scanning ports."""
    found = {}
    for port in range(5080, 5090):
        try:
            r = requests.get(f"http://127.0.0.1:{port}/", timeout=1)
            if r.status_code == 200:
                # Extract title
                import re
                title = re.search(r'<title>([^<]+)', r.text)
                if title:
                    name = title.group(1).replace(' Identity Intelligence', '').strip()
                    found[name] = port
        except:
            continue
    return found

def find_open_port(preferred=5075, range_start=5075, range_end=5079):
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

    print(f"\n{'═' * 60}")
    print(f"  AGENTIC INTELLIGENCE — OPERATIONS")
    print(f"{'═' * 60}")

    TENANT_PORTS = discover_tenants()
    for name, port in sorted(TENANT_PORTS.items()):
        print(f"  ✓ {name} (port {port})")
    print(f"\n  {len(TENANT_PORTS)} tenants discovered")

    port = find_open_port()
    print(f"\n  Operations Dashboard:  http://127.0.0.1:{port}/")
    print(f"{'═' * 60}\n")

    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)
