"""
Head of AI — Command Center
Meta-dashboard linking JD requirements to live tenant demos.
"""
import os, socket
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS

app = Flask(__name__, static_folder="static")
CORS(app)

TENANT_APPS = {
    "honda":     {"name": "Honda",          "port": None, "dir": "honda-identity-intelligence-layer-with-claude"},
    "seahawks":  {"name": "Seahawks",       "port": None, "dir": "seahawks-identity-intelligence-layer-with-claude"},
    "atlas":     {"name": "Atlas Motors",    "port": None, "dir": "atlas-motors-identity-intelligence-layer-with-claude"},
    "nfl":       {"name": "NFL",            "port": None, "dir": "nfl-identity-intelligence-layer-with-claude"},
    "acme":      {"name": "Acme",           "port": None, "dir": "acme-identity-intelligence-layer-with-claude"},
    "macys":     {"name": "Macy's",         "port": None, "dir": "macys-identity-intelligence-layer-with-claude"},
    "gm":        {"name": "GM (3 regions)", "port": None, "dir": "gm-identity-intelligence-layer-with-claude"},
    "portfolio": {"name": "Cross-Tenant",   "port": None, "dir": "cross-tenant-intelligence-dashboard"},
}

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/presentation")
def presentation():
    return send_from_directory("static", "presentation.html")

@app.route("/api/apps")
def list_apps():
    """Check which tenant apps are running."""
    import requests
    results = {}
    for key, info in TENANT_APPS.items():
        for port in range(5080, 5099):
            try:
                r = requests.get(f"http://127.0.0.1:{port}/", timeout=1)
                if r.status_code == 200 and info["name"].split()[0].lower() in r.text.lower():
                    results[key] = {"name": info["name"], "port": port, "status": "running"}
                    break
            except:
                continue
        # Check 5090 range for portfolio
        if key == "portfolio":
            for port in range(5090, 5099):
                try:
                    r = requests.get(f"http://127.0.0.1:{port}/api/tenants", timeout=1)
                    if r.status_code == 200:
                        results[key] = {"name": info["name"], "port": port, "status": "running"}
                        break
                except:
                    continue
        if key not in results:
            results[key] = {"name": info["name"], "port": None, "status": "stopped"}
    return jsonify(results)

def find_open_port(preferred=5070, range_start=5070, range_end=5079):
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
    port = find_open_port()
    print(f"\n{'═' * 60}")
    print(f"  HEAD OF AI — COMMAND CENTER")
    print(f"{'═' * 60}")
    print(f"\n  Dashboard:  http://127.0.0.1:{port}/")
    print(f"{'═' * 60}\n")
    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)
