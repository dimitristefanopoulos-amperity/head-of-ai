"""
MCP Bridge Server — HTTP wrapper around the Nemo Snowflake MCP server.

Spawns the same MCP server process that Claude Code uses, communicates
over JSON-RPC 2.0 via stdio, and exposes it as HTTP endpoints for Flask.

No Anthropic API key needed. No Snowflake SDK needed.
Just talks to the same MCP server you already have configured.
"""

import json
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from flask import Flask, request, jsonify
from flask_cors import CORS


# ---------------------------------------------------------------------------
# MCP Server Process Manager
# ---------------------------------------------------------------------------

PLUGIN_ROOT = os.path.expanduser(
    "~/.claude/plugins/marketplaces/amperity-nemo"
)

SNOWFLAKE_USER = os.getenv(
    "SNOWFLAKE_USER",
    ""  # Set SNOWFLAKE_USER env var
)

MCP_ENV = {
    **os.environ,
    "SNOWFLAKE_ACCOUNT": "xpahnkf-amperity-data-warehouse",
    "SNOWFLAKE_USER": SNOWFLAKE_USER,
    "SNOWFLAKE_DATABASE": "PROD",
    "SNOWFLAKE_WAREHOUSE": "NEMO_READ",
    "SNOWFLAKE_ROLE": "FISHBOWL_R",
}

BRIDGE_DIR = os.path.dirname(os.path.abspath(__file__))

# Use the lazy proxy (defers Snowflake auth until first tool call)
# but point it at the upstream server directly (skip broken patched script)
MCP_CMD = [
    "python3",
    os.path.join(PLUGIN_ROOT, "bin", "lazy-snowflake-mcp.py"),
    "/opt/homebrew/bin/uv", "run", "--with", "snowflake-labs-mcp",
    "python3", os.path.join(BRIDGE_DIR, "start_mcp_server.py"),
    "--service-config-file",
    os.path.join(PLUGIN_ROOT, "config", "snowflake-services.yaml"),
    "--authenticator", "externalbrowser",
    "--transport", "stdio",
]


class MCPServerManager:
    """Manages the MCP server subprocess and JSON-RPC communication."""

    def __init__(self):
        self.proc = None
        self.lock = threading.Lock()
        self.pending = {}  # msg_id -> threading.Event + result
        self.reader_thread = None
        self.initialized = False

    def start(self):
        """Spawn the MCP server and complete the handshake."""
        self.proc = subprocess.Popen(
            MCP_CMD,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=sys.stderr,
            env=MCP_ENV,
            text=True,
            cwd=PLUGIN_ROOT,
        )

        # Start reader thread
        self.reader_thread = threading.Thread(
            target=self._read_responses, daemon=True
        )
        self.reader_thread.start()

        # MCP initialize handshake
        result = self._send_and_wait("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "mcp-bridge", "version": "1.0.0"},
        })

        if result is None:
            raise RuntimeError("MCP server did not respond to initialize")

        # Send initialized notification
        self._send_notification("notifications/initialized")
        self.initialized = True

    def _send(self, msg):
        """Write a JSON-RPC message to the server's stdin."""
        with self.lock:
            self.proc.stdin.write(json.dumps(msg) + "\n")
            self.proc.stdin.flush()

    def _send_notification(self, method, params=None):
        """Send a notification (no response expected)."""
        msg = {"jsonrpc": "2.0", "method": method}
        if params:
            msg["params"] = params
        self._send(msg)

    def _send_and_wait(self, method, params=None, timeout=120):
        """Send a request and wait for the response."""
        msg_id = str(uuid.uuid4())
        msg = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "method": method,
        }
        if params:
            msg["params"] = params

        event = threading.Event()
        self.pending[msg_id] = {"event": event, "result": None, "error": None}

        self._send(msg)
        event.wait(timeout=timeout)

        entry = self.pending.pop(msg_id, None)
        if entry and entry["error"]:
            raise RuntimeError(entry["error"].get("message", "MCP error"))
        if entry:
            return entry["result"]
        raise TimeoutError(f"MCP server did not respond within {timeout}s")

    def _read_responses(self):
        """Background thread: read JSON-RPC responses from server stdout."""
        try:
            for line in self.proc.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue

                msg_id = msg.get("id")
                if msg_id and msg_id in self.pending:
                    entry = self.pending[msg_id]
                    if "error" in msg:
                        entry["error"] = msg["error"]
                    else:
                        entry["result"] = msg.get("result")
                    entry["event"].set()
        except (OSError, ValueError):
            pass

    def call_tool(self, tool_name, arguments, timeout=120):
        """Call an MCP tool and return the result."""
        if not self.proc or self.proc.poll() is not None:
            raise RuntimeError("MCP server is not running")

        result = self._send_and_wait("tools/call", {
            "name": tool_name,
            "arguments": arguments,
        }, timeout=timeout)

        return result

    def list_tools(self):
        """List available MCP tools."""
        return self._send_and_wait("tools/list", {})

    def stop(self):
        """Terminate the MCP server."""
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(timeout=5)


# ---------------------------------------------------------------------------
# Flask HTTP Bridge
# ---------------------------------------------------------------------------

app = Flask(__name__)
CORS(app)

mcp = MCPServerManager()


@app.route("/health")
def health():
    alive = mcp.proc is not None and mcp.proc.poll() is None
    return jsonify({
        "status": "ok" if alive else "mcp_down",
        "service": "mcp-bridge",
        "mcp_initialized": mcp.initialized,
        "mcp_alive": alive,
    })


@app.route("/agents")
def list_agents():
    agents = [
        {
            "id": "nemo",
            "name": "Nemo",
            "description": "Data analytics: consumption (Amps), billing, connectors, Bridge, features, resource analysis, account lookups, tenant info.",
        },
        {
            "id": "nemosupport",
            "name": "NemoSupport",
            "description": "Support operations: workflow execution, task performance, errors, orchestration reliability, JIRA tickets, support metrics.",
        },
        {
            "id": "nemoclientrelations",
            "name": "NemoClientRelations",
            "description": "Client relations: customer sentiment, deal status, sales pipeline, renewals, Gong transcripts, go-to-market insights.",
        },
    ]
    return jsonify({"agents": agents})


@app.route("/ask", methods=["POST"])
def ask():
    """
    Query a Cortex Agent via MCP.

    POST body:
    {
        "question": "What is the current consumption?",
        "agent": "nemo",
        "context": {"tenant": "macys", "region": "MACYS"}
    }
    """
    body = request.get_json(force=True)
    question = body.get("question", "").strip()
    agent = body.get("agent", "nemo")
    context = body.get("context")

    if not question:
        return jsonify({"error": "Missing 'question'"}), 400

    valid_agents = ("nemo", "nemosupport", "nemoclientrelations")
    if agent not in valid_agents:
        return jsonify({"error": f"Unknown agent: {agent}"}), 400

    # Build the query with context
    query = question
    if context:
        context_str = "\n".join(f"{k}: {v}" for k, v in context.items())
        query = f"Context:\n{context_str}\n\nQuestion: {question}"

    try:
        result = mcp.call_tool("cortex_agent", {
            "service_name": agent,
            "database_name": "SNOWFLAKE_INTELLIGENCE",
            "schema_name": "AGENTS",
            "query": query,
        })

        # Debug: log raw result
        import sys
        print(f"[BRIDGE DEBUG] raw result type: {type(result)}", file=sys.stderr)
        print(f"[BRIDGE DEBUG] raw result: {json.dumps(result, default=str)[:500]}", file=sys.stderr)

        # Parse result — MCP returns content blocks
        response_text = ""
        if isinstance(result, dict):
            content = result.get("content", [])
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    response_text += block.get("text", "")
            if not response_text:
                response_text = json.dumps(result, default=str)
        else:
            response_text = str(result)

        print(f"[BRIDGE DEBUG] extracted text (first 300): {response_text[:300]}", file=sys.stderr)

        # The MCP tool returns AgentResponse JSON inside the text block.
        # Try to unwrap it.
        if response_text:
            try:
                parsed = json.loads(response_text)
                if isinstance(parsed, dict) and "results" in parsed:
                    response_text = parsed["results"]
                elif isinstance(parsed, dict) and "result" in parsed:
                    response_text = parsed["result"]
            except (json.JSONDecodeError, TypeError):
                pass

        return jsonify({
            "response": response_text,
            "agent": agent,
        })

    except TimeoutError:
        return jsonify({
            "error": "Query timed out waiting for Cortex Agent response",
            "agent": agent,
        }), 504

    except Exception as e:
        return jsonify({
            "error": str(e),
            "agent": agent,
        }), 500


@app.route("/tools")
def tools():
    """List raw MCP tools available."""
    try:
        result = mcp.list_tools()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

def shutdown(*_):
    mcp.stop()
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    port = int(os.getenv("BRIDGE_PORT", 5081))

    print("\n" + "=" * 60)
    print("  MCP BRIDGE SERVER")
    print("=" * 60)

    print(f"  Plugin root: {PLUGIN_ROOT}")
    print("  Starting MCP server subprocess...")

    try:
        mcp.start()
        print("  MCP server initialized (Snowflake auth deferred until first query)")
    except Exception as e:
        print(f"  ERROR starting MCP server: {e}")
        print("  Bridge will start but queries will fail.")

    print(f"\n  Bridge:  http://127.0.0.1:{port}")
    print(f"  Health:  http://127.0.0.1:{port}/health")
    print(f"  Agents:  http://127.0.0.1:{port}/agents")
    print(f"  Ask:     POST http://127.0.0.1:{port}/ask")
    print(f"  Tools:   http://127.0.0.1:{port}/tools")
    print("=" * 60 + "\n")

    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)
