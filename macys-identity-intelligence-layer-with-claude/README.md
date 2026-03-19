# Macy's Identity Intelligence Layer

An intelligence layer that sits on top of Amperity's identity resolution engine. Stitch is the engine — this is the surface that makes the engine's power visible and actionable.

Built on existing Amperity APIs and the MCP server. Every data call maps 1:1 to an existing MCP tool or API endpoint. Nothing custom on the backend.

## Screenshots

| Presentation | Demo | Data Quality |
|:---:|:---:|:---:|
| ![Presentation](screenshots/presentation.png) | ![Demo](screenshots/demo.png) | ![Data Quality](screenshots/data-quality.png) |

| COTM Value View | AI Assistant | Identity Search |
|:---:|:---:|:---:|
| ![COTM](screenshots/cotm-value-view.png) | ![AI](screenshots/ai-assistant.png) | ![Search](screenshots/identity-search.png) |

## What It Does

**Identity Explainability** — Translates Stitch output into natural language. "Why did these records merge?" answered with a confidence score, merge narrative, signal breakdown, source-by-source analysis, and pairwise score visualization. Pulls from Unified Coalesced and Unified Scores in real time.

**Continuous Quality Monitoring** — After each Stitch run, queries cluster statistics, score distributions, and source field completeness. Compares against historical baselines. Detects cluster count drift, score distribution shift, oversized clusters, and source quality degradation.

**Segment Discovery** — Identifies high-value segments from the identity data itself (multi-source customers, cross-channel loyalists, high-confidence clusters) and generates SQL that copies directly into the platform's query builder.

**COTM Value Framing** — Maps identity metrics to Command of the Message value drivers, creating a direct line from data to business narrative backed by live tenant metrics.

**AI Assistant** — Nemo/Cortex Agent integration via MCP bridge for conversational access to tenant data, consumption, support metrics, and workflows.

## Quick Start

```bash
cp .env.example .env
# Add OAuth2 credentials (Settings > API Keys)
./launch.sh
```

Opens at `http://localhost:5080`

## Views

| Route | View |
|---|---|
| `/` | Identity Search — cluster explainability, confidence scoring, merge narrative |
| `/tools` | Data Quality — dedup scorecard, source coverage, stitch stats, drift monitoring |
| `/cotm` | Value View — COTM-framed metrics for client conversations |
| `/demo` | Internal Demo — live data, not for distribution |
| `/presentation` | Client Presentation — external-facing, business outcomes |
| `/ai` | AI Assistant — Nemo/Cortex Agent via MCP bridge |

## Configuration

```bash
REGION_MACYS_NAME=Macys
REGION_MACYS_TENANT=macys
REGION_MACYS_TOKEN_URL=https://macys.amperity.com/api/v0/oauth2/token
REGION_MACYS_CLIENT_ID=        # OAuth2 client ID
REGION_MACYS_CLIENT_SECRET=    # OAuth2 client secret
REGION_MACYS_DATABASE_ID=      # C360 database ID
REGION_MACYS_SEGMENT_ID=       # Draft SQL segment ID
REGION_MACYS_DATASET_ID=       # From browser DevTools
```

Optional: `MCP_BRIDGE_URL=http://127.0.0.1:5081` for AI Assistant.

## Architecture

```
├── app.py              # Flask server + API routes
├── amperity_api.py     # OAuth2 + Transit+JSON query client
├── explainability.py   # Confidence scoring + merge narrative
├── drift_store.py      # SQLite drift monitoring
├── cortex_agent.py     # Nemo/Cortex Agent integration
├── mcp_bridge.py       # MCP bridge server
├── static/             # View HTML files
└── screenshots/
```
