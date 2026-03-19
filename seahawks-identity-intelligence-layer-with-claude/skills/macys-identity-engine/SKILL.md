---
name: macys-identity-engine
description: >
  Use this skill whenever working on the Macy's Identity Engine Flask app — v1 or v2.
  Trigger when the user mentions identity resolution, identity search, explainability,
  cluster analysis, dedup, stitch, data quality, source completeness, crosswalk,
  fleet fragmentation, drift monitoring, confidence scoring, merge narratives,
  COTM value view, value drivers, dark theme, demo page, presentation page,
  deck, AI assistant, Nemo, Cortex Agent, MCP bridge, or any of the dashboard tabs.
  Also trigger for names like Macy's, Balaji, Jared, or references to Amperity API
  patterns, Transit+JSON, multi-region config, segment creation, or activation.
  Trigger when the user wants to add a new view, modify an existing dashboard,
  fix an API endpoint, change the design system, or build on top of v2.
---

# Macy's Identity Engine

Multi-region identity resolution analytics Flask app for the Macy's Amperity tenant.
Six views (v2), 25+ API endpoints, live production data. Swiss Army Knife pattern —
handles identity search, data quality, client-facing value framing, dark-theme demos,
and AI-powered Cortex Agent queries all in one codebase.

## When to Use

- Any work on the identity engine codebase (v1 or v2)
- Adding/modifying dashboard views, API endpoints, or visualizations
- Working with Amperity API (queries, segments, regions)
- Adjusting the dark theme theme or light theme design system
- MCP bridge or Cortex Agent integration work
- COTM (Command of the Message) value framing
- Identity resolution concepts: clusters, dedup, stitch, confidence scoring

## Core Knowledge

### Project Layout

**v1** (`macys-identity-engine/`) — production, DO NOT modify.
**v2** (`macys-identity-enginev-v2/`) — active development, iterates on v1.

```
app.py                  # Flask server, all routes + 25 API endpoints
amperity_api.py         # Multi-region OAuth2 + Transit+JSON query client
explainability.py       # 4-component confidence scoring (0-100) + merge narrative
drift_store.py          # SQLite drift monitoring with alerting
cortex_agent.py         # HTTP client for MCP bridge (v2 only)
mcp_bridge.py           # Spawns MCP server, JSON-RPC stdio, HTTP endpoints (v2)
start_mcp_server.py     # Patched snowflake-labs-mcp with SSE parser fix (v2)
launch-with-bridge.sh   # Starts both services (v2)
static/
  index.html            # Identity Search (light theme)
  dashboard.html        # Data Quality — 11 tabs (light theme)
  dashboard-cotm.html   # COTM Value View — 3 value drivers (light theme)
  dashboard-demo.html   # Internal Demo — dark theme, NOT FOR DISTRIBUTION
  dashboard-external.html # Client Presentation — dark theme, client-safe
  dashboard-deck.html   # Deck presentation (dark theme)
  ai-assistant.html     # AI Assistant — Cortex Agent chat (v2 only, light theme)
```

### Six Views

| Route | View | Theme | Nav Style |
|-------|------|-------|-----------|
| `/` | Identity Search | Light | `.nav-link` in header |
| `/tools` | Data Quality (11 tabs) | Light | `.nav-link` in header |
| `/cotm` | COTM Value View | Light | `.nav-link` in header |
| `/demo` | Internal Demo | Dark theme | `topbar-nav a` |
| `/presentation` | Client Presentation | Dark theme | `topbar-nav a` or header nav |
| `/deck` | Deck | Dark theme | header nav `a` |
| `/ai` | AI Assistant (v2) | Light | `.nav-link` in header |

### Design Systems

**Light theme** (Search, Data Quality, COTM, AI):
- Font: Inter (body), Montserrat (headings)
- Background: #F8F9FA, cards: white with #E5E7EB borders
- Accents: --amp-teal (#54D3DE), --amp-ocean (#00A0B2)
- Header: dark bar (#1A1A1A) with white nav links

**Dark theme** (Demo, Presentation, Deck):
- Font: Montserrat
- Background: #0C0C0C, panels: #1a1a1a
- Brand: #54d3de, accent: #EAFF5F
- Effects: glassmorphic cards, scroll-reveal with IntersectionObserver
- Region switcher: top-right dots

**Brand palette** (both themes):
DUSK (#004B57), TEAL (#54D3DE), OCEAN (#00A0B2), ICE (#ABF4F7), AMP_YELLOW (#EAFF5F)

### Amperity API Patterns

- **Numbers as strings**: Always `Number()` in JS, `float()`/`int()` in Python
- **Transit+JSON 3-step**: POST (transit+json) → Poll status (transit+json) → GET results (json)
- **Flask threaded**: Must run `threaded=True` — dashboards fire parallel fetches
- **Deep links**: Hash routing SPA. `?sql=` params don't work. Copy SQL + open page instead.
- **Popup blocker**: `window.open()` must fire BEFORE `navigator.clipboard.writeText()` in same click handler.
- **Segment creation**: Tries `/api/v0/segments`, `/api/segments`, `/api/v1/segments`. Skips 405s.
- **apiFetch() wrapper**: Checks Content-Type before `.json()` parse.

### Confidence Scoring (explainability.py)

4-component score, 0-100:
1. **Signal Strength** (0-40): Shared PII anchors across sources
2. **Match Quality** (0-30): Pairwise score values from Unified_Scores
3. **Data Consistency** (0-20): Penalized by surname/state/name variations
4. **Cross-Source Corroboration** (0-10): Unique cross-source pair links

Penalties: overclustering (20+ records + 3+ surnames → cap 25), large clusters (50+ → -10), no anchors (cap 30), transitive-heavy (>70% → -8).

### COTM Value Framework

Three value drivers, client-facing language (GM/Macy's centric, no "Amperity" in content):
1. **Trusted Data**: Dedup Before/After, Source Completeness, Name Variants, Crosswalk, Cluster Quality
2. **Operate Faster**: Fleet Fragmentation, Stitch Performance, Drift Monitoring
3. **Acquire/Grow/Retain**: 5 activation-ready smart segments with live SQL

Badges say "Unique Capability" not "Unique to Amperity". Per-region mantras. Collapsible hero.

### MCP Bridge (v2)

```
Flask (5082) → HTTP → Bridge (5081) → JSON-RPC stdio → Lazy Proxy → Snowflake MCP Server → Cortex Agent
```

- No Anthropic API needed — Cortex Agents have Claude built in
- Three agents: nemo (data), nemosupport (support ops), nemoclientrelations (sales/CS)
- First query triggers browser SSO popup for Snowflake auth
- start_mcp_server.py patches: 300s timeout, SSE parser fix, response filtering
- The Nemo plugin's `snowflake-mcp-patched.py` is broken (bad `create_mcp_server` import)

### Multi-Region Config

Each region needs 8 `.env` values: NAME, TENANT, TOKEN_URL, CLIENT_ID, CLIENT_SECRET, DATABASE_ID, SEGMENT_ID, DATASET_ID. App auto-discovers `REGION_*` blocks.

Macy's: `REGION_MACYS_*`, tenant `macys`, database `db-gF8c22ZVbBB`.

### Visualization Patterns (demo page)

- **Dedup Resolution**: Horizontal bar chart rows, color-coded (yellow 40%+, teal 20%+, ocean 10%+). Source names strip `_stitch` suffix, underscores to spaces.
- **Source Completeness**: Heatmap grid with fill-rate percentages and color legend (yellow 90%+, teal 70%+, ocean 40%+, dusk 1%+, empty 0%).
- **Number formatting**: `fmtNum()` — B/M/K suffixes for large numbers.

## Workflow

When modifying the identity engine:

1. **Always work in v2**. Never touch v1.
2. **Read the target file** before making changes. Understand existing patterns.
3. **Match the theme**: light pages use Inter/Montserrat + white cards; dark pages use dark palette.
4. **Match the nav style**: add links to ALL nav bars when adding a new view (6 files to update).
5. **API endpoints**: add route in `app.py`, use `get_api()` for current region's Amperity client.
6. **New dashboard sections**: use `apiFetch()` wrapper, handle number-as-string conversion.
7. **Segment buttons**: `window.open()` first, then `navigator.clipboard.writeText()`. Pass `this` from onclick.
8. **Test both services** if touching AI/MCP: `python mcp_bridge.py` then `python app.py`.

## References

- Read `references/api-endpoints.md` for the full API endpoint table
- Read `references/env-config.md` for environment variable reference

## Edge Cases

- Amperity API can return empty arrays for some sources — always null-check before rendering
- Some tenants don't have all data sources — handle missing fields gracefully
- Stitch stats field: API returns `total_clusters` not `unique_ids` — COTM handles both
- `currentRegion` / `regionTenants` set by `loadRegions()` — used for dynamic Amperity deep links
- Number consistency: hero and dedup card both use `total_profiles` from cross-region pulse (not per-source sum which double-counts)
- `global` keyword required in Python route handlers that modify `ACTIVE_REGION`, `CORTEX_AGENT`
