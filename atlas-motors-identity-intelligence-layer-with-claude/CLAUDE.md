# CLAUDE.md

## Identity

Dimitri Stefanopoulos. Amperity — enterprise account management, identity resolution, and client-facing analytics. Works across multiple Amperity tenants building identity explainability tools and COTM-framed deliverables.

## Active Projects

- **macys-identity-engine v2** — Flask POC for multi-region identity resolution analytics. Six views: Identity Search, Data Quality, COTM Value View, Demo, Presentation, AI Assistant. Nemo/Cortex Agent integration via MCP bridge. Live production data from Amperity API.

## Communication Style

Direct and technical. No hedging. Co-author register, not assistant register. Lead with the answer, then explain if needed.

## Conventions

- Dark theme aesthetic for demo/presentation views (#0C0C0C bg, #54d3de brand, #EAFF5F accent, Montserrat)
- Amperity brand palette: DUSK (#004B57), TEAL (#54D3DE), OCEAN (#00A0B2), ICE (#ABF4F7), AMP_YELLOW (#EAFF5F)
- COTM (Command of the Message) framework for client-facing language
- Flask + vanilla JS stack. No build tools. Python backend, static HTML frontend.
- Amperity MCP tools available for tenant queries, stitch reports, database operations

## Skills Available

- **macys-identity-engine** (Swiss Army Knife) — Full project context: architecture, API patterns, design systems, confidence scoring, COTM framework, MCP bridge, visualizations. See `skills/macys-identity-engine/SKILL.md`

## Important Context

- Amperity API returns numbers as strings — always wrap with Number() in JS, float()/int() in Python
- Transit+JSON 3-step flow: POST → Poll status → GET results
- Flask must run threaded=True — dashboard fires parallel fetch calls
- Amperity deep links use hash routing (SPA) — draft links only work from within the app
