# API Endpoints

## Identity
| Method | Route | Purpose |
|--------|-------|---------|
| GET | `/api/search?q={query}` | Smart search (name/email/phone/AMPID) |
| GET | `/api/explain/{ampid}` | Full cluster explainability |

## Regions
| Method | Route | Purpose |
|--------|-------|---------|
| GET | `/api/regions` | List regions with active flag + tenant |
| POST | `/api/regions/{id}/activate` | Switch active region |
| GET | `/api/cross-region-pulse` | All regions summary stats |

## Data Quality (active region)
| Method | Route | Purpose |
|--------|-------|---------|
| GET | `/api/dedup-scorecard` | Per-source dedup rates |
| GET | `/api/source-scorecard` | Per-source completeness |
| GET | `/api/source-overlap` | Cross-source shared identities |
| GET | `/api/name-variants` | Name variation analysis (top 50) |
| GET | `/api/name-variants-count` | True total name variant count |
| GET | `/api/crosswalk-sample` | Multi-source golden records |
| GET | `/api/cluster-health/*` | Oversized, multi-surname, distribution |
| GET | `/api/fleet-fragmentation` | B2B account fragmentation (top 50) |
| GET | `/api/fleet-fragmentation-count` | True total fragmented clusters |
| GET | `/api/stitch-stats` | Identity resolution metrics |
| GET | `/api/stitch-score-distribution` | Match score histogram |
| GET | `/api/stitch-records-per-source` | Records by source |

## Segments & Actions
| Method | Route | Purpose |
|--------|-------|---------|
| POST | `/api/save-segment` | Create segment (name + SQL) |
| POST | `/api/segment-preview` | Run ad-hoc SQL, return sample + total |
| POST | `/api/segment-demographics` | State + source distribution |

## Drift Monitoring
| Method | Route | Purpose |
|--------|-------|---------|
| POST | `/api/drift/snapshot` | Take drift measurement |
| GET | `/api/drift/alerts` | Active drift alerts |
| GET | `/api/drift/history` | Drift snapshot history |

## AI Assistant (v2 only)
| Method | Route | Purpose |
|--------|-------|---------|
| POST | `/api/ask-ai` | Query Cortex Agent (nemo/nemosupport/nemoclientrelations) |
| GET | `/api/list-agents` | List available Cortex Agents |
