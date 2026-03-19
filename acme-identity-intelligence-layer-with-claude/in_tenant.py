"""
In-Tenant Agentic Layer
Embedded intelligence that runs inside a single tenant app.
Can observe, diagnose, propose, and (with approval) execute actions via MCP.

This is the client harness — each tenant gets its own policy and its own
agentic loop that operates within the boundaries the client defines.
"""
import os
import json
import time
import uuid
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field, asdict

from pipeline import ClientPolicy, Observation, Proposal, AuditEntry, ProposalAction, audit


@dataclass
class TenantAction:
    """An executable action against the tenant via MCP or API."""
    id: str = field(default_factory=lambda: f"act-{uuid.uuid4().hex[:8]}")
    proposal_id: str = ""
    action_type: str = ""  # bad_values, courier_run, segment_create, stitch_config, query
    description: str = ""
    mcp_tool: str = ""     # which MCP tool to call
    mcp_params: Dict = field(default_factory=dict)
    status: str = "pending"  # pending, approved, executing, completed, failed
    result: Any = None
    estimated_amps: int = 0
    estimated_ai_cost: float = 0.0


class InTenantAgent:
    """
    Agentic intelligence embedded in a single tenant.

    Capabilities:
    - Observe: Pull identity health, dedup, cluster quality from the tenant's own API
    - Diagnose: Detect issues (supersized clusters, bad values, source quality)
    - Propose: Generate specific actions (add bad values, create segments, run queries)
    - Gate: Route through client policy (auto/review/block)
    - Execute: (With approval) Call MCP tools or API endpoints
    - Audit: Log everything
    """

    def __init__(self, base_url: str, tenant_name: str, policy: Optional[ClientPolicy] = None):
        self.base_url = base_url
        self.tenant_name = tenant_name
        self.policy = policy or ClientPolicy()
        self.audit_log: List[AuditEntry] = []
        self.proposals: List[Proposal] = []
        self.actions: List[TenantAction] = []
        self.last_snapshot: Dict = {}

    def _api(self, path: str, timeout: int = 30) -> Any:
        """Call the tenant's own API."""
        r = requests.get(f"{self.base_url}{path}", timeout=timeout)
        return r.json() if r.status_code == 200 else {"error": r.text[:200]}

    # ── OBSERVE ───────────────────────────────────────────────────────────

    def observe(self) -> Dict:
        """Pull current tenant state and detect issues."""
        state = {}

        # Identity health
        try:
            pulse = self._api("/api/cross-region-pulse", timeout=45)
            regions = pulse.get("regions", pulse)
            if isinstance(regions, dict) and regions:
                data = list(regions.values())[0]
                state["total_profiles"] = int(float(data.get("total_profiles", 0)))
                state["total_records"] = int(float(data.get("total_records", 0)))
                state["dedup_rate"] = float(data.get("dedup_rate", data.get("overall_dedup_rate", 0)))
                state["source_count"] = int(float(data.get("source_count", 0)))
        except Exception as e:
            state["error_pulse"] = str(e)

        # Dedup by source
        try:
            dedup = self._api("/api/dedup-scorecard", timeout=45)
            state["sources"] = dedup if isinstance(dedup, list) else []
        except Exception as e:
            state["error_dedup"] = str(e)

        # Cluster health
        try:
            clusters = self._api("/api/cluster-health/distribution", timeout=45)
            state["clusters"] = clusters if isinstance(clusters, list) else []
        except:
            state["clusters"] = []

        # Drift alerts
        try:
            alerts = self._api("/api/drift/alerts")
            state["drift_alerts"] = alerts if isinstance(alerts, list) else []
        except:
            state["drift_alerts"] = []

        self._log("observe", "full_scan", json.dumps({k: v for k, v in state.items() if not isinstance(v, list)}, default=str)[:200])

        self.last_snapshot = state
        return state

    # ── DIAGNOSE ──────────────────────────────────────────────────────────

    def diagnose(self, state: Optional[Dict] = None) -> List[Dict]:
        """Analyze tenant state and identify issues with specific recommendations."""
        state = state or self.last_snapshot
        issues = []

        # Check dedup rate
        dedup = state.get("dedup_rate", 0)
        if dedup < 30:
            issues.append({
                "severity": "critical",
                "issue": f"Very low dedup rate ({dedup:.1f}%)",
                "detail": "Less than 30% of records are being deduplicated. Source data may lack sufficient PII for matching.",
                "suggested_action": "investigate_sources",
            })
        elif dedup < 50:
            issues.append({
                "severity": "high",
                "issue": f"Below-average dedup rate ({dedup:.1f}%)",
                "detail": "Dedup rate below 50% suggests matching opportunities are being missed.",
                "suggested_action": "review_blocking_strategy",
            })

        # Check sources for extreme dedup rates
        for src in state.get("sources", []):
            src_dedup = float(src.get("dedup_rate_pct", 0))
            src_name = src.get("datasource", "unknown")
            src_records = int(float(src.get("source_records", 0)))

            if src_dedup > 85 and src_records > 10000:
                issues.append({
                    "severity": "high",
                    "issue": f"Suspiciously high dedup: {src_name} ({src_dedup:.0f}%)",
                    "detail": f"{src_records:,} records with {src_dedup:.0f}% dedup may indicate over-merging or bad value contamination.",
                    "suggested_action": "investigate_source_quality",
                    "source": src_name,
                })
            elif src_dedup < 2 and src_records > 10000:
                issues.append({
                    "severity": "medium",
                    "issue": f"Very low dedup: {src_name} ({src_dedup:.1f}%)",
                    "detail": f"{src_records:,} records with near-zero dedup. May lack shared PII with other sources.",
                    "suggested_action": "enrich_source_data",
                    "source": src_name,
                })

        # Check for supersized clusters
        for bucket in state.get("clusters", []):
            label = str(bucket.get("bucket", bucket.get("cluster_size_bucket", "")))
            count = int(float(bucket.get("cluster_count", bucket.get("count", 0))))
            if "1000" in label and count > 0:
                issues.append({
                    "severity": "critical",
                    "issue": f"{count} supersized clusters (1000+ records)",
                    "detail": "Clusters this large almost always indicate bad value contamination, FK chain issues, or non-person entities being stitched.",
                    "suggested_action": "audit_supersized_clusters",
                })

        # Check drift alerts
        for alert in state.get("drift_alerts", []):
            issues.append({
                "severity": "high",
                "issue": f"Active drift alert: {alert.get('metric', 'unknown')}",
                "detail": alert.get("detail", ""),
                "suggested_action": "investigate_drift",
            })

        if not issues:
            issues.append({
                "severity": "info",
                "issue": "No issues detected",
                "detail": f"Dedup rate {dedup:.1f}%, {state.get('source_count', 0)} sources, no active drift alerts.",
                "suggested_action": "none",
            })

        self._log("diagnose", "analysis", f"{len(issues)} issues found")
        return issues

    # ── PROPOSE ───────────────────────────────────────────────────────────

    def propose(self, issues: List[Dict]) -> List[Proposal]:
        """Generate executable proposals from diagnosed issues."""
        proposals = []

        for issue in issues:
            if issue["severity"] == "info":
                continue

            action = issue.get("suggested_action", "")

            if action == "audit_supersized_clusters":
                proposals.append(Proposal(
                    tenant=self.tenant_name,
                    action="query_supersized_clusters",
                    description=f"Run diagnostic query to identify the largest clusters and their composition. {issue['detail']}",
                    estimated_amps=200,
                    estimated_ai_cost=0.01,
                    affected_records=0,
                    confidence=0.95,
                    reversible=True,
                    downstream_impact=["diagnostic only — no data changes"],
                ))

            elif action == "investigate_source_quality":
                source = issue.get("source", "")
                proposals.append(Proposal(
                    tenant=self.tenant_name,
                    action="query_source_sample",
                    description=f"Sample records from {source} to identify bad values or over-merging patterns.",
                    estimated_amps=100,
                    estimated_ai_cost=0.01,
                    affected_records=0,
                    confidence=0.90,
                    reversible=True,
                    downstream_impact=["diagnostic only"],
                ))

            elif action == "investigate_sources":
                proposals.append(Proposal(
                    tenant=self.tenant_name,
                    action="full_source_audit",
                    description=f"Run complete source audit: field completeness, PII coverage, and blocking strategy compatibility.",
                    estimated_amps=500,
                    estimated_ai_cost=0.03,
                    affected_records=0,
                    confidence=0.85,
                    reversible=True,
                    downstream_impact=["diagnostic only — generates recommendations"],
                ))

            elif action == "review_blocking_strategy":
                proposals.append(Proposal(
                    tenant=self.tenant_name,
                    action="blocking_review",
                    description="Analyze current blocking strategy effectiveness. Check if additional strategies could improve matching without increasing false positives.",
                    estimated_amps=300,
                    estimated_ai_cost=0.02,
                    affected_records=0,
                    confidence=0.80,
                    reversible=True,
                    downstream_impact=["may recommend config changes for next Stitch run"],
                ))

            elif action == "investigate_drift":
                proposals.append(Proposal(
                    tenant=self.tenant_name,
                    action="drift_investigation",
                    description=f"Investigate drift alert: {issue['detail']}. Compare current vs previous Stitch run output.",
                    estimated_amps=200,
                    estimated_ai_cost=0.02,
                    affected_records=0,
                    confidence=0.90,
                    reversible=True,
                    downstream_impact=["diagnostic only"],
                ))

        # Gate proposals through policy
        from pipeline import gate
        gated = gate(proposals, self.policy)
        self.proposals.extend(gated)

        for p in gated:
            self._log("propose", p.id, f"{p.action}: {p.gate_decision}")

        return gated

    # ── EXECUTE ───────────────────────────────────────────────────────────

    def execute(self, proposal_id: str) -> Optional[TenantAction]:
        """Execute an approved proposal. Currently supports diagnostic queries only."""
        prop = next((p for p in self.proposals if p.id == proposal_id and p.status == "approved"), None)
        if not prop:
            return None

        action = TenantAction(
            proposal_id=proposal_id,
            action_type=prop.action,
            description=prop.description,
            estimated_amps=prop.estimated_amps,
            estimated_ai_cost=prop.estimated_ai_cost,
        )

        try:
            if prop.action == "query_supersized_clusters":
                # Run the diagnostic query
                result = self._api("/api/cluster-health/oversized", timeout=60)
                action.result = result
                action.status = "completed"

            elif prop.action == "query_source_sample":
                result = self._api("/api/source-scorecard", timeout=45)
                action.result = result
                action.status = "completed"

            elif prop.action == "full_source_audit":
                result = self._api("/api/source-scorecard", timeout=45)
                action.result = result
                action.status = "completed"

            elif prop.action == "blocking_review":
                # Pull stitch stats for analysis
                result = self._api("/api/stitch-stats", timeout=45)
                action.result = result
                action.status = "completed"

            elif prop.action == "drift_investigation":
                result = self._api("/api/drift/history", timeout=30)
                action.result = result
                action.status = "completed"

            else:
                action.status = "failed"
                action.result = {"error": f"Unknown action type: {prop.action}"}

        except Exception as e:
            action.status = "failed"
            action.result = {"error": str(e)}

        prop.status = "executed" if action.status == "completed" else "failed"
        self.actions.append(action)
        self._log("execute", action.id, f"{action.action_type}: {action.status}")

        return action

    # ── APPROVE / REJECT ──────────────────────────────────────────────────

    def approve(self, proposal_id: str, user: str = "operator") -> Optional[Proposal]:
        for p in self.proposals:
            if p.id == proposal_id and p.status == "pending":
                p.status = "approved"
                p.gate_decision = "human_approved"
                self._log("gate", p.id, f"Approved by {user}", user)
                return p
        return None

    def reject(self, proposal_id: str, user: str = "operator", reason: str = "") -> Optional[Proposal]:
        for p in self.proposals:
            if p.id == proposal_id and p.status == "pending":
                p.status = "rejected"
                p.gate_decision = "human_rejected"
                self._log("gate", p.id, reason or f"Rejected by {user}", user)
                return p
        return None

    # ── RUN FULL CYCLE ────────────────────────────────────────────────────

    def run_cycle(self) -> Dict:
        """Run complete OBSERVE → DIAGNOSE → PROPOSE → GATE cycle."""
        state = self.observe()
        issues = self.diagnose(state)
        proposals = self.propose(issues)

        return {
            "tenant": self.tenant_name,
            "state": {k: v for k, v in state.items() if not isinstance(v, list)},
            "issues": issues,
            "proposals": [asdict(p) for p in proposals],
            "pending": [asdict(p) for p in self.proposals if p.status == "pending"],
            "audit": [asdict(a) for a in self.audit_log[-20:]],
            "policy": self.policy.to_dict(),
        }

    # ── HELPERS ────────────────────────────────────────────────────────────

    def _log(self, node: str, ref: str, detail: str, user: str = "system"):
        self.audit_log.append(audit(node, self.tenant_name, ref, "", node, detail, user))

    def get_status(self) -> Dict:
        return {
            "tenant": self.tenant_name,
            "proposals_total": len(self.proposals),
            "proposals_pending": sum(1 for p in self.proposals if p.status == "pending"),
            "proposals_approved": sum(1 for p in self.proposals if p.status == "approved"),
            "proposals_executed": sum(1 for p in self.proposals if p.status == "executed"),
            "actions_completed": sum(1 for a in self.actions if a.status == "completed"),
            "audit_entries": len(self.audit_log),
            "total_amps": sum(p.estimated_amps for p in self.proposals),
            "total_ai_cost": round(sum(p.estimated_ai_cost for p in self.proposals), 4),
        }
