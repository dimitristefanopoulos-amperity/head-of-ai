"""
Agentic Intelligence Pipeline
OBSERVE → EVALUATE → PROPOSE → GATE → EXECUTE → ACTIVATE → AUDIT

Each node is a function that transforms state. The pipeline runs as a
sequential chain with the GATE node routing to human review or auto-approval.

All operations use existing Amperity MCP tools and APIs. No new backend.
"""
import os
import json
import time
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field, asdict
from enum import Enum


class Severity(Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class ProposalAction(Enum):
    AUTO_APPROVED = "auto_approved"
    HUMAN_REVIEW = "human_review"
    BLOCKED = "blocked"


class ProposalStatus(Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXECUTED = "executed"
    FAILED = "failed"


@dataclass
class Observation:
    """Output of OBSERVE node."""
    id: str = field(default_factory=lambda: f"obs-{uuid.uuid4().hex[:8]}")
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    tenant: str = ""
    metric: str = ""
    current_value: Any = None
    previous_value: Any = None
    delta: float = 0.0
    delta_pct: float = 0.0
    severity: str = "info"
    detail: str = ""


@dataclass
class Evaluation:
    """Output of EVALUATE node."""
    observation_id: str = ""
    is_expected: bool = False
    business_impact: str = ""
    affected_segments: List[str] = field(default_factory=list)
    confidence: float = 0.0
    reasoning: str = ""


@dataclass
class Proposal:
    """Output of PROPOSE node."""
    id: str = field(default_factory=lambda: f"prop-{uuid.uuid4().hex[:8]}")
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    observation_id: str = ""
    tenant: str = ""
    action: str = ""
    description: str = ""
    estimated_amps: int = 0
    estimated_ai_cost: float = 0.0
    affected_records: int = 0
    confidence: float = 0.0
    reversible: bool = True
    downstream_impact: List[str] = field(default_factory=list)
    status: str = "pending"
    gate_decision: str = ""
    gate_reason: str = ""


@dataclass
class AuditEntry:
    """Output of AUDIT node."""
    id: str = field(default_factory=lambda: f"audit-{uuid.uuid4().hex[:8]}")
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    node: str = ""
    tenant: str = ""
    input_id: str = ""
    output_id: str = ""
    action: str = ""
    detail: str = ""
    user: str = ""


class ClientPolicy:
    """Client-attachable policy layer. Business intent, not Stitch config."""

    DEFAULT = {
        "identity_policies": {
            "default": {
                "confidence_gate": 0.90,
                "max_auto_merge": 10,
                "always_review_if_split": False,
                "skip_monitoring": False,
            }
        },
        "compute_policies": {
            "daily_amps_target": 50000,
            "optimize_for": "quality",  # quality | speed | cost
            "max_amps_per_proposal": 5000,
        },
        "monitoring_policies": {
            "drift_alert_threshold": 0.02,
            "segment_size_alert": 0.05,
            "supersized_cluster_threshold": 1000,
            "auto_approve_threshold": 0.95,
            "human_review_threshold": 0.70,
        },
        "gate_rules": {
            "auto_approve": {
                "min_confidence": 0.95,
                "must_be_reversible": True,
                "max_affected_records": 1000,
                "max_amps_cost": 1000,
            },
            "block": {
                "source_record_modification": True,
                "amps_budget_exceeded": True,
                "cluster_size_exceeded": True,
            }
        }
    }

    def __init__(self, policy_dict: Optional[Dict] = None):
        self.policy = policy_dict or self.DEFAULT.copy()

    def get(self, *keys, default=None):
        d = self.policy
        for k in keys:
            if isinstance(d, dict):
                d = d.get(k, default)
            else:
                return default
        return d

    @classmethod
    def from_yaml(cls, path: str):
        """Load policy from YAML file."""
        try:
            import yaml
            with open(path) as f:
                return cls(yaml.safe_load(f))
        except ImportError:
            # Fall back to JSON
            with open(path) as f:
                return cls(json.load(f))

    def to_dict(self):
        return self.policy


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE NODES
# ══════════════════════════════════════════════════════════════════════════════

def observe(tenant_data: Dict, previous_data: Optional[Dict] = None) -> List[Observation]:
    """
    OBSERVE: Detect changes in identity metrics.
    Compares current tenant state against previous snapshot.
    """
    observations = []
    current = tenant_data
    prev = previous_data or {}

    # Cluster count drift
    curr_profiles = int(float(current.get("total_profiles", 0)))
    prev_profiles = int(float(prev.get("total_profiles", 0)))
    if prev_profiles > 0:
        delta = curr_profiles - prev_profiles
        delta_pct = delta / prev_profiles * 100
        if abs(delta_pct) > 1:
            observations.append(Observation(
                tenant=current.get("name", ""),
                metric="cluster_count",
                current_value=curr_profiles,
                previous_value=prev_profiles,
                delta=delta,
                delta_pct=round(delta_pct, 2),
                severity="high" if abs(delta_pct) > 5 else "medium",
                detail=f"Cluster count {'increased' if delta > 0 else 'decreased'} by {abs(delta):,} ({abs(delta_pct):.1f}%)"
            ))

    # Dedup rate drift
    curr_dedup = float(current.get("dedup_rate", 0))
    prev_dedup = float(prev.get("dedup_rate", 0))
    if prev_dedup > 0:
        delta = curr_dedup - prev_dedup
        if abs(delta) > 1:
            observations.append(Observation(
                tenant=current.get("name", ""),
                metric="dedup_rate",
                current_value=curr_dedup,
                previous_value=prev_dedup,
                delta=round(delta, 2),
                delta_pct=round(delta / prev_dedup * 100, 2),
                severity="high" if abs(delta) > 3 else "medium",
                detail=f"Dedup rate {'improved' if delta > 0 else 'degraded'} by {abs(delta):.1f}pp"
            ))

    # Source count change
    curr_sources = int(float(current.get("source_count", 0)))
    prev_sources = int(float(prev.get("source_count", 0)))
    if prev_sources > 0 and curr_sources != prev_sources:
        observations.append(Observation(
            tenant=current.get("name", ""),
            metric="source_count",
            current_value=curr_sources,
            previous_value=prev_sources,
            delta=curr_sources - prev_sources,
            severity="high" if curr_sources > prev_sources else "medium",
            detail=f"Source count changed from {prev_sources} to {curr_sources}"
        ))

    # If no changes detected
    if not observations:
        observations.append(Observation(
            tenant=current.get("name", ""),
            metric="health_check",
            current_value="stable",
            severity="info",
            detail="No significant changes detected"
        ))

    return observations


def evaluate(observations: List[Observation], policy: ClientPolicy) -> List[Evaluation]:
    """
    EVALUATE: Contextualize observations against business rules.
    """
    evaluations = []
    for obs in observations:
        if obs.severity == "info":
            continue

        threshold = policy.get("monitoring_policies", "drift_alert_threshold", default=0.02)
        is_expected = abs(obs.delta_pct) < (threshold * 100) if obs.delta_pct else False

        impact = "low"
        if obs.metric == "cluster_count" and abs(obs.delta_pct or 0) > 5:
            impact = "high — cluster count shift affects all downstream segments"
        elif obs.metric == "dedup_rate" and (obs.delta or 0) < -2:
            impact = "high — dedup degradation means identity quality declining"
        elif obs.metric == "source_count":
            impact = "medium — new/removed sources change the identity graph"

        evaluations.append(Evaluation(
            observation_id=obs.id,
            is_expected=is_expected,
            business_impact=impact,
            confidence=0.85 if not is_expected else 0.95,
            reasoning=f"{'Expected variation' if is_expected else 'Unexpected change'}: {obs.detail}"
        ))

    return evaluations


def propose(observations: List[Observation], evaluations: List[Evaluation], policy: ClientPolicy) -> List[Proposal]:
    """
    PROPOSE: Generate actionable proposals based on evaluated observations.
    """
    proposals = []

    for obs, evl in zip(observations, evaluations):
        if obs.severity == "info":
            continue
        if evl.is_expected:
            continue

        if obs.metric == "dedup_rate" and (obs.delta or 0) < -2:
            proposals.append(Proposal(
                observation_id=obs.id,
                tenant=obs.tenant,
                action="investigate_dedup_degradation",
                description=f"Dedup rate dropped {abs(obs.delta):.1f}pp. Investigate source data quality changes, new bad values, or blocking strategy drift.",
                estimated_amps=500,
                estimated_ai_cost=0.05,
                affected_records=int(abs(obs.delta or 0) / 100 * int(float(obs.current_value or 0))),
                confidence=evl.confidence,
                reversible=True,
                downstream_impact=["segment sizes may have changed", "campaign audiences affected"],
            ))

        if obs.metric == "cluster_count" and abs(obs.delta_pct or 0) > 5:
            proposals.append(Proposal(
                observation_id=obs.id,
                tenant=obs.tenant,
                action="review_cluster_changes",
                description=f"Cluster count shifted {obs.delta_pct:.1f}%. Review supersized clusters and recent Stitch configuration changes.",
                estimated_amps=1000,
                estimated_ai_cost=0.10,
                affected_records=abs(int(obs.delta or 0)),
                confidence=evl.confidence,
                reversible=True,
                downstream_impact=["identity boundaries may have shifted", "unified customer count changed"],
            ))

        if obs.metric == "source_count" and (obs.delta or 0) > 0:
            proposals.append(Proposal(
                observation_id=obs.id,
                tenant=obs.tenant,
                action="validate_new_source",
                description=f"New data source detected. Validate data quality, PII completeness, and blocking strategy compatibility before next full Stitch run.",
                estimated_amps=2000,
                estimated_ai_cost=0.03,
                affected_records=0,
                confidence=0.90,
                reversible=True,
                downstream_impact=["new source will participate in next Stitch run"],
            ))

    return proposals


def gate(proposals: List[Proposal], policy: ClientPolicy) -> List[Proposal]:
    """
    GATE: Route proposals through approval paths based on client policy.
    """
    auto_rules = policy.get("gate_rules", "auto_approve", default={})
    block_rules = policy.get("gate_rules", "block", default={})
    max_amps = policy.get("compute_policies", "max_amps_per_proposal", default=5000)

    for prop in proposals:
        # Check block conditions
        if prop.estimated_amps > max_amps and block_rules.get("amps_budget_exceeded"):
            prop.gate_decision = ProposalAction.BLOCKED.value
            prop.gate_reason = f"Exceeds Amps budget ({prop.estimated_amps} > {max_amps})"
            prop.status = "rejected"
            continue

        # Check auto-approve conditions
        min_conf = auto_rules.get("min_confidence", 0.95)
        max_records = auto_rules.get("max_affected_records", 1000)
        max_cost = auto_rules.get("max_amps_cost", 1000)

        if (prop.confidence >= min_conf
                and prop.reversible
                and prop.affected_records <= max_records
                and prop.estimated_amps <= max_cost):
            prop.gate_decision = ProposalAction.AUTO_APPROVED.value
            prop.gate_reason = "Meets auto-approve criteria: high confidence, reversible, low impact"
            prop.status = "approved"
        else:
            prop.gate_decision = ProposalAction.HUMAN_REVIEW.value
            reasons = []
            if prop.confidence < min_conf:
                reasons.append(f"confidence {prop.confidence:.0%} < {min_conf:.0%}")
            if prop.affected_records > max_records:
                reasons.append(f"{prop.affected_records:,} records > {max_records:,} limit")
            if prop.estimated_amps > max_cost:
                reasons.append(f"{prop.estimated_amps} Amps > {max_cost} auto-limit")
            prop.gate_reason = f"Requires review: {', '.join(reasons)}"
            prop.status = "pending"

    return proposals


def audit(node: str, tenant: str, input_id: str, output_id: str, action: str, detail: str, user: str = "system") -> AuditEntry:
    """
    AUDIT: Create an append-only record of a pipeline action.
    """
    return AuditEntry(
        node=node,
        tenant=tenant,
        input_id=input_id,
        output_id=output_id,
        action=action,
        detail=detail,
        user=user,
    )


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE RUNNER
# ══════════════════════════════════════════════════════════════════════════════

class AgenticPipeline:
    """Runs the full OBSERVE → EVALUATE → PROPOSE → GATE pipeline."""

    def __init__(self, policy: Optional[ClientPolicy] = None):
        self.policy = policy or ClientPolicy()
        self.audit_log: List[AuditEntry] = []
        self.proposals: List[Proposal] = []
        self.observations: List[Observation] = []

    def run(self, tenant_data: Dict, previous_data: Optional[Dict] = None) -> Dict:
        """Run the full pipeline for a single tenant."""

        # OBSERVE
        observations = observe(tenant_data, previous_data)
        self.observations.extend(observations)
        for obs in observations:
            self.audit_log.append(audit("observe", obs.tenant, "", obs.id, "detected", obs.detail))

        # EVALUATE
        evaluations = evaluate(observations, self.policy)
        for evl in evaluations:
            self.audit_log.append(audit("evaluate", tenant_data.get("name", ""), evl.observation_id, "", "evaluated", evl.reasoning))

        # PROPOSE
        proposals = propose(observations, evaluations, self.policy)
        for prop in proposals:
            self.audit_log.append(audit("propose", prop.tenant, prop.observation_id, prop.id, "proposed", prop.description))

        # GATE
        gated = gate(proposals, self.policy)
        self.proposals.extend(gated)
        for prop in gated:
            self.audit_log.append(audit("gate", prop.tenant, prop.id, "", prop.gate_decision, prop.gate_reason))

        return {
            "tenant": tenant_data.get("name", ""),
            "observations": [asdict(o) for o in observations],
            "evaluations": [asdict(e) for e in evaluations],
            "proposals": [asdict(p) for p in gated],
            "audit": [asdict(a) for a in self.audit_log[-len(observations)*4:]],
            "policy": self.policy.to_dict(),
        }

    def approve(self, proposal_id: str, user: str = "human") -> Optional[Proposal]:
        """Human approves a pending proposal."""
        for p in self.proposals:
            if p.id == proposal_id and p.status == "pending":
                p.status = "approved"
                p.gate_decision = "human_approved"
                self.audit_log.append(audit("gate", p.tenant, p.id, "", "human_approved", f"Approved by {user}", user))
                return p
        return None

    def reject(self, proposal_id: str, user: str = "human", reason: str = "") -> Optional[Proposal]:
        """Human rejects a pending proposal."""
        for p in self.proposals:
            if p.id == proposal_id and p.status == "pending":
                p.status = "rejected"
                p.gate_decision = "human_rejected"
                self.audit_log.append(audit("gate", p.tenant, p.id, "", "human_rejected", reason or "Rejected by human", user))
                return p
        return None

    def get_pending(self) -> List[Dict]:
        """Get all proposals awaiting human review."""
        return [asdict(p) for p in self.proposals if p.status == "pending"]

    def get_audit_log(self) -> List[Dict]:
        """Get the full audit trail."""
        return [asdict(a) for a in self.audit_log]

    def get_cost_summary(self) -> Dict:
        """Summarize estimated costs across all proposals."""
        total_amps = sum(p.estimated_amps for p in self.proposals)
        total_ai = sum(p.estimated_ai_cost for p in self.proposals)
        budget = self.policy.get("compute_policies", "daily_amps_target", default=50000)
        return {
            "total_estimated_amps": total_amps,
            "total_estimated_ai_cost": round(total_ai, 4),
            "daily_amps_budget": budget,
            "budget_utilization_pct": round(total_amps / budget * 100, 1) if budget else 0,
            "proposals_count": len(self.proposals),
            "auto_approved": sum(1 for p in self.proposals if p.gate_decision == "auto_approved"),
            "pending_review": sum(1 for p in self.proposals if p.status == "pending"),
            "blocked": sum(1 for p in self.proposals if p.gate_decision == "blocked"),
        }
