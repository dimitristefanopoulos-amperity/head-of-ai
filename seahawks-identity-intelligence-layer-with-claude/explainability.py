"""
Merge Reasoning Engine & Confidence Scoring — GM Multi-Region
"""

# ── Source Name Maps (per region) ──────────────────────────────────────────────
# These get populated at startup via discover_sources() or manually.
# Fall back to friendly_source() for unknown datasources.

GM_SOURCE_NAMES = {
    # US (examples — will be populated from actual tenant data)
    "CDT_Customer_SFTP": "Customer SFTP",
    "CDT_Delta_Share": "Delta Share",
    # MEO
    "MEO_225931_SALESFORCE_CONTACT": "Salesforce Contact",
    "MEO_225931_SALESFORCE_LEAD": "Salesforce Lead",
    "CDT_Siebel_Service_Stitch": "Siebel Service",
    "CDT_Siebel_Opportunity_Stitch": "Siebel Opportunity",
    "CDT_Siebel_Sales_Stitch": "Siebel Sales",
    "CDT_GAA_Onstar_Stitch": "OnStar",
    "CDT_Siebel_Handraiser": "Siebel Handraiser",
    # Mexico
    "CDT_SalesForce_Opportunity": "Salesforce Opportunity",
    "CDT_Sofia_Sales": "Sofia Sales",
    "CDT_Onstar_MX": "OnStar Mexico",
}


def friendly_source(raw):
    """Strip CDT_/MEO_ prefixes and _stitch suffix for readability."""
    if raw in GM_SOURCE_NAMES:
        return GM_SOURCE_NAMES[raw]
    s = raw
    for prefix in ("CDT_", "MEO_225931_", "MEO_"):
        if s.startswith(prefix):
            s = s[len(prefix):]
    if s.endswith("_Stitch") or s.endswith("_stitch"):
        s = s[:-7]
    return s.replace("_", " ").title()


# ── Field Extraction ──────────────────────────────────────────────────────────

def _extract_fields(records):
    """Extract distinct non-null values per field across all source records."""
    fields = {}
    for r in records:
        for k in ("given_name", "surname", "email", "phone", "address",
                   "city", "state", "postal", "gender", "birthdate"):
            v = r.get(k)
            if v and str(v).strip():
                fields.setdefault(k, set()).add(str(v).strip())
    # Convert sets to sorted lists
    return {k: sorted(v) for k, v in fields.items()}


# ── Signal Detection ──────────────────────────────────────────────────────────

def _detect_signals(records, fields):
    """Identify merge signals: shared PII values across sources."""
    sources_by_field = {}
    for r in records:
        ds = r.get("datasource", "unknown")
        for f in ("email", "phone", "surname", "given_name", "address", "postal"):
            v = r.get(f)
            if v and str(v).strip():
                key = (f, str(v).strip().upper())
                sources_by_field.setdefault(key, set()).add(ds)

    signals = []
    seen = set()
    for (field, value), srcs in sorted(sources_by_field.items(), key=lambda x: -len(x[1])):
        if len(srcs) >= 2 and (field, value) not in seen:
            seen.add((field, value))
            strength = "strong" if len(srcs) >= 3 else "moderate"
            signals.append({
                "field": field,
                "value": value[:60],
                "sources": sorted(srcs),
                "source_count": len(srcs),
                "strength": strength,
            })
    return sorted(signals, key=lambda s: (-s["source_count"], s["field"]))


# ── Variation Detection ───────────────────────────────────────────────────────

def _detect_variations(fields):
    """Detect surname changes, name typos, multi-state, etc."""
    variations = []
    if len(fields.get("surname", [])) > 1:
        variations.append({
            "type": "surname_variation",
            "severity": "high",
            "detail": f"Multiple surnames: {', '.join(fields['surname'][:5])}",
            "values": fields["surname"][:5],
        })
    if len(fields.get("given_name", [])) > 1:
        variations.append({
            "type": "given_name_variation",
            "severity": "low",
            "detail": f"Name variants: {', '.join(fields['given_name'][:5])}",
            "values": fields["given_name"][:5],
        })
    if len(fields.get("state", [])) > 1:
        variations.append({
            "type": "multi_state",
            "severity": "medium",
            "detail": f"Multiple states: {', '.join(fields['state'][:5])}",
            "values": fields["state"][:5],
        })
    if len(fields.get("email", [])) > 2:
        variations.append({
            "type": "email_variation",
            "severity": "medium",
            "detail": f"{len(fields['email'])} distinct emails",
            "values": fields["email"][:5],
        })
    return variations


# ── Pairwise Score Analysis ───────────────────────────────────────────────────

def _analyze_scores(scores):
    """Classify pairwise links, compute stats."""
    if not scores:
        return {
            "avg": 0, "min": 0, "max": 0, "count": 0,
            "direct_pairs": 0, "transitive_pairs": 0, "transitive_ratio": 0,
            "by_category": {},
        }

    score_vals = []
    match_types = {}
    cross_source = set()
    by_category = {}

    for s in scores:
        sv = s.get("score")
        if sv is not None:
            try:
                score_vals.append(float(sv))
            except (ValueError, TypeError):
                pass
        mt = (s.get("match_type") or "scored").lower()
        match_types[mt] = match_types.get(mt, 0) + 1
        cat = s.get("match_category", "UNKNOWN")
        by_category[cat] = by_category.get(cat, 0) + 1
        s1, s2 = s.get("source1", ""), s.get("source2", "")
        if s1 and s2 and s1 != s2:
            cross_source.add(tuple(sorted([s1, s2])))

    trans = match_types.get("scored_transitive", 0)
    direct = match_types.get("scored", 0) + match_types.get("trivial_duplicate", 0)
    total = trans + direct or 1

    return {
        "avg": round(sum(score_vals) / len(score_vals), 2) if score_vals else 0,
        "min": round(min(score_vals), 2) if score_vals else 0,
        "max": round(max(score_vals), 2) if score_vals else 0,
        "count": len(scores),
        "direct_pairs": direct,
        "transitive_pairs": trans,
        "transitive_ratio": round(trans / total, 2),
        "by_category": by_category,
        "cross_source_pairs": len(cross_source),
    }


# ── 4-Component Confidence Score ──────────────────────────────────────────────

def _compute_confidence(signals, score_stats, fields, variations, n_records, n_sources):
    """Compute the 4-component composite confidence score (0-100)."""

    strong = [s for s in signals if s["strength"] == "strong"]
    moderate = [s for s in signals if s["strength"] == "moderate"]

    # Component 1: Signal Strength (0-40)
    signal_pts = min(40, len(strong) * 20 + len(moderate) * 5)
    signal_detail = f"{len(strong)} strong, {len(moderate)} supporting anchors"

    # Component 2: Pairwise Match Quality (0-30)
    score_pts = 0
    if score_stats["count"] > 0 and score_stats["avg"] > 0:
        avg_c = min(score_stats["avg"] / 5.0, 1.0) * 20
        min_c = min(max(score_stats["min"], 0) / 5.0, 1.0) * 10
        score_pts = round(avg_c + min_c)
    elif len(strong) >= 1:
        score_pts = 10
    score_detail = (f"avg={score_stats['avg']}, min={score_stats['min']}, "
                    f"{score_stats['direct_pairs']} direct + {score_stats['transitive_pairs']} transitive")

    # Component 3: Data Consistency (0-20)
    consistency_pts = 20
    surname_vars = [v for v in variations if v["type"] == "surname_variation"]
    multi_state = [v for v in variations if v["type"] == "multi_state"]
    name_vars = [v for v in variations if v["type"] == "given_name_variation"]
    if surname_vars:
        n_sur = len(fields.get("surname", []))
        consistency_pts -= min(10, n_sur * 4)
    if multi_state:
        consistency_pts -= 4
    if name_vars:
        consistency_pts -= 2
    consistency_pts = max(0, consistency_pts)
    consistency_detail = (f"{len(fields.get('surname',[]))} surnames, "
                          f"{len(fields.get('state',[]))} states, "
                          f"{len(variations)} total variations")

    # Component 4: Cross-Source Corroboration (0-10)
    cross_pts = min(10, score_stats.get("cross_source_pairs", 0) * 3) if n_sources >= 2 else 0
    cross_detail = f"{score_stats.get('cross_source_pairs',0)} cross-source pairs across {n_sources} systems"

    # Raw composite
    raw = signal_pts + score_pts + consistency_pts + cross_pts

    # Penalties
    penalties = []
    n_surnames = len(fields.get("surname", []))
    if n_records >= 20 and n_surnames >= 3:
        raw = min(raw, 25)
        penalties.append(f"Overclustering risk: {n_records} records with {n_surnames} surnames")
    if n_records >= 50:
        raw -= 10
        penalties.append(f"Large cluster penalty: {n_records} records")
    if not strong and score_stats["count"] == 0:
        raw = min(raw, 30)
        penalties.append("No strong identity anchors and no pairwise scores")
    if score_stats["transitive_ratio"] > 0.7 and score_stats["count"] > 3:
        raw -= 8
        penalties.append(f"Transitive-heavy: {int(score_stats['transitive_ratio']*100)}% of links are indirect")
    if score_stats["min"] < 3.0 and score_stats["count"] > 0 and not strong:
        raw -= 5
        penalties.append(f"Weak minimum link: min score {score_stats['min']}")

    final = max(0, min(100, raw))
    if final >= 85:
        label = "Very High"
    elif final >= 70:
        label = "High"
    elif final >= 50:
        label = "Moderate"
    else:
        label = "Low"

    return {
        "score": final,
        "label": label,
        "components": {
            "signal_strength": {"points": signal_pts, "max": 40, "detail": signal_detail},
            "match_quality": {"points": score_pts, "max": 30, "detail": score_detail},
            "data_consistency": {"points": consistency_pts, "max": 20, "detail": consistency_detail},
            "cross_source": {"points": cross_pts, "max": 10, "detail": cross_detail},
        },
        "penalties_applied": penalties,
    }


# ── Root Cause Diagnosis & Recommendations ───────────────────────────────────

def _diagnose_cluster(records, fields, signals, variations, score_stats, confidence):
    """Diagnose root cause of problematic clusters and generate recommendations."""
    n = len(records)
    sources = set(r.get("datasource", "") for r in records)
    n_sources = len(sources)
    n_surnames = len(fields.get("surname", []))
    n_phones = len(fields.get("phone", []))
    n_emails = len(fields.get("email", []))
    given_names = fields.get("given_name", [])

    diagnosis = []
    recommendations = []

    # Pattern 1: Non-person entity contamination (Honda inventory case)
    non_person_names = {"INVENTORY", "UNKNOWN", "TEST", "DEALER", "SERVICE", "PARTS",
                        "SHOP", "STORE", "COMPANY", "BUSINESS", "FLEET", "VEHICLE"}
    person_names = [n for n in given_names if n.upper() not in non_person_names]
    non_person = [n for n in given_names if n.upper() in non_person_names]

    if non_person and n > 10:
        entity_type = non_person[0]
        diagnosis.append({
            "type": "non_person_entity",
            "severity": "critical",
            "title": f"Non-person records in cluster",
            "detail": f"Records have given_name='{entity_type}' — these appear to be {entity_type.lower()} records, not customers. Stitch is treating them as person records and matching on shared fields (likely phone numbers).",
        })
        recommendations.append({
            "priority": 1,
            "action": f"Filter '{entity_type}' records from Stitch input",
            "detail": f"Add a filter to the source table excluding records where given_name='{entity_type}' or is blank. These records should not be processed through identity resolution.",
            "type": "data_filter"
        })

    # Pattern 2: Single-source overclustering via shared values
    if n_sources == 1 and n > 20 and n_surnames > 10:
        source = list(sources)[0]
        diagnosis.append({
            "type": "single_source_overclustering",
            "severity": "critical",
            "title": "Single-source cluster with excessive merging",
            "detail": f"All {n} records come from {friendly_source(source)} with {n_surnames} distinct surnames. This suggests a shared field (likely phone) is causing transitive chain clustering within the same source.",
        })
        recommendations.append({
            "priority": 1,
            "action": "Investigate shared match keys in this source",
            "detail": f"Check which phones or emails appear across many records in {friendly_source(source)}. These shared values (likely business/dealer phones) should be added to the bad values blocklist.",
            "type": "bad_values"
        })

    # Pattern 3: Transitive chain explosion
    if score_stats["transitive_ratio"] > 0.5 and score_stats["count"] > 5:
        diagnosis.append({
            "type": "transitive_chain",
            "severity": "high",
            "title": "Transitive chain clustering",
            "detail": f"{int(score_stats['transitive_ratio']*100)}% of links are transitive (indirect). Records are connected through shared intermediaries: A matches B via phone, B matches C via email, so A-B-C merge even though A and C share nothing directly.",
        })
        if not any(r["type"] == "bad_values" for r in recommendations):
            recommendations.append({
                "priority": 2,
                "action": "Review blocking strategy and bad values",
                "detail": "Transitive chains often form around shared bad values (business phones, generic emails). Add high-frequency shared values to the blocklist. Consider tightening the blocking strategy to reduce the candidate pair space.",
                "type": "stitch_config"
            })

    # Pattern 4: Phone-only matching (low field completeness)
    phone_pct = sum(1 for r in records if r.get("phone") and str(r.get("phone")).strip()) / max(n, 1) * 100
    email_pct = sum(1 for r in records if r.get("email") and str(r.get("email")).strip()) / max(n, 1) * 100
    name_pct = sum(1 for r in records if r.get("given_name") and str(r.get("given_name")).strip()) / max(n, 1) * 100
    addr_pct = sum(1 for r in records if r.get("address") and str(r.get("address")).strip()) / max(n, 1) * 100

    if phone_pct > 80 and email_pct < 10 and addr_pct < 10 and n > 10:
        diagnosis.append({
            "type": "phone_only_matching",
            "severity": "high",
            "title": "Phone-only identity signal",
            "detail": f"This cluster has {phone_pct:.0f}% phone coverage but <10% email and address. Matching relies almost entirely on phone numbers, which are unreliable when shared across entities (businesses, dealers, households).",
        })
        recommendations.append({
            "priority": 2,
            "action": "Enrich source data or restrict phone-only matching",
            "detail": "Consider requiring a secondary signal (name + phone, not phone alone) for matches. Alternatively, enrich the source data with email or address to provide additional matching dimensions.",
            "type": "data_quality"
        })

    # Pattern 5: Shared business phone (many records, few unique phones)
    if n_phones > 0 and n > 20:
        phone_to_record_ratio = n / n_phones if n_phones else 0
        if phone_to_record_ratio > 3:
            # Many records sharing few phones
            diagnosis.append({
                "type": "shared_phone_values",
                "severity": "high",
                "title": "Shared phone numbers driving false merges",
                "detail": f"{n} records sharing {n_phones} unique phones ({phone_to_record_ratio:.1f} records per phone). These phones likely belong to businesses or dealers, not individuals.",
            })
            recommendations.append({
                "priority": 1,
                "action": "Add high-frequency phones to bad values blocklist",
                "detail": f"Identify phones appearing on 5+ records and add them to the bad values configuration with a threshold appropriate for this source.",
                "type": "bad_values"
            })

    # Pattern 6: Healthy cluster (no issues)
    if not diagnosis and confidence["score"] >= 70:
        diagnosis.append({
            "type": "healthy",
            "severity": "none",
            "title": "Cluster looks healthy",
            "detail": f"Confidence score {confidence['score']}/100. No red flags detected — matching signals are consistent and cross-source corroboration supports the merge.",
        })

    # Sort recommendations by priority
    recommendations.sort(key=lambda r: r["priority"])

    return {
        "diagnosis": diagnosis,
        "recommendations": recommendations,
        "has_issues": any(d["severity"] in ("critical", "high") for d in diagnosis),
    }


# ── Narrative Generation ──────────────────────────────────────────────────────

def _generate_narrative(records, fields, signals, variations, confidence, score_stats):
    """Generate a plain-language explanation paragraph."""
    n = len(records)
    sources = set(r.get("datasource", "") for r in records)
    source_labels = [friendly_source(s) for s in sorted(sources) if s]

    parts = [f"This cluster contains {n} source records from {len(sources)} system{'s' if len(sources) != 1 else ''} ({', '.join(source_labels[:6])})."]

    strong = [s for s in signals if s["strength"] == "strong"]
    if strong:
        anchors = [f"{s['field']} shared across {s['source_count']} sources" for s in strong[:3]]
        parts.append(f"Strong identity anchors: {'; '.join(anchors)}.")

    if score_stats["count"] > 0:
        parts.append(f"Pairwise analysis shows {score_stats['direct_pairs']} direct and {score_stats['transitive_pairs']} transitive links with an average match score of {score_stats['avg']}.")

    for v in variations:
        if v["severity"] == "high":
            parts.append(f"⚠ {v['detail']} — potential false positive indicator.")
        elif v["severity"] == "medium":
            parts.append(f"Note: {v['detail']}.")

    parts.append(f"Overall confidence: {confidence['score']}/100 ({confidence['label']}).")
    if confidence["penalties_applied"]:
        parts.append(f"Penalties applied: {'; '.join(confidence['penalties_applied'])}.")

    return " ".join(parts)


# ── Main Explainability Function ──────────────────────────────────────────────

def explain_cluster(records, scores):
    """
    Full cluster explainability: merge reasoning, confidence, narrative.
    Takes raw records and scores from AmperityAPI.get_full_cluster().
    """
    if not records:
        return {"error": "No records found for this cluster"}

    fields = _extract_fields(records)
    signals = _detect_signals(records, fields)
    variations = _detect_variations(fields)
    score_stats = _analyze_scores(scores)
    sources = set(r.get("datasource", "") for r in records)

    confidence = _compute_confidence(
        signals, score_stats, fields, variations, len(records), len(sources)
    )

    narrative = _generate_narrative(records, fields, signals, variations, confidence, score_stats)
    diagnosis = _diagnose_cluster(records, fields, signals, variations, score_stats, confidence)

    # Field completeness
    completeness = {}
    for f in ("email", "phone", "address", "city", "state", "postal", "gender", "birthdate"):
        filled = sum(1 for r in records if r.get(f) and str(r.get(f)).strip())
        completeness[f] = round(filled / len(records) * 100, 1)

    return {
        "amperity_id": records[0].get("amperity_id"),
        "record_count": len(records),
        "source_count": len(sources),
        "sources": {s: friendly_source(s) for s in sorted(sources) if s},
        "fields": {k: v[:10] for k, v in fields.items()},  # cap at 10 values
        "signals": signals[:15],
        "variations": variations,
        "score_stats": score_stats,
        "confidence_breakdown": confidence,
        "field_completeness": completeness,
        "narrative": narrative,
        "diagnosis": diagnosis,
        "records": records[:100],  # cap display
        "scores": scores[:100],
    }
