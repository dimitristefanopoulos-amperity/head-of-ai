"""
Stitch Drift Monitoring — SQLite persistence for tracking identity metrics over time.
"""
import sqlite3, json, time, os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "drift_history.db")


def _conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    c = _conn()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS stitch_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            region TEXT NOT NULL,
            taken_at TEXT NOT NULL,
            total_clusters INTEGER,
            total_records INTEGER,
            avg_cluster_size REAL,
            max_cluster_size INTEGER
        );
        CREATE TABLE IF NOT EXISTS score_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id INTEGER REFERENCES stitch_snapshots(id),
            match_category TEXT,
            pair_count INTEGER
        );
        CREATE TABLE IF NOT EXISTS source_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id INTEGER REFERENCES stitch_snapshots(id),
            datasource TEXT,
            record_count INTEGER,
            unique_ids INTEGER
        );
        CREATE TABLE IF NOT EXISTS alert_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            region TEXT NOT NULL,
            created_at TEXT NOT NULL,
            severity TEXT NOT NULL,
            metric TEXT NOT NULL,
            message TEXT NOT NULL,
            prev_value REAL,
            curr_value REAL,
            pct_change REAL,
            acknowledged INTEGER DEFAULT 0
        );
    """)
    c.close()


def save_snapshot(region, stats, score_dist=None, source_dist=None):
    """Save a stitch metrics snapshot. Returns snapshot_id."""
    c = _conn()
    now = datetime.utcnow().isoformat() + "Z"
    s = stats[0] if isinstance(stats, list) and stats else stats

    cur = c.execute("""
        INSERT INTO stitch_snapshots (region, taken_at, total_clusters, total_records, avg_cluster_size, max_cluster_size)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        region, now,
        int(s.get("total_clusters", 0)),
        int(s.get("total_records", 0)),
        float(s.get("avg_cluster_size", 0)),
        int(s.get("max_cluster_size", 0)),
    ))
    snap_id = cur.lastrowid

    if score_dist:
        for row in score_dist:
            c.execute("INSERT INTO score_snapshots (snapshot_id, match_category, pair_count) VALUES (?, ?, ?)",
                      (snap_id, row.get("match_category"), int(row.get("pair_count", 0))))

    if source_dist:
        for row in source_dist:
            c.execute("INSERT INTO source_snapshots (snapshot_id, datasource, record_count, unique_ids) VALUES (?, ?, ?, ?)",
                      (snap_id, row.get("datasource"), int(row.get("record_count", 0)), int(row.get("unique_ids", 0))))

    c.commit()
    c.close()
    return snap_id


def get_history(region, limit=30):
    """Get recent snapshots for charting."""
    c = _conn()
    rows = c.execute("""
        SELECT * FROM stitch_snapshots WHERE region = ? ORDER BY taken_at ASC LIMIT ?
    """, (region, limit)).fetchall()
    c.close()
    return [dict(r) for r in rows]


def get_scores_history(region, limit=30):
    """Score distribution over time."""
    c = _conn()
    snaps = c.execute("""
        SELECT id, taken_at FROM stitch_snapshots WHERE region = ? ORDER BY taken_at DESC LIMIT ?
    """, (region, limit)).fetchall()
    result = []
    for snap in reversed(snaps):
        scores = c.execute("SELECT match_category, pair_count FROM score_snapshots WHERE snapshot_id = ?",
                           (snap["id"],)).fetchall()
        result.append({"taken_at": snap["taken_at"], "scores": [dict(s) for s in scores]})
    c.close()
    return result


def get_source_history(region, limit=30):
    """Source record counts over time."""
    c = _conn()
    snaps = c.execute("""
        SELECT id, taken_at FROM stitch_snapshots WHERE region = ? ORDER BY taken_at DESC LIMIT ?
    """, (region, limit)).fetchall()
    result = []
    for snap in reversed(snaps):
        sources = c.execute("SELECT datasource, record_count, unique_ids FROM source_snapshots WHERE snapshot_id = ?",
                            (snap["id"],)).fetchall()
        result.append({"taken_at": snap["taken_at"], "sources": [dict(s) for s in sources]})
    c.close()
    return result


def compute_drift(region, current_stats):
    """Compare against previous snapshot and generate alerts if thresholds breached."""
    c = _conn()
    prev = c.execute("""
        SELECT * FROM stitch_snapshots WHERE region = ? ORDER BY taken_at DESC LIMIT 1
    """, (region,)).fetchone()
    c.close()

    if not prev:
        return []

    s = current_stats[0] if isinstance(current_stats, list) and current_stats else current_stats
    alerts = []
    now = datetime.utcnow().isoformat() + "Z"

    for metric, col in [("total_clusters", "total_clusters"), ("total_records", "total_records"),
                         ("max_cluster_size", "max_cluster_size")]:
        prev_val = prev[col]
        curr_val = int(s.get(metric, 0))
        if prev_val and prev_val > 0:
            pct = abs(curr_val - prev_val) / prev_val
            if pct >= 0.20:
                sev = "critical"
            elif pct >= 0.05:
                sev = "warning"
            else:
                continue
            alerts.append({
                "region": region,
                "severity": sev,
                "metric": metric,
                "message": f"{metric} changed by {pct*100:.1f}% ({prev_val:,} → {curr_val:,})",
                "prev_value": prev_val,
                "curr_value": curr_val,
                "pct_change": round(pct * 100, 1),
            })

    # Persist alerts
    if alerts:
        c = _conn()
        for a in alerts:
            c.execute("""
                INSERT INTO alert_log (region, created_at, severity, metric, message, prev_value, curr_value, pct_change)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (a["region"], now, a["severity"], a["metric"], a["message"],
                  a["prev_value"], a["curr_value"], a["pct_change"]))
        c.commit()
        c.close()

    return alerts


def get_alerts(region=None, limit=50, unacknowledged_only=False):
    c = _conn()
    sql = "SELECT * FROM alert_log"
    params = []
    clauses = []
    if region:
        clauses.append("region = ?")
        params.append(region)
    if unacknowledged_only:
        clauses.append("acknowledged = 0")
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    rows = c.execute(sql, params).fetchall()
    c.close()
    return [dict(r) for r in rows]


def acknowledge_alert(alert_id):
    c = _conn()
    c.execute("UPDATE alert_log SET acknowledged = 1 WHERE id = ?", (alert_id,))
    c.commit()
    c.close()


# Initialize on import
init_db()
