"""Rebuild CLEAN/quarantine marts from retained RAW, landing in PostgreSQL, marts in PG or Snowflake.

Validation, deduplication and conflict rules stay in pipeline.py (the tested SQLite reference).
RAW rows are fed through that reference in memory and the marts are replaced in one target
transaction. It is a full rebuild sized for a portfolio dataset, not an incremental job.
"""
from pathlib import Path

from .pipeline import connect, rebuild

CLEAN_COLUMNS = ("event_id", "player_id", "transaction_id", "reward_claim_id", "currency", "reason",
                 "amount", "event_time", "period_start", "period_end", "policy_version",
                 "expected_max", "source_raw_id")


def statements(name: str) -> list[str]:
    text = Path(__file__).with_name(name).read_text(encoding="utf-8")
    lines = [line for line in text.splitlines() if not line.lstrip().startswith("--")]
    return [part.strip() for part in "\n".join(lines).split(";") if part.strip()]


def rebuild_rows(raw_rows):
    """[(raw_id, payload, ingested_at)] -> (clean_rows, quarantine_rows) using the reference rules."""
    db = connect(":memory:")
    try:
        with db:
            db.executemany("INSERT INTO raw_events(raw_id, payload, ingested_at) VALUES (?, ?, ?)", raw_rows)
        rebuild(db)
        clean = [tuple(row) for row in db.execute(
            "SELECT %s FROM clean_events ORDER BY event_id" % ", ".join(CLEAN_COLUMNS))]
        quarantine = [tuple(row) for row in db.execute("SELECT raw_id, reason FROM quarantine ORDER BY raw_id")]
        return clean, quarantine
    finally:
        db.close()


def ensure_landing(conn):
    cursor = conn.cursor()
    for statement in statements("landing_postgres.sql"):
        cursor.execute(statement)
    conn.commit()


def read_raw(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT raw_id, payload, ingested_at FROM raw_events ORDER BY raw_id")
    return [(int(r[0]), r[1], str(r[2])) for r in cursor.fetchall()]


def write_marts(conn, clean, quarantine):
    """conn: DB-API connection with %s paramstyle and autocommit off (psycopg, snowflake-connector)."""
    cursor = conn.cursor()
    for statement in statements("marts.sql"):
        cursor.execute(statement)
    conn.commit()
    try:
        cursor.execute("DELETE FROM clean_events")
        cursor.execute("DELETE FROM quarantine")
        if quarantine:
            cursor.executemany("INSERT INTO quarantine(raw_id, reason) VALUES (%s, %s)", quarantine)
        if clean:
            cursor.executemany("INSERT INTO clean_events(%s) VALUES (%s)" % (
                ", ".join(CLEAN_COLUMNS), ", ".join(["%s"] * len(CLEAN_COLUMNS))), clean)
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def rebuild_warehouse(source, targets):
    """Read RAW from the landing connection, write the same marts to every target connection."""
    raw_rows = read_raw(source)
    clean, quarantine = rebuild_rows(raw_rows)
    for target in targets:
        write_marts(target, clean, quarantine)
    return {"raw": len(raw_rows), "clean": len(clean), "quarantine": len(quarantine), "targets": len(targets)}
