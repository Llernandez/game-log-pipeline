"""Rebuild CLEAN/quarantine marts from retained RAW, landing in PostgreSQL, marts in PG or Snowflake.

Validation, deduplication and conflict rules stay in pipeline.py (the tested SQLite reference).
Each target keeps its own RAW watermark. The default run is incremental: only the key closure of the
RAW rows after the watermark is recomputed (see incremental.py) and applied in one target
transaction. A target without a watermark, or mode="full", gets a full rebuild.

The watermark assumes RAW ids become visible in id order, which holds for the single loader that
commits one batch at a time. Parallel writers would need a commit-ordered log or a safety lag.
"""
from pathlib import Path

from .incremental import closure, keys
from .pipeline import connect, rebuild

WATERMARK = "raw_watermark"

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


def backfill_keys(conn, batch=1000):
    """Key RAW rows loaded before keys existed (0.3.x landing). Returns the number of rows keyed."""
    cursor = conn.cursor()
    total = 0
    while True:
        cursor.execute("SELECT raw_id, payload FROM raw_events WHERE NOT keyed ORDER BY raw_id LIMIT %s", (batch,))
        rows = cursor.fetchall()
        if not rows:
            conn.commit()
            return total
        cursor.executemany("UPDATE raw_events SET event_key = %s, txn_key = %s, keyed = TRUE WHERE raw_id = %s",
                           [keys(payload) + (raw_id,) for raw_id, payload in rows])
        conn.commit()
        total += len(rows)


def latest_raw_id(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COALESCE(MAX(raw_id), 0) FROM raw_events")
    return int(cursor.fetchone()[0])


KEYED = "SELECT raw_id, payload, ingested_at, event_key, txn_key FROM raw_events"


def keyed_rows(rows):
    return [(int(r[0]), r[1], str(r[2]), r[3], r[4]) for r in rows]


def read_new_raw(conn, after, upto):
    cursor = conn.cursor()
    cursor.execute(KEYED + " WHERE raw_id > %s AND raw_id <= %s ORDER BY raw_id", (after, upto))
    return keyed_rows(cursor.fetchall())


def raw_by_keys(conn, events, txns):
    cursor = conn.cursor()
    cursor.execute(KEYED + " WHERE event_key = ANY(%s) OR txn_key = ANY(%s)", (sorted(events), sorted(txns)))
    return keyed_rows(cursor.fetchall())


def delta(new_rows, lookup):
    """Recompute the key closure of new keyed RAW rows with the reference rules."""
    rows, events = closure(new_rows, lookup)
    clean, quarantine = rebuild_rows([row[:3] for row in rows])
    return {"events": sorted(events), "raw_ids": [row[0] for row in rows], "clean": clean,
            "quarantine": quarantine, "new_raw": len(new_rows)}


def read_watermark(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM pipeline_state WHERE name = %s", (WATERMARK,))
    row = cursor.fetchone()
    return None if row is None else int(row[0])


def set_watermark(cursor, value):
    # Delete + insert instead of UPSERT/MERGE, which PostgreSQL and Snowflake spell differently.
    cursor.execute("DELETE FROM pipeline_state WHERE name = %s", (WATERMARK,))
    cursor.execute("INSERT INTO pipeline_state(name, value) VALUES (%s, %s)", (WATERMARK, value))


def ensure_marts(conn):
    cursor = conn.cursor()
    for statement in statements("marts.sql"):
        cursor.execute(statement)
    conn.commit()


def insert_rows(cursor, clean, quarantine):
    if quarantine:
        cursor.executemany("INSERT INTO quarantine(raw_id, reason) VALUES (%s, %s)", quarantine)
    if clean:
        cursor.executemany("INSERT INTO clean_events(%s) VALUES (%s)" % (
            ", ".join(CLEAN_COLUMNS), ", ".join(["%s"] * len(CLEAN_COLUMNS))), clean)


def write_delta(conn, change, watermark):
    """Replace the affected events/deliveries and move the watermark in one transaction."""
    cursor = conn.cursor()
    try:
        if change["events"]:
            cursor.executemany("DELETE FROM clean_events WHERE event_id = %s", [(e,) for e in change["events"]])
        if change["raw_ids"]:
            cursor.executemany("DELETE FROM quarantine WHERE raw_id = %s", [(r,) for r in change["raw_ids"]])
        insert_rows(cursor, change["clean"], change["quarantine"])
        set_watermark(cursor, watermark)
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def write_marts(conn, clean, quarantine, watermark=None):
    """Full replace. conn: DB-API connection with %s paramstyle and autocommit off."""
    ensure_marts(conn)
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM clean_events")
        cursor.execute("DELETE FROM quarantine")
        insert_rows(cursor, clean, quarantine)
        if watermark is not None:
            set_watermark(cursor, watermark)
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def rebuild_warehouse(source, targets, mode="incremental"):
    """Bring every target up to the latest RAW; each target is full or incremental on its own."""
    keyed = backfill_keys(source)
    latest = latest_raw_id(source)
    full = None
    runs = []
    for target in targets:
        ensure_marts(target)
        watermark = read_watermark(target)
        if mode == "full" or watermark is None:
            if full is None:
                full = rebuild_rows([row for row in read_raw(source) if row[0] <= latest])
            write_marts(target, full[0], full[1], latest)
            runs.append({"mode": "full", "clean": len(full[0]), "quarantine": len(full[1])})
        elif watermark >= latest:
            runs.append({"mode": "noop", "watermark": watermark})
        else:
            change = delta(read_new_raw(source, watermark, latest), lambda e, t: raw_by_keys(source, e, t))
            write_delta(target, change, latest)
            runs.append({"mode": "incremental", "new_raw": change["new_raw"], "recomputed_raw": len(change["raw_ids"]),
                         "events": len(change["events"]), "clean": len(change["clean"]),
                         "quarantine": len(change["quarantine"])})
    return {"latest_raw_id": latest, "keys_backfilled": keyed, "targets": runs}
