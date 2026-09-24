"""Small replayable SQLite reference pipeline; not a Snowflake emulator."""
import json
import math
import re
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

FIELDS = {"schema_version", "source", "event_type", "event_id", "player_id", "session_id",
          "transaction_id", "reward_claim_id", "currency", "reason", "amount", "event_time",
          "period_start", "period_end", "policy_version"}
POLICY = {"version": "demo-v1", "cap_seconds": 7200, "units_per_minute": 6}

def timestamp(value):
    if not isinstance(value, str):
        raise ValueError("timestamp must be text")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timezone required")
    return parsed.astimezone(timezone.utc)

def validate(item):
    if not isinstance(item, dict) or set(item) != FIELDS:
        raise ValueError("fields must match the public allowlist")
    if type(item["schema_version"]) is not int or item["schema_version"] != 1:
        raise ValueError("unsupported schema")
    if item["source"] != "synthetic" or item["event_type"] != "currency_transaction":
        raise ValueError("only synthetic currency events supported")
    for key in ("event_id", "player_id", "session_id", "transaction_id", "reward_claim_id"):
        if not isinstance(item[key], str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,96}", item[key]):
            raise ValueError("invalid identifier: " + key)
    if item["currency"] != "demo_coin" or item["policy_version"] != POLICY["version"]:
        raise ValueError("unsupported currency or policy")
    if item["reason"] not in ("offline", "quest"):
        raise ValueError("unsupported reason")
    if type(item["amount"]) is not int or not 0 <= item["amount"] <= 1_000_000_000:
        raise ValueError("amount must be a bounded nonnegative integer")
    occurred, start, end = (timestamp(item[k]) for k in ("event_time", "period_start", "period_end"))
    if start > end or end > occurred:
        raise ValueError("invalid settlement interval")
    result = dict(item)
    for key in ("event_time", "period_start", "period_end"):
        result[key] = timestamp(item[key]).isoformat()
    seconds = min((end - start).total_seconds(), POLICY["cap_seconds"])
    result["expected_max"] = math.floor(seconds / 60) * POLICY["units_per_minute"] if item["reason"] == "offline" else None
    return result

def canonical(item):
    return json.dumps(item, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

def connect(path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    schema = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
    db.executescript(schema)
    return db

def ingest(db, lines):
    """Keep every delivery. Network ingestion/offset acknowledgement is out of scope."""
    now = datetime.now(timezone.utc).isoformat()
    with db:
        db.executemany("INSERT INTO raw_events(payload, ingested_at) VALUES (?, ?)",
                       ((line.rstrip("\n"), now) for line in lines))

def rebuild(db):
    """One transaction rebuilds clean, quarantine, and derived views from retained raw."""
    groups, valid, quarantined = defaultdict(list), {}, []
    rows = db.execute("SELECT raw_id, payload FROM raw_events ORDER BY raw_id").fetchall()
    for row in rows:
        try:
            item = validate(json.loads(row["payload"]))
            groups[item["event_id"]].append((row["raw_id"], item))
        except (ValueError, TypeError, OverflowError) as error:
            quarantined.append((row["raw_id"], "invalid_schema: " + str(error)))
    for event_id, deliveries in groups.items():
        if len({canonical(item) for _, item in deliveries}) != 1:
            quarantined.extend((raw_id, "event_id_conflict") for raw_id, _ in deliveries)
        else:
            valid[event_id] = deliveries[0]
    transactions = defaultdict(list)
    for event_id, (raw_id, item) in valid.items():
        transactions[(item["player_id"], item["currency"], item["transaction_id"])].append((event_id, raw_id, item))
    for items in transactions.values():
        signatures = {canonical({k:v for k,v in item.items() if k != "event_id"}) for _, _, item in items}
        if len(signatures) > 1:
            for event_id, _, _ in items:
                quarantined.extend((raw_id, "transaction_conflict") for raw_id, _ in groups[event_id])
                valid.pop(event_id)
    with db:
        db.execute("DELETE FROM clean_events")
        db.execute("DELETE FROM quarantine")
        db.executemany("INSERT INTO quarantine(raw_id, reason) VALUES (?, ?)", quarantined)
        for raw_id, item in valid.values():
            db.execute("""INSERT INTO clean_events
                (event_id, player_id, transaction_id, reward_claim_id, currency, reason, amount,
                 event_time, period_start, period_end, policy_version, expected_max, source_raw_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                 tuple(item[k] for k in ("event_id", "player_id", "transaction_id", "reward_claim_id",
                    "currency", "reason", "amount", "event_time", "period_start", "period_end",
                    "policy_version", "expected_max")) + (raw_id,))

def report(db):
    def rows(query):
        return [dict(row) for row in db.execute(query)]
    return {
        "source": "synthetic", "policy": POLICY,
        "counts": {table: db.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
                   for table in ("raw_events", "clean_events", "quarantine", "transactions")},
        "daily_currency": rows("SELECT * FROM daily_currency ORDER BY event_date, player_id"),
        "anomaly_candidates": rows("SELECT * FROM anomaly_candidates ORDER BY rule, player_id, reward_claim_id"),
        "naive_minute_spikes": rows("SELECT * FROM naive_minute_spikes ORDER BY player_id, minute"),
        "limitations": ["Full rebuild for a small fixture; incremental backfill is not implemented.",
                       "Client-side claims cannot establish authoritative server balances.",
                       "Kafka, Snowflake, Airflow and network deployment are not implemented yet."]
    }
