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
# 0.5.0: progression telemetry. One row per stage attempt on the story or endless track.
ATTEMPT_FIELDS = {"schema_version", "source", "event_type", "event_id", "player_id", "session_id",
                  "track", "stage", "outcome", "duration_ms", "event_time"}
IDENTIFIER = re.compile(r"[A-Za-z0-9_-]{1,96}")

def timestamp(value):
    if not isinstance(value, str):
        raise ValueError("timestamp must be text")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timezone required")
    return parsed.astimezone(timezone.utc)

SCHEMA_VERSIONS = (1, 2, 3)
SEMVER = re.compile(r"\d{1,4}\.\d{1,4}\.\d{1,4}")

def upcast(item):
    """Normalize every supported producer schema to the v1 business shape before validation.

    v2 adds client_version (lineage only, dropped here); v3 renames currency to currency_code.
    Older clients keep sending older versions, so all supported versions stay accepted and
    the same event resent by an upgraded client is not a content conflict.
    """
    if not isinstance(item, dict):
        raise ValueError("event must be an object")
    version = item.get("schema_version")
    if type(version) is not int or version not in SCHEMA_VERSIONS:
        raise ValueError("unsupported schema_version")
    item = dict(item)
    if version >= 2:
        client = item.pop("client_version", None)
        if not isinstance(client, str) or not SEMVER.fullmatch(client):
            raise ValueError("client_version required from schema 2")
    if version >= 3 and item.get("event_type") == "currency_transaction":
        if "currency" in item or "currency_code" not in item:
            raise ValueError("schema 3 uses currency_code")
        item["currency"] = item.pop("currency_code")
    item["schema_version"] = 1
    return item

def bounded_int(value, low, high, name):
    if type(value) is not int or not low <= value <= high:
        raise ValueError(name + " must be an integer in range")

def validate_attempt(item):
    if set(item) != ATTEMPT_FIELDS:
        raise ValueError("fields must match the public allowlist")
    if item["source"] != "synthetic":
        raise ValueError("only synthetic events supported")
    for key in ("event_id", "player_id", "session_id"):
        if not isinstance(item[key], str) or not IDENTIFIER.fullmatch(item[key]):
            raise ValueError("invalid identifier: " + key)
    if item["track"] not in ("story", "endless") or item["outcome"] not in ("clear", "fail"):
        raise ValueError("unsupported track or outcome")
    bounded_int(item["stage"], 1, 10_000, "stage")
    bounded_int(item["duration_ms"], 0, 86_400_000, "duration_ms")
    return dict(item, event_time=timestamp(item["event_time"]).isoformat())

def validate(item):
    item = upcast(item)
    if item.get("event_type") == "stage_attempt":
        return validate_attempt(item)
    if set(item) != FIELDS:
        raise ValueError("fields must match the public allowlist")
    if item["source"] != "synthetic" or item["event_type"] != "currency_transaction":
        raise ValueError("only synthetic currency events supported")
    for key in ("event_id", "player_id", "session_id", "transaction_id", "reward_claim_id"):
        if not isinstance(item[key], str) or not IDENTIFIER.fullmatch(item[key]):
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
    attempts = {event_id: pair for event_id, pair in valid.items() if pair[1]["event_type"] == "stage_attempt"}
    for event_id in attempts:
        valid.pop(event_id)
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
        db.execute("DELETE FROM clean_attempts")
        db.executemany("INSERT INTO quarantine(raw_id, reason) VALUES (?, ?)", quarantined)
        db.executemany("""INSERT INTO clean_attempts
            (event_id, player_id, track, stage, outcome, duration_ms, event_time, source_raw_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [tuple(item[k] for k in ("event_id", "player_id", "track", "stage", "outcome", "duration_ms",
             "event_time")) + (raw_id,) for raw_id, item in attempts.values()])
        for raw_id, item in valid.values():
            db.execute("""INSERT INTO clean_events
                (event_id, player_id, transaction_id, reward_claim_id, currency, reason, amount,
                 event_time, period_start, period_end, policy_version, expected_max, source_raw_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                 tuple(item[k] for k in ("event_id", "player_id", "transaction_id", "reward_claim_id",
                    "currency", "reason", "amount", "event_time", "period_start", "period_end",
                    "policy_version", "expected_max")) + (raw_id,))

def schema_versions(db):
    """Raw deliveries per declared producer schema, so a rollout's version mix is visible."""
    counts = defaultdict(int)
    for (payload,) in db.execute("SELECT payload FROM raw_events"):
        try:
            version = json.loads(payload).get("schema_version")
        except (ValueError, AttributeError):
            version = None
        counts[str(version) if type(version) is int else "unreadable"] += 1
    return dict(sorted(counts.items()))

def report(db):
    def rows(query):
        return [dict(row) for row in db.execute(query)]
    return {
        "source": "synthetic", "policy": POLICY,
        "counts": {table: db.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
                   for table in ("raw_events", "clean_events", "clean_attempts", "quarantine", "transactions")},
        "daily_currency": rows("SELECT * FROM daily_currency ORDER BY event_date, player_id"),
        "anomaly_candidates": rows("SELECT * FROM anomaly_candidates ORDER BY rule, player_id, reward_claim_id"),
        "schema_versions": schema_versions(db),
        "naive_minute_spikes": rows("SELECT * FROM naive_minute_spikes ORDER BY player_id, minute"),
        "stage_funnel": rows("SELECT * FROM stage_funnel ORDER BY track, stage"),
        "difficulty_walls": rows("SELECT * FROM difficulty_walls ORDER BY track, stage"),
        "limitations": ["This SQLite reference always rebuilds in full; the warehouse job is incremental (warehouse.py) and is tested against it.",
                       "Client-side claims cannot establish authoritative server balances.",
                       "This SQLite path is the reference; the Kafka/PostgreSQL/Airflow deployment reuses the same rules."]
    }
