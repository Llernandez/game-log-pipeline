"""Incremental rebuild: recompute only the RAW deliveries that can change a result.

The affected set is found by keys, not by time. A new delivery can change
  1. every delivery of the same event_id (content conflicts, which delivery counts), and
  2. every event with the same player/currency/transaction_id (transaction conflicts).
Closing the new rows under both keys and running the reference rules on that closure gives exactly
the rows a full rebuild would produce for those keys; everything outside it is untouched. Late
arrivals need no special case: they are new rows, and the daily views re-aggregate from clean events.
"""
import json

from .pipeline import validate


def keys(payload):
    """(event_key, txn_key) of one RAW payload; None where the payload cannot provide the key.

    An invalid event keeps its event_key so a later valid delivery of the same id recomputes with it,
    but gets no txn_key because the reference rules never put invalid events into transactions.
    """
    try:
        item = json.loads(payload)
    except ValueError:
        return None, None
    event = item.get("event_id") if isinstance(item, dict) else None
    event = event if isinstance(event, str) else None
    try:
        valid = validate(item)
    except (ValueError, TypeError, OverflowError, AttributeError):
        return event, None
    if valid["event_type"] == "stage_attempt":
        return event, None  # attempts only conflict per event_id
    return event, "|".join((valid["player_id"], valid["currency"], valid["transaction_id"]))


def closure(seed_rows, lookup):
    """Grow the new rows to every row sharing an event or transaction key, until nothing changes.

    Rows are (raw_id, payload, ingested_at, event_key, txn_key). lookup(events, txns) returns the rows
    whose event_key is in events or whose txn_key is in txns.
    """
    rows = {row[0]: row for row in seed_rows}
    events = {row[3] for row in seed_rows if row[3]}
    txns = {row[4] for row in seed_rows if row[4]}
    while True:
        found = lookup(events, txns) if events or txns else []
        rows.update({row[0]: row for row in found})
        grown_events = {row[3] for row in found if row[3]} - events
        grown_txns = {row[4] for row in found if row[4]} - txns
        if not grown_events and not grown_txns:
            return [rows[raw_id] for raw_id in sorted(rows)], events
        events |= grown_events
        txns |= grown_txns
