"""Adapter: the game's on-device diagnostic JSONL (schema 1-3) to pipeline stage_attempt events.

The game exports at most its last 256 diagnostic rows as a file the player chooses to share.
Only `battle_result` rows become stage attempts. The export has no player identifier by design,
so the caller supplies a pseudonymous tester id. The client event_id is kept, so re-exporting an
overlapping file is a resend, not new data.

Excluded, with a count per reason:
- rows with `assisted` or `session_assisted` (developer 4x speed / test grain): not balance evidence
- every other event type (save, offline, recruit, ...): currency rows carry no server
  transaction or claim id, so they must not feed the duplicate-payout rules
- malformed JSON and contract violations
"""
import json
from collections import Counter

TICK_MS = 100  # battle.gd reports seconds as tick / 10, independent of the playback speed
DEVICE_TYPES = {"save", "offline", "idle", "idle_toggle", "stage_complete", "recruit", "train",
                "arrange", "battle_result", "rebirth", "upgrade", "tower_clear"}


def _whole(value):
    return type(value) is int or (type(value) is float and value.is_integer())


def adapt(lines, player_id):
    events, skipped = [], Counter()
    for line in lines:
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            skipped["malformed_json"] += 1
            continue
        if (not isinstance(row, dict) or row.get("source") != "device_diagnostic"
                or row.get("schema_version") not in (1, 2, 3) or row.get("event_type") not in DEVICE_TYPES):
            skipped["contract"] += 1
            continue
        if row["event_type"] != "battle_result":
            skipped["not_a_battle"] += 1
            continue
        if row.get("assisted") is True or row.get("session_assisted") is True:
            skipped["assisted"] += 1
            continue
        stage, ticks, won, floor = row.get("stage"), row.get("ticks"), row.get("won"), row.get("tower_floor", 0)
        if not (_whole(stage) and stage >= 0 and _whole(ticks) and ticks >= 0
                and type(won) is bool and _whole(floor) and floor >= 0):
            skipped["contract"] += 1
            continue
        events.append(dict(
            schema_version=1, source="device_diagnostic", event_type="stage_attempt",
            event_id=row.get("event_id"), player_id=player_id, session_id=row.get("session_id"),
            # Tower floors are the endless track; story stages are 0-based in the client (P01 = 0).
            track="endless" if floor > 0 else "story",
            stage=int(floor) if floor > 0 else int(stage) + 1,
            outcome="clear" if won else "fail",
            duration_ms=int(ticks) * TICK_MS,
            event_time=row.get("occurred_at")))
    return events, dict(sorted(skipped.items()))
