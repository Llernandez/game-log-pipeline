"""Transport-side helpers shared by the ingest API and the Kafka loader.

Pure functions only, so they are tested without Kafka or a database. The ingest API keeps every
delivery as raw text (schema validation happens later in rebuild), so this layer only bounds size
and shape of a request; it never decides whether an event is valid.
"""
import json

MAX_BATCH_BYTES = 1_048_576
MAX_BATCH_LINES = 1_000
MAX_LINE_BYTES = 16_384


class BatchRejected(ValueError):
    """The whole request is refused (HTTP 413/400); nothing from it is produced."""


def split_batch(body: bytes) -> list[str]:
    """JSON Lines body -> non-empty raw lines, byte-for-byte as sent (minus line endings)."""
    if len(body) > MAX_BATCH_BYTES:
        raise BatchRejected("batch larger than %d bytes" % MAX_BATCH_BYTES)
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as error:
        raise BatchRejected("body must be UTF-8") from error
    lines = [line.rstrip("\r") for line in text.split("\n") if line.strip()]
    if not lines:
        raise BatchRejected("empty batch")
    if len(lines) > MAX_BATCH_LINES:
        raise BatchRejected("more than %d lines" % MAX_BATCH_LINES)
    for line in lines:
        if len(line.encode("utf-8")) > MAX_LINE_BYTES:
            raise BatchRejected("line larger than %d bytes" % MAX_LINE_BYTES)
    return lines


def partition_key(line: str) -> bytes | None:
    """Key by event_id when readable so retries of one event land on one partition.

    Unparseable lines still go through (unkeyed) and are quarantined by rebuild, not dropped here.
    """
    try:
        value = json.loads(line)
    except ValueError:
        return None
    event_id = value.get("event_id") if isinstance(value, dict) else None
    return event_id.encode("utf-8") if isinstance(event_id, str) and event_id else None


def raw_record(topic: str, partition: int, offset: int, payload: str, ingested_at: str) -> tuple:
    """Row for the warehouse raw table; (topic, partition, offset) makes redelivery idempotent."""
    return (topic, partition, offset, payload, ingested_at)
