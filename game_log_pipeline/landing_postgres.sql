-- Operational landing zone in PostgreSQL. The Kafka loader writes here at-least-once and the
-- unique (topic, kafka_partition, kafka_offset) key makes a redelivered record a no-op.
CREATE TABLE IF NOT EXISTS raw_events (
 raw_id BIGSERIAL PRIMARY KEY,
 topic TEXT NOT NULL,
 kafka_partition INTEGER NOT NULL,
 kafka_offset BIGINT NOT NULL,
 payload TEXT NOT NULL,
 ingested_at TEXT NOT NULL,
 UNIQUE (topic, kafka_partition, kafka_offset)
);
-- 0.4.0: keys for incremental rebuilds, computed by the loader. keyed = FALSE marks older rows for
-- a one-time backfill (NULL keys alone cannot tell "not computed" from "payload has no key").
ALTER TABLE raw_events ADD COLUMN IF NOT EXISTS event_key TEXT;
ALTER TABLE raw_events ADD COLUMN IF NOT EXISTS txn_key TEXT;
ALTER TABLE raw_events ADD COLUMN IF NOT EXISTS keyed BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX IF NOT EXISTS raw_events_event_key ON raw_events (event_key);
CREATE INDEX IF NOT EXISTS raw_events_txn_key ON raw_events (txn_key)
