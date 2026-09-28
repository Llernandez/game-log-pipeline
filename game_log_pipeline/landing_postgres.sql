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
)
