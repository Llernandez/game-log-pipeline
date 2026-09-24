PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS raw_events (
 raw_id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL, ingested_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS quarantine (
 raw_id INTEGER PRIMARY KEY REFERENCES raw_events(raw_id), reason TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS clean_events (
 event_id TEXT PRIMARY KEY, player_id TEXT NOT NULL, transaction_id TEXT NOT NULL,
 reward_claim_id TEXT NOT NULL, currency TEXT NOT NULL, reason TEXT NOT NULL,
 amount INTEGER NOT NULL CHECK(amount >= 0), event_time TEXT NOT NULL,
 period_start TEXT NOT NULL, period_end TEXT NOT NULL, policy_version TEXT NOT NULL,
 expected_max INTEGER, source_raw_id INTEGER NOT NULL REFERENCES raw_events(raw_id)
);
-- Payload conflicts are quarantined before this deterministic transaction deduplication.
CREATE VIEW IF NOT EXISTS transactions AS
 SELECT player_id, currency, transaction_id, MIN(event_id) AS event_id,
        MAX(reward_claim_id) AS reward_claim_id, MAX(reason) AS reason, MAX(amount) AS amount,
        MAX(event_time) AS event_time, MAX(expected_max) AS expected_max
 FROM clean_events GROUP BY player_id, currency, transaction_id;
CREATE VIEW IF NOT EXISTS daily_currency AS
 SELECT substr(event_time, 1, 10) AS event_date, player_id, currency,
        COUNT(*) AS transactions, SUM(amount) AS amount
 FROM transactions GROUP BY event_date, player_id, currency;
CREATE VIEW IF NOT EXISTS anomaly_candidates AS
 SELECT 'offline_policy_exceeded' AS rule, player_id, currency, reward_claim_id,
        COUNT(*) AS transaction_count, SUM(amount) AS amount
 FROM transactions WHERE reason = 'offline' AND amount > expected_max
 GROUP BY player_id, currency, reward_claim_id
 UNION ALL
 SELECT 'duplicate_reward_claim' AS rule, player_id, currency, reward_claim_id,
        COUNT(*) AS transaction_count, SUM(amount) AS amount
 FROM transactions GROUP BY player_id, currency, reward_claim_id HAVING COUNT(*) > 1;
-- A deliberately naive fixed-minute bucket, NOT a sliding-window detector.
CREATE VIEW IF NOT EXISTS naive_minute_spikes AS
 SELECT player_id, substr(event_time, 1, 16) AS minute, SUM(amount) AS amount
 FROM transactions GROUP BY player_id, minute HAVING SUM(amount) > 100;
