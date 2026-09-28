-- Derived tables and views written by the rebuild job. Kept to SQL that PostgreSQL and Snowflake
-- both accept, so one file serves either target. Semicolons separate statements (no ; in comments).
CREATE TABLE IF NOT EXISTS clean_events (
 event_id TEXT PRIMARY KEY, player_id TEXT NOT NULL, transaction_id TEXT NOT NULL,
 reward_claim_id TEXT NOT NULL, currency TEXT NOT NULL, reason TEXT NOT NULL,
 amount BIGINT NOT NULL, event_time TEXT NOT NULL, period_start TEXT NOT NULL,
 period_end TEXT NOT NULL, policy_version TEXT NOT NULL, expected_max BIGINT,
 source_raw_id BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS quarantine (raw_id BIGINT PRIMARY KEY, reason TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS pipeline_state (name TEXT PRIMARY KEY, value BIGINT NOT NULL);
CREATE OR REPLACE VIEW transactions AS
 SELECT player_id, currency, transaction_id, MIN(event_id) AS event_id,
        MAX(reward_claim_id) AS reward_claim_id, MAX(reason) AS reason, MAX(amount) AS amount,
        MAX(event_time) AS event_time, MAX(expected_max) AS expected_max
 FROM clean_events GROUP BY player_id, currency, transaction_id;
CREATE OR REPLACE VIEW daily_currency AS
 SELECT substr(event_time, 1, 10) AS event_date, player_id, currency,
        COUNT(*) AS transactions, SUM(amount) AS amount
 FROM transactions GROUP BY substr(event_time, 1, 10), player_id, currency;
CREATE OR REPLACE VIEW anomaly_candidates AS
 SELECT 'offline_policy_exceeded' AS rule, player_id, currency, reward_claim_id,
        COUNT(*) AS transaction_count, SUM(amount) AS amount
 FROM transactions WHERE reason = 'offline' AND amount > expected_max
 GROUP BY player_id, currency, reward_claim_id
 UNION ALL
 SELECT 'duplicate_reward_claim' AS rule, player_id, currency, reward_claim_id,
        COUNT(*) AS transaction_count, SUM(amount) AS amount
 FROM transactions GROUP BY player_id, currency, reward_claim_id HAVING COUNT(*) > 1
