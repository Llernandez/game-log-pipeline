import tempfile
import unittest
from pathlib import Path
from game_log_pipeline.device import TICK_MS, adapt
from game_log_pipeline.pipeline import canonical, connect, ingest, rebuild, report

SAMPLE = Path(__file__).resolve().parent.parent / "examples" / "device_diagnostic_sample.jsonl"


def sample_lines():
    return SAMPLE.read_text(encoding="utf-8").splitlines()


class DeviceAdapterTests(unittest.TestCase):
    def test_battles_become_stage_attempts_and_the_rest_is_counted(self):
        rows, skipped = adapt(sample_lines(), "tester_01")
        # 8 battle rows, 1 of them assisted; the duplicated line is still two rows here (the pipeline dedupes).
        self.assertEqual(len(rows), 7)
        self.assertEqual(skipped, {"assisted": 1, "malformed_json": 1, "not_a_battle": 2})
        self.assertEqual({row["source"] for row in rows}, {"device_diagnostic"})
        self.assertEqual({row["player_id"] for row in rows}, {"tester_01"})

    def test_story_stage_is_one_based_and_tower_floor_is_the_endless_track(self):
        rows, _ = adapt(sample_lines(), "tester_01")
        first = rows[0]
        self.assertEqual((first["track"], first["stage"], first["outcome"]), ("story", 1, "clear"))
        self.assertEqual(first["duration_ms"], 400 * TICK_MS)
        tower = [row for row in rows if row["track"] == "endless"]
        self.assertEqual({(row["stage"], row["outcome"]) for row in tower}, {(3, "fail")})

    def test_adapted_attempts_flow_through_the_pipeline_and_resends_do_not_count(self):
        rows, _ = adapt(sample_lines(), "tester_01")
        with tempfile.TemporaryDirectory() as tmp:
            db = connect(Path(tmp) / "device.sqlite")
            try:
                ingest(db, [canonical(row) for row in rows])
                rebuild(db)
                first = report(db)
                self.assertEqual(first["counts"]["clean_attempts"], 6)  # the resent event_id counts once
                self.assertEqual(first["counts"]["quarantine"], 0)
                ingest(db, [canonical(row) for row in rows])  # same export shared twice
                rebuild(db)
                second = report(db)
                self.assertEqual(second["counts"]["clean_attempts"], 6)
                self.assertEqual(second["stage_funnel"], first["stage_funnel"])
            finally:
                db.close()

    def test_device_source_is_rejected_for_currency_events(self):
        currency = dict(schema_version=1, source="device_diagnostic", event_type="currency_transaction",
                        event_id="ev_device", player_id="tester_01", session_id="s1", transaction_id="t1",
                        reward_claim_id="c1", currency="demo_coin", reason="offline", amount=10,
                        event_time="2030-03-01T10:00:00Z", period_start="2030-03-01T09:00:00Z",
                        period_end="2030-03-01T10:00:00Z", policy_version="demo-v1")
        with tempfile.TemporaryDirectory() as tmp:
            db = connect(Path(tmp) / "device.sqlite")
            try:
                ingest(db, [canonical(currency)])
                rebuild(db)
                counts = report(db)["counts"]
                self.assertEqual((counts["clean_events"], counts["quarantine"]), (0, 1))
            finally:
                db.close()


if __name__ == "__main__":
    unittest.main()
