import json
import sqlite3
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from game_log_pipeline.generator import events
from game_log_pipeline.pipeline import canonical, connect, ingest, rebuild, report, validate

class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = connect(Path(self.tmp.name) / "test.sqlite")
    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()
    def load(self, items):
        ingest(self.db, [canonical(item) for item in items])
        rebuild(self.db)
        return report(self.db)
    def normal(self):
        return next(item for item in events() if item["event_id"] == "ev_normal")
    def test_synthetic_baseline(self):
        result = self.load(events())
        self.assertEqual(result["counts"], dict(raw_events=10, clean_events=6, quarantine=3, transactions=6))
        self.assertEqual({(x["rule"],x["player_id"]) for x in result["anomaly_candidates"]},
                         {("duplicate_reward_claim","p_repeat"),("offline_policy_exceeded","p_excess")})
    def test_offline_payout_is_not_automatically_abuse(self):
        result = self.load([self.normal()])
        self.assertEqual(len(result["naive_minute_spikes"]), 1)
        self.assertEqual(result["anomaly_candidates"], [])
    def test_retry_and_rebuild_preserve_business_totals(self):
        first = self.load(events())
        rebuild(self.db)
        self.assertEqual(report(self.db), first)
        second = self.load(events())
        for key in ("daily_currency","anomaly_candidates","naive_minute_spikes"):
            self.assertEqual(second[key], first[key])
        self.assertEqual(second["counts"]["raw_events"], 20)
        self.assertEqual(second["counts"]["transactions"], 6)
    def test_late_event_recalculates_previous_date(self):
        items = events()
        late = next(item for item in items if item["event_id"] == "ev_late")
        self.load([item for item in items if item is not late])
        result = self.load([late])
        self.assertEqual(next(x for x in result["daily_currency"] if x["player_id"] == "p_late")["event_date"], "2029-12-31")
    def test_event_conflicts_are_order_independent(self):
        items = events()
        first = self.load(items)
        self.db.execute("DELETE FROM clean_events")
        self.db.execute("DELETE FROM quarantine")
        self.db.execute("DELETE FROM raw_events")
        self.db.commit()
        second = self.load(list(reversed(items)))
        self.assertEqual(first, second)
    def test_new_event_id_same_transaction_does_not_double_count(self):
        item = self.normal()
        retry = deepcopy(item)
        retry["event_id"] = "ev_reissued"
        result = self.load([item, retry])
        self.assertEqual(result["counts"]["clean_events"], 2)
        self.assertEqual(result["counts"]["transactions"], 1)
        self.assertEqual(result["daily_currency"][0]["amount"], 720)
    def test_transaction_conflict_is_quarantined(self):
        item = self.normal()
        conflict = deepcopy(item)
        conflict.update(event_id="ev_changed", amount=500)
        result = self.load([item, conflict])
        self.assertEqual(result["counts"]["quarantine"], 2)
        self.assertEqual(result["counts"]["transactions"], 0)
    def test_invalid_json_and_unexpected_private_fields_are_quarantined(self):
        item = self.normal()
        item["private_character_name"] = "must not enter clean"
        ingest(self.db, ["{broken", canonical(item)])
        rebuild(self.db)
        self.assertEqual(report(self.db)["counts"]["quarantine"], 2)
    def test_timezone_boolean_and_unknown_policy_rejected(self):
        for changes in ({"event_time":"2030-01-01T12:00:00"}, {"amount":True}, {"policy_version":"unknown"}, {"source":"real_player"}):
            with self.subTest(changes=changes):
                item = self.normal()
                item.update(changes)
                with self.assertRaises(ValueError):
                    validate(item)
    def test_failed_rebuild_rolls_back_clean_and_quarantine(self):
        before = self.load([self.normal()])
        self.db.execute("CREATE TRIGGER block_write BEFORE INSERT ON clean_events BEGIN SELECT RAISE(ABORT, 'test failure'); END")
        self.db.commit()
        with self.assertRaises(sqlite3.IntegrityError):
            rebuild(self.db)
        self.assertEqual(report(self.db), before)
    def test_public_policy_cap(self):
        item = self.normal()
        item["period_start"] = "2029-12-31T12:00:00Z"
        self.assertEqual(validate(item)["expected_max"], 720)
    def test_generator_reproducibility(self):
        self.assertEqual(events(42), events(42))
        self.assertEqual(sorted(map(canonical, events(42))), sorted(map(canonical, events(7))))

if __name__ == "__main__":
    unittest.main()
