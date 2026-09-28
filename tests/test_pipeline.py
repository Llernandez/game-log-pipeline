import json
import sqlite3
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from game_log_pipeline.generator import events
from game_log_pipeline.pipeline import canonical, connect, ingest, rebuild, report, upcast, validate

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

class SchemaEvolutionTests(unittest.TestCase):
    """Producers upgrade gradually: v1-v3 arrive together and normalize to one business shape."""
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = connect(Path(self.tmp.name) / "evolution.sqlite")
    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()
    def base(self, event="ev_v", transaction="tx_v"):
        item = deepcopy(next(x for x in events() if x["event_id"] == "ev_normal"))
        item.update(event_id=event, transaction_id=transaction, reward_claim_id="claim_" + event)
        return item
    def v2(self, item, client="1.4.0"):
        return dict(item, schema_version=2, client_version=client)
    def v3(self, item):
        upgraded = self.v2(item, "2.0.0")
        upgraded["schema_version"] = 3
        upgraded["currency_code"] = upgraded.pop("currency")
        return upgraded
    def load(self, items):
        ingest(self.db, [canonical(x) for x in items])
        rebuild(self.db)
        return report(self.db)
    def test_all_supported_versions_become_clean_events(self):
        result = self.load([self.base("ev_1", "tx_1"), self.v2(self.base("ev_2", "tx_2")), self.v3(self.base("ev_3", "tx_3"))])
        self.assertEqual(result["counts"]["clean_events"], 3)
        self.assertEqual(result["counts"]["quarantine"], 0)
        self.assertEqual(result["schema_versions"], {"1": 1, "2": 1, "3": 1})
    def test_same_event_from_an_upgraded_client_is_a_retry_not_a_conflict(self):
        item = self.base()
        result = self.load([item, self.v3(item)])
        self.assertEqual(result["counts"]["clean_events"], 1)
        self.assertEqual(result["counts"]["quarantine"], 0)
    def test_contract_breaks_are_quarantined(self):
        missing_client = dict(self.base("ev_a", "tx_a"), schema_version=2)
        old_field_in_v3 = dict(self.v2(self.base("ev_b", "tx_b")), schema_version=3)
        future = dict(self.base("ev_c", "tx_c"), schema_version=4)
        result = self.load([missing_client, old_field_in_v3, future])
        self.assertEqual(result["counts"]["clean_events"], 0)
        reasons = [row[0] for row in self.db.execute("SELECT reason FROM quarantine ORDER BY raw_id")]
        self.assertEqual(reasons, ["invalid_schema: client_version required from schema 2",
                                   "invalid_schema: schema 3 uses currency_code",
                                   "invalid_schema: unsupported schema_version"])
    def test_upcast_keeps_v1_untouched(self):
        item = self.base()
        self.assertEqual(upcast(item), item)

