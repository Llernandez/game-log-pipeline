import json
import unittest

from game_log_pipeline.generator import events
from game_log_pipeline.pipeline import canonical, connect, ingest, rebuild
from game_log_pipeline.transport import (MAX_BATCH_BYTES, MAX_BATCH_LINES, MAX_LINE_BYTES, BatchRejected,
                                         partition_key, split_batch)
from game_log_pipeline.warehouse import CLEAN_COLUMNS, rebuild_rows, statements


class TransportTest(unittest.TestCase):
    def test_split_keeps_raw_lines_and_skips_blank(self):
        body = b'{"event_id":"a"}\r\n\n  \n{"event_id":"b"}\n'
        self.assertEqual(split_batch(body), ['{"event_id":"a"}', '{"event_id":"b"}'])

    def test_invalid_json_is_kept_for_quarantine_not_rejected(self):
        self.assertEqual(split_batch(b"not json\n"), ["not json"])

    def test_limits_reject_whole_batch(self):
        for body in (b"", b"\n\n", "\xff".encode("latin-1"), b"x" * (MAX_BATCH_BYTES + 1),
                     b"{}\n" * (MAX_BATCH_LINES + 1), b"x" * (MAX_LINE_BYTES + 1)):
            with self.assertRaises(BatchRejected):
                split_batch(body)

    def test_partition_key_groups_retries(self):
        self.assertEqual(partition_key('{"event_id":"ev_1"}'), b"ev_1")
        for line in ("[]", "nope", '{"event_id":""}', '{"event_id":3}'):
            self.assertIsNone(partition_key(line))


class WarehouseTest(unittest.TestCase):
    def test_rebuild_rows_matches_reference_pipeline(self):
        lines = [canonical(item) for item in events(42)]
        reference = connect(":memory:")
        ingest(reference, [line + "\n" for line in lines])
        rebuild(reference)
        expected_clean = [tuple(r) for r in reference.execute(
            "SELECT %s FROM clean_events ORDER BY event_id" % ", ".join(CLEAN_COLUMNS))]
        expected_quarantine = [tuple(r) for r in reference.execute("SELECT raw_id, reason FROM quarantine ORDER BY raw_id")]
        raw = [(index + 1, line, "2030-01-02T00:00:00+00:00") for index, line in enumerate(lines)]
        clean, quarantine, _ = rebuild_rows(raw)
        self.assertEqual((clean, quarantine), (expected_clean, expected_quarantine))
        self.assertEqual((len(clean), len(quarantine)), (6, 3))

    def test_warehouse_raw_ids_are_preserved(self):
        raw = [(1000 + index, canonical(item), "t") for index, item in enumerate(events(7))]
        clean, quarantine, _ = rebuild_rows(raw)
        self.assertTrue(all(row[-1] >= 1000 for row in clean))
        self.assertTrue(all(raw_id >= 1000 for raw_id, _ in quarantine))

    def test_mart_sql_has_no_sqlite_only_syntax(self):
        text = " ".join(statements("marts.sql")).upper()
        self.assertNotIn("IF NOT EXISTS TRANSACTIONS", text)
        self.assertIn("CREATE OR REPLACE VIEW ANOMALY_CANDIDATES", text)
        self.assertEqual(len(statements("marts.sql")), 9)
        self.assertEqual(len(statements("landing_postgres.sql")), 6)


if __name__ == "__main__":
    unittest.main()
