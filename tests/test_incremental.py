"""Incremental rebuild must equal a full rebuild after every batch, whatever arrives when."""
import random
import unittest
from copy import deepcopy

from game_log_pipeline.generator import events
from game_log_pipeline.incremental import closure, keys
from game_log_pipeline.pipeline import canonical
from game_log_pipeline.warehouse import delta, rebuild_rows


def fixture_lines():
    """The demo fixture plus the cases that make incremental processing hard."""
    base = events(42)
    normal = next(item for item in base if item["event_id"] == "ev_normal")
    extra = []
    # A conflicting delivery of an event that was already clean, arriving later.
    altered = deepcopy(normal)
    altered["amount"] = 721
    extra.append(altered)
    # Same transaction under a new event id with different content: a transaction conflict.
    clash = deepcopy(normal)
    clash.update(event_id="ev_normal_clash", amount=10)
    extra.append(clash)
    # The same event re-sent by an upgraded client (schema 3): a retry, not a conflict.
    quest = deepcopy(next(item for item in base if item["event_id"] == "ev_quest"))
    quest.update(schema_version=3, client_version="2.0.0", currency_code=quest.pop("currency"))
    extra.append(quest)
    # A second claim on an existing reward claim, and a much later late arrival.
    second = deepcopy(next(item for item in base if item["event_id"] == "ev_repeat1"))
    second.update(event_id="ev_repeat3", transaction_id="tx_repeat3")
    extra.append(second)
    late = deepcopy(next(item for item in base if item["event_id"] == "ev_late"))
    late.update(event_id="ev_late2", transaction_id="tx_late2", reward_claim_id="claim_late2")
    extra.append(late)
    return [canonical(item) for item in base + extra] + ["{not json", '{"event_id": 5}']


class IncrementalRebuildTests(unittest.TestCase):
    def test_keys(self):
        normal = next(item for item in events(42) if item["event_id"] == "ev_normal")
        self.assertEqual(keys(canonical(normal)), ("ev_normal", "p_normal|demo_coin|tx_normal"))
        invalid = dict(normal, amount=-1)
        self.assertEqual(keys(canonical(invalid)), ("ev_normal", None))
        self.assertEqual(keys("{not json"), (None, None))

    def test_closure_follows_event_and_transaction_keys(self):
        rows = [(1, "", "t", "a", "t1"), (2, "", "t", "b", "t1"), (3, "", "t", "c", "t2"), (4, "", "t", "b", None)]
        lookup = lambda events, txns: [r for r in rows if r[3] in events or r[4] in txns]
        found, events = closure([rows[0]], lookup)
        self.assertEqual([r[0] for r in found], [1, 2, 4])
        self.assertEqual(events, {"a", "b"})

    def test_incremental_equals_full_rebuild_after_every_batch(self):
        lines = fixture_lines()
        for seed in range(40):
            order = lines[:]
            random.Random(seed).shuffle(order)
            # Retries: some deliveries arrive twice, anywhere in the stream.
            order += random.Random(seed + 1000).sample(order, 4)
            random.Random(seed + 2000).shuffle(order)
            raw = [(index + 1, line, "t") + keys(line) for index, line in enumerate(order)]
            lookup = lambda events, txns: [r for r in raw[:upto] if r[3] in events or r[4] in txns]
            clean, quarantine, watermark = {}, {}, 0
            cuts = sorted(random.Random(seed + 3000).sample(range(1, len(raw)), 5)) + [len(raw)]
            for upto in cuts:
                change = delta(raw[watermark:upto], lookup)
                for event_id in change["events"]:
                    clean.pop(event_id, None)
                for raw_id in change["raw_ids"]:
                    quarantine.pop(raw_id, None)
                clean.update({row[0]: row for row in change["clean"]})
                quarantine.update(dict(change["quarantine"]))
                watermark = upto
                full_clean, full_quarantine = rebuild_rows([row[:3] for row in raw[:upto]])
                with self.subTest(seed=seed, upto=upto):
                    self.assertEqual(sorted(clean.values()), sorted(full_clean))
                    self.assertEqual(sorted(quarantine.items()), sorted(full_quarantine))

    def test_one_new_row_recomputes_only_its_keys(self):
        lines = fixture_lines()
        raw = [(index + 1, line, "t") + keys(line) for index, line in enumerate(lines)]
        lookup = lambda events, txns: [r for r in raw if r[3] in events or r[4] in txns]
        late = next(r for r in raw if r[3] == "ev_late2")
        change = delta([late], lookup)
        self.assertEqual(change["events"], ["ev_late2"])
        self.assertEqual(change["raw_ids"], [late[0]])


if __name__ == "__main__":
    unittest.main()
