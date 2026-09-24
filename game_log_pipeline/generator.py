"""Independent synthetic economy: no private game data or balance values."""
import random
from copy import deepcopy

def events(seed=42):
    def reward(event, player, transaction, claim, amount, start="2030-01-01T10:00:00Z",
               end="2030-01-01T12:00:00Z", reason="offline"):
        return dict(schema_version=1, source="synthetic", event_type="currency_transaction",
                    event_id=event, player_id=player, session_id="session_" + player,
                    transaction_id=transaction, reward_claim_id=claim, currency="demo_coin",
                    reason=reason, amount=amount, event_time=end,
                    period_start=start, period_end=end, policy_version="demo-v1")
    normal = reward("ev_normal", "p_normal", "tx_normal", "claim_normal", 720)
    quest = reward("ev_quest", "p_normal", "tx_quest", "claim_quest", 20, reason="quest")
    excess = reward("ev_excess", "p_excess", "tx_excess", "claim_excess", 999,
                    start="2030-01-01T11:50:00Z")
    repeated1 = reward("ev_repeat1", "p_repeat", "tx_repeat1", "claim_shared", 30,
                       start="2030-01-01T11:55:00Z")
    repeated2 = deepcopy(repeated1)
    repeated2.update(event_id="ev_repeat2", transaction_id="tx_repeat2")
    late = reward("ev_late", "p_late", "tx_late", "claim_late", 18,
                  start="2029-12-31T23:50:00Z", end="2029-12-31T23:53:00Z")
    invalid = reward("ev_invalid", "p_bad", "tx_bad", "claim_bad", -1)
    conflict = reward("ev_conflict", "p_conflict", "tx_conflict", "claim_conflict", 4, reason="quest")
    altered = deepcopy(conflict)
    altered["amount"] = 5
    result = [normal, quest, deepcopy(quest), excess, repeated1, repeated2, late, invalid, conflict, altered]
    random.Random(seed).shuffle(result)
    return result
