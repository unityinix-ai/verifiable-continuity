"""Tests: SQLite event log persistence and SHA-256 hash chaining."""
import os
import tempfile
import unittest

from verifiable_continuity import EventLog
from verifiable_continuity.hash_chain import (
    GENESIS_HASH,
    compute_hash,
    verify_chain,
    assert_chain,
    ChainError,
)


def _make_event(node_id, node_seq, clock, ops, prev_hash):
    ts = "2026-10-19T00:00:00+00:00"
    h = compute_hash(
        node_id=node_id, node_seq=node_seq, clock=clock,
        ops=ops, prev_hash=prev_hash, ts=ts,
    )
    return {
        "node_id": node_id, "node_seq": node_seq, "clock": clock,
        "ops": ops, "prev_hash": prev_hash, "hash": h, "ts": ts,
    }


class TestEventLog(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "log.db")
        self.log = EventLog(self.db)

    def tearDown(self):
        self.log.close()
        self.tmp.cleanup()

    def test_append_and_read_roundtrip(self):
        ok = self.log.append(
            node_id="n1", node_seq=1, clock={"n1": 1},
            ops=[{"op": "add", "path": "/a", "value": 1}],
            prev_hash=GENESIS_HASH, hash="abc123",
        )
        self.assertTrue(ok)
        events = self.log.all_events()
        self.assertEqual(len(events), 1)
        e = events[0]
        self.assertEqual(e["node_id"], "n1")
        self.assertEqual(e["node_seq"], 1)
        self.assertEqual(e["clock"], {"n1": 1})
        self.assertEqual(e["ops"], [{"op": "add", "path": "/a", "value": 1}])
        self.assertEqual(e["hash"], "abc123")

    def test_duplicate_node_seq_ignored(self):
        kwargs = dict(node_id="n1", node_seq=1, clock={"n1": 1},
                      ops=[], prev_hash=GENESIS_HASH, hash="h1")
        self.assertTrue(self.log.append(**kwargs))
        self.assertFalse(self.log.append(**{**kwargs, "hash": "h2"}))
        self.assertEqual(len(self.log.all_events()), 1)

    def test_max_node_seq(self):
        self.assertEqual(self.log.max_node_seq("n1"), 0)
        for i in (1, 2, 3):
            self.log.append(node_id="n1", node_seq=i, clock={"n1": i},
                            ops=[], prev_hash="p", hash=f"h{i}")
        self.assertEqual(self.log.max_node_seq("n1"), 3)
        self.assertEqual(self.log.max_node_seq("n2"), 0)

    def test_persists_across_reopen(self):
        self.log.append(node_id="n1", node_seq=1, clock={"n1": 1},
                        ops=[], prev_hash=GENESIS_HASH, hash="h1")
        self.log.close()
        log2 = EventLog(self.db)
        try:
            self.assertEqual(len(log2.all_events()), 1)
        finally:
            log2.close()


class TestHashChain(unittest.TestCase):
    def _chain(self, n=3):
        events, prev = [], GENESIS_HASH
        for i in range(1, n + 1):
            e = _make_event("n1", i, {"n1": i},
                            [{"op": "add", "path": f"/k{i}", "value": i}],
                            prev)
            events.append(e)
            prev = e["hash"]
        return events

    def test_intact_chain_verifies(self):
        events = self._chain(4)
        ok, where = verify_chain(events)
        self.assertTrue(ok)
        self.assertIsNone(where)
        assert_chain(events)  # must not raise

    def test_two_nodes_two_chains(self):
        e1 = _make_event("a", 1, {"a": 1}, [], GENESIS_HASH)
        e2 = _make_event("b", 1, {"b": 1}, [], GENESIS_HASH)
        ok, _ = verify_chain([e1, e2])
        self.assertTrue(ok)

    def test_tampered_ops_detected(self):
        events = self._chain(3)
        events[1]["ops"] = [{"op": "add", "path": "/evil", "value": True}]
        ok, where = verify_chain(events)
        self.assertFalse(ok)
        self.assertEqual(where, ("n1", 2))
        with self.assertRaises(ChainError):
            assert_chain(events)

    def test_tampered_clock_detected(self):
        events = self._chain(3)
        events[0]["clock"] = {"n1": 99}
        ok, where = verify_chain(events)
        self.assertFalse(ok)
        self.assertEqual(where, ("n1", 1))

    def test_broken_prev_link_detected(self):
        events = self._chain(3)
        events[2]["prev_hash"] = "f" * 64
        ok, where = verify_chain(events)
        self.assertFalse(ok)
        self.assertEqual(where, ("n1", 3))

    def test_sneaky_recomputed_hash_still_detected(self):
        # Attacker rewrites event 2's ops AND recomputes its hash, so the
        # event looks self-consistent -- the next link in the chain still
        # exposes it, because event 3's prev_hash points at the original.
        events = self._chain(3)
        evil = dict(events[1])
        evil["ops"] = [{"op": "add", "path": "/evil", "value": 1}]
        evil["hash"] = compute_hash(
            node_id=evil["node_id"], node_seq=evil["node_seq"],
            clock=evil["clock"], ops=evil["ops"],
            prev_hash=evil["prev_hash"], ts=evil["ts"],
        )
        events[1] = evil
        ok, where = verify_chain(events)
        self.assertFalse(ok)
        self.assertEqual(where, ("n1", 3))


if __name__ == "__main__":
    unittest.main()
