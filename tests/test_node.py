"""Tests: nodes -- sync convergence, causal order, offline restore, tampering."""
import os
import sqlite3
import tempfile
import unittest

from verifiable_continuity import Node, TamperError


class NodeTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.nodes = []

    def tearDown(self):
        for n in self.nodes:
            try:
                n.close()
            except Exception:
                pass
        self.tmp.cleanup()

    def make_node(self, node_id):
        n = Node(node_id, os.path.join(self.tmp.name, f"{node_id}.db"))
        self.nodes.append(n)
        return n


class TestNodeBasics(NodeTestCase):
    def test_local_updates_build_state(self):
        a = self.make_node("a")
        a.update([{"op": "add", "path": "/name", "value": "aurora"}])
        a.update([{"op": "add", "path": "/tags", "value": []}])
        a.update([{"op": "add", "path": "/tags/-", "value": "v1"}])
        self.assertEqual(a.state, {"name": "aurora", "tags": ["v1"]})

    def test_update_returns_hash_and_verifies(self):
        a = self.make_node("a")
        h = a.update([{"op": "add", "path": "/x", "value": 1}])
        self.assertEqual(len(h), 64)
        ok, where = a.verify()
        self.assertTrue(ok)
        self.assertIsNone(where)

    def test_invalid_patch_not_committed(self):
        a = self.make_node("a")
        with self.assertRaises(Exception):
            a.update([{"op": "remove", "path": "/missing"}])
        self.assertEqual(a.state, {})
        self.assertEqual(len(a.export_events()), 0)


class TestSync(NodeTestCase):
    def test_two_nodes_converge(self):
        a = self.make_node("a")
        b = self.make_node("b")
        a.update([{"op": "add", "path": "/from_a", "value": 1}])
        b.update([{"op": "add", "path": "/from_b", "value": 2}])
        # Before sync they disagree.
        self.assertNotEqual(a.state, b.state)
        a.sync_with(b)
        self.assertEqual(a.state, b.state)
        self.assertEqual(a.state, {"from_a": 1, "from_b": 2})

    def test_causal_order_respected(self):
        a = self.make_node("a")
        b = self.make_node("b")
        a.update([{"op": "add", "path": "/doc", "value": {"title": "draft"}}])
        a.sync_with(b)  # b now causally follows a's write
        b.update([{"op": "replace", "path": "/doc/title", "value": "final"}])
        a.sync_with(b)
        self.assertEqual(a.state["doc"]["title"], "final")
        self.assertEqual(b.state["doc"]["title"], "final")

    def test_three_nodes_converge(self):
        nodes = [self.make_node(n) for n in ("a", "b", "c")]
        nodes[0].update([{"op": "add", "path": "/a", "value": 1}])
        nodes[1].update([{"op": "add", "path": "/b", "value": 2}])
        nodes[2].update([{"op": "add", "path": "/c", "value": 3}])
        nodes[0].sync_with(nodes[1])
        nodes[1].sync_with(nodes[2])
        nodes[0].sync_with(nodes[2])
        states = [n.state for n in nodes]
        self.assertTrue(all(s == states[0] for s in states))
        self.assertEqual(states[0], {"a": 1, "b": 2, "c": 3})

    def test_sync_is_idempotent(self):
        a = self.make_node("a")
        b = self.make_node("b")
        a.update([{"op": "add", "path": "/x", "value": 1}])
        a.sync_with(b)
        count_before = len(a.export_events())
        a.sync_with(b)
        self.assertEqual(len(a.export_events()), count_before)
        self.assertEqual(a.state, b.state)

    def test_offline_node_catches_up(self):
        a = self.make_node("a")
        b = self.make_node("b")
        a.update([{"op": "add", "path": "/v", "value": 1}])
        a.update([{"op": "replace", "path": "/v", "value": 2}])
        a.update([{"op": "replace", "path": "/v", "value": 3}])
        # b was offline for all three; one sync brings it fully current.
        b.sync_with(a)
        self.assertEqual(b.state, {"v": 3})
        ok, _ = b.verify()
        self.assertTrue(ok)


class TestOfflineRestore(NodeTestCase):
    def test_restore_reconstructs_exact_state(self):
        db = os.path.join(self.tmp.name, "sess.db")
        n1 = Node("solo", db)
        n1.update([{"op": "add", "path": "/session", "value": {"step": 1}}])
        n1.update([{"op": "replace", "path": "/session/step", "value": 2}])
        n1.update([{"op": "add", "path": "/session/tags", "value": ["a", "b"]}])
        expected = dict(n1.state)
        n1.close()

        # Simulate a restart with no network: reopen the same file.
        n2 = Node.restore("solo", db)
        self.nodes.append(n2)
        self.assertEqual(n2.state, expected)
        ok, _ = n2.verify()
        self.assertTrue(ok)
        # And the restored node can keep writing on the same chain.
        n2.update([{"op": "replace", "path": "/session/step", "value": 3}])
        self.assertEqual(n2.state["session"]["step"], 3)
        ok, _ = n2.verify()
        self.assertTrue(ok)


class TestTamperDetection(NodeTestCase):
    def _tamper_db(self, db_path, node_seq, new_ops_json):
        conn = sqlite3.connect(db_path)
        conn.execute(
            "UPDATE events SET ops = ? WHERE node_id = 'v' AND node_seq = ?",
            (new_ops_json, node_seq),
        )
        conn.commit()
        conn.close()

    def test_tampered_log_detected_on_verify(self):
        db = os.path.join(self.tmp.name, "v.db")
        v = Node("v", db)
        v.update([{"op": "add", "path": "/balance", "value": 100}])
        v.update([{"op": "add", "path": "/owner", "value": "alex"}])
        v.close()

        self._tamper_db(db, 1, '[{"op": "add", "path": "/balance", "value": 999999}]')

        v2 = Node.restore("v", db)
        self.nodes.append(v2)
        ok, where = v2.verify()
        self.assertFalse(ok)
        self.assertEqual(where, ("v", 1))
        with self.assertRaises(TamperError):
            # importing the tampered log into a clean node is rejected
            clean = self.make_node("clean")
            clean.import_events(v2.export_events())

    def test_import_rejects_forged_event(self):
        a = self.make_node("a")
        b = self.make_node("b")
        a.update([{"op": "add", "path": "/x", "value": 1}])
        forged = dict(a.export_events()[0])
        forged["ops"] = [{"op": "add", "path": "/x", "value": "forged"}]
        with self.assertRaises(TamperError):
            b.import_events([forged])
        self.assertEqual(b.state, {})


if __name__ == "__main__":
    unittest.main()
