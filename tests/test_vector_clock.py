"""Tests: vector clock causal ordering and merge."""
import unittest

from verifiable_continuity import VectorClock


class TestVectorClock(unittest.TestCase):
    def test_tick_increments(self):
        c = VectorClock().tick("a").tick("a").tick("b")
        self.assertEqual(c.to_dict(), {"a": 2, "b": 1})

    def test_tick_returns_new_clock(self):
        c = VectorClock()
        c2 = c.tick("a")
        self.assertEqual(c.to_dict(), {})
        self.assertEqual(c2.to_dict(), {"a": 1})

    def test_equal(self):
        a = VectorClock({"x": 1, "y": 2})
        b = VectorClock({"x": 1, "y": 2})
        self.assertEqual(a.compare(b), "equal")

    def test_before_after(self):
        a = VectorClock({"x": 1})
        b = a.tick("x")
        self.assertEqual(a.compare(b), "before")
        self.assertEqual(b.compare(a), "after")
        self.assertTrue(a.happened_before(b))

    def test_before_with_extra_node(self):
        a = VectorClock({"x": 2})
        b = VectorClock({"x": 2, "y": 1})
        self.assertEqual(a.compare(b), "before")

    def test_concurrent(self):
        a = VectorClock({"x": 1})
        b = VectorClock({"y": 1})
        self.assertEqual(a.compare(b), "concurrent")
        self.assertTrue(a.concurrent_with(b))

    def test_merge_takes_max(self):
        a = VectorClock({"x": 3, "y": 1})
        b = VectorClock({"x": 1, "y": 5, "z": 2})
        self.assertEqual(a.merge(b).to_dict(), {"x": 3, "y": 5, "z": 2})

    def test_merge_dominates_merged_events(self):
        local = VectorClock({"a": 2})
        seen = VectorClock({"a": 1, "b": 4})
        merged = local.merge(seen)
        self.assertEqual(merged.compare(seen), "after")

    def test_roundtrip_dict(self):
        c = VectorClock({"a": 7})
        self.assertEqual(VectorClock.from_dict(c.to_dict()), c)


if __name__ == "__main__":
    unittest.main()
