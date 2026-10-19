"""Tests: RFC 6902 JSON Patch engine (add / remove / replace)."""
import unittest

from verifiable_continuity import apply_patch, PatchError


class TestJsonPatch(unittest.TestCase):
    def test_add_object_member(self):
        doc = {"a": 1}
        out = apply_patch(doc, [{"op": "add", "path": "/b", "value": 2}])
        self.assertEqual(out, {"a": 1, "b": 2})
        self.assertEqual(doc, {"a": 1})  # input not mutated

    def test_add_nested_missing_parent_raises(self):
        # RFC 6902: the target location's parent must already exist.
        with self.assertRaises(PatchError):
            apply_patch({}, [{"op": "add", "path": "/a/b/c", "value": 1}])

    def test_add_nested_with_parents(self):
        out = apply_patch(
            {"a": {"b": {}}},
            [{"op": "add", "path": "/a/b/c", "value": 1}],
        )
        self.assertEqual(out, {"a": {"b": {"c": 1}}})

    def test_add_array_index(self):
        out = apply_patch({"l": [1, 3]}, [{"op": "add", "path": "/l/1", "value": 2}])
        self.assertEqual(out, {"l": [1, 2, 3]})

    def test_add_array_append_dash(self):
        out = apply_patch({"l": [1]}, [{"op": "add", "path": "/l/-", "value": 2}])
        self.assertEqual(out, {"l": [1, 2]})

    def test_remove_member(self):
        out = apply_patch({"a": 1, "b": 2}, [{"op": "remove", "path": "/a"}])
        self.assertEqual(out, {"b": 2})

    def test_remove_array_element(self):
        out = apply_patch({"l": [1, 2, 3]}, [{"op": "remove", "path": "/l/0"}])
        self.assertEqual(out, {"l": [2, 3]})

    def test_replace_member(self):
        out = apply_patch(
            {"a": 1}, [{"op": "replace", "path": "/a", "value": 99}]
        )
        self.assertEqual(out, {"a": 99})

    def test_replace_root(self):
        out = apply_patch(
            {"a": 1}, [{"op": "replace", "path": "", "value": [1, 2]}]
        )
        self.assertEqual(out, [1, 2])

    def test_add_root(self):
        out = apply_patch({"a": 1}, [{"op": "add", "path": "", "value": 5}])
        self.assertEqual(out, 5)

    def test_escaped_pointer(self):
        out = apply_patch(
            {"a/b": {"~key": 1}},
            [{"op": "replace", "path": "/a~1b/~0key", "value": 2}],
        )
        self.assertEqual(out, {"a/b": {"~key": 2}})

    def test_sequential_ops(self):
        out = apply_patch(
            {},
            [
                {"op": "add", "path": "/a", "value": 1},
                {"op": "add", "path": "/b", "value": 2},
                {"op": "remove", "path": "/a"},
                {"op": "replace", "path": "/b", "value": 3},
            ],
        )
        self.assertEqual(out, {"b": 3})

    def test_remove_missing_member_raises(self):
        with self.assertRaises(PatchError):
            apply_patch({"a": 1}, [{"op": "remove", "path": "/zzz"}])

    def test_replace_missing_member_raises(self):
        with self.assertRaises(PatchError):
            apply_patch({"a": 1}, [{"op": "replace", "path": "/zzz", "value": 1}])

    def test_remove_root_raises(self):
        with self.assertRaises(PatchError):
            apply_patch({"a": 1}, [{"op": "remove", "path": ""}])

    def test_unknown_op_raises(self):
        with self.assertRaises(PatchError):
            apply_patch({"a": 1}, [{"op": "move", "path": "/a", "value": 1}])

    def test_bad_pointer_raises(self):
        with self.assertRaises(PatchError):
            apply_patch({"a": 1}, [{"op": "add", "path": "a", "value": 1}])

    def test_add_without_value_raises(self):
        with self.assertRaises(PatchError):
            apply_patch({"a": 1}, [{"op": "add", "path": "/b"}])


if __name__ == "__main__":
    unittest.main()
