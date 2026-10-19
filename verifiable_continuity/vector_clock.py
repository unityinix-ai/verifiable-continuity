"""Vector clocks for causal ordering across decentralized nodes.

Each node keeps a counter per known node id. Comparing two clocks tells us
whether one event causally precedes another, or whether they are concurrent
(neither saw the other). Merging takes the element-wise maximum so a node
that has seen an event always dominates it afterwards.
"""

from __future__ import annotations


class VectorClock:
    """A vector clock: mapping of node id -> logical counter."""

    def __init__(self, counters: dict | None = None):
        self._c: dict[str, int] = dict(counters) if counters else {}

    def copy(self) -> "VectorClock":
        return VectorClock(self._c)

    def tick(self, node_id: str) -> "VectorClock":
        """Return a new clock with this node's counter incremented."""
        nxt = self.copy()
        nxt._c[node_id] = nxt._c.get(node_id, 0) + 1
        return nxt

    def merge(self, other: "VectorClock") -> "VectorClock":
        """Return a new clock holding the element-wise maximum."""
        keys = set(self._c) | set(other._c)
        return VectorClock(
            {k: max(self._c.get(k, 0), other._c.get(k, 0)) for k in keys}
        )

    def compare(self, other: "VectorClock") -> str:
        """Return 'before', 'after', 'equal', or 'concurrent'.

        'before'  -- every counter of self <= other's, and at least one <.
        'after'   -- the reverse.
        'equal'   -- all counters identical.
        'concurrent' -- neither dominates (causally unrelated events).
        """
        keys = set(self._c) | set(other._c)
        less = greater = False
        for k in keys:
            a, b = self._c.get(k, 0), other._c.get(k, 0)
            if a < b:
                less = True
            elif a > b:
                greater = True
        if less and greater:
            return "concurrent"
        if less:
            return "before"
        if greater:
            return "after"
        return "equal"

    def happened_before(self, other: "VectorClock") -> bool:
        return self.compare(other) == "before"

    def concurrent_with(self, other: "VectorClock") -> bool:
        return self.compare(other) == "concurrent"

    def to_dict(self) -> dict:
        return dict(self._c)

    @classmethod
    def from_dict(cls, d: dict) -> "VectorClock":
        return cls(d)

    def __eq__(self, other) -> bool:
        return isinstance(other, VectorClock) and self._c == other._c

    def __repr__(self) -> str:
        return f"VectorClock({self._c!r})"
