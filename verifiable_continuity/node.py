"""A continuity node: local-first state with sync, verify, and offline restore.

A node owns:
  - an :class:`EventLog` (SQLite persistence of discrete events + deltas),
  - a :class:`VectorClock` (causal ordering),
  - a live JSON document (rebuilt by replaying the log),
  - a per-node SHA-256 hash chain over its own events.

Sync merges event sets with a peer and replays both in one deterministic
causal order, so all nodes converge on identical state. Because the log is
the source of truth, a node restarted from its database file restores the
exact session it had -- offline, with zero data loss.
"""

from __future__ import annotations

import datetime
from functools import cmp_to_key

from .event_log import EventLog
from .hash_chain import GENESIS_HASH, compute_hash, verify_chain, assert_chain
from .json_patch import apply_patch
from .vector_clock import VectorClock


class TamperError(Exception):
    """Raised when an event fails integrity checks during import or verify."""


def _causal_order(events: list[dict]) -> list[dict]:
    """Total order of events that respects causal (happens-before) order.

    Concurrent events are tie-broken deterministically by
    (node_id, node_seq), so every node holding the same event set replays
    the identical sequence and converges on identical state.
    """

    def cmp(a: dict, b: dict) -> int:
        ca = VectorClock.from_dict(a["clock"])
        cb = VectorClock.from_dict(b["clock"])
        rel = ca.compare(cb)
        if rel == "before":
            return -1
        if rel == "after":
            return 1
        ka = (a["node_id"], a["node_seq"])
        kb = (b["node_id"], b["node_seq"])
        if ka < kb:
            return -1
        if ka > kb:
            return 1
        return 0

    return sorted(events, key=cmp_to_key(cmp))


class Node:
    """One participant in the continuity fabric."""

    def __init__(self, node_id: str, db_path: str):
        self.node_id = node_id
        self.log = EventLog(db_path)
        self.clock = VectorClock()
        self.state = {}
        self._rebuild()

    # -- local writes ----------------------------------------------------

    def update(self, ops: list[dict]) -> str:
        """Apply a JSON Patch locally: tick clock, chain, persist, apply.

        Returns the event's SHA-256 hash.
        """
        # Validate the patch against current state before committing anything.
        new_state = apply_patch(self.state, ops)
        self.clock = self.clock.tick(self.node_id)
        node_seq = self.log.max_node_seq(self.node_id) + 1
        prev_hash = self.log.last_hash(self.node_id) or GENESIS_HASH
        ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
        clock_dict = self.clock.to_dict()
        event_hash = compute_hash(
            node_id=self.node_id, node_seq=node_seq, clock=clock_dict,
            ops=ops, prev_hash=prev_hash, ts=ts,
        )
        self.log.append(
            node_id=self.node_id, node_seq=node_seq, clock=clock_dict,
            ops=ops, prev_hash=prev_hash, hash=event_hash, ts=ts,
        )
        self.state = new_state
        return event_hash

    # -- sync ------------------------------------------------------------

    def export_events(self) -> list[dict]:
        return self.log.all_events()

    def import_events(self, events: list[dict]) -> int:
        """Merge foreign events after integrity checks. Returns count added."""
        added = 0
        for e in events:
            if self.log.has(e["node_id"], e["node_seq"]):
                continue
            self._check_event(e)
            self.log.append(
                node_id=e["node_id"], node_seq=e["node_seq"],
                clock=e["clock"], ops=e["ops"],
                prev_hash=e["prev_hash"], hash=e["hash"], ts=e["ts"],
            )
            added += 1
        if added:
            self._rebuild()
        return added

    def _check_event(self, e: dict) -> None:
        recomputed = compute_hash(
            node_id=e["node_id"], node_seq=e["node_seq"], clock=e["clock"],
            ops=e["ops"], prev_hash=e["prev_hash"], ts=e["ts"],
        )
        if recomputed != e["hash"]:
            raise TamperError(
                f"event from node {e['node_id']!r} seq {e['node_seq']} "
                "fails hash check -- rejected"
            )

    def sync_with(self, peer: "Node") -> None:
        """Bidirectional sync: exchange logs, then both converge."""
        peer.import_events(self.export_events())
        self.import_events(peer.export_events())

    # -- verify / reconstruct --------------------------------------------

    def verify(self) -> tuple[bool, tuple | None]:
        """Verify the hash chains of the local log.

        Returns (True, None) when intact, else (False, (node_id, seq))
        of the first broken link.
        """
        return verify_chain(self.log.all_events())

    def assert_verified(self) -> None:
        assert_chain(self.log.all_events())

    def _rebuild(self) -> None:
        """Reconstruct clock and state by replaying the log in causal order."""
        events = self.log.all_events()
        clock = VectorClock()
        for e in events:
            clock = clock.merge(VectorClock.from_dict(e["clock"]))
        self.clock = clock
        state = {}
        for e in _causal_order(events):
            state = apply_patch(state, e["ops"])
        self.state = state

    @classmethod
    def restore(cls, node_id: str, db_path: str) -> "Node":
        """Offline session restore: reopen a persisted log, replay it exactly."""
        return cls(node_id, db_path)

    def close(self) -> None:
        self.log.close()
