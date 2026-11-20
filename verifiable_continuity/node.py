"""A continuity node: local-first state with sync, verify, and offline restore.

A node owns:
  - an :class:`EventLog` (SQLite persistence of discrete events + deltas),
  - a :class:`VectorClock` (causal ordering),
  - a live JSON document (rebuilt by replaying the log),
  - a per-node SHA-256 hash chain over its own events.

Sync merges event sets with a peer and replays both in one deterministic
causal order, so all nodes converge on identical state. Integrity is checked
before imported events are committed and before any persisted state is replayed.
"""

from __future__ import annotations

import datetime
from functools import cmp_to_key

from .event_log import EventLog
from .hash_chain import GENESIS_HASH, compute_hash, verify_chain, assert_chain
from .json_patch import apply_patch
from .vector_clock import VectorClock


class TamperError(Exception):
    """Raised when an event fails integrity checks during import or restore."""


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


def _assert_log_integrity(events: list[dict], *, context: str) -> None:
    """Fail closed unless every per-node chain in the candidate log verifies."""
    try:
        ok, where = verify_chain(events)
    except (KeyError, TypeError, ValueError) as exc:
        raise TamperError(f"{context}: malformed event record") from exc
    if not ok:
        node_id, node_seq = where
        raise TamperError(
            f"{context}: hash chain broken at node={node_id!r} "
            f"seq={node_seq}; no state was accepted"
        )


class Node:
    """One participant in the continuity fabric."""

    def __init__(self, node_id: str, db_path: str):
        self.node_id = node_id
        self.log = EventLog(db_path)
        self.clock = VectorClock()
        self.state = {}
        # _rebuild verifies the complete persisted log before replaying it.
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
        """Atomically merge foreign events after full-chain validation.

        Existing sequence keys must match byte-for-byte at the decoded event
        level. Conflicting duplicates are rejected rather than silently
        ignored. The complete candidate log is verified before any insert.
        """
        existing = self.log.all_events()
        existing_by_key = {
            (e["node_id"], e["node_seq"]): e for e in existing
        }
        new_events = []
        seen_in_batch = {}

        for event in events:
            try:
                key = (event["node_id"], event["node_seq"])
            except (KeyError, TypeError) as exc:
                raise TamperError("import rejected: malformed event identity") from exc

            if key in seen_in_batch:
                if seen_in_batch[key] != event:
                    raise TamperError(
                        f"import rejected: conflicting duplicate {key!r}"
                    )
                continue
            seen_in_batch[key] = event

            if key in existing_by_key:
                if existing_by_key[key] != event:
                    raise TamperError(
                        f"import rejected: conflicting existing event {key!r}"
                    )
                continue
            new_events.append(event)

        candidate = existing + new_events
        _assert_log_integrity(candidate, context="import rejected")

        inserted = self.log.append_many(new_events)
        if inserted != len(new_events):
            # This should only occur if another writer raced this node. Do not
            # replay; force the caller to reopen and inspect the persisted log.
            raise TamperError(
                "import interrupted by a concurrent database writer; "
                "reopen and verify the log before continuing"
            )

        if new_events:
            self._rebuild()
        return inserted

    def _check_event(self, e: dict) -> None:
        """Check one event's self-hash; full chain checks happen at batch level."""
        try:
            recomputed = compute_hash(
                node_id=e["node_id"], node_seq=e["node_seq"], clock=e["clock"],
                ops=e["ops"], prev_hash=e["prev_hash"], ts=e["ts"],
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise TamperError("malformed event record") from exc
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
        """Verify the hash chains of the local log."""
        return verify_chain(self.log.all_events())

    def assert_verified(self) -> None:
        assert_chain(self.log.all_events())

    def _rebuild(self) -> None:
        """Verify first, then reconstruct clock and state from the event log."""
        events = self.log.all_events()
        _assert_log_integrity(events, context="restore rejected")

        clock = VectorClock()
        for event in events:
            clock = clock.merge(VectorClock.from_dict(event["clock"]))
        self.clock = clock

        state = {}
        for event in _causal_order(events):
            state = apply_patch(state, event["ops"])
        self.state = state

    @classmethod
    def restore(cls, node_id: str, db_path: str) -> "Node":
        """Offline restore; corrupted logs raise TamperError before replay."""
        return cls(node_id, db_path)

    def close(self) -> None:
        self.log.close()
