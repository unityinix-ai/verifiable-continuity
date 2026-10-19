"""SHA-256 hash chaining over the event log.

Every event commits to its own content (node id, per-node sequence, vector
clock, patch operations, timestamp) and to the hash of the previous event
from the *same* node. Chains are per-node, so events keep their integrity
no matter how logs are merged or reordered during sync.

Tampering with any logged record -- its operations, its clock, or its
link to the previous record -- breaks the chain and is detected by
:func:`verify_chain`.
"""

from __future__ import annotations

import hashlib
import json

GENESIS_HASH = "0" * 64


class ChainError(Exception):
    """Raised when a hash chain fails verification."""


def canonical_payload(*, node_id: str, node_seq: int, clock: dict,
                      ops: list, prev_hash: str, ts: str) -> str:
    """Canonical JSON encoding of everything an event commits to."""
    return json.dumps(
        {
            "node_id": node_id,
            "node_seq": node_seq,
            "clock": clock,
            "ops": ops,
            "prev_hash": prev_hash,
            "ts": ts,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def compute_hash(*, node_id: str, node_seq: int, clock: dict,
                 ops: list, prev_hash: str, ts: str) -> str:
    """SHA-256 hex digest committing to an event and its predecessor."""
    payload = canonical_payload(
        node_id=node_id, node_seq=node_seq, clock=clock,
        ops=ops, prev_hash=prev_hash, ts=ts,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _event_hash(event: dict) -> str:
    return compute_hash(
        node_id=event["node_id"],
        node_seq=event["node_seq"],
        clock=event["clock"],
        ops=event["ops"],
        prev_hash=event["prev_hash"],
        ts=event["ts"],
    )


def verify_chain(events: list[dict]) -> tuple[bool, tuple | None]:
    """Verify per-node hash chains over a list of logged events.

    Returns ``(True, None)`` when every chain is intact, otherwise
    ``(False, (node_id, node_seq))`` identifying the first broken link --
    the event whose stored hash does not match its content, or whose
    ``prev_hash`` does not match the previous event from the same node.
    """
    by_node: dict[str, list[dict]] = {}
    for e in events:
        by_node.setdefault(e["node_id"], []).append(e)
    for node_id, node_events in by_node.items():
        node_events = sorted(node_events, key=lambda e: e["node_seq"])
        prev = GENESIS_HASH
        for e in node_events:
            if e["prev_hash"] != prev:
                return False, (node_id, e["node_seq"])
            if _event_hash(e) != e["hash"]:
                return False, (node_id, e["node_seq"])
            prev = e["hash"]
    return True, None


def assert_chain(events: list[dict]) -> None:
    """Verify chains, raising :class:`ChainError` on the first broken link."""
    ok, where = verify_chain(events)
    if not ok:
        node_id, node_seq = where
        raise ChainError(
            f"hash chain broken at node={node_id!r} seq={node_seq}: "
            "log was tampered with or corrupted"
        )
