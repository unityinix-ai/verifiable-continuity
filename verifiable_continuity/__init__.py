"""Verifiable data continuity across distributed environments.

Local-first architecture: every node keeps a hash-chained, SQLite-persisted
event log of JSON Patch deltas, ordered causally with vector clocks, so state
can be reconstructed and verified anywhere -- even offline.
"""

from .vector_clock import VectorClock
from .json_patch import apply_patch, PatchError
from .event_log import EventLog
from .hash_chain import (
    GENESIS_HASH,
    compute_hash,
    verify_chain,
    ChainError,
)
from .node import Node, TamperError

__all__ = [
    "VectorClock",
    "apply_patch",
    "PatchError",
    "EventLog",
    "GENESIS_HASH",
    "compute_hash",
    "verify_chain",
    "ChainError",
    "Node",
    "TamperError",
]

__version__ = "0.1.0"
