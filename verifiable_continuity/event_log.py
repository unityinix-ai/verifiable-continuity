"""SQLite-persisted discrete event log.

Each row is one discrete event: the node that produced it, its per-node
sequence number, the vector clock at production time, the JSON Patch delta
that was applied, and the SHA-256 hash-chain link. The log is the source of
truth -- state is always reconstructed by replaying it, so a node that was
offline can restore its exact session from local storage alone.
"""

from __future__ import annotations

import datetime
import json
import sqlite3

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq       INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id   TEXT NOT NULL,
    node_seq  INTEGER NOT NULL,
    clock     TEXT NOT NULL,
    ops       TEXT NOT NULL,
    prev_hash TEXT NOT NULL,
    hash      TEXT NOT NULL,
    ts        TEXT NOT NULL,
    UNIQUE (node_id, node_seq)
);
"""


def _row_to_event(row) -> dict:
    return {
        "node_id": row[0],
        "node_seq": row[1],
        "clock": json.loads(row[2]),
        "ops": json.loads(row[3]),
        "prev_hash": row[4],
        "hash": row[5],
        "ts": row[6],
    }


class EventLog:
    """SQLite-backed event log.

    Integrity is enforced by Node before import or replay. SQLite storage is
    not an authentication boundary: a writer with direct database access can
    alter rows, which must be detected by hash-chain verification.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.execute(_SCHEMA)
        self._conn.commit()

    @staticmethod
    def _serialized(event: dict) -> tuple:
        return (
            event["node_id"],
            event["node_seq"],
            json.dumps(event["clock"], sort_keys=True),
            json.dumps(event["ops"]),
            event["prev_hash"],
            event["hash"],
            event["ts"],
        )

    def append(self, *, node_id: str, node_seq: int, clock: dict,
               ops: list, prev_hash: str, hash: str,
               ts: str | None = None) -> bool:
        """Append one event. Returns False if its node sequence already exists."""
        ts = ts or datetime.datetime.now(datetime.timezone.utc).isoformat()
        cur = self._conn.execute(
            "INSERT OR IGNORE INTO events "
            "(node_id, node_seq, clock, ops, prev_hash, hash, ts) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (node_id, node_seq, json.dumps(clock, sort_keys=True),
             json.dumps(ops), prev_hash, hash, ts),
        )
        self._conn.commit()
        return cur.rowcount == 1

    def append_many(self, events: list[dict]) -> int:
        """Append a batch in one SQLite transaction; return rows inserted.

        Callers must validate the complete candidate chain before invoking
        this method. A failed SQL write rolls back the entire batch.
        """
        if not events:
            return 0
        inserted = 0
        with self._conn:
            for event in events:
                cur = self._conn.execute(
                    "INSERT OR IGNORE INTO events "
                    "(node_id, node_seq, clock, ops, prev_hash, hash, ts) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    self._serialized(event),
                )
                inserted += cur.rowcount
        return inserted

    def all_events(self) -> list[dict]:
        rows = self._conn.execute(
            "SELECT node_id, node_seq, clock, ops, prev_hash, hash, ts "
            "FROM events ORDER BY seq"
        ).fetchall()
        return [_row_to_event(r) for r in rows]

    def has(self, node_id: str, node_seq: int) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM events WHERE node_id = ? AND node_seq = ?",
            (node_id, node_seq),
        ).fetchone()
        return row is not None

    def max_node_seq(self, node_id: str) -> int:
        row = self._conn.execute(
            "SELECT MAX(node_seq) FROM events WHERE node_id = ?", (node_id,)
        ).fetchone()
        return row[0] or 0

    def last_hash(self, node_id: str) -> str | None:
        """Hash of the latest event from a node, or None if it has none."""
        row = self._conn.execute(
            "SELECT hash FROM events WHERE node_id = ? "
            "ORDER BY node_seq DESC LIMIT 1",
            (node_id,),
        ).fetchone()
        return row[0] if row else None

    def close(self) -> None:
        self._conn.close()
