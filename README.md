# Verifiable Continuity

A framework for **verifiable data continuity across distributed environments** —
local-first architecture for mobile terminals, desktop workstations, and cloud
infrastructure. Zero data loss, verifiable consistency, offline-capable session
restoration.

Standard library only. No dependencies.

## The four mechanisms

1. **Local Persistence** (`verifiable_continuity/event_log.py`)
   Every node keeps a SQLite-backed, append-only log of discrete events. Each
   event stores the JSON Patch delta that was applied — never just the
   resulting state. The log is the source of truth; state is always
   reconstructed by replaying it.

2. **Vector Clocks** (`verifiable_continuity/vector_clock.py`)
   Every event carries the vector clock at production time, giving strictly
   quantifiable causal ordering across decentralized nodes. Comparison yields
   `before` / `after` / `equal` / `concurrent`. Sync replays events in a
   deterministic causal order (concurrent events tie-broken by node id), so all
   nodes converge on identical state.

3. **JSON Patch Engine** (`verifiable_continuity/json_patch.py`)
   Bandwidth-efficient state updates as RFC 6902 deltas (`add`, `remove`,
   `replace` with full JSON Pointer support), implemented from the spec with no
   external packages. Nodes exchange small patches, not whole documents.

4. **SHA-256 Hash Chaining** (`verifiable_continuity/hash_chain.py`)
   Every event commits to its content *and* to the previous event from the same
   node, forming per-node tamper-evident chains. Verification recomputes every
   link and names the exact `(node_id, seq)` where a chain breaks — including
   the sneaky case where an attacker rewrites a record *and* recomputes its
   hash (the next link still exposes it).

## Quickstart

```python
from verifiable_continuity import Node

# Two nodes, each with its own local database file.
phone  = Node("phone",   "/tmp/phone.db")
laptop = Node("laptop",  "/tmp/laptop.db")

# Local-first writes: tick clock, chain hash, persist, apply.
phone.update([{"op": "add", "path": "/session/step", "value": 1}])
laptop.update([{"op": "add", "path": "/session/notes", "value": ["a"]}])

# Sync when the network is available: exchange logs, converge.
phone.sync_with(laptop)
assert phone.state == laptop.state
# {'session': {'step': 1, 'notes': ['a']}}

# Verify nothing was tampered with.
assert phone.verify() == (True, None)

# Offline session restore: reopen the database, replay the log exactly.
phone.close()
restored = Node.restore("phone", "/tmp/phone.db")
assert restored.state == {'session': {'step': 1, 'notes': ['a']}}
```

## The verification story

Trust is not asserted — it is recomputed:

- **Intact log** → `verify()` returns `(True, None)`.
- **Any record altered** (operations, clock, timestamp, or link) →
  `verify()` returns `(False, (node_id, seq))` pinpointing the first broken
  link, and `assert_verified()` raises `ChainError`.
- **Forged event during sync** → the import hash check rejects it outright
  with `TamperError`; the forged event never enters the log.
- **Offline restore** → state is rebuilt solely from the local log, so a
  device that never reaches the network still recovers its exact session.

## Running the tests

```bash
cd verifiable-continuity
python3 -m unittest discover -s tests -v
```

The suite proves: multi-node sync converges with correct causal ordering;
delta application reproduces state; hash-chain verification passes on intact
logs; tampering with any logged record is detected (including hash-recomputed
forgeries); offline session restore reconstructs exact state from the local
log.

## Layout

```
verifiable_continuity/
    __init__.py       public API
    vector_clock.py   causal ordering + merge
    json_patch.py     RFC 6902 add/remove/replace
    event_log.py      SQLite discrete-event persistence
    hash_chain.py     SHA-256 chaining + verify/reconstruct
    node.py           Node: update / sync / verify / restore
tests/
    test_vector_clock.py
    test_json_patch.py
    test_event_log.py   (log persistence + hash-chain tamper tests)
    test_node.py        (sync convergence, causal order, offline restore)
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
