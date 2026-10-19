"""Minimal RFC 6902 JSON Patch engine: add, remove, replace.

Implemented from the spec with the standard library only -- no external
dependencies. JSON Pointers (RFC 6901) are supported, including the
``~0`` / ``~1`` escapes and the ``-`` array-append token.

Each operation is a dict: {"op": "add"|"remove"|"replace",
"path": "/a/b/0", "value": ...} ("value" omitted for "remove").
"""

from __future__ import annotations

import copy

_VALID_OPS = ("add", "remove", "replace")


class PatchError(Exception):
    """Raised when a patch operation cannot be applied."""


def _unescape(token: str) -> str:
    # RFC 6901: evaluate "~1" first, then "~0".
    return token.replace("~1", "/").replace("~0", "~")


def _parse_pointer(pointer: str) -> list[str]:
    if not isinstance(pointer, str):
        raise PatchError(f"pointer must be a string, got {type(pointer).__name__}")
    if pointer == "":
        return []
    if not pointer.startswith("/"):
        raise PatchError(f"invalid JSON pointer: {pointer!r}")
    return [_unescape(t) for t in pointer.split("/")[1:]]


def _child(container, token: str):
    if isinstance(container, dict):
        if token not in container:
            raise PatchError(f"object has no member {token!r}")
        return container[token]
    if isinstance(container, list):
        if token == "-":
            raise PatchError("'-' is only valid as an add target")
        try:
            idx = int(token)
        except ValueError:
            raise PatchError(f"invalid array index: {token!r}")
        if not 0 <= idx < len(container):
            raise PatchError(f"array index out of range: {token!r}")
        return container[idx]
    raise PatchError("cannot traverse into a scalar value")


def _traverse(doc, tokens: list[str]):
    cur = doc
    for t in tokens:
        cur = _child(cur, t)
    return cur


def _apply_one(doc, op: dict):
    name = op.get("op")
    if name not in _VALID_OPS:
        raise PatchError(f"unsupported op: {name!r}")
    tokens = _parse_pointer(op.get("path"))

    if name == "add":
        if "value" not in op:
            raise PatchError("'add' requires a value")
        value = copy.deepcopy(op["value"])
        if not tokens:
            return value
        parent = _traverse(doc, tokens[:-1])
        last = tokens[-1]
        if isinstance(parent, dict):
            parent[last] = value
        elif isinstance(parent, list):
            if last == "-":
                parent.append(value)
            else:
                try:
                    idx = int(last)
                except ValueError:
                    raise PatchError(f"invalid array index: {last!r}")
                if not 0 <= idx <= len(parent):
                    raise PatchError(f"array index out of range: {last!r}")
                parent.insert(idx, value)
        else:
            raise PatchError("add target parent must be an object or array")
        return doc

    if name == "remove":
        if not tokens:
            raise PatchError("cannot remove the document root")
        parent = _traverse(doc, tokens[:-1])
        last = tokens[-1]
        if isinstance(parent, dict):
            if last not in parent:
                raise PatchError(f"object has no member {last!r}")
            del parent[last]
        elif isinstance(parent, list):
            try:
                idx = int(last)
            except ValueError:
                raise PatchError(f"invalid array index: {last!r}")
            if not 0 <= idx < len(parent):
                raise PatchError(f"array index out of range: {last!r}")
            del parent[idx]
        else:
            raise PatchError("remove target parent must be an object or array")
        return doc

    # replace
    if "value" not in op:
        raise PatchError("'replace' requires a value")
    value = copy.deepcopy(op["value"])
    if not tokens:
        return value
    parent = _traverse(doc, tokens[:-1])
    last = tokens[-1]
    if isinstance(parent, dict):
        if last not in parent:
            raise PatchError(f"object has no member {last!r}")
        parent[last] = value
    elif isinstance(parent, list):
        try:
            idx = int(last)
        except ValueError:
            raise PatchError(f"invalid array index: {last!r}")
        if not 0 <= idx < len(parent):
            raise PatchError(f"array index out of range: {last!r}")
        parent[idx] = value
    else:
        raise PatchError("replace target parent must be an object or array")
    return doc


def apply_patch(doc, ops: list[dict]):
    """Apply a list of RFC 6902 operations to a JSON-like document.

    Returns a new document; the input is never mutated. Raises PatchError
    if any operation is invalid or inapplicable.
    """
    if not isinstance(ops, list):
        raise PatchError("patch must be a list of operations")
    result = copy.deepcopy(doc)
    for op in ops:
        if not isinstance(op, dict):
            raise PatchError(f"operation must be an object, got {op!r}")
        result = _apply_one(result, op)
    return result
