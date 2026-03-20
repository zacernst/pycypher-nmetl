"""API surface snapshot and compatibility checking utilities.

Provides tools for detecting breaking changes between PyCypher releases by
capturing and comparing public API surfaces.  This module supports two
primary workflows:

1. **Snapshot** — capture the current public API to a JSON file for
   future comparison::

       from shared.compat import snapshot_api_surface, save_snapshot

       surface = snapshot_api_surface("pycypher")
       save_snapshot(surface, "api_v0.0.19.json")

2. **Diff** — compare two snapshots to find added, removed, and changed
   symbols::

       from shared.compat import load_snapshot, diff_surfaces

       old = load_snapshot("api_v0.0.18.json")
       new = load_snapshot("api_v0.0.19.json")
       report = diff_surfaces(old, new)
       print(report.summary())

The snapshot captures every name in ``__all__`` (or ``dir()`` filtered to
public names), along with its kind (function, class, exception, constant)
and call signature where available.

Neo4j Cypher compatibility
~~~~~~~~~~~~~~~~~~~~~~~~~~

The :data:`NEO4J_COMPAT_NOTES` dictionary documents known syntax
differences between standard Neo4j Cypher and the PyCypher dialect so
that users migrating queries can check compatibility up front.
"""

from __future__ import annotations

import importlib
import inspect
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Neo4j Cypher dialect compatibility notes
# ---------------------------------------------------------------------------

#: Known syntax/feature differences between Neo4j Cypher and PyCypher.
#: Keys are feature names; values are dicts with ``supported``, ``notes``,
#: and optional ``workaround`` fields.
NEO4J_COMPAT_NOTES: dict[str, dict[str, Any]] = {
    "CREATE": {
        "supported": True,
        "notes": "Supported for node and relationship creation.",
    },
    "MERGE": {
        "supported": True,
        "notes": "Supported with ON CREATE SET / ON MATCH SET.",
    },
    "DELETE": {
        "supported": True,
        "notes": "Supported for node and relationship deletion.",
    },
    "DETACH DELETE": {
        "supported": False,
        "notes": "Not yet supported. Use explicit relationship deletion first.",
        "workaround": "DELETE relationships manually before deleting nodes.",
    },
    "CALL { ... } IN TRANSACTIONS": {
        "supported": False,
        "notes": "PyCypher operates in-memory; transaction batching is not applicable.",
    },
    "CALL procedure()": {
        "supported": True,
        "notes": "CALL clause supported for registered procedures.",
    },
    "LOAD CSV": {
        "supported": False,
        "notes": "Use ContextBuilder with pandas DataFrames or DuckDBReader instead.",
        "workaround": "pd.read_csv() → ContextBuilder().add_entity(...)",
    },
    "CREATE INDEX": {
        "supported": False,
        "notes": "PyCypher uses DataFrame column access; no index DDL needed.",
    },
    "CREATE CONSTRAINT": {
        "supported": False,
        "notes": "Constraints are enforced at the DataFrame level.",
    },
    "APOC functions": {
        "supported": False,
        "notes": "APOC is Neo4j-specific. PyCypher has its own scalar function library.",
        "workaround": "Check `nmetl functions` for available PyCypher functions.",
    },
    "shortestPath": {
        "supported": True,
        "notes": "BFS-based implementation with configurable hop limits.",
    },
    "allShortestPaths": {
        "supported": True,
        "notes": "Returns all minimum-hop paths between node pairs.",
    },
    "Variable-length paths [*m..n]": {
        "supported": True,
        "notes": "Supported with safety limits (max 20 unbounded hops, 1M frontier rows).",
    },
    "OPTIONAL MATCH": {
        "supported": True,
        "notes": "Left-outer-join semantics, same as Neo4j.",
    },
    "UNION / UNION ALL": {
        "supported": True,
        "notes": "Both variants supported.",
    },
    "FOREACH": {
        "supported": True,
        "notes": "Supported for mutation within loops.",
    },
    "UNWIND": {
        "supported": True,
        "notes": "Supported for list expansion.",
    },
    "WITH": {
        "supported": True,
        "notes": "Projection and aggregation piping supported.",
    },
    "CASE expressions": {
        "supported": True,
        "notes": "Both simple and generic CASE WHEN forms supported.",
    },
    "List comprehensions": {
        "supported": True,
        "notes": "Supported: [x IN list WHERE pred | expr]",
    },
    "Pattern comprehensions": {
        "supported": True,
        "notes": "Supported: [(a)-->(b) | b.name]",
    },
    "Map projections": {
        "supported": True,
        "notes": "Supported: node { .prop1, .prop2, key: expr }",
    },
    "EXISTS subqueries": {
        "supported": True,
        "notes": "Supported in WHERE clauses.",
    },
    "Temporal types": {
        "supported": True,
        "notes": "date(), time(), datetime(), duration() supported.",
    },
    "Parameters ($param)": {
        "supported": True,
        "notes": "Supported via execute_query(params={...}).",
    },
}


# ---------------------------------------------------------------------------
# API surface snapshot
# ---------------------------------------------------------------------------


def _classify_symbol(obj: Any) -> str:
    """Classify a Python object into a human-readable kind string."""
    if isinstance(obj, type):
        if issubclass(obj, BaseException):
            return "exception"
        return "class"
    if callable(obj):
        return "function"
    return "constant"


def _get_signature(obj: Any) -> str | None:
    """Return the string signature of a callable, or None."""
    try:
        return str(inspect.signature(obj))
    except (ValueError, TypeError):
        return None


@dataclass
class SymbolInfo:
    """Metadata about a single public API symbol."""

    name: str
    kind: str  # "function", "class", "exception", "constant"
    signature: str | None = None
    module: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a JSON-friendly dictionary."""
        d: dict[str, Any] = {"name": self.name, "kind": self.kind}
        if self.signature is not None:
            d["signature"] = self.signature
        if self.module:
            d["module"] = self.module
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SymbolInfo:
        """Deserialise from a dictionary."""
        return cls(
            name=data["name"],
            kind=data["kind"],
            signature=data.get("signature"),
            module=data.get("module", ""),
        )


@dataclass
class APISurface:
    """Snapshot of a module's public API at a point in time."""

    module_name: str
    version: str
    symbols: dict[str, SymbolInfo] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialise the full surface to JSON-friendly dict."""
        return {
            "module_name": self.module_name,
            "version": self.version,
            "symbols": {
                k: v.to_dict() for k, v in sorted(self.symbols.items())
            },
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> APISurface:
        """Deserialise from a dictionary."""
        symbols = {
            k: SymbolInfo.from_dict(v)
            for k, v in data.get("symbols", {}).items()
        }
        return cls(
            module_name=data["module_name"],
            version=data["version"],
            symbols=symbols,
        )


def snapshot_api_surface(module_name: str) -> APISurface:
    """Capture the public API surface of a module.

    Imports the module, reads its ``__all__`` (falling back to public
    ``dir()`` entries), and records each symbol's kind and signature.

    Args:
        module_name: Fully-qualified module name (e.g. ``"pycypher"``).

    Returns:
        An :class:`APISurface` snapshot.

    """
    mod = importlib.import_module(module_name)
    version = getattr(mod, "__version__", "unknown")
    names: list[str] = getattr(
        mod, "__all__", [n for n in dir(mod) if not n.startswith("_")]
    )

    symbols: dict[str, SymbolInfo] = {}
    for name in names:
        try:
            obj = getattr(mod, name)
        except (AttributeError, ImportError):
            continue
        kind = _classify_symbol(obj)
        sig = _get_signature(obj) if kind in ("function", "class") else None
        symbols[name] = SymbolInfo(
            name=name,
            kind=kind,
            signature=sig,
            module=getattr(obj, "__module__", ""),
        )

    return APISurface(
        module_name=module_name, version=version, symbols=symbols
    )


# ---------------------------------------------------------------------------
# Snapshot persistence
# ---------------------------------------------------------------------------


def save_snapshot(surface: APISurface, path: str | Path) -> None:
    """Write an API surface snapshot to a JSON file.

    Args:
        surface: The snapshot to save.
        path: Destination file path.

    """
    Path(path).write_text(
        json.dumps(surface.to_dict(), indent=2) + "\n",
        encoding="utf-8",
    )


def load_snapshot(path: str | Path) -> APISurface:
    """Load an API surface snapshot from a JSON file.

    Args:
        path: Path to the JSON snapshot.

    Returns:
        The deserialised :class:`APISurface`.

    """
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return APISurface.from_dict(data)


# ---------------------------------------------------------------------------
# Surface diff
# ---------------------------------------------------------------------------


@dataclass
class SurfaceDiff:
    """Result of comparing two API surface snapshots."""

    old_version: str
    new_version: str
    added: list[SymbolInfo] = field(default_factory=list)
    removed: list[SymbolInfo] = field(default_factory=list)
    changed: list[tuple[SymbolInfo, SymbolInfo]] = field(default_factory=list)

    @property
    def has_breaking_changes(self) -> bool:
        """Whether any removals or signature changes were detected."""
        return bool(self.removed) or bool(self.changed)

    def summary(self) -> str:
        """Human-readable summary of the diff."""
        lines: list[str] = [
            f"API diff: {self.old_version} → {self.new_version}",
            f"  Added:   {len(self.added)}",
            f"  Removed: {len(self.removed)}",
            f"  Changed: {len(self.changed)}",
        ]
        if self.removed:
            lines.append("\nRemoved (BREAKING):")
            for sym in self.removed:
                lines.append(f"  - {sym.name} ({sym.kind})")
        if self.changed:
            lines.append("\nSignature changes (BREAKING):")
            for old, new in self.changed:
                lines.append(
                    f"  - {old.name}: {old.signature} → {new.signature}"
                )
        if self.added:
            lines.append("\nAdded:")
            for sym in self.added:
                lines.append(f"  + {sym.name} ({sym.kind})")
        if not self.has_breaking_changes:
            lines.append("\nNo breaking changes detected.")
        return "\n".join(lines)


def diff_surfaces(old: APISurface, new: APISurface) -> SurfaceDiff:
    """Compare two API surface snapshots.

    Args:
        old: The baseline snapshot.
        new: The current snapshot.

    Returns:
        A :class:`SurfaceDiff` describing additions, removals, and changes.

    """
    old_names = set(old.symbols)
    new_names = set(new.symbols)

    added = [new.symbols[n] for n in sorted(new_names - old_names)]
    removed = [old.symbols[n] for n in sorted(old_names - new_names)]

    changed: list[tuple[SymbolInfo, SymbolInfo]] = []
    for name in sorted(old_names & new_names):
        o = old.symbols[name]
        n = new.symbols[name]
        if o.signature != n.signature and o.signature is not None:
            changed.append((o, n))

    return SurfaceDiff(
        old_version=old.version,
        new_version=new.version,
        added=added,
        removed=removed,
        changed=changed,
    )


# ---------------------------------------------------------------------------
# Convenience: check Neo4j feature compatibility
# ---------------------------------------------------------------------------


def check_neo4j_compat(feature: str) -> dict[str, Any] | None:
    """Look up compatibility notes for a Neo4j Cypher feature.

    Args:
        feature: Feature name (case-insensitive substring match).

    Returns:
        The compatibility note dict, or ``None`` if no match.

    """
    feature_lower = feature.lower()
    for key, info in NEO4J_COMPAT_NOTES.items():
        if feature_lower in key.lower():
            return {"feature": key, **info}
    return None
