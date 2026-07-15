"""Out-of-core relation execution path (Approach A, Phase 4+).

A separate, **opt-in** execution path that runs an *eligible* subset of Cypher
queries directly as DuckDB relations instead of the pandas ``BindingFrame``
engine.  Anything not eligible falls back to the existing engine, so coverage
grows one query-feature at a time while the suite stays green.

Disabled by default — enabled per-:class:`Context` via a truthy
``_relation_engine_enabled`` attribute or the
``PYCYPHER_DUCKDB_RELATION_ENGINE`` environment variable.  When disabled the
dispatch never fires, guaranteeing zero behaviour change.

Eligibility is intentionally tiny at this phase (single-label ``MATCH`` with an
aliased property projection); later phases widen it (WHERE, joins, aggregation,
…).  See ``docs/duckdb_out_of_core_design.md``.

Correctness note: the relation is currently built by registering the entity's
already-loaded ``source_obj`` on the DuckDB connection, so results are correct
but not yet end-to-end out-of-core — Phase 5 wires ``read_relation`` sources and
the shared connection through so scans stream from files.
"""

from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd

    from pycypher.relational_models import Context

_ENABLE_ENV_VAR = "PYCYPHER_DUCKDB_RELATION_ENGINE"
_TRUTHY = frozenset({"1", "true", "yes", "on"})


class RelationBindings:
    """Relation-backed bindings — the out-of-core counterpart to the pandas
    ``BindingFrame``.

    Wraps a ``DuckDBLazyFrame`` and exposes :meth:`to_pandas` as the single
    materialisation boundary.  Kept deliberately thin at this phase; later
    phases grow it into the full relation IR.
    """

    __slots__ = ("_lazy",)

    def __init__(self, lazy: Any) -> None:
        self._lazy = lazy

    @property
    def columns(self) -> list[str]:
        """Column names, from the relation schema (no materialisation)."""
        return list(self._lazy.columns)

    def to_pandas(self) -> pd.DataFrame:
        """Materialise the relation to a pandas DataFrame."""
        return self._lazy.to_pandas()


def relation_engine_enabled(context: Context) -> bool:
    """Return True if the opt-in relation engine is enabled for *context*."""
    if getattr(context, "_relation_engine_enabled", False):
        return True
    return os.environ.get(_ENABLE_ENV_VAR, "").strip().lower() in _TRUTHY


def is_relation_eligible(query: Any, context: Context) -> bool:
    """Return True if *query* is in the subset the relation engine can execute.

    Conservative by design: any construct not explicitly handled returns
    ``False`` so the caller falls back to the pandas engine.  At this phase the
    only eligible shape is a single-label ``MATCH`` of one node with an aliased
    property projection and no ``WHERE``/``WITH``/relationships/ordering/
    aggregation.
    """
    from pycypher.ast_models import (
        Match,
        NodePattern,
        PropertyLookup,
        Query,
        Return,
        Variable,
    )

    # Backend must be DuckDB and expose a connection for relation building.
    if getattr(context, "backend_name", None) != "duckdb":
        return False
    if not hasattr(getattr(context, "backend", None), "connection"):
        return False

    if not isinstance(query, Query) or len(query.clauses) != 2:
        return False
    match, ret = query.clauses
    if not isinstance(match, Match) or not isinstance(ret, Return):
        return False

    # --- MATCH: single non-optional single-label node, no WHERE, no props ---
    if match.optional or match.where is not None:
        return False
    paths = match.pattern.paths
    if len(paths) != 1:
        return False
    path = paths[0]
    if path.variable is not None:
        return False
    if getattr(path, "shortest_path_mode", "none") not in ("none", None):
        return False
    if len(path.elements) != 1:
        return False
    node = path.elements[0]
    if not isinstance(node, NodePattern):
        return False
    if node.variable is None or len(node.labels) != 1 or node.properties:
        return False
    var_name = node.variable.name
    label = node.labels[0]
    entity = context.entity_mapping.mapping.get(label)
    if entity is None:
        return False

    # --- RETURN: aliased property lookups on the match variable only ---
    if ret.distinct or ret.order_by or ret.skip is not None or ret.limit is not None:
        return False
    if not ret.items:
        return False
    for item in ret.items:
        expr = item.expression
        if not isinstance(expr, PropertyLookup):
            return False
        if not isinstance(expr.expression, Variable) or expr.expression.name != var_name:
            return False
        if expr.property not in entity.attribute_map:
            return False

    return True


def _output_column(item: Any) -> str:
    """Output column name for a return *item*, matching the pandas engine.

    An explicit ``AS alias`` wins; otherwise a bare property lookup ``n.prop``
    is named after the property (``prop``).  This mirrors the pandas engine and
    is robust to the shared/cached AST being mutated in place (the engine
    rewrites a missing alias to exactly the property name during execution).
    """
    if item.alias is not None:
        return str(item.alias)
    return str(item.expression.property)


def execute_relation_query(query: Any, context: Context) -> pd.DataFrame:
    """Execute an eligible query via a DuckDB relation, returning pandas.

    Precondition: :func:`is_relation_eligible` returned ``True`` for *query*.
    """
    from pycypher.backends.duckdb_backend import DuckDBLazyFrame
    from pycypher.ingestion.security import sanitize_sql_identifier

    match, ret = query.clauses
    node = match.pattern.paths[0].elements[0]
    label = node.labels[0]
    entity = context.entity_mapping.mapping[label]

    con = context.backend.connection
    view = f"_rel_{uuid.uuid4().hex}"
    con.register(view, entity.source_obj)
    try:
        select_parts = []
        for item in ret.items:
            src_col = sanitize_sql_identifier(entity.attribute_map[item.expression.property])
            alias = sanitize_sql_identifier(_output_column(item))
            select_parts.append(f'"{src_col}" AS "{alias}"')
        sql = f'SELECT {", ".join(select_parts)} FROM "{view}"'  # nosec B608 — identifiers validated by sanitize_sql_identifier; view is a generated uuid
        lazy = DuckDBLazyFrame(con.sql(sql), con)
        return RelationBindings(lazy).to_pandas()
    finally:
        con.unregister(view)
