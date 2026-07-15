"""Out-of-core relation execution path (Approach A, Phase 4+).

A separate, **opt-in** execution path that runs an *eligible* subset of Cypher
queries directly as DuckDB relations instead of the pandas ``BindingFrame``
engine.  Anything not eligible falls back to the existing engine, so coverage
grows one query-feature at a time while the suite stays green.

Disabled by default — enabled per-:class:`Context` via a truthy
``_relation_engine_enabled`` attribute or the
``PYCYPHER_DUCKDB_RELATION_ENGINE`` environment variable.  When disabled the
dispatch never fires, guaranteeing zero behaviour change.

Eligible subset so far: a single-label ``MATCH`` of one node, an optional
``WHERE`` that compiles to a SQL predicate (:mod:`pycypher.relation_sql`), and a
``RETURN`` of compilable expressions (property lookups, arithmetic, literals;
non-property expressions require an explicit alias).  Not yet: relationships,
aggregation, ORDER/SKIP/LIMIT, DISTINCT, WITH, functions.  Later phases widen
it.  See ``docs/duckdb_out_of_core_design.md``.

Source modes: with :func:`register_streaming_source` the base relation is a
lazy ``read_relation`` view over a file (genuinely out-of-core); otherwise it
falls back to the entity's in-memory ``source_obj``.  Combined with
``materialize=False`` + ``write_relation_to_uri`` (COPY), an eligible query
streams file → relation → sink without a pandas frame, and ``nmetl run`` uses
this automatically when enabled (see ``cli/pipeline.py`` ``_try_streaming_run``).
"""

from __future__ import annotations

import os
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
    def lazy(self) -> Any:
        """The underlying ``DuckDBLazyFrame`` (for streaming to a sink)."""
        return self._lazy

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


def register_streaming_source(
    context: Context,
    label: str,
    data_source: Any,
    *,
    id_col: str | None = None,
) -> None:
    """Register a file-backed entity as a streaming DuckDB relation.

    Reads *data_source* via :meth:`DataSource.read_relation` on the context's
    shared DuckDB connection (so the file is scanned lazily, never fully
    loaded), derives a property→column map from the relation schema, and stores
    both on ``context._streaming_sources`` for the relation engine to use as a
    base relation.  Requires a DuckDB-backed context.

    Args:
        context: A DuckDB-backed :class:`Context`.
        label: The entity label to register the source under.
        data_source: A :class:`DataSource` (typically from
            :func:`data_source_from_uri`).
        id_col: The column designated as the entity ID.  Excluded from the
            property map so semantics match the in-memory path (where the ID
            column is consumed into ``__ID__`` and is not a property).

    """
    con = context.backend.connection
    lazy = data_source.read_relation(con)
    # Every non-ID column is exposed as a property named after the column
    # (identity map), mirroring the ContextBuilder convention for file sources.
    attr_map = {col: col for col in lazy.columns if col != id_col}
    store = getattr(context, "_streaming_sources", None)
    if store is None:
        store = {}
        context._streaming_sources = store
    store[label] = (lazy, attr_map)


def _entity_attr_map(context: Context, label: str) -> dict[str, str] | None:
    """Property→column map for *label* from a streaming source or EntityTable."""
    streaming = getattr(context, "_streaming_sources", {})
    if label in streaming:
        return streaming[label][1]
    entity = context.entity_mapping.mapping.get(label)
    return entity.attribute_map if entity is not None else None


def _base_relation(context: Context, label: str, con: Any) -> Any:
    """Return a lazy DuckDB relation for *label*'s source rows.

    Prefers a registered streaming relation (file-backed, out-of-core); falls
    back to the entity's in-memory ``source_obj``.
    """
    streaming = getattr(context, "_streaming_sources", {})
    if label in streaming:
        return streaming[label][0].relation
    entity = context.entity_mapping.mapping[label]
    src = entity.source_obj
    import pandas as pd

    if isinstance(src, pd.DataFrame):
        return con.from_df(src)
    try:
        import pyarrow as pa

        if isinstance(src, pa.Table):
            return con.from_arrow(src)
    except ImportError:
        pass
    from pycypher.backends._helpers import _to_pandas

    return con.from_df(_to_pandas(src))


def is_relation_eligible(query: Any, context: Context) -> bool:
    """Return True if *query* is in the subset the relation engine can execute.

    Conservative by design: any construct not explicitly handled returns
    ``False`` so the caller falls back to the pandas engine.  Eligible: a
    single-label ``MATCH`` of one node, an optional compilable ``WHERE``, and a
    ``RETURN`` of compilable expressions (non-property expressions need an
    explicit alias).  Ineligible: relationships, aggregation, ORDER/SKIP/LIMIT,
    DISTINCT, WITH, and any expression using an unsupported function/operator.
    """
    from pycypher.ast_models import (
        Match,
        NodePattern,
        PropertyLookup,
        Query,
        Return,
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

    # --- MATCH: single non-optional single-label node, no inline props ---
    # A WHERE clause is allowed when it compiles to a SQL predicate (checked
    # once the variable and attribute map are known, below).
    if match.optional:
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
    attr_map = _entity_attr_map(context, label)
    if attr_map is None:
        return False

    # --- WHERE: must compile to a SQL predicate when present ---
    if match.where is not None:
        from pycypher.relation_sql import compile_expression

        if compile_expression(match.where, var_name, attr_map) is None:
            return False

    # --- RETURN: any compilable expression over the match variable ---
    # A bare property lookup is named after the property (or its alias); any
    # other expression (arithmetic, literal, …) requires an explicit alias so
    # the output column name is unambiguous and matches the pandas engine.
    if ret.distinct or ret.order_by or ret.skip is not None or ret.limit is not None:
        return False
    if not ret.items:
        return False
    from pycypher.relation_sql import compile_expression

    for item in ret.items:
        if compile_expression(item.expression, var_name, attr_map) is None:
            return False
        if not isinstance(item.expression, PropertyLookup) and item.alias is None:
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


def execute_relation_query(
    query: Any,
    context: Context,
    *,
    materialize: bool = True,
) -> pd.DataFrame | RelationBindings:
    """Execute an eligible query via a DuckDB relation.

    Builds a lazy projected relation (``DuckDBPyRelation.project``) over the
    entity's base relation — a streaming file view when registered, else the
    in-memory ``source_obj`` — so nothing is materialised until the boundary.

    Precondition: :func:`is_relation_eligible` returned ``True`` for *query*.

    Args:
        query: The parsed, eligible query AST.
        context: The DuckDB-backed context.
        materialize: When ``True`` (default) return a pandas DataFrame; when
            ``False`` return a :class:`RelationBindings` so the caller can
            stream it to a sink without building a pandas frame.

    """
    from pycypher.backends.duckdb_backend import DuckDBLazyFrame
    from pycypher.ingestion.security import sanitize_sql_identifier
    from pycypher.relation_sql import compile_expression

    match, ret = query.clauses
    node = match.pattern.paths[0].elements[0]
    label = node.labels[0]
    var_name = node.variable.name
    attr_map = _entity_attr_map(context, label)
    con = context.backend.connection

    base_rel = _base_relation(context, label, con)

    # WHERE → SQL predicate composed onto the relation (lazy).
    if match.where is not None:
        predicate = compile_expression(match.where, var_name, attr_map)
        base_rel = base_rel.filter(predicate)

    project_parts = []
    for item in ret.items:
        sql_expr = compile_expression(item.expression, var_name, attr_map)
        alias = sanitize_sql_identifier(_output_column(item))
        project_parts.append(f'{sql_expr} AS "{alias}"')
    projected = base_rel.project(", ".join(project_parts))

    bindings = RelationBindings(DuckDBLazyFrame(projected, con))
    return bindings.to_pandas() if materialize else bindings
