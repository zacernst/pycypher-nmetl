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


def register_relation_udf(
    context: Context,
    name: str,
    fn: Any,
    *,
    param_types: list[str],
    return_type: str,
) -> None:
    """Register a scalar Python function as a DuckDB UDF for the relation engine.

    Makes ``fn`` callable from eligible out-of-core queries as ``name(args)``.
    Types must be given explicitly (DuckDB type strings, e.g. ``"DOUBLE"``,
    ``"VARCHAR"``, ``"BIGINT"``) — the query engine can't infer them.  DuckDB's
    default null handling returns NULL for NULL input without invoking ``fn``.

    Args:
        context: A DuckDB-backed :class:`Context`.
        name: Cypher function name used in queries (case-insensitive).
        fn: A plain scalar Python callable (one value per argument → one value).
        param_types: DuckDB type string per positional argument.
        return_type: DuckDB return type string.

    """
    con = context.backend.connection
    lname = name.lower()
    con.create_function(lname, fn, param_types, return_type)
    store = getattr(context, "_relation_udfs", None)
    if store is None:
        store = set()
        context._relation_udfs = store
    store.add(lname)


def _udf_names(context: Context) -> frozenset[str]:
    """Return the set of registered relation-engine UDF names (lowercase)."""
    return frozenset(getattr(context, "_relation_udfs", ()))


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


def _rel_attr_map(context: Context, label: str) -> dict[str, str] | None:
    """Property→column map for a relationship *label* (in-memory rel source)."""
    rel = context.relationship_mapping.mapping.get(label)
    return rel.attribute_map if rel is not None else None


def _rel_base_relation(context: Context, label: str, con: Any) -> Any:
    """Return a DuckDB relation over a relationship's in-memory source rows."""
    rel = context.relationship_mapping.mapping[label]
    src = rel.source_obj
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


def _make_resolve(variables: dict[str, tuple[str, dict[str, str]]]) -> Any:
    """Build a ``resolve(var, prop)`` closure over the pattern's variables.

    *variables* maps each bound variable to ``(sql_alias, attr_map)``.  An empty
    alias references the column unqualified (single-relation case); otherwise it
    is qualified as ``alias."col"`` (joins).
    """
    from pycypher.ingestion.security import sanitize_sql_identifier

    def resolve(var: str, prop: str) -> str | None:
        entry = variables.get(var)
        if entry is None:
            return None
        alias, attr = entry
        col = attr.get(prop)
        if col is None:
            return None
        quoted = f'"{sanitize_sql_identifier(col)}"'
        return f"{alias}.{quoted}" if alias else quoted

    return resolve


def _valid_node(node: Any) -> bool:
    """True if *node* is a single-label variable node with no inline props."""
    from pycypher.ast_models import NodePattern

    return (
        isinstance(node, NodePattern)
        and node.variable is not None
        and len(node.labels) == 1
        and not node.properties
    )


class _Plan:
    """A resolved, eligible relation-query plan."""

    __slots__ = (
        "build",
        "distinct",
        "items",
        "limit",
        "order_by",
        "requires_alias",
        "resolve",
        "skip",
        "where",
    )

    def __init__(
        self,
        resolve: Any,
        build: Any,
        where: Any,
        ret: Any,
        *,
        requires_alias: bool,
    ) -> None:
        self.resolve = resolve  # resolve(var, prop) -> str | None
        self.build = build  # build(con) -> DuckDBPyRelation
        self.where = where  # WHERE expr or None
        self.items = ret.items  # RETURN items
        self.requires_alias = requires_alias  # force explicit aliases (joins)
        self.distinct = bool(ret.distinct)
        self.order_by = ret.order_by  # list[OrderByItem] | None
        self.skip = ret.skip  # int | None
        self.limit = ret.limit  # int | None


def _analyze_match(query: Any, context: Context) -> _Plan | None:
    """Return an execution plan for *query* if eligible, else ``None``.

    Handles two pattern shapes: a single node, and a single directed
    relationship between two nodes (with an optional relationship variable).
    """
    from pycypher.ast_models import (
        Match,
        Query,
        RelationshipDirection,
        RelationshipPattern,
        Return,
    )

    if getattr(context, "backend_name", None) != "duckdb":
        return None
    if not hasattr(getattr(context, "backend", None), "connection"):
        return None
    if not isinstance(query, Query) or len(query.clauses) != 2:
        return None
    match, ret = query.clauses
    if not isinstance(match, Match) or not isinstance(ret, Return):
        return None
    if match.optional:
        return None
    if not ret.items:
        return None
    # SKIP without LIMIT has no DuckDBPyRelation form here — fall back.
    if ret.skip is not None and ret.limit is None:
        return None
    paths = match.pattern.paths
    if len(paths) != 1:
        return None
    path = paths[0]
    if path.variable is not None:
        return None
    if getattr(path, "shortest_path_mode", "none") not in ("none", None):
        return None
    elements = path.elements

    # --- Single node ---
    if len(elements) == 1:
        node = elements[0]
        if not _valid_node(node):
            return None
        attr = _entity_attr_map(context, node.labels[0])
        if attr is None:
            return None
        variables = {node.variable.name: ("", attr)}
        label = node.labels[0]

        def build(con: Any, label: str = label) -> Any:
            return _base_relation(context, label, con)

        return _Plan(
            _make_resolve(variables), build, match.where, ret,
            requires_alias=False,
        )

    # --- Single directed relationship: (n1)-[r]->(n2) or (n1)<-[r]-(n2) ---
    if len(elements) == 3:
        n1, relp, n2 = elements
        if not (_valid_node(n1) and _valid_node(n2)):
            return None
        if not isinstance(relp, RelationshipPattern):
            return None
        if relp.length is not None or getattr(relp, "properties", None):
            return None
        if len(relp.labels) != 1:
            return None
        if relp.direction not in (
            RelationshipDirection.RIGHT,
            RelationshipDirection.LEFT,
        ):
            return None
        v1, v2 = n1.variable.name, n2.variable.name
        if v1 == v2:
            return None
        a1 = _entity_attr_map(context, n1.labels[0])
        a2 = _entity_attr_map(context, n2.labels[0])
        rel_attr = _rel_attr_map(context, relp.labels[0])
        if a1 is None or a2 is None or rel_attr is None:
            return None
        variables: dict[str, tuple[str, dict[str, str]]] = {
            v1: ("a", a1),
            v2: ("b", a2),
        }
        if relp.variable is not None:
            rv = relp.variable.name
            if rv in variables:
                return None
            variables[rv] = ("r", rel_attr)

        l1, l2, rlabel = n1.labels[0], n2.labels[0], relp.labels[0]
        right = relp.direction == RelationshipDirection.RIGHT

        def build(
            con: Any,
            l1: str = l1,
            l2: str = l2,
            rlabel: str = rlabel,
            right: bool = right,  # noqa: FBT001
        ) -> Any:
            a = _base_relation(context, l1, con).set_alias("a")
            b = _base_relation(context, l2, con).set_alias("b")
            r = _rel_base_relation(context, rlabel, con).set_alias("r")
            if right:
                c1 = 'a."__ID__" = r."__SOURCE__"'
                c2 = 'r."__TARGET__" = b."__ID__"'
            else:
                c1 = 'a."__ID__" = r."__TARGET__"'
                c2 = 'r."__SOURCE__" = b."__ID__"'
            return a.join(r, c1).join(b, c2)

        return _Plan(
            _make_resolve(variables), build, match.where, ret,
            requires_alias=True,
        )

    return None


def _build_order_clause(order_by: Any, items: Any) -> str | None:
    """Build a DuckDB ORDER BY clause referencing RETURN output columns.

    Supports ordering by an output alias (``ORDER BY name``) or by a returned
    property lookup (``ORDER BY n.age`` when ``n.age`` is in the RETURN).  Other
    order keys, or an explicit NULLS placement, return ``None`` (fall back).
    Emits ``NULLS LAST`` to match the pandas engine's null ordering.
    """
    from pycypher.ast_models import PropertyLookup, Variable
    from pycypher.ingestion.security import sanitize_sql_identifier

    output_names: set[str] = set()
    prop_to_output: dict[tuple[str, str], str] = {}
    for item in items:
        name = _output_column(item)
        output_names.add(name)
        expr = item.expression
        if isinstance(expr, PropertyLookup) and isinstance(expr.expression, Variable):
            prop_to_output[(expr.expression.name, expr.property)] = name

    parts: list[str] = []
    for ob in order_by:
        if getattr(ob, "nulls_placement", None) is not None:
            return None  # explicit NULLS FIRST/LAST not mapped yet
        expr = ob.expression
        col: str | None = None
        if isinstance(expr, Variable) and expr.name in output_names:
            col = expr.name
        elif isinstance(expr, PropertyLookup) and isinstance(expr.expression, Variable):
            col = prop_to_output.get((expr.expression.name, expr.property))
        if col is None:
            return None
        direction = "ASC" if ob.ascending else "DESC"
        parts.append(f'"{sanitize_sql_identifier(col)}" {direction} NULLS LAST')

    return ", ".join(parts)


def is_relation_eligible(query: Any, context: Context) -> bool:
    """Return True if *query* is in the subset the relation engine can execute.

    Conservative by design: any construct not explicitly handled makes the
    query ineligible so the caller falls back to the pandas engine.  Eligible:
    a single-node ``MATCH`` or a single directed relationship between two nodes,
    an optional compilable ``WHERE``, and a ``RETURN`` of compilable expressions
    (non-property expressions — and all expressions in a join — require an
    explicit alias).  Ineligible: undirected/variable-length/multi-hop paths,
    OPTIONAL MATCH, aggregation, ORDER/SKIP/LIMIT, DISTINCT, WITH, and
    unsupported functions/operators.
    """
    from pycypher.ast_models import PropertyLookup
    from pycypher.relation_sql import (
        compile_aggregate,
        compile_expression,
        is_aggregate,
    )

    plan = _analyze_match(query, context)
    if plan is None:
        return False

    udfs = _udf_names(context)
    if plan.where is not None and compile_expression(plan.where, plan.resolve, udfs) is None:
        return False

    if plan.order_by and _build_order_clause(plan.order_by, plan.items) is None:
        return False

    for item in plan.items:
        if is_aggregate(item.expression):
            # Aggregates must compile and be explicitly aliased.
            if compile_aggregate(item.expression, plan.resolve) is None:
                return False
            if item.alias is None:
                return False
            continue
        # Non-aggregate: a group key (agg query) or a plain projection.
        if compile_expression(item.expression, plan.resolve, udfs) is None:
            return False
        needs_alias = plan.requires_alias or not isinstance(
            item.expression, PropertyLookup,
        )
        if needs_alias and item.alias is None:
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

    Builds a lazy relation from the pattern plan (a single entity relation, or a
    join across two entities and a relationship), composes ``WHERE`` as a
    predicate and ``RETURN`` as a projection — all lazy ``DuckDBPyRelation``
    operations — so nothing materialises until the boundary.

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
    from pycypher.relation_sql import (
        compile_aggregate,
        compile_expression,
        is_aggregate,
    )

    plan = _analyze_match(query, context)
    con = context.backend.connection
    udfs = _udf_names(context)

    base_rel = plan.build(con)

    # WHERE → SQL predicate composed onto the relation (lazy).
    if plan.where is not None:
        base_rel = base_rel.filter(compile_expression(plan.where, plan.resolve, udfs))

    aggregating = any(is_aggregate(item.expression) for item in plan.items)
    if aggregating:
        # Non-aggregate RETURN items are GROUP BY keys (Cypher's implicit
        # grouping); aggregates become SQL aggregate expressions.
        select_parts = []
        group_parts = []
        for item in plan.items:
            alias = sanitize_sql_identifier(_output_column(item))
            if is_aggregate(item.expression):
                sql_expr = compile_aggregate(item.expression, plan.resolve)
            else:
                sql_expr = compile_expression(item.expression, plan.resolve, udfs)
                group_parts.append(sql_expr)
            select_parts.append(f'{sql_expr} AS "{alias}"')
        result_rel = base_rel.aggregate(
            ", ".join(select_parts), ", ".join(group_parts),
        )
    else:
        project_parts = []
        for item in plan.items:
            sql_expr = compile_expression(item.expression, plan.resolve, udfs)
            alias = sanitize_sql_identifier(_output_column(item))
            project_parts.append(f'{sql_expr} AS "{alias}"')
        result_rel = base_rel.project(", ".join(project_parts))

    # RETURN modifiers, in Cypher order: DISTINCT → ORDER BY → SKIP/LIMIT.
    if plan.distinct:
        result_rel = result_rel.distinct()
    if plan.order_by:
        result_rel = result_rel.order(_build_order_clause(plan.order_by, plan.items))
    if plan.limit is not None:
        result_rel = result_rel.limit(plan.limit, offset=plan.skip or 0)

    bindings = RelationBindings(DuckDBLazyFrame(result_rel, con))
    return bindings.to_pandas() if materialize else bindings
