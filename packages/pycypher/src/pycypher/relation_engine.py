"""Out-of-core relation execution path (Approach A, Phase 4+).

A separate, **opt-in** execution path that runs an *eligible* subset of Cypher
queries directly as DuckDB relations instead of the pandas ``BindingFrame``
engine.  Anything not eligible falls back to the existing engine, so coverage
grows one query-feature at a time while the suite stays green.

Disabled by default — enabled per-:class:`Context` via a truthy
``_relation_engine_enabled`` attribute or the
``PYCYPHER_DUCKDB_RELATION_ENGINE`` environment variable.  When disabled the
dispatch never fires, guaranteeing zero behaviour change.

Eligible subset so far: a single node or a single directed relationship
``MATCH``; an optional ``WHERE`` that compiles to a SQL predicate
(:mod:`pycypher.relation_sql`); a ``RETURN`` of compilable expressions
(property lookups, arithmetic, literals, registered scalar UDFs, and
``count/sum/avg/min/max`` aggregates with implicit GROUP BY); and
DISTINCT / ORDER BY / SKIP+LIMIT.  Duplicate output column names are rejected.
Not yet: multi-hop / undirected / variable-length paths, OPTIONAL MATCH, WITH
chaining, ``collect()``, and unregistered functions.  See
``docs/duckdb_out_of_core_design.md``.

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
    """A resolved, eligible relation-query plan: a leading pattern + stages."""

    __slots__ = ("build", "match_where", "node_vars", "qualified", "resolve", "stages")

    def __init__(
        self,
        resolve: Any,
        build: Any,
        match_where: Any,
        qualified: bool,  # noqa: FBT001
        node_vars: frozenset[str],
        stages: list[Any],
    ) -> None:
        self.resolve = resolve  # (var, prop) -> col | None (pattern scope)
        self.build = build  # build(con) -> DuckDBPyRelation
        self.match_where = match_where  # WHERE from the MATCH clause | None
        self.qualified = qualified  # multi-variable pattern → qualified names
        self.node_vars = node_vars  # pattern variable names
        self.stages = stages  # [With, …, Return]


class _Scope:
    """Name resolution for one pipeline stage."""

    __slots__ = ("node_vars", "qualified", "resolve", "resolve_var")

    def __init__(
        self,
        resolve: Any,
        resolve_var: Any,
        *,
        qualified: bool,
        node_vars: frozenset[str],
    ) -> None:
        self.resolve = resolve  # (var, prop) -> col | None
        self.resolve_var = resolve_var  # (name) -> col | None (bare scalars)
        self.qualified = qualified
        self.node_vars = node_vars


def _no_prop(_var: str, _prop: str) -> None:
    return None


def _no_var(_name: str) -> None:
    return None


def _scalar_scope(output_names: list[str]) -> _Scope:
    """A scope where bare variables resolve to a prior stage's output columns."""
    names = set(output_names)

    def resolve_var(name: str) -> str | None:
        return _quote_output_alias(name) if name in names else None

    return _Scope(_no_prop, resolve_var, qualified=False, node_vars=frozenset())


def _analyze_pattern(match: Any, context: Context) -> tuple[Any, Any, bool, frozenset[str]] | None:
    """Analyse the leading MATCH pattern.

    Returns ``(resolve, build, qualified, node_vars)`` or ``None`` if the
    pattern is outside the supported subset (single node, or single directed
    relationship between two nodes with an optional relationship variable).
    """
    from pycypher.ast_models import RelationshipDirection, RelationshipPattern

    if match.optional:
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

        return _make_resolve(variables), build, False, frozenset(variables)

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
        variables: dict[str, tuple[str, dict[str, str]]] = {v1: ("a", a1), v2: ("b", a2)}
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

        return _make_resolve(variables), build, True, frozenset(variables)

    return None


def _analyze_query(query: Any, context: Context) -> _Plan | None:
    """Return a pipeline plan for *query* if eligible, else ``None``.

    Shape: a leading pattern ``MATCH``, then zero or more ``WITH`` stages, ending
    in a ``RETURN``.  A ``MATCH`` after a ``WITH`` (second/correlated pattern) is
    not supported.
    """
    from pycypher.ast_models import Match, Query, Return, With

    if getattr(context, "backend_name", None) != "duckdb":
        return None
    if not hasattr(getattr(context, "backend", None), "connection"):
        return None
    if not isinstance(query, Query):
        return None
    clauses = query.clauses
    if len(clauses) < 2:
        return None
    match = clauses[0]
    if not isinstance(match, Match) or not isinstance(clauses[-1], Return):
        return None
    for clause in clauses[1:-1]:
        if not isinstance(clause, With):
            return None
    pattern = _analyze_pattern(match, context)
    if pattern is None:
        return None
    resolve, build, qualified, node_vars = pattern
    return _Plan(resolve, build, match.where, qualified, node_vars, list(clauses[1:]))


def _quote_output_alias(name: str) -> str:
    """Quote *name* as a DuckDB output identifier (safe for dots etc.).

    Output aliases can legitimately contain dots (e.g. the pandas engine names a
    bare join return ``a.name``), so we escape for a quoted identifier rather
    than using the stricter :func:`sanitize_sql_identifier` (source columns).
    """
    return '"' + name.replace('"', '""') + '"'


def _output_column(item: Any, *, qualified: bool) -> str:
    """Output column name for a return/with *item*, matching the pandas engine.

    An explicit ``AS alias`` wins.  A bare variable (post-``WITH`` scalar) is
    named after the variable.  A bare property lookup is named after the
    property (single-variable patterns) or as ``var.property`` (multi-variable /
    join patterns).  Deterministic regardless of cached-AST alias mutation.
    """
    from pycypher.ast_models import Variable

    if item.alias is not None:
        return str(item.alias)
    expr = item.expression
    if isinstance(expr, Variable):
        return str(expr.name)
    if qualified:
        return f"{expr.expression.name}.{expr.property}"
    return str(expr.property)


def _build_order_clause(order_by: Any, items: Any, *, qualified: bool) -> str | None:
    """Build a DuckDB ORDER BY clause referencing a stage's output columns.

    Supports ordering by an output alias (``ORDER BY name``) or by a returned
    property lookup (``ORDER BY n.age`` when ``n.age`` is in the output).  Other
    order keys, or an explicit NULLS placement, return ``None`` (fall back).
    Emits ``NULLS LAST`` to match the pandas engine's null ordering.
    """
    from pycypher.ast_models import PropertyLookup, Variable

    output_names: set[str] = set()
    prop_to_output: dict[tuple[str, str], str] = {}
    for item in items:
        name = _output_column(item, qualified=qualified)
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
        parts.append(f"{_quote_output_alias(col)} {direction} NULLS LAST")

    return ", ".join(parts)


class _StageSQL:
    """Compiled SQL pieces for one pipeline stage."""

    __slots__ = (
        "aggregating",
        "distinct",
        "group_parts",
        "having_sql",
        "limit",
        "new_scope",
        "order_clause",
        "passthrough",
        "select_parts",
        "skip",
        "where_sql",
    )

    def __init__(self, **kw: Any) -> None:
        for slot in self.__slots__:
            setattr(self, slot, kw.get(slot))


def _stage_is_passthrough(stage: Any, scope: _Scope) -> bool:
    """True if *stage* just passes the in-scope pattern variables through."""
    from pycypher.ast_models import Variable

    if stage.distinct or stage.order_by or stage.skip is not None or stage.limit is not None:
        return False
    if not stage.items:
        return False
    return all(
        it.alias is None
        and isinstance(it.expression, Variable)
        and it.expression.name in scope.node_vars
        for it in stage.items
    )


def _plan_stage(
    stage: Any,
    scope: _Scope,
    udfs: frozenset[str],
    *,
    is_return: bool,
) -> _StageSQL | None:
    """Compile one WITH/RETURN stage over *scope*, or return ``None``.

    Returns SQL pieces plus the resulting scope for the next stage.
    """
    from pycypher.ast_models import PropertyLookup, Variable
    from pycypher.relation_sql import (
        compile_aggregate,
        compile_expression,
        is_aggregate,
    )

    # --- Node pass-through WITH (filter only, scope unchanged) ---
    if not is_return and _stage_is_passthrough(stage, scope):
        stage_where = getattr(stage, "where", None)
        where_sql = None
        if stage_where is not None:
            where_sql = compile_expression(
                stage_where, scope.resolve, udfs, scope.resolve_var,
            )
            if where_sql is None:
                return None
        return _StageSQL(passthrough=True, where_sql=where_sql, new_scope=scope)

    if not stage.items:
        return None
    if stage.skip is not None and stage.limit is None:
        return None

    aggregating = any(is_aggregate(it.expression) for it in stage.items)
    select_parts: list[str] = []
    group_parts: list[str] = []
    output_names: list[str] = []
    for it in stage.items:
        if is_aggregate(it.expression):
            sql = compile_aggregate(it.expression, scope.resolve)
            if sql is None or it.alias is None:
                return None
        else:
            sql = compile_expression(it.expression, scope.resolve, udfs, scope.resolve_var)
            if sql is None:
                return None
            if (
                not isinstance(it.expression, (PropertyLookup, Variable))
                and it.alias is None
            ):
                return None
            group_parts.append(sql)
        name = _output_column(it, qualified=scope.qualified)
        output_names.append(name)
        select_parts.append(f"{sql} AS {_quote_output_alias(name)}")

    if len(set(output_names)) != len(output_names):
        return None

    new_scope = _scalar_scope(output_names)

    stage_where = getattr(stage, "where", None)
    having_sql = None
    if stage_where is not None:
        having_sql = compile_expression(
            stage_where, new_scope.resolve, udfs, new_scope.resolve_var,
        )
        if having_sql is None:
            return None

    order_clause = None
    if stage.order_by:
        order_clause = _build_order_clause(
            stage.order_by, stage.items, qualified=scope.qualified,
        )
        if order_clause is None:
            return None

    return _StageSQL(
        passthrough=False,
        aggregating=aggregating,
        select_parts=select_parts,
        group_parts=group_parts,
        having_sql=having_sql,
        distinct=bool(stage.distinct),
        order_clause=order_clause,
        skip=stage.skip,
        limit=stage.limit,
        new_scope=new_scope,
    )


def is_relation_eligible(query: Any, context: Context) -> bool:
    """Return True if *query* is in the subset the relation engine can execute.

    Conservative by design: anything not explicitly handled makes the query
    ineligible so the caller falls back to the pandas engine.  Eligible: a
    single-node or single directed-relationship ``MATCH``; an optional
    compilable ``WHERE``; zero or more ``WITH`` stages (projection / aggregation
    / filter / DISTINCT / ORDER BY / SKIP+LIMIT, or a node pass-through); and a
    ``RETURN`` of compilable expressions.  Ineligible: multi-hop / undirected /
    variable-length paths, OPTIONAL MATCH, a MATCH after a WITH, unsupported
    functions/operators, and ``collect()``.
    """
    from pycypher.relation_sql import compile_expression

    plan = _analyze_query(query, context)
    if plan is None:
        return False

    udfs = _udf_names(context)
    if plan.match_where is not None and (
        compile_expression(plan.match_where, plan.resolve, udfs) is None
    ):
        return False

    scope = _Scope(
        plan.resolve, _no_var, qualified=plan.qualified, node_vars=plan.node_vars,
    )
    last = len(plan.stages) - 1
    for i, stage in enumerate(plan.stages):
        sp = _plan_stage(stage, scope, udfs, is_return=(i == last))
        if sp is None:
            return False
        scope = sp.new_scope
    return True


def execute_relation_query(
    query: Any,
    context: Context,
    *,
    materialize: bool = True,
) -> pd.DataFrame | RelationBindings:
    """Execute an eligible query via a pipeline of lazy DuckDB relations.

    Builds the base relation from the pattern, applies the MATCH ``WHERE``, then
    runs each ``WITH``/``RETURN`` stage (projection / aggregation / filter /
    DISTINCT / ORDER BY / SKIP+LIMIT) as lazy ``DuckDBPyRelation`` ops.

    Precondition: :func:`is_relation_eligible` returned ``True`` for *query*.

    Args:
        query: The parsed, eligible query AST.
        context: The DuckDB-backed context.
        materialize: When ``True`` (default) return a pandas DataFrame; when
            ``False`` return a :class:`RelationBindings` for streaming to a sink.

    """
    from pycypher.backends.duckdb_backend import DuckDBLazyFrame
    from pycypher.relation_sql import compile_expression

    plan = _analyze_query(query, context)
    con = context.backend.connection
    udfs = _udf_names(context)

    rel = plan.build(con)
    if plan.match_where is not None:
        rel = rel.filter(compile_expression(plan.match_where, plan.resolve, udfs))

    scope = _Scope(
        plan.resolve, _no_var, qualified=plan.qualified, node_vars=plan.node_vars,
    )
    last = len(plan.stages) - 1
    for i, stage in enumerate(plan.stages):
        sp = _plan_stage(stage, scope, udfs, is_return=(i == last))
        if sp.passthrough:
            if sp.where_sql is not None:
                rel = rel.filter(sp.where_sql)
            continue
        if sp.aggregating:
            rel = rel.aggregate(", ".join(sp.select_parts), ", ".join(sp.group_parts))
        else:
            rel = rel.project(", ".join(sp.select_parts))
        if sp.having_sql is not None:
            rel = rel.filter(sp.having_sql)
        if sp.distinct:
            rel = rel.distinct()
        if sp.order_clause is not None:
            rel = rel.order(sp.order_clause)
        if sp.limit is not None:
            rel = rel.limit(sp.limit, offset=sp.skip or 0)
        scope = sp.new_scope

    bindings = RelationBindings(DuckDBLazyFrame(rel, con))
    return bindings.to_pandas() if materialize else bindings
