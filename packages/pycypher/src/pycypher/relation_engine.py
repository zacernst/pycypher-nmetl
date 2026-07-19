"""Out-of-core relation execution path (Approach A, Phase 4+).

A separate, **opt-in** execution path that runs an *eligible* subset of Cypher
queries directly as DuckDB relations instead of the pandas ``BindingFrame``
engine.  Anything not eligible falls back to the existing engine, so coverage
grows one query-feature at a time while the suite stays green.

Disabled by default — enabled per-:class:`Context` via a truthy
``_relation_engine_enabled`` attribute or the
``PYCYPHER_DUCKDB_RELATION_ENGINE`` environment variable.  When disabled the
dispatch never fires, guaranteeing zero behaviour change.

Eligible subset so far: a required leading ``MATCH`` (single node or a
fixed-length directed path of one or more hops) with optional inline node
properties; zero or more ``OPTIONAL MATCH`` LEFT-join extensions from a bound
node; an optional ``WHERE`` (compiled to a SQL predicate via
:mod:`pycypher.relation_sql`); zero or more ``WITH`` stages; and a ``RETURN`` of
compilable expressions (property lookups, arithmetic, literals, registered
scalar UDFs, and ``count/sum/avg/min/max`` aggregates with implicit GROUP BY),
plus DISTINCT / ORDER BY / SKIP+LIMIT.  Also: a leading ``UNWIND`` of a list, a
leading ``WITH`` of constants, and ``UNWIND`` of a scalar list column in a
``WITH`` stage.  Duplicate output column names are rejected.  Not yet:
undirected / variable-length paths, a second required MATCH, OPTIONAL MATCH
combined with aggregation, ``UNWIND`` in pattern scope (right after MATCH),
``collect()``, and unregistered functions.  See
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
    shared DuckDB connection, then materialises it once into a real DuckDB
    table (``relation.create()``, a streaming ``CREATE TABLE AS SELECT`` that
    never loads the file into pandas) instead of leaving it as a lazy view the
    relation engine would otherwise re-scan from disk on every query that
    reads this source (see docs/duckdb_full_parity_design.md, Phase 1).
    Derives a property→column map from the relation schema, and stores both
    on ``context._streaming_sources`` for the relation engine to use as a base
    relation.  Requires a DuckDB-backed context.

    Args:
        context: A DuckDB-backed :class:`Context`.
        label: The entity label to register the source under.
        data_source: A :class:`DataSource` (typically from
            :func:`data_source_from_uri`).
        id_col: The column designated as the entity ID.  Excluded from the
            property map so semantics match the in-memory path (where the ID
            column is consumed into ``__ID__`` and is not a property).

    """
    from pycypher.backends._helpers import validate_identifier
    from pycypher.backends.duckdb_backend import DuckDBLazyFrame

    con = context.backend.connection
    lazy = data_source.read_relation(con)
    # Every non-ID column is exposed as a property named after the column
    # (identity map), mirroring the ContextBuilder convention for file sources.
    attr_map = {col: col for col in lazy.columns if col != id_col}

    table_name = f"_streaming_source_{validate_identifier(label)}"
    lazy.relation.create(table_name)
    materialized = DuckDBLazyFrame(con.table(table_name), con)
    context._streaming_sources[label] = (materialized, attr_map, id_col)


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
    lname = name.lower()
    if lname in context._relation_udfs:  # idempotent — bridging may re-run over the same context
        return
    context.backend.connection.create_function(lname, fn, param_types, return_type)
    context._relation_udfs.add(lname)


def _udf_names(context: Context) -> frozenset[str]:
    """Return the set of registered relation-engine UDF names (lowercase)."""
    return frozenset(context._relation_udfs)


#: Python annotation type → DuckDB type string for bridging user functions.
_PY_TO_DUCKDB: dict[type, str] = {
    int: "BIGINT",
    float: "DOUBLE",
    str: "VARCHAR",
    bool: "BOOLEAN",
}


def _duckdb_types_from_annotations(func: Any) -> tuple[list[str], str] | None:
    """Derive ``(param_types, return_type)`` from *func*'s type annotations.

    Returns ``None`` if the signature has non-positional params, or any
    parameter / the return lacks a mappable annotation — such functions can't be
    bridged and stay on the pandas engine.
    """
    import inspect
    import typing

    try:
        hints = typing.get_type_hints(func)
        sig = inspect.signature(func)
    except (TypeError, ValueError, NameError):
        return None

    param_types: list[str] = []
    for param in sig.parameters.values():
        if param.kind not in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            return None
        duckdb_type = _PY_TO_DUCKDB.get(hints.get(param.name))
        if duckdb_type is None:
            return None
        param_types.append(duckdb_type)

    return_type = _PY_TO_DUCKDB.get(hints.get("return"))
    if return_type is None or not param_types:
        return None
    return param_types, return_type


def bridge_user_functions(context: Context) -> None:
    """Bridge annotated user scalar functions to DuckDB UDFs for out-of-core use.

    Iterates the :class:`ScalarFunctionRegistry`; for each function registered
    from a plain scalar callable (recoverable via ``__wrapped__``) whose type
    annotations map cleanly to DuckDB types, registers it on the context's
    connection via :func:`register_relation_udf`.  Functions without a
    recoverable original or mappable annotations (e.g. built-ins, unannotated
    user functions) are skipped and continue to fall back to the pandas engine.
    """
    if getattr(context, "backend_name", None) != "duckdb":
        return
    from pycypher.scalar_functions import ScalarFunctionRegistry

    registry = ScalarFunctionRegistry.get_instance()
    for name, meta in registry._functions.items():  # noqa: SLF001 — read-only bridge
        original = getattr(meta.callable, "__wrapped__", None)
        if original is None:
            continue
        types = _duckdb_types_from_annotations(original)
        if types is None:
            continue
        param_types, return_type = types
        try:
            register_relation_udf(
                context, name, original,
                param_types=param_types, return_type=return_type,
            )
        except Exception:  # noqa: BLE001 — best-effort; skip anything DuckDB rejects
            from shared.logger import LOGGER

            LOGGER.debug("could not bridge user function %r", name, exc_info=True)


def _entity_attr_map(context: Context, label: str) -> dict[str, str] | None:
    """Property→column map for *label* from a streaming source or EntityTable."""
    streaming = context._streaming_sources
    if label in streaming:
        return streaming[label][1]
    entity = context.entity_mapping.mapping.get(label)
    return entity.attribute_map if entity is not None else None


def _base_relation(context: Context, label: str, con: Any) -> Any:
    """Return a lazy DuckDB relation for *label*'s source rows.

    Prefers a registered streaming relation (file-backed, out-of-core); falls
    back to the entity's in-memory ``source_obj``.
    """
    streaming = context._streaming_sources
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
    """True if *node* is a single-label variable node.

    Inline properties (``{prop: value}``) are allowed — they are desugared into
    equality predicates during pattern analysis.
    """
    from pycypher.ast_models import NodePattern

    return (
        isinstance(node, NodePattern)
        and node.variable is not None
        and len(node.labels) == 1
    )


def _inline_predicates(nodes: list[Any]) -> list[tuple[str, str, Any]]:
    """Collect ``(var, property, value_ast)`` triples from nodes' inline props."""
    preds: list[tuple[str, str, Any]] = []
    for node in nodes:
        for prop, value in (node.properties or {}).items():
            preds.append((node.variable.name, prop, value))
    return preds


def _compile_inline_predicates(
    inline_preds: list[tuple[str, str, Any]],
    resolve: Any,
    udfs: frozenset[str],
) -> list[str] | None:
    """Compile inline-property predicates to ``col = value`` SQL, or ``None``."""
    from pycypher.relation_sql import compile_expression

    out: list[str] = []
    for var, prop, value_ast in inline_preds:
        col = resolve(var, prop)
        if col is None:
            return None
        val_sql = compile_expression(value_ast, resolve, udfs)
        if val_sql is None:
            return None
        out.append(f"{col} = {val_sql}")
    return out


class _Plan:
    """A resolved, eligible relation-query plan: a base + pipeline stages."""

    __slots__ = (
        "alias_gen",
        "build",
        "initial_scope",
        "inline_preds",
        "match_where",
        "stages",
        "unwind_expr",
    )

    def __init__(
        self,
        initial_scope: _Scope,
        build: Any,
        match_where: Any,
        stages: list[Any],
        inline_preds: list[tuple[str, str, Any]],
        unwind_expr: Any = None,
        alias_gen: Any = None,
    ) -> None:
        self.initial_scope = initial_scope  # scope over the base relation
        self.build = build  # build(con) -> DuckDBPyRelation
        self.match_where = match_where  # WHERE from the MATCH clause | None
        self.stages = stages  # [With | Unwind, …, Return]
        self.inline_preds = inline_preds  # (var, prop, value_ast) equality preds
        self.unwind_expr = unwind_expr  # list expr of a leading UNWIND | None
        # Shared alias counter, reused for any embedded second MATCH so its
        # aliases can never collide with the leading pattern's (DuckDB raises
        # "Ambiguous reference to table" if two crossed relations reuse an alias).
        self.alias_gen = alias_gen


class _Scope:
    """Name resolution for one pipeline stage."""

    __slots__ = ("names", "node_vars", "qualified", "resolve", "resolve_var")

    def __init__(
        self,
        resolve: Any,
        resolve_var: Any,
        *,
        qualified: bool,
        node_vars: frozenset[str],
        names: frozenset[str] | None = None,
    ) -> None:
        self.resolve = resolve  # (var, prop) -> col | None
        self.resolve_var = resolve_var  # (name) -> col | None (bare scalars)
        self.qualified = qualified
        self.node_vars = node_vars
        self.names = names  # scalar column names (for UNWIND '*'), None in pattern scope


def _no_prop(_var: str, _prop: str) -> None:
    return None


def _no_var(_name: str) -> None:
    return None


def _scalar_scope(output_names: list[str]) -> _Scope:
    """A scope where bare variables resolve to a prior stage's output columns."""
    names = frozenset(output_names)

    def resolve_var(name: str) -> str | None:
        return _quote_output_alias(name) if name in names else None

    return _Scope(
        _no_prop, resolve_var, qualified=False, node_vars=frozenset(), names=names,
    )


def _analyze_leading_pattern(
    match: Any,
    context: Context,
    alias_gen: Any,
) -> tuple[dict[str, tuple[str, dict[str, str]]], Any, list[tuple[str, str, Any]]] | None:
    """Analyse the required leading MATCH pattern.

    Returns ``(variables, build, inline_preds)`` — variables maps each bound
    Cypher variable to ``(sql_alias, attr_map)``, build(con) returns the
    pattern's relation, inline_preds are the nodes' inline-property equalities.
    Aliases come from *alias_gen* so they are unique across the whole query.
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
        alias = alias_gen()
        variables = {node.variable.name: (alias, attr)}
        label = node.labels[0]

        def build(con: Any, label: str = label, alias: str = alias) -> Any:
            return _base_relation(context, label, con).set_alias(alias)

        return variables, build, _inline_predicates([node])

    # --- Fixed-length directed path (one or more hops) ---
    if len(elements) >= 3 and len(elements) % 2 == 1:
        nodes = elements[0::2]
        rels = elements[1::2]
        if not all(_valid_node(nd) for nd in nodes):
            return None
        for rp in rels:
            if not isinstance(rp, RelationshipPattern):
                return None
            if rp.length is not None or getattr(rp, "properties", None):
                return None
            if len(rp.labels) != 1:
                return None
            if rp.direction not in (
                RelationshipDirection.RIGHT,
                RelationshipDirection.LEFT,
            ):
                return None

        node_attrs = [_entity_attr_map(context, nd.labels[0]) for nd in nodes]
        rel_attrs = [_rel_attr_map(context, rp.labels[0]) for rp in rels]
        if any(a is None for a in node_attrs) or any(a is None for a in rel_attrs):
            return None

        node_aliases = [alias_gen() for _ in nodes]
        rel_aliases = [alias_gen() for _ in rels]
        variables = {}
        for i, nd in enumerate(nodes):
            variables[nd.variable.name] = (node_aliases[i], node_attrs[i])
        for j, rp in enumerate(rels):
            if rp.variable is not None:
                variables[rp.variable.name] = (rel_aliases[j], rel_attrs[j])
        n_named = len(nodes) + sum(1 for rp in rels if rp.variable is not None)
        if len(variables) != n_named:
            return None

        node_labels = [nd.labels[0] for nd in nodes]
        rel_labels = [rp.labels[0] for rp in rels]
        rights = [rp.direction == RelationshipDirection.RIGHT for rp in rels]

        def build(
            con: Any,
            node_labels: list[str] = node_labels,
            rel_labels: list[str] = rel_labels,
            rights: list[bool] = rights,
            node_aliases: list[str] = node_aliases,
            rel_aliases: list[str] = rel_aliases,
        ) -> Any:
            node_rels = [
                _base_relation(context, lbl, con).set_alias(al)
                for lbl, al in zip(node_labels, node_aliases, strict=True)
            ]
            rel_rels = [
                _rel_base_relation(context, lbl, con).set_alias(al)
                for lbl, al in zip(rel_labels, rel_aliases, strict=True)
            ]
            acc = node_rels[0]
            for j, right in enumerate(rights):
                na, nb, ea = node_aliases[j], node_aliases[j + 1], rel_aliases[j]
                if right:
                    c1 = f'{na}."__ID__" = {ea}."__SOURCE__"'
                    c2 = f'{ea}."__TARGET__" = {nb}."__ID__"'
                else:
                    c1 = f'{na}."__ID__" = {ea}."__TARGET__"'
                    c2 = f'{ea}."__SOURCE__" = {nb}."__ID__"'
                acc = acc.join(rel_rels[j], c1).join(node_rels[j + 1], c2)
            return acc

        return variables, build, _inline_predicates(nodes)

    return None


def _analyze_optional_pattern(
    bound: dict[str, tuple[str, dict[str, str]]],
    context: Context,
    opt_match: Any,
    alias_gen: Any,
) -> tuple[dict[str, tuple[str, dict[str, str]]], Any] | None:
    """Analyse one OPTIONAL MATCH as a LEFT-join extension.

    Supports a single directed relationship ``(x)-[e]->(y)`` / ``(x)<-[e]-(y)``
    where the left node *x* is already bound and the right node *y* (and an
    optional relationship variable) is new.  Returns ``(new_variables,
    extend)`` where ``extend(con, base_rel)`` LEFT-joins the hop onto *base_rel*.
    """
    from pycypher.ast_models import (
        NodePattern,
        RelationshipDirection,
        RelationshipPattern,
    )

    if opt_match.where is not None:
        return None  # WHERE on an optional pattern would need join-condition placement
    paths = opt_match.pattern.paths
    if len(paths) != 1:
        return None
    path = paths[0]
    if path.variable is not None:
        return None
    if getattr(path, "shortest_path_mode", "none") not in ("none", None):
        return None
    elements = path.elements
    if len(elements) != 3:
        return None
    n_left, rp, n_right = elements
    # The left node is already bound: referenced by variable, its label is
    # optional (and ignored).  The right node is new and needs a single label.
    if not (isinstance(n_left, NodePattern) and n_left.variable is not None):
        return None
    if not _valid_node(n_right):
        return None
    if getattr(n_left, "properties", None) or getattr(n_right, "properties", None):
        return None  # inline props on an optional pattern not supported
    if not isinstance(rp, RelationshipPattern):
        return None
    if rp.length is not None or getattr(rp, "properties", None):
        return None
    if len(rp.labels) != 1:
        return None
    if rp.direction not in (
        RelationshipDirection.RIGHT,
        RelationshipDirection.LEFT,
    ):
        return None

    x_var, y_var = n_left.variable.name, n_right.variable.name
    if x_var not in bound or y_var in bound:
        return None  # left must be bound, right must be new
    rel_attr = _rel_attr_map(context, rp.labels[0])
    y_attr = _entity_attr_map(context, n_right.labels[0])
    if rel_attr is None or y_attr is None:
        return None

    x_alias = bound[x_var][0]
    y_alias, e_alias = alias_gen(), alias_gen()
    new_vars: dict[str, tuple[str, dict[str, str]]] = {y_var: (y_alias, y_attr)}
    if rp.variable is not None:
        rv = rp.variable.name
        if rv in bound or rv == y_var:
            return None
        new_vars[rv] = (e_alias, rel_attr)

    right = rp.direction == RelationshipDirection.RIGHT
    y_label, e_label = n_right.labels[0], rp.labels[0]

    def extend(
        con: Any,
        base_rel: Any,
        x_alias: str = x_alias,
        y_alias: str = y_alias,
        e_alias: str = e_alias,
        y_label: str = y_label,
        e_label: str = e_label,
        right: bool = right,  # noqa: FBT001
    ) -> Any:
        e_rel = _rel_base_relation(context, e_label, con).set_alias(e_alias)
        y_rel = _base_relation(context, y_label, con).set_alias(y_alias)
        if right:
            c1 = f'{x_alias}."__ID__" = {e_alias}."__SOURCE__"'
            c2 = f'{e_alias}."__TARGET__" = {y_alias}."__ID__"'
        else:
            c1 = f'{x_alias}."__ID__" = {e_alias}."__TARGET__"'
            c2 = f'{e_alias}."__SOURCE__" = {y_alias}."__ID__"'
        return base_rel.join(e_rel, c1, how="left").join(y_rel, c2, how="left")

    return new_vars, extend


def _compose_build(base_build: Any, extend: Any) -> Any:
    """Return a build that applies *extend* to *base_build*'s relation."""

    def composed(con: Any, base_build: Any = base_build, extend: Any = extend) -> Any:
        return extend(con, base_build(con))

    return composed


def _leading_unwind_build(var: str, list_expr: Any) -> Any:
    """Build for a leading ``UNWIND <list> AS var`` (base = the unnested list)."""

    def build(con: Any, var: str = var, list_expr: Any = list_expr) -> Any:
        from pycypher.relation_sql import compile_expression

        list_sql = compile_expression(list_expr, _no_prop, resolve_var=_no_var)
        return con.sql(f"SELECT UNNEST({list_sql}) AS {_quote_output_alias(var)}")  # nosec B608 — list_sql from internal AST compiler, var safely double-quoted via _quote_output_alias

    return build


def _analyze_query(query: Any, context: Context) -> _Plan | None:
    """Return a pipeline plan for *query* if eligible, else ``None``.

    Shape: either a required leading ``MATCH`` (then zero or more ``OPTIONAL
    MATCH`` LEFT-join extensions), or a leading ``UNWIND`` of a list; followed by
    zero or more ``WITH``/``UNWIND`` stages, optionally including exactly one
    additional required ``MATCH`` immediately after a ``WITH`` (a cross-joined
    multi-pattern query); ending in ``RETURN``.  Any other ``MATCH`` after the
    pattern phase is not supported.
    """
    from pycypher.ast_models import Match, Query, Return, Unwind, With

    if getattr(context, "backend_name", None) != "duckdb":
        return None
    if not hasattr(getattr(context, "backend", None), "connection"):
        return None
    if not isinstance(query, Query):
        return None
    clauses = query.clauses
    if len(clauses) < 2 or not isinstance(clauses[-1], Return):
        return None

    counter = [0]

    def alias_gen() -> str:
        alias = f"v{counter[0]}"
        counter[0] += 1
        return alias

    def _valid_stages(stages: list[Any]) -> bool:
        # Middle stages (all but the final RETURN) must be WITH or UNWIND,
        # except for at most one embedded MATCH: non-optional, not first,
        # and immediately preceded by a WITH (a cross-joined second pattern).
        non_terminal = stages[:-1]
        match_idxs = [i for i, c in enumerate(non_terminal) if isinstance(c, Match)]
        if len(match_idxs) > 1:
            return False
        if match_idxs:
            i = match_idxs[0]
            if (
                i == 0
                or not isinstance(non_terminal[i - 1], With)
                or non_terminal[i].optional
            ):
                return False
        skip = match_idxs[0] if match_idxs else -1
        return all(
            isinstance(c, (With, Unwind))
            for j, c in enumerate(non_terminal)
            if j != skip
        )

    # --- Leading UNWIND of a list ---
    if isinstance(clauses[0], Unwind):
        uw = clauses[0]
        if uw.alias is None:
            return None
        stages = list(clauses[1:])
        if not _valid_stages(stages):
            return None
        return _Plan(
            _scalar_scope([uw.alias]),
            _leading_unwind_build(uw.alias, uw.expression),
            None,
            stages,
            [],
            unwind_expr=uw.expression,
            alias_gen=alias_gen,
        )

    # --- Leading WITH of constants (no source) → single-row base ---
    if isinstance(clauses[0], With):
        stages = list(clauses)
        if not _valid_stages(stages):
            return None

        def build(con: Any) -> Any:
            return con.sql("SELECT 1 AS __unit")

        return _Plan(_scalar_scope([]), build, None, stages, [], alias_gen=alias_gen)

    # --- Leading MATCH pattern (+ optional matches) ---
    if not isinstance(clauses[0], Match) or clauses[0].optional:
        return None

    idx = 1
    opt_matches: list[Any] = []
    while idx < len(clauses) - 1 and isinstance(clauses[idx], Match):
        if not clauses[idx].optional:
            return None  # a second required MATCH is not supported
        opt_matches.append(clauses[idx])
        idx += 1
    stages = list(clauses[idx:])
    if not _valid_stages(stages):
        return None

    # Aggregating over an OPTIONAL pattern is not supported: count(<optional
    # node>) must count non-null matches, but the engine's count(node) →
    # COUNT(*) shortcut would over-count.  Fall back for correctness.
    if opt_matches:
        from pycypher.relation_sql import is_aggregate

        if any(
            is_aggregate(it.expression)
            for stage in stages
            for it in getattr(stage, "items", [])
        ):
            return None

    lead = _analyze_leading_pattern(clauses[0], context, alias_gen)
    if lead is None:
        return None
    variables, build, inline_preds = lead

    for opt_match in opt_matches:
        ext = _analyze_optional_pattern(variables, context, opt_match, alias_gen)
        if ext is None:
            return None
        new_vars, extend = ext
        variables = {**variables, **new_vars}
        build = _compose_build(build, extend)

    initial_scope = _Scope(
        _make_resolve(variables),
        _no_var,
        qualified=len(variables) > 1,
        node_vars=frozenset(variables),
    )
    return _Plan(
        initial_scope, build, clauses[0].where, stages, inline_preds, alias_gen=alias_gen,
    )


def _analyze_second_match(
    prior_scope: _Scope, match: Any, context: Context, alias_gen: Any,
) -> tuple[_Scope, Any, Any, list[tuple[str, str, Any]], str] | None:
    """Analyse a ``MATCH`` embedded after a ``WITH`` (a cross-joined pattern).

    Returns ``(new_scope, build, where, inline_preds, acc_alias)``. *build*
    is the second pattern's own relation builder (reused verbatim from
    :func:`_analyze_leading_pattern`). *new_scope* resolves both the new
    pattern's variables and the prior stage's scalar outputs, the latter
    qualified against *acc_alias* — the alias the caller must
    ``set_alias()`` on the accumulated relation before crossing, so that
    column names shared between the two sides of the cross join never
    collide. *alias_gen* must be the plan's shared counter (not a fresh
    one), or the second pattern's aliases could collide with the leading
    pattern's, which DuckDB rejects as an ambiguous table reference.
    """
    if match.optional:
        return None
    lead = _analyze_leading_pattern(match, context, alias_gen)
    if lead is None:
        return None
    new_vars, build, inline_preds = lead
    prior_names = prior_scope.names or frozenset()
    if prior_names & new_vars.keys():
        return None  # name collision between WITH output and new pattern var
    acc_alias = alias_gen()
    resolve = _make_resolve(new_vars)

    def resolve_var(name: str) -> str | None:
        return f"{acc_alias}.{_quote_output_alias(name)}" if name in prior_names else None

    new_scope = _Scope(
        resolve, resolve_var, qualified=True, node_vars=frozenset(new_vars),
    )
    return new_scope, build, match.where, inline_preds, acc_alias


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
        "unwind",
        "unwind_select",
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
    from pycypher.ast_models import PropertyLookup, Unwind, Variable
    from pycypher.relation_sql import (
        compile_aggregate,
        compile_expression,
        is_aggregate,
    )

    # --- UNWIND stage: expand a list column, keeping current columns ---
    if isinstance(stage, Unwind):
        if scope.names is None or stage.alias is None:
            return None  # only supported in scalar scope (post-WITH / leading UNWIND)
        if stage.alias in scope.names:
            return None  # would shadow an existing column
        expr_sql = compile_expression(
            stage.expression, scope.resolve, udfs, scope.resolve_var,
        )
        if expr_sql is None:
            return None
        select = f"*, UNNEST({expr_sql}) AS {_quote_output_alias(stage.alias)}"
        new_names = [*sorted(scope.names), stage.alias]
        return _StageSQL(
            unwind=True, unwind_select=select, new_scope=_scalar_scope(new_names),
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
            sql = compile_aggregate(
                it.expression, scope.resolve, udfs, scope.resolve_var,
            )
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
    single-node or fixed-length directed-path ``MATCH`` (one or more hops);
    zero or more ``OPTIONAL MATCH`` LEFT-join extensions from a bound node;
    an optional compilable ``WHERE``; zero or more ``WITH`` stages (projection
    / aggregation / filter / DISTINCT / ORDER BY / SKIP+LIMIT, or a node
    pass-through); a cross-joined second required ``MATCH`` immediately after
    a ``WITH``; and a ``RETURN`` of compilable expressions.  Ineligible:
    undirected / variable-length paths, more than one embedded second
    ``MATCH`` or one not preceded by a ``WITH``, ``OPTIONAL MATCH`` combined
    with aggregation, ``UNWIND`` in pattern scope, unsupported
    functions/operators, and ``collect()``.
    """
    from pycypher.ast_models import Match
    from pycypher.relation_sql import compile_expression

    plan = _analyze_query(query, context)
    if plan is None:
        return False

    udfs = _udf_names(context)
    resolve = plan.initial_scope.resolve
    if plan.match_where is not None and (
        compile_expression(plan.match_where, resolve, udfs) is None
    ):
        return False
    if plan.unwind_expr is not None and (
        compile_expression(plan.unwind_expr, _no_prop, resolve_var=_no_var) is None
    ):
        return False
    if _compile_inline_predicates(plan.inline_preds, resolve, udfs) is None:
        return False

    scope = plan.initial_scope
    last = len(plan.stages) - 1
    for i, stage in enumerate(plan.stages):
        if isinstance(stage, Match):
            ext = _analyze_second_match(scope, stage, context, plan.alias_gen)
            if ext is None:
                return False
            new_scope, _build, where, inline_preds, _acc_alias = ext
            if where is not None and (
                compile_expression(where, new_scope.resolve, udfs, new_scope.resolve_var)
                is None
            ):
                return False
            if _compile_inline_predicates(inline_preds, new_scope.resolve, udfs) is None:
                return False
            scope = new_scope
            continue
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
    from pycypher.ast_models import Match
    from pycypher.backends.duckdb_backend import DuckDBLazyFrame
    from pycypher.relation_sql import compile_expression

    plan = _analyze_query(query, context)
    con = context.backend.connection
    udfs = _udf_names(context)

    resolve = plan.initial_scope.resolve
    rel = plan.build(con)
    if plan.match_where is not None:
        rel = rel.filter(compile_expression(plan.match_where, resolve, udfs))
    for pred in _compile_inline_predicates(plan.inline_preds, resolve, udfs) or []:
        rel = rel.filter(pred)

    scope = plan.initial_scope
    last = len(plan.stages) - 1
    for i, stage in enumerate(plan.stages):
        if isinstance(stage, Match):
            new_scope, build, where, inline_preds, acc_alias = _analyze_second_match(
                scope, stage, context, plan.alias_gen,
            )
            # `.join(other, "true")` rather than `.cross()`: DuckDB's relation
            # API drops component-alias info after a `.filter()`/`.project()`
            # chained onto a `.cross()` result (verified — raises "Referenced
            # table ... not found"), but preserves it across `.join()`.
            rel = rel.set_alias(acc_alias).join(build(con), "true")
            if where is not None:
                rel = rel.filter(
                    compile_expression(where, new_scope.resolve, udfs, new_scope.resolve_var),
                )
            for pred in _compile_inline_predicates(inline_preds, new_scope.resolve, udfs) or []:
                rel = rel.filter(pred)
            scope = new_scope
            continue
        sp = _plan_stage(stage, scope, udfs, is_return=(i == last))
        if sp.unwind:
            rel = rel.project(sp.unwind_select)
            scope = sp.new_scope
            continue
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


# ---------------------------------------------------------------------------
# Mutations (docs/duckdb_full_parity_design.md, Phase 2: SET / DELETE / CREATE)
# ---------------------------------------------------------------------------


def _analyze_single_node_match(
    query: Any,
    context: Context,
    clause_type: type,
) -> tuple[Any, Any, str, str, Any] | None:
    """Return ``(match, next_clause, label, var_name, resolve)`` for a
    required, non-optional, single-node ``MATCH [WHERE ...]`` immediately
    followed by exactly one clause of *clause_type*, on a label with a
    registered streaming source — the shared eligibility shape for ``SET``
    and ``DELETE``.  Callers must still validate the following clause's own
    item/expression shape (this only checks its type and position).

    *Label* must have a registered streaming source (:func:`register_streaming_source`)
    — the real, writable DuckDB table the mutation compiles a native
    statement against; entities only available via the in-memory
    ``entity_mapping`` fallback are ineligible since there is no table to
    write to.
    """
    from pycypher.ast_models import Match, Query

    if getattr(context, "backend_name", None) != "duckdb":
        return None
    if not hasattr(getattr(context, "backend", None), "connection"):
        return None
    if not isinstance(query, Query):
        return None
    clauses = query.clauses
    if len(clauses) != 2:
        return None
    match, next_clause = clauses
    if not isinstance(match, Match) or match.optional:
        return None
    if not isinstance(next_clause, clause_type):
        return None

    paths = match.pattern.paths
    if len(paths) != 1 or paths[0].variable is not None:
        return None
    elements = paths[0].elements
    if len(elements) != 1:
        return None
    node = elements[0]
    if not _valid_node(node):
        return None

    label = node.labels[0]
    if label not in context._streaming_sources:
        return None
    attr = _entity_attr_map(context, label)
    if attr is None:
        return None

    var_name = node.variable.name
    resolve = _make_resolve({var_name: ("", attr)})

    return match, next_clause, label, var_name, resolve


def _analyze_set_query(
    query: Any,
    context: Context,
) -> tuple[Any, Any, str, str, Any] | None:
    """Return ``(match, set_clause, label, var_name, resolve)`` if *query* is
    a single-table ``SET``-eligible mutation, else ``None``.

    Eligible shape: exactly ``MATCH (var:Label) [WHERE ...] SET ...`` — see
    :func:`_analyze_single_node_match` for the shared MATCH-shape checks
    (no relationship pattern, no ``OPTIONAL MATCH``, no ``WITH`` stages,
    label has a registered streaming source), plus a ``SET`` whose items are
    all plain ``var.prop = expr`` assignments targeting *var*.  The AST
    converter represents every ``SET`` item form (property assignment,
    ``SET n:Label``, ``SET n = {..}``, ``SET n += {..}``) as the same
    :class:`SetItem` class, distinguished only by which fields are
    populated: a plain property assignment has a non-sentinel ``property``
    (not ``None``, ``"*"``, or ``"*+"``), a populated ``expression``, and no
    ``labels`` — anything else (label sets, whole-map sets/merges) is
    rejected here.
    """
    from pycypher.ast_models import Set

    info = _analyze_single_node_match(query, context, Set)
    if info is None:
        return None
    match, set_clause, label, var_name, resolve = info
    if not set_clause.items:
        return None

    if not all(
        item.variable is not None
        and item.variable.name == var_name
        and item.property is not None
        and item.property not in ("*", "*+")
        and item.expression is not None
        and not item.labels
        for item in set_clause.items
    ):
        return None

    return match, set_clause, label, var_name, resolve


def is_relation_set_eligible(query: Any, context: Context) -> bool:
    """Return True if *query* is a single-table ``SET`` the relation engine
    can execute as a native ``UPDATE`` (see :func:`_analyze_set_query`).
    """
    from pycypher.relation_sql import compile_expression

    info = _analyze_set_query(query, context)
    if info is None:
        return False
    match, set_clause, _label, var_name, resolve = info

    udfs = _udf_names(context)
    if match.where is not None and compile_expression(match.where, resolve, udfs) is None:
        return False
    node = match.pattern.paths[0].elements[0]
    if _compile_inline_predicates(_inline_predicates([node]), resolve, udfs) is None:
        return False
    for item in set_clause.items:
        if resolve(var_name, item.property) is None:
            return False
        if compile_expression(item.expression, resolve, udfs) is None:
            return False
    return True


def execute_relation_set(query: Any, context: Context) -> None:
    """Execute an eligible single-table ``SET`` as a native ``UPDATE``.

    Precondition: :func:`is_relation_set_eligible` returned ``True`` for
    *query*. No return value — mirrors the pandas path's convention of an
    empty result for a non-``RETURN``-terminal mutation query.
    """
    from pycypher.backends._helpers import validate_identifier
    from pycypher.relation_sql import compile_expression

    match, set_clause, label, var_name, resolve = _analyze_set_query(query, context)  # type: ignore[misc]
    udfs = _udf_names(context)

    where_parts: list[str] = []
    if match.where is not None:
        where_parts.append(compile_expression(match.where, resolve, udfs))  # type: ignore[arg-type]
    node = match.pattern.paths[0].elements[0]
    where_parts.extend(
        _compile_inline_predicates(_inline_predicates([node]), resolve, udfs) or [],
    )

    set_parts = [
        f"{resolve(var_name, item.property)} = {compile_expression(item.expression, resolve, udfs)}"
        for item in set_clause.items
    ]

    table = f"_streaming_source_{validate_identifier(label)}"
    sql = f'UPDATE "{table}" SET {", ".join(set_parts)}'  # nosec B608 — table validated by validate_identifier; set/where parts built exclusively from relation_sql.compile_expression's whitelisted compiler
    if where_parts:
        sql += " WHERE " + " AND ".join(where_parts)

    context.backend.connection.execute(sql)


def _analyze_delete_query(
    query: Any,
    context: Context,
) -> tuple[Any, Any, str, str, Any] | None:
    """Return ``(match, delete_clause, label, var_name, resolve)`` if *query*
    is a single-table ``DELETE``-eligible mutation, else ``None``.

    Eligible shape: exactly ``MATCH (var:Label) [WHERE ...] DELETE var`` —
    see :func:`_analyze_single_node_match` for the shared MATCH-shape checks,
    plus a non-``DETACH`` ``DELETE`` whose sole expression is a ``Variable``
    matching *var* (mirrors the pandas ``process_delete``'s Variable-only
    handling, ``mutation_engine.py:647-666``).  ``DETACH DELETE`` and
    deleting anything other than the bound node (e.g. a relationship
    variable, or multiple expressions) are out of scope for this slice.
    """
    from pycypher.ast_models import Delete, Variable

    info = _analyze_single_node_match(query, context, Delete)
    if info is None:
        return None
    match, delete_clause, label, var_name, resolve = info
    if delete_clause.detach:
        return None
    if len(delete_clause.expressions) != 1:
        return None
    expr = delete_clause.expressions[0]
    if not (isinstance(expr, Variable) and expr.name == var_name):
        return None

    return match, delete_clause, label, var_name, resolve


def is_relation_delete_eligible(query: Any, context: Context) -> bool:
    """Return True if *query* is a single-table ``DELETE`` the relation
    engine can execute as a native ``DELETE FROM`` (see
    :func:`_analyze_delete_query`).
    """
    from pycypher.relation_sql import compile_expression

    info = _analyze_delete_query(query, context)
    if info is None:
        return False
    match, _delete_clause, _label, _var_name, resolve = info

    udfs = _udf_names(context)
    if match.where is not None and compile_expression(match.where, resolve, udfs) is None:
        return False
    node = match.pattern.paths[0].elements[0]
    return _compile_inline_predicates(_inline_predicates([node]), resolve, udfs) is not None


def execute_relation_delete(query: Any, context: Context) -> None:
    """Execute an eligible single-table ``DELETE`` as a native ``DELETE FROM``.

    Precondition: :func:`is_relation_delete_eligible` returned ``True`` for
    *query*. No return value — mirrors the pandas path's convention of an
    empty result for a non-``RETURN``-terminal mutation query.  Compiles the
    ``MATCH``'s ``WHERE`` + inline predicates straight into the ``DELETE``'s
    ``WHERE`` (no ``id IN (...)`` subquery) since the eligible shape only
    ever has the one bound variable being deleted.
    """
    from pycypher.backends._helpers import validate_identifier
    from pycypher.relation_sql import compile_expression

    match, _delete_clause, label, _var_name, resolve = _analyze_delete_query(  # type: ignore[misc]
        query, context,
    )
    udfs = _udf_names(context)

    where_parts: list[str] = []
    if match.where is not None:
        where_parts.append(compile_expression(match.where, resolve, udfs))  # type: ignore[arg-type]
    node = match.pattern.paths[0].elements[0]
    where_parts.extend(
        _compile_inline_predicates(_inline_predicates([node]), resolve, udfs) or [],
    )

    table = f"_streaming_source_{validate_identifier(label)}"
    sql = f'DELETE FROM "{table}"'  # nosec B608 — table validated by validate_identifier; where parts built exclusively from relation_sql.compile_expression's whitelisted compiler
    if where_parts:
        sql += " WHERE " + " AND ".join(where_parts)

    context.backend.connection.execute(sql)


#: DuckDB integer type names for which sequence-based ``MAX(id)+1`` ID
#: generation is well-defined.
_INTEGER_DUCKDB_TYPES = frozenset({
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
    "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT",
})


def _streaming_id_col(context: Context, label: str) -> str | None:
    """Return the registered ID column for *label*'s streaming source, or
    ``None`` if there is no streaming source or it has no ``id_col``.
    """
    entry = context._streaming_sources.get(label)
    return entry[2] if entry is not None else None


def _streaming_id_is_integer(context: Context, label: str) -> bool:
    """True if *label*'s streaming ``id_col`` (if any) has an integer type.

    ``True`` when there is no ``id_col`` to validate (nothing to check).
    Sequence-based ``nextval()`` ID generation only produces integers, so a
    non-integer ID column makes ``CREATE`` ineligible for this slice.
    """
    id_col = _streaming_id_col(context, label)
    if id_col is None:
        return True
    materialized = context._streaming_sources[label][0]
    relation = materialized.relation
    try:
        idx = relation.columns.index(id_col)
    except ValueError:
        return False
    return str(relation.types[idx]).upper() in _INTEGER_DUCKDB_TYPES


def _analyze_create_query(
    query: Any,
    context: Context,
) -> tuple[str, dict[str, Any]] | None:
    """Return ``(label, properties)`` if *query* is a standalone single-node
    ``CREATE``-eligible mutation, else ``None``.

    Eligible shape: exactly ``CREATE (var:Label {...})`` — a single clause
    (no preceding ``MATCH``), a single path, a single node with exactly one
    label and no relationship.  Row-per-matched-row ``CREATE`` (following a
    ``MATCH``) and relationship ``CREATE``, both supported by the pandas
    ``process_create``, are out of scope for this slice.  *Label* must have
    a registered streaming source, and every property key must already
    resolve to an existing column via :func:`_entity_attr_map` — creating a
    brand-new property column would require ``ALTER TABLE``, which this
    slice does not attempt (an unknown property key falls back to pandas,
    which grows the shadow schema dynamically).
    """
    from pycypher.ast_models import Create, NodePattern, Query

    if getattr(context, "backend_name", None) != "duckdb":
        return None
    if not hasattr(getattr(context, "backend", None), "connection"):
        return None
    if not isinstance(query, Query):
        return None
    clauses = query.clauses
    if len(clauses) != 1:
        return None
    create = clauses[0]
    if not isinstance(create, Create) or create.pattern is None:
        return None
    paths = create.pattern.paths
    if len(paths) != 1:
        return None
    path = paths[0]
    if path.variable is not None:
        return None
    elements = path.elements
    if len(elements) != 1:
        return None
    node = elements[0]
    if not (isinstance(node, NodePattern) and len(node.labels) == 1):
        return None

    label = node.labels[0]
    if label not in context._streaming_sources:
        return None
    attr = _entity_attr_map(context, label)
    if attr is None:
        return None
    if any(key not in attr for key in node.properties):
        return None

    return label, node.properties


def is_relation_create_eligible(query: Any, context: Context) -> bool:
    """Return True if *query* is a standalone single-node ``CREATE`` the
    relation engine can execute as a native ``INSERT`` (see
    :func:`_analyze_create_query`).
    """
    from pycypher.relation_sql import compile_expression

    info = _analyze_create_query(query, context)
    if info is None:
        return False
    label, properties = info
    if not _streaming_id_is_integer(context, label):
        return False

    udfs = _udf_names(context)
    return all(
        compile_expression(expr, _no_prop, udfs, resolve_var=_no_var) is not None
        for expr in properties.values()
    )


def execute_relation_create(query: Any, context: Context) -> None:
    """Execute an eligible standalone single-node ``CREATE`` as a native
    ``INSERT``.

    Precondition: :func:`is_relation_create_eligible` returned ``True`` for
    *query*. No return value — mirrors the pandas path's convention of an
    empty result for a non-``RETURN``-terminal mutation query.  When *label*
    has a registered ``id_col``, generates the new ID via a DuckDB
    ``SEQUENCE`` (created lazily, idempotently — ``START`` only applies the
    first time — seeded above the current max), replacing
    ``MutationEngine._next_ids``'s pandas max-scan for this path only; the
    pandas path itself is untouched.
    """
    from pycypher.backends._helpers import validate_identifier
    from pycypher.relation_sql import compile_expression

    label, properties = _analyze_create_query(query, context)  # type: ignore[misc]
    attr = _entity_attr_map(context, label)  # type: ignore[assignment]
    udfs = _udf_names(context)
    con = context.backend.connection
    table = f"_streaming_source_{validate_identifier(label)}"

    cols: list[str] = []
    values: list[str] = []

    id_col = _streaming_id_col(context, label)
    if id_col is not None:
        quoted_id_col = validate_identifier(id_col)
        seq = f"_streaming_seq_{validate_identifier(label)}"
        max_id = con.execute(
            f'SELECT COALESCE(MAX("{quoted_id_col}"), 0) FROM "{table}"',  # nosec B608 — table/id_col validated identifiers
        ).fetchone()[0]
        con.execute(
            f'CREATE SEQUENCE IF NOT EXISTS "{seq}" START {int(max_id) + 1}',  # nosec B608 — seq name validated; start value is an int, not user SQL
        )
        cols.append(quoted_id_col)
        values.append(f"nextval('{seq}')")

    for key, expr in properties.items():
        cols.append(validate_identifier(attr[key]))
        values.append(compile_expression(expr, _no_prop, udfs, resolve_var=_no_var))  # type: ignore[arg-type]

    col_list = ", ".join(f'"{c}"' for c in cols)
    val_list = ", ".join(values)
    sql = f'INSERT INTO "{table}" ({col_list}) SELECT {val_list}'  # nosec B608 — table/columns validated identifiers; values from relation_sql.compile_expression's whitelisted compiler or nextval() sequence call
    con.execute(sql)


def is_relation_mutation_eligible(query: Any, context: Context) -> str | None:
    """Return ``"set"``/``"create"``/``"delete"`` if *query* is an eligible
    single-table mutation the relation engine can execute natively, else
    ``None``.  Checked in that order; a single query shape can only ever
    match one kind.
    """
    if is_relation_set_eligible(query, context):
        return "set"
    if is_relation_create_eligible(query, context):
        return "create"
    if is_relation_delete_eligible(query, context):
        return "delete"
    return None


def execute_relation_mutation(query: Any, context: Context, kind: str) -> None:
    """Dispatch to the native execute function for *kind* (see
    :func:`is_relation_mutation_eligible`).
    """
    if kind == "set":
        execute_relation_set(query, context)
    elif kind == "create":
        execute_relation_create(query, context)
    elif kind == "delete":
        execute_relation_delete(query, context)
    else:
        msg = f"Unknown relation mutation kind: {kind!r}"
        raise ValueError(msg)
