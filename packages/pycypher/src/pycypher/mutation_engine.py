"""Mutation engine — all write-path operations for Cypher query execution.

Extracted from the Star god-object to provide a focused, single-responsibility
module for CREATE, SET, DELETE, DETACH DELETE, MERGE, FOREACH, and REMOVE
clause execution.

All mutations are staged in the per-query shadow layer on the
:class:`~pycypher.relational_models.Context` and only committed when the
enclosing query succeeds.

Usage::

    engine = MutationEngine(context=ctx)
    engine.process_create(clause, current_frame)
    engine.set_properties(clause, frame)
    engine.process_delete(clause, frame)

"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import pandas as pd
from shared.logger import LOGGER

from pycypher.constants import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
    _null_series,
)

if TYPE_CHECKING:
    from pycypher.ast_models import Call, Create, Delete, Foreach, Merge, Remove, Set
    from pycypher.binding_frame import BindingFrame
    from pycypher.relational_models import Context


class MutationEngine:
    """Encapsulates all write-path operations for Cypher query execution.

    Each method corresponds to a mutation clause type (SET, CREATE, DELETE,
    MERGE, FOREACH, REMOVE).  All mutations go through the shadow-write
    layer on the ``Context``, ensuring atomicity at the query level.

    State transition model
    ~~~~~~~~~~~~~~~~~~~~~~

    Mutations follow a two-phase pattern:

    1. **Stage** — each mutation method writes to the Context's shadow
       layer (a copy-on-write overlay).  The original DataFrames are
       never modified in-place during execution.
    2. **Commit** — after all clauses execute successfully, the shadow
       layer is merged into the live entity/relationship tables.  On
       failure, the shadow layer is discarded (rollback semantics).

    This ensures that a query like
    ``MATCH (a) SET a.x = 1 SET a.y = a.x + 1`` sees consistent state
    within the same query, and a failing query leaves the context unchanged.

    Supported clauses:

    - ``CREATE`` — :meth:`process_create` — insert new nodes/relationships
    - ``SET`` — :meth:`set_properties` — update properties on matched entities
    - ``DELETE`` / ``DETACH DELETE`` — :meth:`process_delete` — remove entities
    - ``MERGE`` — :meth:`process_merge` — match-or-create with ON CREATE/MATCH SET
    - ``FOREACH`` — :meth:`process_foreach` — iterate and mutate
    - ``REMOVE`` — handled via SET with null values

    Args:
        context: The :class:`~pycypher.relational_models.Context` holding
            entity and relationship mappings.

    """

    def __init__(self, context: Context) -> None:
        """Initialize mutation engine.

        Args:
            context: The execution context holding entity and relationship
                mappings. Mutations are applied through the shadow-write layer.
        """
        self.context: Context = context

    # ------------------------------------------------------------------
    # SET clause
    # ------------------------------------------------------------------

    def set_properties(
        self,
        set_clause: Set,
        frame: BindingFrame,
    ) -> None:
        """Apply a SET clause against a BindingFrame (mutates context in place).

        Only :class:`~pycypher.ast_models.SetPropertyItem` is supported.  Each
        item evaluates its value expression via
        :class:`~pycypher.binding_evaluator.BindingExpressionEvaluator` and
        calls :meth:`~pycypher.binding_frame.BindingFrame.mutate` to write
        back to the entity table.

        Args:
            set_clause: AST :class:`~pycypher.ast_models.Set` node.
            frame: Current :class:`~pycypher.binding_frame.BindingFrame`.

        """
        t0 = time.perf_counter()
        from pycypher.ast_models import MapLiteral, SetItem, SetPropertyItem
        from pycypher.binding_evaluator import BindingExpressionEvaluator

        n_items = len(set_clause.items)
        n_rows = len(frame)
        LOGGER.debug("mutation SET: %d items, %d rows", n_items, n_rows)

        evaluator = BindingExpressionEvaluator(frame)

        for item in set_clause.items:
            if not isinstance(item, (SetPropertyItem, SetItem)):
                msg = (
                    f"SET item type '{type(item).__name__}' is not yet supported "
                    "in the BindingFrame path."
                )
                raise NotImplementedError(msg)

            prop = item.property
            expr = (
                item.value
                if isinstance(item, SetPropertyItem)
                else item.expression
            )

            if prop is None:
                # SET p:Label — label assignment; no DataFrame property to update
                continue

            if prop in ("*", "*+"):
                if isinstance(expr, MapLiteral):
                    for key, val_expr in expr.entries.items():
                        values = evaluator.evaluate(val_expr)
                        frame.mutate(item.variable.name, key, values)
                    LOGGER.debug(
                        "mutation SET: map literal expansion  var=%s  keys=%d",
                        item.variable.name,
                        len(expr.entries),
                    )
                elif expr is not None:
                    map_series = evaluator.evaluate(expr)
                    all_keys: set[str] = set()
                    for v in map_series:
                        if isinstance(v, dict):
                            all_keys.update(v.keys())
                    for key in sorted(all_keys):
                        col_values = pd.Series(
                            [
                                v.get(key) if isinstance(v, dict) else None
                                for v in map_series
                            ],
                            dtype=object,
                        )
                        frame.mutate(item.variable.name, key, col_values)
                    LOGGER.debug(
                        "mutation SET: map expression expansion  var=%s  keys=%d",
                        item.variable.name,
                        len(all_keys),
                    )
                continue

            # Standard property assignment: SET p.prop = expr
            if expr is None:
                values = _null_series(len(frame))
            else:
                values = evaluator.evaluate(expr)
            frame.mutate(item.variable.name, prop, values)

        LOGGER.debug(
            "mutation SET: completed in %.3fms",
            (time.perf_counter() - t0) * 1000,
        )

    # ------------------------------------------------------------------
    # ID generation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _max_id_from_df(
        df: pd.DataFrame, type_label: str, source_label: str,
    ) -> int:
        """Extract the maximum integer ID from a DataFrame's ID column.

        Returns 0 when the DataFrame is empty, has no valid IDs, or
        contains non-integer values (with a debug log in the latter case).

        Args:
            df: DataFrame with an ``ID_COLUMN`` column.
            type_label: Type label for diagnostic logging.
            source_label: Label describing the source (e.g. ``"shadow"``).

        """
        if len(df) == 0:
            return 0
        candidate = df[ID_COLUMN].max()
        if pd.isna(candidate):
            return 0
        try:
            return int(candidate)
        except (ValueError, TypeError):
            LOGGER.debug(
                "Non-integer %s ID %r for %s; defaulting to 0",
                source_label,
                candidate,
                type_label,
            )
            return 0

    def _next_ids(
        self,
        type_label: str,
        mapping: dict[str, Any],
        shadow_dict: dict[str, pd.DataFrame],
        n: int,
    ) -> pd.Series:
        """Generate *n* unique new IDs for an entity or relationship type.

        Scans both the live table and the current shadow layer for the
        maximum existing ID, then allocates a contiguous range above it.

        Args:
            type_label: Type name (e.g. ``"Person"`` or ``"KNOWS"``).
            mapping: The mapping dict to look up the source object.
            shadow_dict: The shadow layer dict (``_shadow`` or ``_shadow_rels``).
            n: Number of new IDs to generate.

        Returns:
            A ``pd.Series`` of *n* integer IDs.

        """
        from pycypher.binding_frame import _source_to_pandas

        max_id: int = 0
        if type_label in mapping:
            source_df = _source_to_pandas(mapping[type_label].source_obj)
            max_id = max(max_id, self._max_id_from_df(source_df, type_label, "live"))
        shadow_df = shadow_dict.get(type_label)
        if shadow_df is not None:
            max_id = max(max_id, self._max_id_from_df(shadow_df, type_label, "shadow"))
        start = max_id + 1
        return pd.Series(range(start, start + n), dtype=int)

    def next_entity_ids(self, entity_type: str, n: int) -> pd.Series:
        """Generate *n* unique new IDs for *entity_type*."""
        return self._next_ids(
            entity_type,
            self.context.entity_mapping.mapping,
            self.context._shadow,
            n,
        )

    def next_relationship_ids(self, rel_type: str, n: int) -> pd.Series:
        """Generate *n* unique new IDs for *rel_type*."""
        return self._next_ids(
            rel_type,
            self.context.relationship_mapping.mapping,
            self.context._shadow_rels,
            n,
        )

    # ------------------------------------------------------------------
    # Shadow layer operations
    # ------------------------------------------------------------------

    @staticmethod
    def _get_base_dataframe(
        type_label: str,
        shadow_dict: dict[str, pd.DataFrame],
        mapping: dict[str, Any],
        default_columns: list[str],
    ) -> pd.DataFrame:
        """Resolve the base DataFrame for a shadow-write operation.

        Three-way fallback: shadow layer -> live mapping -> empty DataFrame
        with the given default columns.

        Args:
            type_label: Entity or relationship type label.
            shadow_dict: The shadow layer dict (``_shadow`` or ``_shadow_rels``).
            mapping: The mapping dict to look up the live source object.
            default_columns: Columns for the fallback empty DataFrame.

        Returns:
            A ``pd.DataFrame`` to append new rows to.

        """
        from pycypher.binding_frame import _source_to_pandas

        if type_label in shadow_dict:
            return shadow_dict[type_label]
        if type_label in mapping:
            return _source_to_pandas(mapping[type_label].source_obj)
        return pd.DataFrame(columns=default_columns)

    def shadow_create_entity(
        self,
        entity_type: str,
        new_ids: list[Any],
        props: dict[str, list[Any]],
    ) -> None:
        """Append new rows to the entity shadow layer for *entity_type*.

        If the entity type does not yet exist in the shadow, the method seeds
        it from the live entity table (or an empty DataFrame if the type is
        brand-new).

        Args:
            entity_type: Label of the entity to insert.
            new_ids: List of new integer IDs (one per new row).
            props: Mapping of property name -> list of values.

        """
        base_df = self._get_base_dataframe(
            entity_type,
            self.context._shadow,
            self.context.entity_mapping.mapping,
            [ID_COLUMN],
        )

        new_row: dict[str, list[Any]] = {ID_COLUMN: new_ids}
        new_row.update(props)
        new_df = pd.DataFrame(new_row)

        combined = pd.concat([base_df, new_df], ignore_index=True)
        self.context._shadow[entity_type] = combined
        LOGGER.debug(
            "shadow_create_entity  type=%s  new_rows=%d  total_rows=%d  props=%s",
            entity_type,
            len(new_ids),
            len(combined),
            list(props.keys()),
        )

        if entity_type in self.context.entity_mapping.mapping:
            et = self.context.entity_mapping.mapping[entity_type]
            for col in props:
                if col not in et.attribute_map:
                    et.attribute_map[col] = col
                    et.source_obj_attribute_map[col] = col

    def shadow_create_relationship(
        self,
        rel_type: str,
        new_ids: list[Any],
        src_ids: list[Any],
        tgt_ids: list[Any],
    ) -> None:
        """Append new rows to the relationship shadow layer for *rel_type*.

        Args:
            rel_type: Relationship type label (e.g. ``"KNOWS"``).
            new_ids: New relationship IDs.
            src_ids: Source node IDs for each new relationship.
            tgt_ids: Target node IDs for each new relationship.

        """
        base_df = self._get_base_dataframe(
            rel_type,
            self.context._shadow_rels,
            self.context.relationship_mapping.mapping,
            [ID_COLUMN, RELATIONSHIP_SOURCE_COLUMN, RELATIONSHIP_TARGET_COLUMN],
        )

        new_df = pd.DataFrame(
            {
                ID_COLUMN: new_ids,
                RELATIONSHIP_SOURCE_COLUMN: src_ids,
                RELATIONSHIP_TARGET_COLUMN: tgt_ids,
            },
        )
        combined = pd.concat(
            [base_df, new_df],
            ignore_index=True,
        )
        self.context._shadow_rels[rel_type] = combined
        LOGGER.debug(
            "shadow_create_relationship  type=%s  new_rows=%d  total_rows=%d",
            rel_type,
            len(new_ids),
            len(combined),
        )

    # ------------------------------------------------------------------
    # CREATE clause
    # ------------------------------------------------------------------

    def process_create(
        self,
        clause: Create,
        current_frame: BindingFrame | None,
        *,
        make_seed_frame: Callable[[], BindingFrame],
    ) -> BindingFrame | None:
        """Execute a CREATE clause — insert new entities/relationships.

        For each :class:`~pycypher.ast_models.PatternPath` in the clause
        pattern, node and relationship patterns are evaluated against the
        current frame.  When a preceding ``MATCH`` is present, one new
        entity/relationship row is created **per frame row**.

        Args:
            clause: AST :class:`~pycypher.ast_models.Create` node.
            current_frame: The current binding frame (may be ``None``).
            make_seed_frame: Callable that produces a single-row seed frame.

        Returns:
            An updated :class:`~pycypher.binding_frame.BindingFrame`.

        """
        t0 = time.perf_counter()
        from pycypher.ast_models import (
            NodePattern,
            RelationshipDirection,
            RelationshipPattern,
        )
        from pycypher.binding_evaluator import BindingExpressionEvaluator
        from pycypher.binding_frame import BindingFrame

        if clause.pattern is None:
            return current_frame

        n_rows = (
            len(current_frame.bindings) if current_frame is not None else 1
        )
        LOGGER.debug(
            "mutation CREATE: %d paths, %d rows",
            len(clause.pattern.paths),
            n_rows,
        )

        eval_frame = (
            make_seed_frame() if current_frame is None else current_frame
        )
        evaluator = BindingExpressionEvaluator(eval_frame)

        new_vars: dict[str, pd.Series] = {}
        new_type_reg: dict[str, str] = {}
        node_id_cache: dict[int, pd.Series] = {}

        for path in clause.pattern.paths:
            node_id_cache.clear()

            for pos, element in enumerate(path.elements):
                if isinstance(element, NodePattern):
                    var_name = (
                        element.variable.name if element.variable else None
                    )

                    if (
                        var_name is not None
                        and current_frame is not None
                        and var_name in current_frame.type_registry
                        and var_name in current_frame.bindings.columns
                    ):
                        node_id_cache[pos] = current_frame.bindings[var_name]
                        continue

                    label = element.labels[0] if element.labels else "Node"
                    new_ids = self.next_entity_ids(label, n_rows)

                    props: dict[str, list[Any]] = {}
                    for key, expr in element.properties.items():
                        val_series = evaluator.evaluate(expr)
                        props[key] = val_series.tolist()

                    self.shadow_create_entity(label, new_ids.tolist(), props)

                    node_id_cache[pos] = new_ids
                    if var_name is not None:
                        new_vars[var_name] = new_ids
                        new_type_reg[var_name] = label

            for pos in range(1, len(path.elements) - 1, 2):
                rel = path.elements[pos]
                if not isinstance(rel, RelationshipPattern):
                    continue

                src_pos = pos - 1
                tgt_pos = pos + 1

                if rel.direction == RelationshipDirection.LEFT:
                    src_ids_series = node_id_cache.get(tgt_pos)
                    tgt_ids_series = node_id_cache.get(src_pos)
                else:
                    src_ids_series = node_id_cache.get(src_pos)
                    tgt_ids_series = node_id_cache.get(tgt_pos)

                if src_ids_series is None or tgt_ids_series is None:
                    continue

                rel_type = rel.labels[0] if rel.labels else "REL"
                rel_ids = self.next_relationship_ids(rel_type, n_rows)
                self.shadow_create_relationship(
                    rel_type,
                    rel_ids.tolist(),
                    src_ids_series.tolist(),
                    tgt_ids_series.tolist(),
                )

                rel_var = rel.variable.name if rel.variable else None
                if rel_var is not None:
                    new_vars[rel_var] = rel_ids

        if not new_vars:
            return current_frame

        if current_frame is None:
            new_df = pd.DataFrame({k: v.tolist() for k, v in new_vars.items()})
        else:
            new_df = current_frame.bindings.assign(
                **{
                    k: v.values if hasattr(v, "values") else list(v)
                    for k, v in new_vars.items()
                },
            )

        combined_type_registry = dict(
            current_frame.type_registry if current_frame is not None else {},
        )
        combined_type_registry.update(new_type_reg)

        LOGGER.debug(
            "mutation CREATE: completed in %.3fms  new_vars=%d  types=%s",
            (time.perf_counter() - t0) * 1000,
            len(new_vars),
            list(new_type_reg.values()),
        )
        return BindingFrame(
            bindings=new_df,
            type_registry=combined_type_registry,
            context=self.context,
        )

    # ------------------------------------------------------------------
    # DELETE / DETACH DELETE clause
    # ------------------------------------------------------------------

    def process_delete(
        self,
        clause: Delete,
        frame: BindingFrame,
    ) -> None:
        """Execute a DELETE or DETACH DELETE clause.

        Args:
            clause: AST :class:`~pycypher.ast_models.Delete` node.
            frame: Current :class:`~pycypher.binding_frame.BindingFrame`.

        """
        t0 = time.perf_counter()
        from pycypher.ast_models import Variable
        from pycypher.binding_evaluator import BindingExpressionEvaluator
        from pycypher.binding_frame import _source_to_pandas

        detach = getattr(clause, "detach", False)
        LOGGER.debug(
            "mutation %s: %d expressions, %d rows",
            "DETACH DELETE" if detach else "DELETE",
            len(clause.expressions),
            len(frame),
        )

        evaluator = BindingExpressionEvaluator(frame)

        for expr in clause.expressions:
            id_series = evaluator.evaluate(expr)
            ids_to_delete: set[Any] = set(id_series.dropna().unique())
            if not ids_to_delete:
                continue

            entity_type: str | None = None
            if isinstance(expr, Variable):
                entity_type = frame.type_registry.get(expr.name)

            if entity_type is None:
                var_name = (
                    expr.name if isinstance(expr, Variable) else repr(expr)
                )
                LOGGER.warning(
                    "DELETE skipped: cannot determine entity type for variable '%s'. "
                    "Ensure it was bound by a preceding MATCH clause.",
                    var_name,
                )
                continue

            if entity_type in self.context._shadow:
                base_df: pd.DataFrame = self.context._shadow[entity_type]
            elif entity_type in self.context.entity_mapping.mapping:
                base_df = _source_to_pandas(
                    self.context.entity_mapping.mapping[
                        entity_type
                    ].source_obj,
                )
            else:
                LOGGER.warning(
                    "DELETE skipped: entity type '%s' not found in context or shadow layer.",
                    entity_type,
                )
                continue

            filtered_df = base_df[
                ~base_df[ID_COLUMN].isin(ids_to_delete)
            ].reset_index(drop=True)
            _deleted_count = len(base_df) - len(filtered_df)
            self.context._shadow[entity_type] = filtered_df
            LOGGER.debug(
                "mutation DELETE: entity_type=%s  deleted=%d  remaining=%d",
                entity_type,
                _deleted_count,
                len(filtered_df),
            )

            if clause.detach:
                for (
                    rel_type,
                    rel_table,
                ) in self.context.relationship_mapping.mapping.items():
                    if rel_type in self.context._shadow_rels:
                        rel_df: pd.DataFrame = self.context._shadow_rels[
                            rel_type
                        ]
                    else:
                        rel_df = _source_to_pandas(rel_table.source_obj)

                    dead_mask = rel_df[RELATIONSHIP_SOURCE_COLUMN].isin(
                        ids_to_delete,
                    ) | rel_df[RELATIONSHIP_TARGET_COLUMN].isin(ids_to_delete)
                    detached_count = int(dead_mask.sum())
                    self.context._shadow_rels[rel_type] = rel_df[
                        ~dead_mask
                    ].reset_index(drop=True)
                    if detached_count > 0:
                        LOGGER.debug(
                            "mutation DETACH DELETE: rel_type=%s  detached=%d",
                            rel_type,
                            detached_count,
                        )

        LOGGER.debug(
            "mutation DELETE: completed in %.3fms",
            (time.perf_counter() - t0) * 1000,
        )

    # ------------------------------------------------------------------
    # MERGE clause
    # ------------------------------------------------------------------

    def process_merge(
        self,
        clause: Merge,
        current_frame: BindingFrame | None,
        *,
        match_to_binding_frame: Callable[..., BindingFrame],
        merge_frames_for_match: Callable[[BindingFrame, BindingFrame], BindingFrame],
        make_seed_frame: Callable[[], BindingFrame],
    ) -> BindingFrame | None:
        """Execute a MERGE clause — match existing or create new.

        Args:
            clause: AST :class:`~pycypher.ast_models.Merge` node.
            current_frame: The current binding frame.
            match_to_binding_frame: Callback to Star._match_to_binding_frame.
            merge_frames_for_match: Callback to Star._merge_frames_for_match.
            make_seed_frame: Callback to Star._make_seed_frame.

        Returns:
            An updated :class:`~pycypher.binding_frame.BindingFrame`.

        """
        t0 = time.perf_counter()
        from pycypher.ast_models import Create, Match
        from pycypher.exceptions import GraphTypeNotFoundError

        LOGGER.debug("mutation MERGE: pattern=%s", clause.pattern)

        if clause.pattern is None:
            return current_frame

        synthetic_match = Match(
            pattern=clause.pattern,
            where=None,
            optional=False,
        )
        match_frame = None
        try:
            candidate = match_to_binding_frame(
                synthetic_match,
                context_frame=current_frame,
            )
            if len(candidate.bindings) > 0:
                match_frame = candidate
        except GraphTypeNotFoundError:
            LOGGER.debug(
                "MERGE: entity type not found, will create",
                exc_info=True,
            )

        if match_frame is not None:
            LOGGER.debug(
                "mutation MERGE: matched %d existing rows  on_match=%s",
                len(match_frame.bindings),
                bool(clause.on_match),
            )
            if clause.on_match:
                from pycypher.ast_models import Set as SetClause

                self.set_properties(
                    SetClause(items=clause.on_match),
                    match_frame,
                )
            if current_frame is None:
                return match_frame
            return merge_frames_for_match(current_frame, match_frame)

        LOGGER.debug("mutation MERGE: no match found, creating new entities")
        synthetic_create = Create(pattern=clause.pattern)
        created_frame = self.process_create(
            synthetic_create,
            current_frame,
            make_seed_frame=make_seed_frame,
        )
        if clause.on_create and created_frame is not None:
            from pycypher.ast_models import Set as SetClause

            self.set_properties(
                SetClause(items=clause.on_create),
                created_frame,
            )
        LOGGER.debug(
            "mutation MERGE: completed in %.3fms",
            (time.perf_counter() - t0) * 1000,
        )
        return created_frame

    # ------------------------------------------------------------------
    # FOREACH clause
    # ------------------------------------------------------------------

    def process_foreach(
        self,
        clause: Foreach,
        current_frame: BindingFrame | None,
        *,
        make_seed_frame: Callable[[], BindingFrame],
    ) -> BindingFrame | None:
        """Execute a FOREACH clause — iterative mutation over a list.

        Args:
            clause: AST :class:`~pycypher.ast_models.Foreach` node.
            current_frame: The current binding frame.
            make_seed_frame: Callback to Star._make_seed_frame.

        Returns:
            *current_frame* unchanged.

        """
        t0 = time.perf_counter()
        from pycypher.ast_models import Create, Delete, Merge, Remove, Set
        from pycypher.binding_evaluator import BindingExpressionEvaluator
        from pycypher.binding_frame import BindingFrame

        n_clauses = len(clause.clauses) if clause.clauses else 0
        LOGGER.debug("mutation FOREACH: %d inner clauses", n_clauses)

        if clause.list_expression is None:
            return current_frame

        var_name: str = clause.variable or "_foreach_var"

        def _execute_inner_clauses(
            loop_frame: BindingFrame,
            *,
            iteration_index: int = 0,
            element_value: Any = None,
        ) -> None:
            """Run each inner update clause against *loop_frame* in-place."""
            for clause_idx, inner in enumerate(clause.clauses):
                try:
                    if isinstance(inner, Create):
                        self.process_create(
                            inner,
                            loop_frame,
                            make_seed_frame=make_seed_frame,
                        )
                    elif isinstance(inner, Set):
                        self.set_properties(inner, loop_frame)
                    elif isinstance(inner, Merge):
                        # MERGE inside FOREACH requires pattern matching callbacks,
                        # which are not available here. Fall back to CREATE semantics.
                        self.process_create(
                            Create(pattern=inner.pattern),
                            loop_frame,
                            make_seed_frame=make_seed_frame,
                        )
                    elif isinstance(inner, Delete):
                        self.process_delete(inner, loop_frame)
                    elif isinstance(inner, Remove):
                        self.remove_properties(inner, loop_frame)
                    else:
                        from pycypher.ast_models import Foreach as _Foreach

                        if isinstance(inner, _Foreach):
                            self.process_foreach(
                                inner,
                                loop_frame,
                                make_seed_frame=make_seed_frame,
                            )
                except Exception as exc:  # noqa: BLE001
                    clause_type = type(inner).__name__
                    msg = (
                        f"FOREACH failed at iteration {iteration_index} "
                        f"(variable '{var_name}' = {element_value!r}), "
                        f"inner clause #{clause_idx} ({clause_type}): {exc}"
                    )
                    raise type(exc)(msg) from exc

        inferred_loop_type: str | None = None
        if current_frame is not None:
            from pycypher.ast_models import ListLiteral
            from pycypher.ast_models import Variable as _Variable

            list_expr = clause.list_expression
            if isinstance(list_expr, ListLiteral) and list_expr.elements:
                types: set[str] = set()
                for elem in list_expr.elements:
                    if isinstance(elem, _Variable):
                        et = current_frame.type_registry.get(elem.name)
                        if et:
                            types.add(et)
                if len(types) == 1:
                    inferred_loop_type = next(iter(types))

        if current_frame is None:
            synthetic_frame = make_seed_frame()
            evaluator = BindingExpressionEvaluator(frame=synthetic_frame)
            list_series = evaluator.evaluate(clause.list_expression)
            raw_list = list_series.iloc[0] if len(list_series) > 0 else []
            if raw_list is None:
                raw_list = []
            LOGGER.debug(
                "mutation FOREACH: var=%s  iterations=%d  clauses=%d",
                var_name,
                len(raw_list),
                n_clauses,
            )
            for elem_idx, element in enumerate(raw_list):
                loop_frame = BindingFrame(
                    bindings=pd.DataFrame({var_name: [element]}),
                    type_registry={},
                    context=self.context,
                )
                _execute_inner_clauses(
                    loop_frame,
                    iteration_index=elem_idx,
                    element_value=element,
                )
        else:
            evaluator = BindingExpressionEvaluator(frame=current_frame)
            list_series = evaluator.evaluate(clause.list_expression)

            # Pre-compute rows as dicts and list values to avoid
            # repeated .iloc[] access inside the loop (perf: O(1) dict
            # lookup vs O(n) pandas index machinery per access).
            _outer_rows = current_frame.bindings.to_dict("records")
            _list_values = list_series.tolist()
            # Build the loop type registry once (immutable across rows).
            loop_type_registry = dict(current_frame.type_registry)
            if inferred_loop_type is not None:
                loop_type_registry[var_name] = inferred_loop_type

            for row_idx, outer_row in enumerate(_outer_rows):
                raw_list = (
                    _list_values[row_idx]
                    if row_idx < len(_list_values)
                    else []
                )
                if raw_list is None:
                    raw_list = []
                for elem_idx, element in enumerate(raw_list):
                    row_bindings = {**outer_row, var_name: element}
                    loop_frame = BindingFrame(
                        bindings=pd.DataFrame(
                            {k: [v] for k, v in row_bindings.items()},
                        ),
                        type_registry=loop_type_registry,
                        context=self.context,
                    )
                    _execute_inner_clauses(
                        loop_frame,
                        iteration_index=elem_idx,
                        element_value=element,
                    )

        LOGGER.debug(
            "mutation FOREACH: completed in %.3fms",
            (time.perf_counter() - t0) * 1000,
        )
        return current_frame

    # ------------------------------------------------------------------
    # REMOVE clause
    # ------------------------------------------------------------------

    def remove_properties(
        self,
        remove_clause: Remove,
        frame: BindingFrame,
    ) -> None:
        """Apply a REMOVE clause against a BindingFrame.

        Args:
            remove_clause: AST :class:`~pycypher.ast_models.Remove` node.
            frame: Current :class:`~pycypher.binding_frame.BindingFrame`.

        """
        t0 = time.perf_counter()
        LOGGER.debug(
            "mutation REMOVE: %d items, %d rows",
            len(remove_clause.items),
            len(frame),
        )
        for item in remove_clause.items:
            if item.property is not None:
                null_values = _null_series(len(frame))
                frame.mutate(item.variable.name, item.property, null_values)
        LOGGER.debug(
            "mutation REMOVE: completed in %.3fms",
            (time.perf_counter() - t0) * 1000,
        )

    # ------------------------------------------------------------------
    # CALL clause
    # ------------------------------------------------------------------

    def process_call(
        self,
        clause: Call,
        current_frame: BindingFrame | None,
    ) -> BindingFrame | None:
        """Execute a CALL clause — invoke a registered procedure and YIELD results.

        Args:
            clause: AST :class:`~pycypher.ast_models.Call` node.
            current_frame: The current binding frame.

        Returns:
            An updated :class:`~pycypher.binding_frame.BindingFrame` with the
            YIELDed columns.

        """
        from pycypher.binding_evaluator import BindingExpressionEvaluator
        from pycypher.binding_frame import BindingFrame
        from pycypher.relational_models import PROCEDURE_REGISTRY

        if clause.procedure_name is None:
            return current_frame

        t0 = time.perf_counter()
        n_args = len(clause.arguments) if clause.arguments else 0
        LOGGER.debug(
            "mutation CALL: procedure=%s  args=%d",
            clause.procedure_name,
            n_args,
        )

        args: list[Any] = []
        if clause.arguments and current_frame is not None:
            evaluator = BindingExpressionEvaluator(current_frame)
            for arg_expr in clause.arguments:
                series = evaluator.evaluate(arg_expr)
                args.append(series.iloc[0] if len(series) > 0 else None)

        rows: list[dict[str, Any]] = PROCEDURE_REGISTRY.execute(
            clause.procedure_name,
            self.context,
            args,
        )

        LOGGER.debug(
            "mutation CALL: procedure=%s  result_rows=%d  elapsed=%.3fms",
            clause.procedure_name,
            len(rows),
            (time.perf_counter() - t0) * 1000,
        )

        if not clause.yield_items:
            return current_frame

        if not rows:
            columns: dict[str, list[Any]] = {}
            for item in clause.yield_items:
                var = item.variable
                source_key = var.name if var is not None else None
                alias = item.alias or source_key
                if alias:
                    columns[alias] = []
            proc_df = pd.DataFrame(columns)
        else:
            columns = {}
            for item in clause.yield_items:
                var = item.variable
                source_key = var.name if var is not None else None
                alias = item.alias or source_key
                if alias is None:
                    continue
                columns[alias] = [row.get(source_key, None) for row in rows]
            proc_df = pd.DataFrame(columns)

        return BindingFrame(
            bindings=proc_df,
            type_registry={},
            context=self.context,
        )
