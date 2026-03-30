"""Pattern matching engine for translating Cypher MATCH patterns to BindingFrames.

Extracted from :class:`~pycypher.star.Star` to reduce the god object.
The ``PatternMatcher`` handles all pattern-related operations:

1. **Node scanning** — scan a NodePattern into a BindingFrame.
2. **Pattern path traversal** — translate a PatternPath into a BindingFrame
   via scans, joins, and BFS expansion.
3. **MATCH clause translation** — combine multiple pattern paths and apply
   WHERE predicates.

Architecture
------------

::

    PatternMatcher
    ├── node_pattern_to_binding_frame()  — single node scan
    ├── pattern_path_to_binding_frame()  — full path traversal
    └── match_to_binding_frame()  — MATCH clause coordinator
        └── delegates to PathExpander for variable-length paths
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pandas as pd
from shared.logger import LOGGER

from pycypher.ast_models import (
    NodePattern,
    PatternPath,
    RelationshipDirection,
    RelationshipPattern,
)
from pycypher.binding_frame import PATH_HOP_COLUMN_PREFIX, BindingFrame
from pycypher.path_expander import PathExpander

if TYPE_CHECKING:
    from pycypher.relational_models import Context

#: Synthetic variable name prefix for anonymous nodes.
_ANON_NODE_PREFIX: str = "_anon_node_"

#: Synthetic variable name prefix for anonymous relationships.
_ANON_REL_PREFIX: str = "_anon_rel_"


@dataclass(frozen=True, slots=True)
class VariableLengthHopParams:
    """Grouped parameters for :meth:`PatternMatcher._expand_variable_length_hop`.

    Bundles the 11 positional arguments into a single object with logical
    groupings, improving call-site readability and making the method signature
    self-documenting.
    """

    # Current binding state
    frame: BindingFrame

    # Relationship pattern metadata
    rel_ast: RelationshipPattern
    scan_rel_types: list[str]
    rel_var: str

    # Node binding endpoints
    prev_var: str
    next_var: str
    direction: RelationshipDirection

    # Optional configuration
    next_type: str | None = None
    path_var_name: str | None = None
    row_limit: int | None = None
    anon_counter: list[int] = field(default_factory=lambda: [0])


class PatternMatcher:
    """Translates Cypher MATCH patterns into BindingFrame instances.

    Delegates BFS-based operations to :class:`~pycypher.path_expander.PathExpander`.

    Algorithm overview
    ~~~~~~~~~~~~~~~~~~

    For a Cypher pattern like ``MATCH (a:Person)-[:KNOWS]->(b:Person)``:

    1. **Node scan** — ``node_pattern_to_binding_frame()`` scans each node
       pattern into a single-column BindingFrame (e.g. column ``"a"`` with
       all Person IDs).
    2. **Relationship join** — for each relationship in the path, the
       relationship table is joined on source/target IDs to connect adjacent
       node frames.
    3. **Multi-path merge** — when a MATCH has multiple comma-separated
       patterns, each is translated independently and then joined on shared
       variable names.
    4. **Predicate pushdown** — WHERE predicates referencing only one path
       are pushed down *before* the cross-path join to reduce row counts.

    For variable-length paths (``[*1..3]``), step 2 delegates to
    :class:`~pycypher.path_expander.PathExpander` for BFS expansion.

    Args:
        context: The execution context with entity/relationship tables.
        path_expander: PathExpander instance for variable-length paths.
        coerce_join_fn: Callable for joining two BindingFrames (inner or cross join).
        apply_where_fn: Callable for applying a WHERE predicate to a BindingFrame.

    """

    def __init__(
        self,
        context: Context,
        path_expander: PathExpander,
        coerce_join_fn: Any,
        apply_where_fn: Any,
    ) -> None:
        """Initialize pattern matcher.

        Args:
            context: Execution context with entity/relationship tables.
            path_expander: PathExpander for variable-length path BFS.
            coerce_join_fn: Callable ``(left, right) -> BindingFrame`` for
                joining two frames (inner or cross join).
            apply_where_fn: Callable ``(frame, predicate) -> BindingFrame``
                for applying a WHERE predicate to a frame.
        """
        self.context = context
        self._path_expander = path_expander
        self._coerce_join = coerce_join_fn
        self._apply_where_filter = apply_where_fn

    def node_pattern_to_binding_frame(
        self,
        node: NodePattern,
        anon_counter: list[int],
        context_frame: BindingFrame | None = None,
    ) -> BindingFrame:
        """Scan a single NodePattern into a BindingFrame.

        Assigns a synthetic variable name for anonymous nodes. Applies inline
        property equality filters.

        Args:
            node: AST NodePattern to scan.
            anon_counter: Mutable counter for synthetic variable names.
            context_frame: Optional preceding BindingFrame for unlabelled
                node variable resolution.

        Returns:
            A BindingFrame with one column per node.

        """
        from pycypher.ast_models import Comparison, PropertyLookup
        from pycypher.ast_models import Variable as _Var
        from pycypher.binding_frame import (
            BindingFilter,
            BindingFrame,
            EntityScan,
        )

        if node.variable is not None:
            var_name = node.variable.name
        else:
            var_name = f"{_ANON_NODE_PREFIX}{anon_counter[0]}"
            anon_counter[0] += 1

        # --- Extract pushable property predicates from inline {prop: val} ---
        # Only literal equality predicates can be pushed into EntityScan.
        from pycypher.ast_models import Literal as _Literal

        _pushable: dict[str, Any] = {}
        _remaining_props: dict[str, Any] = {}
        for prop_name, prop_val in (node.properties or {}).items():
            if isinstance(prop_val, _Literal) and not isinstance(
                prop_val, (type(None),)
            ):
                _pushable[prop_name] = prop_val.value
            else:
                _remaining_props[prop_name] = prop_val

        if not node.labels:
            if (
                context_frame is not None
                and var_name in context_frame.type_registry
            ):
                entity_type = context_frame.type_registry[var_name]
                frame = EntityScan(entity_type, var_name).scan(self.context)
            else:
                _scan_t0 = time.perf_counter()
                _n_types = len(self.context.entity_mapping.mapping)
                all_frames = [
                    EntityScan(etype, var_name).scan(self.context)
                    for etype in self.context.entity_mapping.mapping
                ]
                LOGGER.debug(
                    f"multi-type scan  var={var_name}  types={_n_types}  elapsed={time.perf_counter() - _scan_t0:.3f}s",
                )
                if not all_frames:
                    from pycypher.exceptions import GraphTypeNotFoundError

                    raise GraphTypeNotFoundError(
                        "",
                        "Cannot match unlabeled node pattern: no entity types "
                        "are registered in the context. Add entity types via "
                        "ContextBuilder.add_entity(), or use a labelled node "
                        "pattern such as MATCH (n:Person).",
                    )
                if len(all_frames) == 1:
                    frame = all_frames[0]
                else:
                    _be = getattr(self.context, "backend", None)
                    if _be is not None:
                        combined = _be.concat(
                            [f.bindings for f in all_frames],
                            ignore_index=True,
                        )
                    else:
                        combined = pd.concat(
                            [f.bindings for f in all_frames],
                            ignore_index=True,
                        )
                    frame = BindingFrame(
                        bindings=combined,
                        type_registry={var_name: "__MULTI__"},
                        context=self.context,
                    )
        else:
            entity_type = node.labels[0]
            frame = EntityScan(entity_type, var_name).scan(
                self.context,
                property_filters=_pushable or None,
            )

        # Apply remaining inline property filters not pushed down
        for prop_name, prop_val in _remaining_props.items():
            predicate = Comparison(
                operator="=",
                left=PropertyLookup(
                    expression=_Var(name=var_name),
                    property=prop_name,
                ),
                right=prop_val,
            )
            frame = BindingFilter(predicate=predicate).apply(frame)
        # For labelled nodes with pushdown, the pushed predicates have already
        # filtered at scan time — no need to re-apply them.

        return frame

    def pattern_path_to_binding_frame(
        self,
        path: PatternPath,
        anon_counter: list[int],
        context_frame: BindingFrame | None = None,
        row_limit: int | None = None,
    ) -> BindingFrame:
        """Translate a PatternPath into a BindingFrame via scans and joins.

        Walks the path elements (alternating NodePattern / RelationshipPattern)
        and builds the frame incrementally.

        Args:
            path: AST PatternPath.
            anon_counter: Mutable counter for synthetic names.
            context_frame: Optional preceding BindingFrame.
            row_limit: If given, forward to variable-length path expansion.

        Returns:
            A BindingFrame for this path.

        """
        from pycypher.binding_frame import BindingFrame

        elements = path.elements
        if not elements:
            from pycypher.exceptions import PatternComprehensionError

            raise PatternComprehensionError(
                "MATCH pattern path is empty — expected at least one node "
                "pattern. Use e.g. MATCH (n:Person) or MATCH (a)-[:KNOWS]->(b)."
            )

        path_var_name: str | None = (
            path.variable.name if path.variable is not None else None
        )

        # Route shortest-path patterns to the BFS shortest-path implementation
        if path.shortest_path_mode != "none":
            return self._path_expander.shortest_path_to_binding_frame(
                path=path,
                anon_counter=anon_counter,
                context_frame=context_frame,
                node_scanner=self.node_pattern_to_binding_frame,
            )

        # Start with first node
        first_node = elements[0]
        assert isinstance(first_node, NodePattern)
        frame = self.node_pattern_to_binding_frame(
            first_node,
            anon_counter,
            context_frame=context_frame,
        )
        prev_var = frame.var_names[0]

        i = 1
        while i + 1 <= len(elements) - 1:
            rel_ast = elements[i]  # RelationshipPattern
            assert isinstance(rel_ast, RelationshipPattern)
            node_ast = elements[i + 1]  # NodePattern
            assert isinstance(node_ast, NodePattern)
            i += 2

            # Assign variable to relationship
            if rel_ast.variable is not None:
                rel_var = rel_ast.variable.name
            else:
                rel_var = f"{_ANON_REL_PREFIX}{anon_counter[0]}"
                anon_counter[0] += 1

            # Determine which relationship types to scan.
            if rel_ast.labels:
                scan_rel_types = list(rel_ast.labels)
            else:
                scan_rel_types = list(
                    self.context.relationship_mapping.mapping.keys(),
                )
                if not scan_rel_types:
                    from pycypher.exceptions import GraphTypeNotFoundError

                    raise GraphTypeNotFoundError(
                        "",
                        f"Relationship variable '{rel_var}' has no type label and the "
                        f"context contains no relationship tables. "
                        f"Add relationships via ContextBuilder.add_relationship(), "
                        f"or use a typed pattern such as -[:KNOWS]->.",
                    )

            # Assign variable to next node
            if node_ast.variable is not None:
                next_var = node_ast.variable.name
            else:
                next_var = f"{_ANON_NODE_PREFIX}{anon_counter[0]}"
                anon_counter[0] += 1

            next_type = node_ast.labels[0] if node_ast.labels else None
            direction = rel_ast.direction

            if rel_ast.length is not None:
                frame = self._expand_variable_length_hop(
                    VariableLengthHopParams(
                        frame=frame,
                        rel_ast=rel_ast,
                        scan_rel_types=scan_rel_types,
                        rel_var=rel_var,
                        prev_var=prev_var,
                        next_var=next_var,
                        next_type=next_type,
                        direction=direction,
                        path_var_name=path_var_name,
                        row_limit=row_limit,
                        anon_counter=anon_counter,
                    ),
                )
            elif next_var in frame.var_names:
                frame = self._join_cyclic_back_reference(
                    frame, scan_rel_types, rel_var,
                    prev_var, next_var, direction,
                )
            else:
                frame = self._traverse_fixed_hop(
                    frame, node_ast, scan_rel_types,
                    rel_var, prev_var, next_var, next_type, direction,
                )

            prev_var = next_var

        # Path variable hop-count column for fixed-length paths
        if path_var_name is not None:
            hop_col = f"{PATH_HOP_COLUMN_PREFIX}{path_var_name}"
            if hop_col not in frame.bindings.columns:
                fixed_hops = (len(elements) - 1) // 2
                new_bindings = frame.bindings.assign(**{hop_col: fixed_hops})
                # type_registry shared — hop count column is internal metadata
                # not tracked in the registry.
                frame = BindingFrame(
                    bindings=new_bindings.reset_index(drop=True),
                    type_registry=frame.type_registry,
                    context=frame.context,
                )

        return frame

    # ------------------------------------------------------------------
    # Private helpers for pattern_path_to_binding_frame
    # ------------------------------------------------------------------

    def _expand_variable_length_hop(
        self,
        params: VariableLengthHopParams,
    ) -> BindingFrame:
        """Expand a variable-length relationship pattern via BFS delegation.

        Args:
            params: Grouped parameters for the hop expansion.  See
                :class:`VariableLengthHopParams` for field documentation.

        Returns:
            Updated BindingFrame after BFS expansion.

        """
        if len(params.scan_rel_types) != 1:
            from pycypher.exceptions import PatternComprehensionError

            raise PatternComprehensionError(
                "Variable-length relationship patterns [*m..n] require "
                "exactly one relationship type. "
                "Use e.g. -[:KNOWS*1..3]-> instead of -[*1..3]->"
            )
        rel_type = params.scan_rel_types[0]
        if rel_type not in self.context.relationship_mapping.mapping:
            from pycypher.exceptions import GraphTypeNotFoundError

            available = sorted(
                self.context.relationship_mapping.mapping.keys(),
            )
            raise GraphTypeNotFoundError(
                rel_type,
                f"Relationship type {rel_type!r} is not registered in the context. "
                f"Available relationship types: {available or []}. "
                f"Check your variable-length path pattern [:{rel_type}*].",
            )
        path_len = params.rel_ast.length
        assert path_len is not None
        min_hops = path_len.min if path_len.min is not None else 1
        max_hops = path_len.max if not path_len.unbounded else None

        path_length_col: str | None = None
        if params.path_var_name is not None:
            path_length_col = f"{PATH_HOP_COLUMN_PREFIX}{params.path_var_name}"

        return self._path_expander.expand_variable_length_path(
            start_frame=params.frame,
            start_var=params.prev_var,
            rel_type=rel_type,
            direction=params.direction,
            end_var=params.next_var,
            end_type=params.next_type,
            min_hops=min_hops,
            max_hops=max_hops,
            anon_counter=params.anon_counter,
            path_length_col=path_length_col,
            row_limit=params.row_limit,
        )

    def _join_cyclic_back_reference(
        self,
        frame: BindingFrame,
        scan_rel_types: list[str],
        rel_var: str,
        prev_var: str,
        next_var: str,
        direction: RelationshipDirection,
    ) -> BindingFrame:
        """Handle cyclic back-reference where next_var already exists in the frame.

        Joins the relationship table and filters rows where the endpoint
        matches the existing variable binding.

        Args:
            frame: Current BindingFrame.
            scan_rel_types: Relationship types to scan.
            rel_var: Variable name for the relationship.
            prev_var: Variable name of the preceding node.
            next_var: Variable name of the already-bound node.
            direction: Relationship traversal direction.

        Returns:
            Filtered BindingFrame with cyclic constraint applied.

        """
        from pycypher.binding_frame import BindingFrame as _BF
        from pycypher.binding_frame import RelationshipScan

        if len(scan_rel_types) != 1:
            from pycypher.exceptions import PatternComprehensionError

            raise PatternComprehensionError(
                "Cyclic back-reference patterns require exactly one "
                "relationship type. Use e.g. (a)-[:KNOWS]->(a) "
                "instead of (a)-[]->(a)"
            )
        rel_type = scan_rel_types[0]
        rs = RelationshipScan(rel_type, rel_var)
        src_push = frame.bindings.get(prev_var)
        tgt_push = frame.bindings.get(next_var)
        if direction == RelationshipDirection.LEFT:
            rel_frame = rs.scan(
                self.context, source_ids=tgt_push, target_ids=src_push,
            )
        else:
            rel_frame = rs.scan(
                self.context, source_ids=src_push, target_ids=tgt_push,
            )

        if direction == RelationshipDirection.LEFT:
            frame = frame.join(rel_frame, prev_var, rs.tgt_col)
            endpoint_col = rs.src_col
        else:
            frame = frame.join(rel_frame, prev_var, rs.src_col)
            endpoint_col = rs.tgt_col

        mask = frame.bindings[endpoint_col] == frame.bindings[next_var]
        frame = frame.filter(mask)
        new_bindings = frame.bindings.drop(columns=[endpoint_col])
        return _BF(
            bindings=new_bindings.reset_index(drop=True),
            type_registry=frame.type_registry,
            context=frame.context,
        )

    def _traverse_fixed_hop(
        self,
        frame: BindingFrame,
        node_ast: NodePattern,
        scan_rel_types: list[str],
        rel_var: str,
        prev_var: str,
        next_var: str,
        next_type: str | None,
        direction: RelationshipDirection,
    ) -> BindingFrame:
        """Traverse a normal fixed-hop relationship step.

        Scans relationship tables, joins with the current frame, renames
        endpoint columns, and applies inline property filters.

        Args:
            frame: Current BindingFrame.
            node_ast: Next NodePattern AST node.
            scan_rel_types: Relationship types to scan.
            rel_var: Variable name for the relationship.
            prev_var: Variable name of the preceding node.
            next_var: Variable name of the next node.
            next_type: Label of the next node, or ``None``.
            direction: Relationship traversal direction.

        Returns:
            Updated BindingFrame after the fixed-hop join.

        """
        from pycypher.ast_models import Comparison, PropertyLookup
        from pycypher.ast_models import Variable as _Var
        from pycypher.binding_frame import (
            BindingFilter,
            RelationshipScan,
        )
        from pycypher.binding_frame import (
            BindingFrame as _BF,
        )
        directions_to_try: list[RelationshipDirection] = (
            [RelationshipDirection.RIGHT, RelationshipDirection.LEFT]
            if direction == RelationshipDirection.UNDIRECTED
            else [direction]
        )

        hop_frames: list[BindingFrame] = []
        pushdown_ids: pd.Series | None = None
        if prev_var in frame.bindings.columns and len(frame.bindings) > 0:
            pushdown_ids = frame.bindings[prev_var]
        for rt in scan_rel_types:
            for d in directions_to_try:
                rs = RelationshipScan(rt, rel_var)
                if pushdown_ids is not None:
                    if d == RelationshipDirection.LEFT:
                        rel_frame = rs.scan(
                            self.context, target_ids=pushdown_ids,
                        )
                    else:
                        rel_frame = rs.scan(
                            self.context, source_ids=pushdown_ids,
                        )
                else:
                    rel_frame = rs.scan(self.context)

                if d == RelationshipDirection.LEFT:
                    hop_f = frame.join(rel_frame, prev_var, rs.tgt_col)
                    hop_f = hop_f.rename(
                        rs.src_col, next_var, new_type=next_type,
                    )
                else:
                    hop_f = frame.join(rel_frame, prev_var, rs.src_col)
                    hop_f = hop_f.rename(
                        rs.tgt_col, next_var, new_type=next_type,
                    )

                # Apply inline property filters for next node.
                for prop_name, prop_val in (
                    node_ast.properties or {}
                ).items():
                    predicate = Comparison(
                        operator="=",
                        left=PropertyLookup(
                            expression=_Var(name=next_var),
                            property=prop_name,
                        ),
                        right=prop_val,
                    )
                    hop_f = BindingFilter(predicate=predicate).apply(
                        hop_f,
                    )

                hop_frames.append(hop_f)

        if len(hop_frames) == 1:
            return hop_frames[0]

        _be = getattr(self.context, "backend", None)
        if _be is not None:
            all_bindings = _be.concat(
                [f.bindings for f in hop_frames], ignore_index=True,
            )
            all_bindings = _be.distinct(all_bindings)
        else:
            all_bindings = pd.concat(
                [f.bindings for f in hop_frames], ignore_index=True,
            )
            all_bindings = all_bindings.drop_duplicates().reset_index(drop=True)
        combined_registry: dict[str, str] = {}
        for hop_f in hop_frames:
            combined_registry.update(hop_f.type_registry)
        return _BF(
            bindings=all_bindings,
            type_registry=combined_registry,
            context=self.context,
        )

    def match_to_binding_frame(
        self,
        match_clause: Any,
        context_frame: BindingFrame | None = None,
        row_limit: int | None = None,
    ) -> BindingFrame:
        """Translate a MATCH clause to a BindingFrame.

        Translates each PatternPath separately, then joins them on shared
        variable names. Applies the optional WHERE predicate.

        **Predicate pushdown**: when the MATCH has multiple pattern paths and
        a WHERE clause, the predicate is analysed to determine if it only
        references variables from a single path.  If so, the filter is applied
        to that path's frame *before* the join, reducing the join's input
        size.  Cross-path predicates are applied after the join as before.

        Args:
            match_clause: AST Match node.
            context_frame: Optional preceding BindingFrame.
            row_limit: If given, forward to variable-length path expansion.

        Returns:
            A BindingFrame satisfying the MATCH pattern.

        """
        anon_counter: list[int] = [0]
        pattern = match_clause.pattern

        frames = [
            self.pattern_path_to_binding_frame(
                path,
                anon_counter,
                context_frame=context_frame,
                row_limit=row_limit,
            )
            for path in pattern.paths
        ]

        if not frames:
            from pycypher.exceptions import PatternComprehensionError

            raise PatternComprehensionError(
                "MATCH clause contains no pattern paths. "
                "Provide at least one pattern, e.g. MATCH (n:Person)."
            )

        # --- Predicate pushdown for multi-path MATCH ---
        # When WHERE references variables from only one path, apply the
        # filter before joining to reduce intermediate frame size.
        where_applied = False
        if (
            match_clause.where is not None
            and context_frame is None
            and len(frames) > 1
        ):
            from pycypher.lazy_eval import _extract_variables_from_predicate

            where_vars = _extract_variables_from_predicate(match_clause.where)
            if where_vars:
                for i, frame in enumerate(frames):
                    frame_vars = set(frame.var_names)
                    if where_vars.issubset(frame_vars):
                        # All WHERE variables belong to this frame — push down
                        LOGGER.debug(
                            "predicate pushdown: WHERE applied to path %d "
                            "before join (vars=%s)",
                            i,
                            where_vars,
                        )
                        frames[i] = self._apply_where_filter(
                            match_clause.where,
                            frame,
                        )
                        where_applied = True
                        break

        result = frames[0]
        for frame in frames[1:]:
            result = self._coerce_join(result, frame)

        if (
            match_clause.where is not None
            and context_frame is None
            and not where_applied
        ):
            result = self._apply_where_filter(match_clause.where, result)

        return result
