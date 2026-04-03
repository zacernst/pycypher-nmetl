"""FrameJoiner — frame merging and optional-match join orchestration.

Extracted from :mod:`pycypher.star` to isolate the cohesive join
orchestration family (optional match, frame merging, coerce-join) into
a focused, independently testable module.

Handles:

- OPTIONAL MATCH left-join semantics with null-column injection on failure
- Multi-MATCH frame merging via keyed or cross joins
- Join strategy selection (shared variable → inner join, else cross join)
- Seed frame creation for clause-first queries
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import pandas as pd
from shared.logger import LOGGER

if TYPE_CHECKING:
    from pycypher.ast_models import ASTNode, Match
    from pycypher.binding_frame import BindingFrame
    from pycypher.query_planner import JoinPlan
    from pycypher.relational_models import Context


class FrameJoiner:
    """Orchestrates BindingFrame joins for MATCH clause processing.

    Args:
        context: The query execution context.
        match_fn: Callback to execute a MATCH clause against a frame.
        where_fn: Callback to apply a WHERE filter to a frame.

    """

    def __init__(
        self,
        context: Context,
        match_fn: Callable[..., BindingFrame],
        where_fn: Callable[..., BindingFrame],
    ) -> None:
        """Initialise the frame joiner.

        Args:
            context: The query execution context providing entity and
                relationship tables.
            match_fn: Callback to execute a MATCH clause against a
                :class:`BindingFrame`.
            where_fn: Callback to apply a WHERE filter to a
                :class:`BindingFrame`.

        """
        self._context = context
        self._match_fn = match_fn
        self._where_fn = where_fn
        self._precomputed_join_plans: deque[JoinPlan] = deque()

    def set_join_plans(self, plans: list[JoinPlan]) -> None:
        """Store pre-computed join plans from QueryPlanAnalyzer.

        Plans are consumed in FIFO order by :meth:`coerce_join`.
        """
        self._precomputed_join_plans = deque(plans)

    def coerce_join(
        self,
        frame_a: BindingFrame,
        frame_b: BindingFrame,
        *,
        join_plan: object | None = None,
    ) -> BindingFrame:
        """Join two BindingFrame objects.

        Selects the join strategy based on shared variables:

        * **Shared variable**: keyed inner-join on the first shared column name.
        * **No shared variable**: Cartesian cross-join.

        Args:
            frame_a: The left :class:`~pycypher.binding_frame.BindingFrame`.
            frame_b: The right :class:`~pycypher.binding_frame.BindingFrame`.
            join_plan: Optional pre-computed :class:`JoinPlan` from
                :class:`QueryPlanAnalyzer` to avoid redundant planning.

        Returns:
            A merged :class:`~pycypher.binding_frame.BindingFrame`.

        """
        # Consume pre-computed plan from the queue if no explicit plan given.
        if join_plan is None and self._precomputed_join_plans:
            join_plan = self._precomputed_join_plans.popleft()

        common_vars = set(frame_a.var_names) & set(frame_b.var_names)
        if common_vars:
            var = next(iter(common_vars))
            return frame_a.join(frame_b, var, var, join_plan=join_plan)
        return frame_a.cross_join(frame_b)

    def multi_way_join(self, frames: list[BindingFrame]) -> BindingFrame:
        """Join multiple frames using LeapfrogTriejoin when applicable.

        When 3+ frames share a common variable, uses the worst-case optimal
        LeapfrogTriejoin algorithm. Otherwise falls back to iterated pairwise
        joins.

        Args:
            frames: Two or more BindingFrames to join.

        Returns:
            A single merged BindingFrame.

        """
        if len(frames) < 2:
            return frames[0] if frames else self.make_seed_frame()

        if len(frames) >= 3:
            from pycypher.leapfrog_triejoin import (
                can_use_leapfrog,
                leapfrog_triejoin,
            )

            applicable, join_var = can_use_leapfrog(frames)
            if applicable and join_var is not None:
                LOGGER.info(
                    "Using LeapfrogTriejoin for %d-way join on '%s'",
                    len(frames),
                    join_var,
                )
                return leapfrog_triejoin(frames, join_var)

        # Fallback: iterated pairwise joins
        result = frames[0]
        for frame in frames[1:]:
            result = self.coerce_join(result, frame)
        return result

    def merge_frames_for_match(
        self,
        current_frame: BindingFrame,
        match_frame: BindingFrame,
        where_clause: ASTNode | None = None,
    ) -> BindingFrame:
        """Merge a new MATCH frame into the existing execution frame.

        When a query contains multiple MATCH clauses, each new result frame
        must be combined with the accumulated frame via either a keyed join
        (shared variable) or a cross-join (no shared variables).

        **Filter pushdown**: WHERE conjuncts that reference only variables
        from ``match_frame`` are applied *before* the join to reduce
        intermediate result sizes.  Remaining predicates (those referencing
        variables from both frames) are applied after the join.

        Args:
            current_frame: The accumulated BindingFrame from preceding clauses.
            match_frame: The new frame produced by the current MATCH clause.
            where_clause: Optional WHERE predicate node from the MATCH clause.

        Returns:
            The merged :class:`~pycypher.binding_frame.BindingFrame`.

        """
        if where_clause is not None:
            pre_filter, post_filter = _split_pushdown_predicates(
                where_clause,
                set(match_frame.var_names),
                set(current_frame.var_names),
            )
            if pre_filter is not None:
                LOGGER.debug(
                    "filter pushdown: applying predicate to match_frame "
                    "before join (%d rows)",
                    len(match_frame.bindings),
                )
                match_frame = self._where_fn(pre_filter, match_frame)
        else:
            post_filter = None

        result = self.coerce_join(current_frame, match_frame)
        if post_filter is not None:
            result = self._where_fn(post_filter, result)
        return result

    def process_optional_match(
        self, clause: Match, current_frame: BindingFrame
    ) -> BindingFrame:
        """Execute an OPTIONAL MATCH clause against an existing frame.

        Attempts a regular match and left-joins the result on any shared
        variable.  If the match fails (entity/relationship type absent),
        delegates to :meth:`process_optional_match_failure` to add null
        columns.

        Args:
            clause: An ``optional=True`` Match clause.
            current_frame: The preceding BindingFrame.

        Returns:
            Updated BindingFrame.

        """
        try:
            match_frame = self._match_fn(
                clause,
                context_frame=current_frame,
            )
        except (ValueError, KeyError):
            # GraphTypeNotFoundError (ValueError subclass) is the expected
            # failure when an OPTIONAL MATCH references a label/type absent
            # from the context.  KeyError can surface from missing columns
            # during join construction.  Both are normal for OPTIONAL MATCH.
            LOGGER.debug(
                "OPTIONAL MATCH: pattern matching failed, processing as optional match failure",
            )
            return self.process_optional_match_failure(clause, current_frame)

        # An empty match frame means the pattern produced no rows — treat
        # it as an optional-match failure so every row gets NULLs for new
        # variables (same semantics as a missing entity/relationship type).
        if len(match_frame.bindings) == 0:
            return self.process_optional_match_failure(clause, current_frame)

        common_vars = set(current_frame.var_names) & set(match_frame.var_names)
        if common_vars:
            var = next(iter(common_vars))
            # Coerce join-column types to prevent pandas merge failures
            # when one side has object dtype (e.g. empty frames) and the
            # other has int64.
            left_col = current_frame.bindings[var]
            right_col = match_frame.bindings[var]
            if left_col.dtype != right_col.dtype:
                common_dtype = pd.api.types.find_common_type(
                    [left_col.dtype, right_col.dtype]
                )
                current_frame.bindings[var] = left_col.astype(common_dtype)
                match_frame.bindings[var] = right_col.astype(common_dtype)
            return current_frame.left_join(match_frame, var, var)

        # No shared variables — cross-product OPTIONAL MATCH.
        return current_frame.cross_join(match_frame)

    def process_optional_match_failure(
        self,
        clause: Match,
        current_frame: BindingFrame,
    ) -> BindingFrame:
        """Return a frame with NULL columns for variables the failed OPTIONAL MATCH would have bound.

        Args:
            clause: The failing Match clause.
            current_frame: The BindingFrame immediately before the OPTIONAL MATCH.

        Returns:
            An updated BindingFrame with null columns for every new variable.

        """
        from pycypher.ast_models import NodePattern, RelationshipPattern
        from pycypher.binding_frame import BindingFrame

        new_var_types: dict[str, str] = {}
        for path in clause.pattern.paths:
            for el in path.elements:
                if (
                    isinstance(el, NodePattern)
                    and el.variable is not None
                    and el.variable.name not in current_frame.var_names
                ):
                    label = el.labels[0] if el.labels else ""
                    new_var_types[el.variable.name] = label
                elif (
                    isinstance(el, RelationshipPattern)
                    and el.variable is not None
                    and el.variable.name not in current_frame.var_names
                ):
                    rel_t = el.labels[0] if el.labels else ""
                    new_var_types[el.variable.name] = rel_t

        if not new_var_types:
            return current_frame

        null_df = current_frame.bindings
        for v in new_var_types:
            null_df[v] = pd.NA
        updated_registry = {
            **current_frame.type_registry,
            **{k: v for k, v in new_var_types.items() if v},
        }
        return BindingFrame(
            bindings=null_df,
            type_registry=updated_registry,
            context=current_frame.context,
        )

    def make_seed_frame(self) -> BindingFrame:
        """Return a synthetic single-row BindingFrame for clause-first queries.

        Used when a clause appears as the first in a query with no preceding
        MATCH, UNWIND, or other data source.

        Returns:
            BindingFrame with a single row and no entity bindings.

        """
        from pycypher.binding_frame import BindingFrame

        return BindingFrame(
            bindings=pd.DataFrame({"_row": [0]}),
            type_registry={},
            context=self._context,
        )


# ---------------------------------------------------------------------------
# Filter pushdown helpers
# ---------------------------------------------------------------------------


def _extract_conjuncts(predicate: ASTNode) -> list[ASTNode]:
    """Split an AND-connected predicate into its conjunct list.

    Non-AND predicates are returned as a single-element list.
    """
    from pycypher.ast_models import And

    if isinstance(predicate, And) and predicate.operands:
        conjuncts: list[ASTNode] = []
        for op in predicate.operands:
            conjuncts.extend(_extract_conjuncts(op))
        return conjuncts
    return [predicate]


def _rebuild_and(conjuncts: list[ASTNode]) -> ASTNode | None:
    """Rebuild an AND node from a list of conjuncts, or return None if empty."""
    if not conjuncts:
        return None
    if len(conjuncts) == 1:
        return conjuncts[0]
    from pycypher.ast_models import And

    return And(operands=conjuncts)


def _predicate_variables(predicate: ASTNode) -> set[str]:
    """Extract variable names referenced by a predicate."""
    from pycypher.ast_models import ASTNode, extract_referenced_variables

    if isinstance(predicate, ASTNode):
        return extract_referenced_variables(predicate)
    return set()


def _split_pushdown_predicates(
    where_clause: ASTNode,
    match_vars: set[str],
    current_vars: set[str],
) -> tuple[ASTNode | None, ASTNode | None]:
    """Split a WHERE clause into pre-join and post-join predicates.

    Conjuncts whose variables are all in *match_vars* (and NOT in
    *current_vars*, unless also in *match_vars*) can be pushed down to
    filter ``match_frame`` before the join.

    Args:
        where_clause: The WHERE predicate AST node.
        match_vars: Variables available in the new MATCH frame.
        current_vars: Variables available in the accumulated frame.

    Returns:
        ``(pre_filter, post_filter)`` — either may be ``None``.

    """
    conjuncts = _extract_conjuncts(where_clause)
    pre: list[Any] = []
    post: list[Any] = []

    for conj in conjuncts:
        refs = _predicate_variables(conj)
        if refs and refs.issubset(match_vars):
            pre.append(conj)
        else:
            post.append(conj)

    return _rebuild_and(pre), _rebuild_and(post)
