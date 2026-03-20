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

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import pandas as pd
from shared.logger import LOGGER

if TYPE_CHECKING:
    from pycypher.binding_frame import BindingFrame
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

    def coerce_join(self, frame_a: BindingFrame, frame_b: BindingFrame) -> BindingFrame:
        """Join two BindingFrame objects.

        Selects the join strategy based on shared variables:

        * **Shared variable**: keyed inner-join on the first shared column name.
        * **No shared variable**: Cartesian cross-join.

        Args:
            frame_a: The left :class:`~pycypher.binding_frame.BindingFrame`.
            frame_b: The right :class:`~pycypher.binding_frame.BindingFrame`.

        Returns:
            A merged :class:`~pycypher.binding_frame.BindingFrame`.

        """
        common_vars = set(frame_a.var_names) & set(frame_b.var_names)
        if common_vars:
            var = next(iter(common_vars))
            return frame_a.join(frame_b, var, var)
        return frame_a.cross_join(frame_b)

    def merge_frames_for_match(
        self,
        current_frame: BindingFrame,
        match_frame: BindingFrame,
        where_clause: Any = None,
    ) -> BindingFrame:
        """Merge a new MATCH frame into the existing execution frame.

        When a query contains multiple MATCH clauses, each new result frame
        must be combined with the accumulated frame via either a keyed join
        (shared variable) or a cross-join (no shared variables).  If the
        MATCH carries a WHERE clause, it is applied after the join so that
        cross-MATCH predicates have access to all bound variables.

        Args:
            current_frame: The accumulated BindingFrame from preceding clauses.
            match_frame: The new frame produced by the current MATCH clause.
            where_clause: Optional WHERE predicate node from the MATCH clause.

        Returns:
            The merged :class:`~pycypher.binding_frame.BindingFrame`.

        """
        result = self.coerce_join(current_frame, match_frame)
        if where_clause is not None:
            result = self._where_fn(where_clause, result)
        return result

    def process_optional_match(self, clause: Any, current_frame: BindingFrame) -> BindingFrame:
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

        common_vars = set(current_frame.var_names) & set(match_frame.var_names)
        if common_vars:
            var = next(iter(common_vars))
            return current_frame.left_join(match_frame, var, var)

        # No shared variables — cross-product OPTIONAL MATCH.
        if len(match_frame.bindings) == 0:
            return self.process_optional_match_failure(clause, current_frame)
        return current_frame.cross_join(match_frame)

    def process_optional_match_failure(
        self,
        clause: Any,
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
                    rel_t = el.rel_types[0] if el.rel_types else ""
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
