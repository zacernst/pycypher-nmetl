"""ExistsEvaluator — EXISTS predicate and pattern comprehension evaluation.

Extracted from :mod:`pycypher.binding_evaluator` to isolate the EXISTS and
pattern comprehension logic (the two largest inline method families) into a
focused, independently testable module.

Handles:

- ``EXISTS { (a)-[:TYPE]->(b) }`` — single-hop pattern existence check
- ``EXISTS { MATCH ... WHERE ... }`` — full subquery existence check
- ``[(a)-[:TYPE]->(b) WHERE pred | expr]`` — pattern comprehensions
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
from shared.logger import LOGGER

from pycypher.constants import _broadcast_series
from pycypher.exceptions import PatternComprehensionError
from pycypher.relational_models import (
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
)
from pycypher.cypher_types import FrameSeries

if TYPE_CHECKING:
    from pycypher.ast_models import PatternComprehension
    from pycypher.binding_frame import BindingFrame
    from pycypher.evaluator_protocol import ExpressionEvaluatorProtocol

_DEBUG_ENABLED: bool = LOGGER.isEnabledFor(logging.DEBUG)

# Column name injected into the initial frame and RETURN to trace source rows.
_EXISTS_SENTINEL: str = "__exists_row_idx__"


class ExistsEvaluator:
    """Evaluates EXISTS predicates and pattern comprehensions.

    Follows the established evaluator pattern: takes a
    :class:`~pycypher.binding_frame.BindingFrame` at init time and receives
    the parent evaluator as a callback parameter on each public method.

    Args:
        frame: The current binding frame to evaluate against.

    """

    def __init__(self, frame: BindingFrame) -> None:
        """Initialise with the current binding frame.

        Args:
            frame: The :class:`BindingFrame` to evaluate EXISTS predicates
                and pattern comprehensions against.

        """
        self.frame = frame

    # ------------------------------------------------------------------
    # EXISTS predicate
    # ------------------------------------------------------------------

    def evaluate_exists(
        self, content: Any, evaluator: ExpressionEvaluatorProtocol
    ) -> FrameSeries:
        """Evaluate an ``EXISTS { pattern }`` predicate.

        For each row in the current BindingFrame, returns ``True`` if the
        inner pattern has at least one match anchored on the row's bound
        variables, and ``False`` otherwise.

        Args:
            content: The content of the EXISTS clause — either a
                :class:`~pycypher.ast_models.Pattern` or a
                :class:`~pycypher.ast_models.Query`.
            evaluator: The parent :class:`BindingExpressionEvaluator` for
                recursive expression evaluation.

        Returns:
            A ``pd.Series`` of ``bool`` values, one per row.

        """
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "exists: content_type=%s  rows=%d",
                type(content).__name__,
                len(self.frame),
            )
        from pycypher.ast_models import Pattern, PatternComprehension, Query

        if isinstance(content, Query):
            from pycypher.ast_models import IntegerLiteral, Return, ReturnItem

            has_return = any(isinstance(c, Return) for c in content.clauses)
            if not has_return:
                synthetic_return = Return(
                    items=[
                        ReturnItem(
                            expression=IntegerLiteral(value=1),
                            alias="_exists_flag",
                        ),
                    ],
                )
                content = Query(
                    clauses=[*list(content.clauses), synthetic_return],
                )

            return self._exists_via_query_execution(content)

        if not isinstance(content, Pattern):
            return _broadcast_series(False, len(self.frame.bindings))

        # Multi-hop patterns: fall back to full MATCH execution.
        if (
            content.paths
            and content.paths[0].elements
            and len(content.paths[0].elements) != 3
        ):
            from pycypher.ast_models import (
                IntegerLiteral,
                Match,
                Query,
                Return,
                ReturnItem,
            )

            synthetic_return = Return(
                items=[
                    ReturnItem(
                        expression=IntegerLiteral(value=1),
                        alias="_exists_flag",
                    ),
                ],
            )
            match_clause = Match(pattern=content, where=None)
            subquery = Query(clauses=[match_clause, synthetic_return])
            return self._exists_via_query_execution(subquery)

        # Single-hop: reuse pattern comprehension; EXISTS is True iff non-empty.
        pc = PatternComprehension(
            pattern=content,
            variable=None,
            where=None,
            map_expr=None,
        )
        match_lists = self.evaluate_pattern_comprehension(pc, evaluator)
        return pd.Series(
            [len(lst) > 0 for lst in match_lists],
            dtype=bool,
        )

    # ------------------------------------------------------------------
    # EXISTS via full query execution (batch path)
    # ------------------------------------------------------------------

    def _exists_via_query_execution(self, subquery: Any) -> FrameSeries:
        """Execute *subquery* against the entire frame in one pass.

        A sentinel column is injected into the initial frame and RETURN
        clause to trace which outer rows had at least one inner match.

        Args:
            subquery: A :class:`~pycypher.ast_models.Query` to execute.

        Returns:
            A ``pd.Series`` of ``bool`` values, one per row in ``self.frame``.

        """
        from pycypher.ast_models import (
            Query,
            Return,
            ReturnItem,
            Variable,
            With,
        )
        from pycypher.binding_frame import BindingFrame
        from pycypher.star import Star

        n_rows = len(self.frame.bindings)
        orig_index = self.frame.bindings.index
        sentinel = _EXISTS_SENTINEL

        augmented_bindings = self.frame.bindings.assign(
            **{sentinel: np.arange(n_rows, dtype=np.intp)},
        )
        augmented_frame = BindingFrame(
            bindings=augmented_bindings,
            type_registry=dict(self.frame.type_registry),
            context=self.frame.context,
        )

        sentinel_item = ReturnItem(
            expression=Variable(name=sentinel),
            alias=sentinel,
        )

        batch_clauses: list[Any] = []
        for clause in subquery.clauses:
            if isinstance(clause, With):
                new_with = With(
                    distinct=clause.distinct,
                    items=[*clause.items, sentinel_item],
                    where=clause.where,
                    order_by=clause.order_by,
                    skip=clause.skip,
                    limit=clause.limit,
                )
                batch_clauses.append(new_with)
            elif not isinstance(clause, Return):
                batch_clauses.append(clause)
        batch_clauses.append(Return(items=[sentinel_item]))

        batch_query = Query(clauses=batch_clauses)

        temp_star = Star(context=self.frame.context)
        result_df = temp_star._execute_query_binding_frame_inner(
            batch_query,
            initial_frame=augmented_frame,
        )

        if result_df.empty or sentinel not in result_df.columns:
            return _broadcast_series(False, n_rows, index=orig_index)

        matched = result_df[sentinel].dropna().astype(int).unique()
        all_indices = np.arange(n_rows)
        return pd.Series(
            np.isin(all_indices, matched),
            dtype=bool,
            index=orig_index,
        )

    # ------------------------------------------------------------------
    # Pattern comprehension
    # ------------------------------------------------------------------

    def evaluate_pattern_comprehension(
        self,
        pc: PatternComprehension,
        evaluator: ExpressionEvaluatorProtocol,
    ) -> FrameSeries:
        """Evaluate a Cypher pattern comprehension row by row.

        Supports single-hop directed patterns:

        * ``[(src)-[:TYPE]->(tgt) | map_expr]``
        * ``[(src)-[:TYPE]->(tgt) WHERE pred | map_expr]``
        * ``[(src)<-[:TYPE]-(tgt) | map_expr]``  (incoming direction)

        Args:
            pc: The :class:`~pycypher.ast_models.PatternComprehension` AST node.
            evaluator: The parent :class:`BindingExpressionEvaluator` for
                recursive expression evaluation.

        Returns:
            A ``pd.Series`` of lists, one per row in the current BindingFrame.

        Raises:
            PatternComprehensionError: If the pattern is not a single-hop path.

        """
        from pycypher.ast_models import (
            NodePattern,
            RelationshipDirection,
            RelationshipPattern,
        )

        if pc.pattern is None or not pc.pattern.paths:
            return _broadcast_series([], len(self.frame.bindings))

        path = pc.pattern.paths[0]
        elements = path.elements

        if len(elements) != 3:
            msg = (
                f"PatternComprehension supports only single-hop patterns "
                f"(node)-[rel]->(node).  Got a path with {len(elements)} elements."
            )
            raise PatternComprehensionError(msg)

        source_pattern, rel_pattern, target_pattern = elements

        if not isinstance(source_pattern, NodePattern) or not isinstance(
            target_pattern,
            NodePattern,
        ):
            raise PatternComprehensionError(
                "First and third elements of a PatternComprehension path "
                "must be NodePattern instances.",
            )
        if not isinstance(rel_pattern, RelationshipPattern):
            raise PatternComprehensionError(
                "Second element of a PatternComprehension path must be a "
                "RelationshipPattern instance.",
            )

        source_var = (
            source_pattern.variable.name
            if source_pattern.variable
            else "_pc_src"
        )
        target_var = (
            target_pattern.variable.name
            if target_pattern.variable
            else "_pc_tgt"
        )

        source_type = self.frame.type_registry.get(source_var) or (
            source_pattern.labels[0] if source_pattern.labels else None
        )
        target_type = (
            target_pattern.labels[0] if target_pattern.labels else None
        )

        rel_type = rel_pattern.labels[0] if rel_pattern.labels else None
        direction = rel_pattern.direction

        rel_mapping = self.frame.context.relationship_mapping.mapping
        if rel_type not in rel_mapping:
            return _broadcast_series([], len(self.frame.bindings))
        rel_df = rel_mapping[rel_type].source_obj

        anchor_var = source_var
        if direction == RelationshipDirection.LEFT:
            anchor_col, result_col = (
                RELATIONSHIP_TARGET_COLUMN,
                RELATIONSHIP_SOURCE_COLUMN,
            )
            anchor_var = source_var
        else:
            anchor_col, result_col = (
                RELATIONSHIP_SOURCE_COLUMN,
                RELATIONSHIP_TARGET_COLUMN,
            )

        sub_type_reg: dict[str, str] = {}
        if source_type:
            sub_type_reg[source_var] = source_type
        if target_type:
            sub_type_reg[target_var] = target_type

        if anchor_var not in self.frame.bindings.columns:
            other_var = target_var if anchor_var == source_var else source_var
            if other_var not in self.frame.bindings.columns:
                return _broadcast_series([], len(self.frame.bindings))
            anchor_var = other_var
            anchor_col, result_col = result_col, anchor_col

        anchor_ids = self.frame.bindings[anchor_var]
        n_rows = len(anchor_ids)
        orig_index = self.frame.bindings.index

        # Vectorised path: one pandas merge replaces O(n_rows × m_matches).
        from pycypher.binding_evaluator import BindingExpressionEvaluator
        from pycypher.binding_frame import BindingFrame

        anchor_frame = pd.DataFrame(
            {
                "__row_idx__": np.arange(n_rows, dtype=np.intp),
                "__anchor__": anchor_ids.to_numpy(),
            },
        )
        pairs_df = anchor_frame.merge(
            rel_df[[anchor_col, result_col]],
            left_on="__anchor__",
            right_on=anchor_col,
            how="inner",
        ).reset_index(drop=True)

        if len(pairs_df) == 0:
            return _broadcast_series([], n_rows, index=orig_index)

        if direction == RelationshipDirection.LEFT:
            src_vals = pairs_df[result_col].to_numpy()
            tgt_vals = pairs_df["__anchor__"].to_numpy()
        else:
            src_vals = pairs_df["__anchor__"].to_numpy()
            tgt_vals = pairs_df[result_col].to_numpy()

        sub_bindings_df = pd.DataFrame(
            {source_var: src_vals, target_var: tgt_vals},
        )

        sub_frame = BindingFrame(
            bindings=sub_bindings_df,
            type_registry=sub_type_reg,
            context=self.frame.context,
        )
        pairs_eval = BindingExpressionEvaluator(sub_frame)

        if pc.where is not None:
            keep_raw = pairs_eval.evaluate(pc.where)
            keep_mask = keep_raw.fillna(False).astype(bool).to_numpy()
            if not keep_mask.any():
                return _broadcast_series([], n_rows, index=orig_index)
            pairs_df = pairs_df[keep_mask].reset_index(drop=True)
            sub_bindings_df = sub_bindings_df[keep_mask].reset_index(drop=True)
            sub_frame = BindingFrame(
                bindings=sub_bindings_df,
                type_registry=sub_type_reg,
                context=self.frame.context,
            )
            pairs_eval = BindingExpressionEvaluator(sub_frame)

        if pc.map_expr is not None:
            mapped = pairs_eval.evaluate(pc.map_expr)
            values = mapped.to_numpy(dtype=object)
        else:
            values = pairs_df[result_col].to_numpy()

        row_indices = pairs_df["__row_idx__"].to_numpy(dtype=np.intp)
        lists: list[list] = [[] for _ in range(n_rows)]
        for idx, val in zip(row_indices, values, strict=False):
            lists[idx].append(val)

        return pd.Series(lists, dtype=object, index=orig_index)
