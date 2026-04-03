"""Clause-by-clause execution engine for the BindingFrame IR.

Extracted from :mod:`pycypher.star` to separate clause dispatch and
execution logic from the main orchestration facade.  The
:class:`ClauseExecutor` handles:

* Clause-type dispatch (MATCH, WITH, RETURN, SET, DELETE, CREATE, etc.)
* MATCH clause handling (regular, OPTIONAL, merge)
* UNWIND clause processing with seed-frame management
* WHERE filter application
* Dead column elimination
* The main clause execution loop
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import pandas as pd
from shared.logger import LOGGER
from shared.metrics import get_rss_mb

from pycypher.binding_frame import BindingFrame

if TYPE_CHECKING:
    from pycypher.frame_joiner import FrameJoiner
    from pycypher.mutation_engine import MutationEngine
    from pycypher.pattern_matcher import PatternMatcher
    from pycypher.projection_planner import ProjectionPlanner
    from pycypher.query_analyzer import QueryAnalyzer
    from pycypher.relational_models import Context


class ClauseExecutor:
    """Execute Cypher clauses against the BindingFrame IR.

    Encapsulates clause dispatch, MATCH handling, UNWIND processing,
    WHERE filtering, and the main execution loop.

    Args:
        context: The PyCypher :class:`Context`.
        pattern_matcher: PatternMatcher for MATCH operations.
        mutations: MutationEngine for CREATE/SET/DELETE operations.
        frame_joiner: FrameJoiner for frame merging.
        projection_planner: ProjectionPlanner for RETURN/WITH clauses.
        query_analyzer: QueryAnalyzer for pre-execution planning.

    """

    def __init__(
        self,
        context: Context,
        pattern_matcher: PatternMatcher,
        mutations: MutationEngine,
        frame_joiner: FrameJoiner,
        projection_planner: ProjectionPlanner,
        query_analyzer: QueryAnalyzer,
    ) -> None:
        self._context = context
        self._pattern_matcher = pattern_matcher
        self._mutations = mutations
        self._frame_joiner = frame_joiner
        self._projection_planner = projection_planner
        self._query_analyzer = query_analyzer

    # ------------------------------------------------------------------
    # WHERE filter
    # ------------------------------------------------------------------

    @staticmethod
    def apply_where_filter(
        where_expr: Any,
        result_frame: BindingFrame,
        fallback_frame: BindingFrame | None = None,
    ) -> BindingFrame:
        """Apply a WHERE predicate to *result_frame*, with optional fallback.

        First tries evaluating *where_expr* against *result_frame*.  If that
        raises ``ValueError`` or ``KeyError``, falls back to evaluating against
        *fallback_frame* and applying the boolean mask positionally.

        Args:
            where_expr: AST expression node for the WHERE predicate.
            result_frame: The frame to filter.
            fallback_frame: Optional pre-projection frame for variable lookup.

        Returns:
            A new filtered :class:`BindingFrame`.

        """
        from pycypher.binding_evaluator import (
            BindingExpressionEvaluator as _BEE,
        )
        from pycypher.binding_frame import BindingFilter

        if fallback_frame is None:
            return BindingFilter(predicate=where_expr).apply(result_frame)

        try:
            _mask = _BEE(result_frame).evaluate(where_expr).fillna(False)
            return result_frame.filter(_mask)
        except (ValueError, KeyError):
            LOGGER.debug(
                "WHERE: evaluation failed on result frame, falling back to pre-projection frame",
            )
            _pre_mask = _BEE(fallback_frame).evaluate(where_expr).fillna(False)
            return result_frame.filter(_pre_mask)

    # ------------------------------------------------------------------
    # UNWIND
    # ------------------------------------------------------------------

    def unwind_binding_frame(
        self,
        clause: Any,
        frame: BindingFrame,
    ) -> BindingFrame:
        """Evaluate an UNWIND clause and return the exploded BindingFrame.

        Args:
            clause: AST :class:`~pycypher.ast_models.Unwind` node.
            frame: Current :class:`BindingFrame`.

        Returns:
            A new :class:`BindingFrame` with rows expanded from list elements.

        """
        from pycypher.binding_evaluator import BindingExpressionEvaluator

        alias: str = clause.alias or "_unwind_col"
        evaluator = BindingExpressionEvaluator(frame)
        list_series = evaluator.evaluate(clause.expression).reset_index(
            drop=True,
        )

        # Guard against memory exhaustion
        from pycypher.config import MAX_COLLECTION_SIZE
        from pycypher.exceptions import SecurityError

        max_list_len = 0
        for val in list_series:
            if isinstance(val, (list, tuple)):
                max_list_len = max(max_list_len, len(val))
        if max_list_len > MAX_COLLECTION_SIZE:
            msg = (
                f"UNWIND list contains {max_list_len:,} elements, "
                f"exceeding limit of {MAX_COLLECTION_SIZE:,}. "
                f"Adjust PYCYPHER_MAX_COLLECTION_SIZE to increase."
            )
            raise SecurityError(msg)

        bindings = frame.bindings
        idx = bindings.index
        if not (
            isinstance(idx, pd.RangeIndex)
            and idx.start == 0
            and idx.step == 1
            and idx.stop == len(bindings)
        ):
            bindings = bindings.reset_index(drop=True)
        df = bindings.assign(**{alias: list_series})

        df = df.explode(alias, ignore_index=True)
        df = df.dropna(subset=[alias])
        idx = df.index
        if not (
            isinstance(idx, pd.RangeIndex)
            and idx.start == 0
            and idx.step == 1
            and idx.stop == len(df)
        ):
            df = df.reset_index(drop=True)

        return BindingFrame(
            bindings=df,
            type_registry=frame.type_registry,
            context=frame.context,
        )

    def process_unwind_clause(self, clause: Any, current_frame: Any) -> Any:
        """Execute an UNWIND clause, seeding a synthetic frame if needed.

        Args:
            clause: The :class:`~pycypher.ast_models.Unwind` clause.
            current_frame: The preceding BindingFrame, or ``None``.

        Returns:
            A new :class:`BindingFrame` with the unwound variable bound.

        """
        if current_frame is None:
            current_frame = self._frame_joiner.make_seed_frame()

        result = self.unwind_binding_frame(clause, current_frame)

        if "_row" in result.bindings.columns:
            result = BindingFrame(
                bindings=result.bindings.drop(columns=["_row"]),
                type_registry=result.type_registry,
                context=result.context,
            )
        return result

    # ------------------------------------------------------------------
    # MATCH handling
    # ------------------------------------------------------------------

    def handle_match_clause(
        self,
        clause: Any,
        current_frame: Any,
        limit_hint: int | None,
    ) -> Any | pd.DataFrame:
        """Execute a MATCH or OPTIONAL MATCH clause.

        Args:
            clause: The :class:`~pycypher.ast_models.Match` clause.
            current_frame: The preceding BindingFrame, or ``None``.
            limit_hint: Optional row limit for MATCH pushdown.

        Returns:
            The updated BindingFrame, or an empty ``pd.DataFrame`` when
            an OPTIONAL MATCH as first clause finds no matches.

        """
        if clause.optional and current_frame is not None:
            return self._frame_joiner.process_optional_match(
                clause, current_frame,
            )

        try:
            match_frame = self._pattern_matcher.match_to_binding_frame(
                clause,
                context_frame=current_frame,
                row_limit=limit_hint,
            )
        except ValueError:
            LOGGER.debug(
                "MATCH: ValueError during pattern matching (optional=%s)",
                clause.optional,
            )
            if clause.optional:
                return pd.DataFrame()
            raise

        if current_frame is None:
            return match_frame
        return self._frame_joiner.merge_frames_for_match(
            current_frame,
            match_frame,
            clause.where,
        )

    # ------------------------------------------------------------------
    # Static helpers
    # ------------------------------------------------------------------

    @staticmethod
    def require_bound_frame(current_frame: Any, clause_name: str) -> None:
        """Raise ``ValueError`` if *current_frame* is ``None``."""
        if current_frame is None:
            msg = (
                f"{clause_name} clause requires a preceding MATCH or CREATE clause "
                f"to bind variables."
            )
            raise ValueError(msg)

    @staticmethod
    def frame_size(frame: Any) -> str:
        """Return row count string for a BindingFrame, or ``'(none)'``."""
        if frame is None:
            return "(none)"
        try:
            return str(len(frame.bindings))
        except AttributeError:
            return "(unknown)"

    # ------------------------------------------------------------------
    # Clause dispatch
    # ------------------------------------------------------------------

    def dispatch_clause(
        self,
        clause: Any,
        current_frame: Any,
        limit_hint: int | None,
    ) -> Any:
        """Dispatch a single clause and return the updated frame.

        Returns a ``pd.DataFrame`` for RETURN clauses and early-exit
        OPTIONAL MATCH, or a BindingFrame / ``None`` for all other clauses.

        """
        from pycypher.ast_models import (
            Call,
            Create,
            Delete,
            Foreach,
            Match,
            Merge,
            Remove,
            Return,
            Set,
            Unwind,
            With,
        )

        if isinstance(clause, Match):
            result = self.handle_match_clause(
                clause,
                current_frame,
                limit_hint,
            )
            if isinstance(result, pd.DataFrame):
                return result
            return result

        if isinstance(clause, With):
            if current_frame is None:
                current_frame = self._frame_joiner.make_seed_frame()
            return self._projection_planner.with_to_binding_frame(
                clause, current_frame,
            )

        if isinstance(clause, Return):
            if current_frame is None:
                current_frame = self._frame_joiner.make_seed_frame()
            return self._projection_planner.return_from_frame(
                clause, current_frame,
            )

        if isinstance(clause, Set):
            self.require_bound_frame(current_frame, "SET")
            self._mutations.set_properties(clause, current_frame)
            current_frame._property_cache.clear()
            return current_frame

        if isinstance(clause, Remove):
            self.require_bound_frame(current_frame, "REMOVE")
            self._mutations.remove_properties(clause, current_frame)
            current_frame._property_cache.clear()
            return current_frame

        if isinstance(clause, Delete):
            self.require_bound_frame(current_frame, "DELETE")
            self._mutations.process_delete(clause, current_frame)
            current_frame._property_cache.clear()
            return current_frame

        if isinstance(clause, Unwind):
            return self.process_unwind_clause(clause, current_frame)

        if isinstance(clause, Create):
            return self._mutations.process_create(
                clause,
                current_frame,
                make_seed_frame=self._frame_joiner.make_seed_frame,
            )

        if isinstance(clause, Call):
            return self._mutations.process_call(clause, current_frame)

        if isinstance(clause, Merge):
            return self._mutations.process_merge(
                clause,
                current_frame,
                match_to_binding_frame=self._pattern_matcher.match_to_binding_frame,
                merge_frames_for_match=self._frame_joiner.merge_frames_for_match,
                make_seed_frame=self._frame_joiner.make_seed_frame,
            )

        if isinstance(clause, Foreach):
            return self._mutations.process_foreach(
                clause,
                current_frame,
                make_seed_frame=self._frame_joiner.make_seed_frame,
            )

        msg = (
            f"Clause type '{type(clause).__name__}' is not yet supported "
            "in the BindingFrame execution path."
        )
        raise NotImplementedError(msg)

    # ------------------------------------------------------------------
    # Main execution loop
    # ------------------------------------------------------------------

    def execute_query_inner(
        self,
        query: Any,
        initial_frame: Any = None,
    ) -> pd.DataFrame:
        """Execute a Cypher query using the BindingFrame IR.

        Handles all clause types (MATCH, WHERE, WITH, RETURN, SET, DELETE,
        CREATE, UNWIND, MERGE, CALL, FOREACH) with dead column elimination.

        Args:
            query: A parsed :class:`~pycypher.ast_models.Query` AST node.
            initial_frame: Optional pre-seeded BindingFrame.

        Returns:
            A ``pd.DataFrame`` with columns matching the RETURN aliases.

        """
        if not query.clauses:
            msg = (
                "Query must have at least one clause (e.g. MATCH, RETURN, CREATE). "
                "Example: MATCH (n:Person) RETURN n.name"
            )
            raise ValueError(msg)

        _clause_timings: dict[str, float] = {}
        _clause_memory: dict[str, float] = {}
        _limit_hint = self._query_analyzer.analyze_and_plan(query)
        current_frame: Any = initial_frame

        # --- Dead column elimination: compute live columns per clause ---
        from pycypher.lazy_eval import compute_live_columns

        _live_columns = compute_live_columns(query.clauses)

        for clause_idx, clause in enumerate(query.clauses):
            self._context.check_timeout()

            _clause_name = type(clause).__name__
            _size_before = self.frame_size(current_frame)
            _clause_rss_before = get_rss_mb()
            _clause_t0 = time.perf_counter()

            result = self.dispatch_clause(
                clause,
                current_frame,
                _limit_hint,
            )

            _clause_elapsed = time.perf_counter() - _clause_t0
            _clause_rss_after = get_rss_mb()
            _clause_timings[_clause_name] = (
                _clause_timings.get(_clause_name, 0.0)
                + _clause_elapsed * 1000.0
            )
            _clause_memory[_clause_name] = (
                _clause_memory.get(_clause_name, 0.0)
                + (_clause_rss_after - _clause_rss_before)
            )

            # Check if this is a final result (DataFrame) requiring early return
            if isinstance(result, pd.DataFrame):
                LOGGER.debug(
                    "clause %s  elapsed=%.3fs  frame_before=%s  frame_after=%s",
                    _clause_name,
                    _clause_elapsed,
                    _size_before,
                    str(len(result)),
                )
                self.last_clause_timings = _clause_timings
                self.last_clause_memory = _clause_memory
                self._query_analyzer.record_cardinality_feedback(len(result))
                return result

            # --- Dead column elimination ---
            _live = _live_columns[clause_idx]
            if _live is not None and hasattr(result, "bindings"):
                from pycypher.binding_frame import PATH_HOP_COLUMN_PREFIX

                def _is_live(col: str) -> bool:
                    if col in _live:
                        return True
                    if col.startswith(PATH_HOP_COLUMN_PREFIX):
                        path_var = col[len(PATH_HOP_COLUMN_PREFIX) :]
                        return path_var in _live
                    return False

                _dead_cols = [
                    c for c in result.bindings.columns if not _is_live(c)
                ]
                if _dead_cols:
                    LOGGER.debug(
                        "dead column elimination after %s: dropping %s",
                        _clause_name,
                        _dead_cols,
                    )
                    result = BindingFrame(
                        bindings=result.bindings.drop(columns=_dead_cols),
                        type_registry={
                            k: v
                            for k, v in result.type_registry.items()
                            if k not in _dead_cols
                        },
                        context=result.context,
                    )

            current_frame = result
            LOGGER.debug(
                "clause %s  elapsed=%.3fs  frame_before=%s  frame_after=%s",
                _clause_name,
                _clause_elapsed,
                _size_before,
                self.frame_size(current_frame),
            )

        self.last_clause_timings = _clause_timings
        self.last_clause_memory = _clause_memory
        self._query_analyzer.record_cardinality_feedback(0)
        return pd.DataFrame()

    def execute_query_binding_frame(
        self,
        query: Any,
    ) -> pd.DataFrame:
        """Execute with query-scoped shadow write atomicity.

        Wraps :meth:`execute_query_inner` in a begin/commit/rollback
        transaction.

        """
        self._context.begin_query()
        _committed = False
        try:
            result = self.execute_query_inner(query)
            self._context.commit_query()
            _committed = True
            return result
        finally:
            if not _committed:
                self._context.rollback_query()

    def execute_union_query(self, union_query: Any) -> pd.DataFrame:
        """Execute a UNION [ALL] query.

        Args:
            union_query: A :class:`~pycypher.ast_models.UnionQuery` AST node.

        Returns:
            A ``pd.DataFrame`` combining all sub-query results.

        """
        self._context.begin_query()
        _committed = False
        try:
            frames: list[pd.DataFrame] = []
            for stmt in union_query.statements:
                frames.append(self.execute_query_inner(stmt))

            if not frames:
                self._context.commit_query()
                _committed = True
                return pd.DataFrame()

            combined = pd.concat(frames, ignore_index=True)

            any_union = any(not flag for flag in union_query.all_flags)
            if any_union:
                combined = combined.drop_duplicates().reset_index(drop=True)

            self._context.commit_query()
            _committed = True
            return combined
        finally:
            if not _committed:
                self._context.rollback_query()
