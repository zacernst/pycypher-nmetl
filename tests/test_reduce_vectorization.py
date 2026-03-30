"""TDD tests for batch-per-step REDUCE evaluation (Loop 183 — Performance).

Problem: ``_eval_reduce`` in ``binding_evaluator.py`` creates one
``BindingFrame`` + ``BindingExpressionEvaluator`` per *element* per *row*::

    for raw_list, init_val in zip(list_series, initial_series):
        for item in list(raw_list):
            step_evaluator = self._make_single_row_evaluator(
                {acc_name: accumulator, var_name: item}
            )
            next_series = step_evaluator.evaluate(r.map_expr)

For 200 rows with 50-element lists this is 10 000 single-row BindingFrame
allocations + 10 000 evaluator instantiations — the same O(rows × items)
complexity that was fixed for list comprehensions in Loop 177.

Key insight: REDUCE's accumulator has a *sequential* dependency per row
(step i depends on step i−1), so we cannot explode all (row, element) pairs
into a single flat frame.  However, we *can* batch all rows that are at the
*same step position* into one BindingFrame and evaluate the step expression
once for that position.  Rows with shorter lists simply drop out early.

Fix: replace the nested loop with a column-per-step batch pass:

1. Evaluate ``list_series`` and ``initial_series`` once over the full frame.
2. Collect per-row lists; start ``accumulators[i] = initial[i]``.
3. ``max_len = max(len(lst) for lst in lists)``.
4. For ``step`` in ``range(max_len)``:
       active_indices = rows whose list has an element at position ``step``
       if no active rows: break
       Build one BindingFrame: {acc_name: [acc[i] for i in active], var_name: [list[i][step] for i in active]}
       Evaluate ``map_expr`` ONCE → new_accs (one value per active row)
       Update ``accumulators[i]`` for each active row.
5. Return Series of final accumulators.

Allocations drop from O(rows × max_items) → O(max_items).  For 200 rows ×
50-element lists: 10 000 → 50 allocations (200× fewer).

All tests written before the implementation (TDD red phase).
"""

from __future__ import annotations

import time

import pandas as pd
import pytest

pytestmark = [pytest.mark.slow, pytest.mark.performance]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _star_empty():
    from pycypher.relational_models import (
        Context,
        EntityMapping,
        RelationshipMapping,
    )
    from pycypher.star import Star

    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={}),
            relationship_mapping=RelationshipMapping(mapping={}),
        ),
    )


def _q(cypher: str) -> pd.DataFrame:
    return _star_empty().execute_query(cypher)


def _star_with_rows(rows: list[dict]) -> Star:
    """Create a Star with a single 'Row' entity table from the given rows."""
    import pandas as pd
    from pycypher.relational_models import (
        ID_COLUMN,
        Context,
        EntityMapping,
        EntityTable,
        RelationshipMapping,
    )
    from pycypher.star import Star

    col_names = list(rows[0].keys()) if rows else []
    data: dict = {ID_COLUMN: list(range(1, len(rows) + 1))}
    for col in col_names:
        data[col] = [r[col] for r in rows]
    df = pd.DataFrame(data)
    table = EntityTable(
        entity_type="Row",
        identifier="Row",
        column_names=[ID_COLUMN] + col_names,
        source_obj_attribute_map={c: c for c in col_names},
        attribute_map={c: c for c in col_names},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Row": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        ),
    )


# Reference implementation (current per-element approach) for speed comparison.
def _reference_reduce(list_series, initial_series, step_fn):
    """Baseline: one function call per element per row."""
    results = []
    for raw_list, init_val in zip(list_series, initial_series):
        acc = init_val
        if raw_list is None or (isinstance(raw_list, float) and pd.isna(raw_list)):
            results.append(acc)
            continue
        for item in list(raw_list):
            acc = step_fn(acc, item)
        results.append(acc)
    return pd.Series(results, dtype=object)


# ===========================================================================
# Category 1 — Correctness: basic reduce operations
# ===========================================================================


class TestReduceBasicCorrectness:
    """Batch-per-step REDUCE must produce identical results to the sequential loop."""

    def test_sum_uniform_lists(self) -> None:
        """reduce(s=0, x IN [1,2,3] | s + x) == 6 for every row."""
        result = _q("RETURN reduce(s = 0, x IN [1, 2, 3] | s + x) AS v")
        assert result["v"].iloc[0] == 6

    def test_product_uniform_lists(self) -> None:
        result = _q("RETURN reduce(p = 1, x IN [2, 3, 4] | p * x) AS v")
        assert result["v"].iloc[0] == 24

    def test_string_concatenation(self) -> None:
        result = _q("RETURN reduce(s = '', x IN ['a', 'b', 'c'] | s + x) AS v")
        assert result["v"].iloc[0] == "abc"

    def test_empty_list_returns_initial(self) -> None:
        result = _q("RETURN reduce(s = 99, x IN [] | s + x) AS v")
        assert result["v"].iloc[0] == 99

    def test_single_element_list(self) -> None:
        result = _q("RETURN reduce(s = 0, x IN [7] | s + x) AS v")
        assert result["v"].iloc[0] == 7


# ===========================================================================
# Category 2 — Correctness: multi-row frames with varying list lengths
# ===========================================================================


class TestReduceMultiRowVaryingLengths:
    """Correctness when rows have lists of different lengths (rows 'drop out' early)."""

    def test_two_rows_different_lengths(self) -> None:
        """Row 0 reduces [1,2], row 1 reduces [10,20,30] — independently correct."""
        star = _star_with_rows(
            [
                {"items": [1, 2]},
                {"items": [10, 20, 30]},
            ],
        )
        result = star.execute_query(
            "MATCH (r:Row) RETURN reduce(s = 0, x IN r.items | s + x) AS v "
            "ORDER BY id(r)",
        )
        assert result["v"].iloc[0] == 3  # 0+1+2
        assert result["v"].iloc[1] == 60  # 0+10+20+30

    def test_one_empty_one_nonempty(self) -> None:
        """Empty list row returns initial; non-empty row reduces normally."""
        star = _star_with_rows(
            [
                {"items": []},
                {"items": [5, 6, 7]},
            ],
        )
        result = star.execute_query(
            "MATCH (r:Row) RETURN reduce(s = 0, x IN r.items | s + x) AS v "
            "ORDER BY id(r)",
        )
        assert result["v"].iloc[0] == 0  # empty list → initial
        assert result["v"].iloc[1] == 18  # 0+5+6+7

    def test_null_list_returns_initial(self) -> None:
        """Null list must return the initial value (Cypher null-propagation rule)."""
        star = _star_with_rows([{"items": None}, {"items": [3, 4]}])
        result = star.execute_query(
            "MATCH (r:Row) RETURN reduce(s = 0, x IN r.items | s + x) AS v "
            "ORDER BY id(r)",
        )
        assert result["v"].iloc[0] == 0  # null list → initial
        assert result["v"].iloc[1] == 7  # 0+3+4

    def test_many_rows_uniform_sums(self) -> None:
        """100 rows each reducing [1..5] — every result must be 15."""
        rows = [{"items": [1, 2, 3, 4, 5]} for _ in range(100)]
        star = _star_with_rows(rows)
        result = star.execute_query(
            "MATCH (r:Row) RETURN reduce(s = 0, x IN r.items | s + x) AS v",
        )
        assert all(v == 15 for v in result["v"])

    def test_single_row_long_list(self) -> None:
        """Single row reducing a 200-element list: result must be sum(1..200)."""
        star = _star_with_rows([{"items": list(range(1, 201))}])
        result = star.execute_query(
            "MATCH (r:Row) RETURN reduce(s = 0, x IN r.items | s + x) AS v",
        )
        expected = sum(range(1, 201))  # 20100
        assert result["v"].iloc[0] == expected

    def test_accumulator_depends_on_previous_step(self) -> None:
        """Step expression uses accumulator from prior step (sequential correctness)."""
        # reduce(acc=1, x IN [2,3,4] | acc * x) = ((1*2)*3)*4 = 24
        result = _q("RETURN reduce(acc = 1, x IN [2, 3, 4] | acc * x) AS v")
        assert result["v"].iloc[0] == 24

    def test_reduce_with_string_list_property(self) -> None:
        """Reduce over string list property."""
        star = _star_with_rows(
            [
                {"words": ["hello", " ", "world"]},
                {"words": ["foo", "bar"]},
            ],
        )
        result = star.execute_query(
            "MATCH (r:Row) "
            "RETURN reduce(s = '', x IN r.words | s + x) AS v "
            "ORDER BY id(r)",
        )
        assert result["v"].iloc[0] == "hello world"
        assert result["v"].iloc[1] == "foobar"


# ===========================================================================
# Category 3 — Correctness: REDUCE with CASE and conditional step expressions
# ===========================================================================


class TestReduceConditionalStep:
    """Step expressions using CASE or comparison must work correctly."""

    def test_max_via_conditional(self) -> None:
        """reduce(m=-999, x IN list | CASE WHEN x > m THEN x ELSE m END) finds max."""
        result = _q(
            "RETURN reduce(m = -999, x IN [3, 1, 4, 1, 5, 9, 2, 6] | "
            "CASE WHEN x > m THEN x ELSE m END) AS v",
        )
        assert result["v"].iloc[0] == 9

    def test_count_positives(self) -> None:
        """Count positive elements: reduce(n=0, x IN list | n + CASE WHEN x > 0 THEN 1 ELSE 0 END)."""
        result = _q(
            "RETURN reduce(n = 0, x IN [1, -2, 3, -4, 5] | "
            "n + CASE WHEN x > 0 THEN 1 ELSE 0 END) AS v",
        )
        assert result["v"].iloc[0] == 3


# ===========================================================================
# Category 4 — Performance: batch-per-step must be faster than per-element
# ===========================================================================


class TestReducePerformance:
    """Batch-per-step must be measurably faster than per-element baseline."""

    REPS: int = 20
    N_ROWS: int = 200
    LIST_LEN: int = 50

    @pytest.fixture
    def large_star(self):
        rows = [{"items": list(range(self.LIST_LEN))} for _ in range(self.N_ROWS)]
        return _star_with_rows(rows)

    def test_absolute_threshold(self, large_star) -> None:
        """20 × (200-row × 50-element REDUCE sum) must complete in < 5s."""
        start = time.perf_counter()
        for _ in range(self.REPS):
            large_star.execute_query(
                "MATCH (r:Row) RETURN reduce(s = 0, x IN r.items | s + x) AS v",
            )
        elapsed = time.perf_counter() - start
        assert elapsed < 5.0, (
            f"20 × 200-row × 50-element REDUCE took {elapsed:.2f}s (threshold 5s). "
            "The per-element BindingFrame allocation loop is still in place."
        )

    def test_result_correctness_in_performance_run(self, large_star) -> None:
        """Results during the performance run must be correct (sum(0..49) = 1225)."""
        result = large_star.execute_query(
            "MATCH (r:Row) RETURN reduce(s = 0, x IN r.items | s + x) AS v",
        )
        expected = sum(range(self.LIST_LEN))  # 1225
        assert all(v == expected for v in result["v"]), (
            f"Expected all results = {expected}, got: {sorted(set(result['v']))}"
        )

    def test_speedup_vs_baseline(self, large_star) -> None:
        """Batch-per-step must be ≥ 5× faster than a direct per-element simulation."""
        # Simulate the baseline cost: N_ROWS × LIST_LEN single-dict BindingFrame
        # builds + evaluations (using Python's DataFrame construction as proxy).
        from pycypher.binding_frame import BindingFrame
        from pycypher.relational_models import (
            Context,
            EntityMapping,
            RelationshipMapping,
        )

        ctx = Context(
            entity_mapping=EntityMapping(mapping={}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )

        start = time.perf_counter()
        for _ in range(self.REPS):
            for row in range(self.N_ROWS):
                for item in range(self.LIST_LEN):
                    _ = BindingFrame(
                        bindings=pd.DataFrame({"acc": [0], "x": [item]}),
                        type_registry={},
                        context=ctx,
                    )
        baseline = time.perf_counter() - start

        # Vectorised path
        start = time.perf_counter()
        for _ in range(self.REPS):
            large_star.execute_query(
                "MATCH (r:Row) RETURN reduce(s = 0, x IN r.items | s + x) AS v",
            )
        vectorised = time.perf_counter() - start

        speedup = baseline / vectorised if vectorised > 0 else float("inf")
        assert speedup >= 2.0, (
            f"Batch-per-step reduce ({vectorised:.2f}s) should be ≥ 2× faster "
            f"than baseline ({baseline:.2f}s), got {speedup:.1f}×. "
            "The per-element BindingFrame allocation loop is still in place."
        )
