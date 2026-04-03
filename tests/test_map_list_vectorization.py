"""TDD tests for vectorised map literal, map projection, and list literal
assembly (Loop 185 — Performance).

Problem: three expression assemblers in ``binding_evaluator.py`` use a
nested per-row Python loop to build per-row dicts/lists from already-evaluated
Series::

    # Map literal  (_eval_map_literal, lines 2434-2437)
    [{k: evaluated[k].iloc[i] for k in keys} for i in range(n)]

    # Map projection (_eval_map_projection, lines 2486-2492)
    for i in range(n):
        for key, series in columns:
            row_dict[key] = series.iloc[i] ...

    # List literal  (ListLiteral branch, lines 867-871)
    [[s.iloc[i] ... for s in elem_series] for i in range(n)]

For 500 rows with 10 keys/elements each, these loops execute 5 000
Python ``iloc`` calls each — pure Python overhead with no algorithmic
necessity.

Fix: replace all three with a single vectorised pass:

1. Stack all evaluated Series into a ``pd.DataFrame`` (each Series becomes
   a column — C-level operation).
2. Call ``DataFrame.to_dict('records')`` (map literal / map projection) or
   ``DataFrame.values.tolist()`` (list literal) — both are Cython paths that
   avoid per-element Python dispatch.

Allocations / Python function calls drop from O(n × m) to O(m) (one
Series→column conversion per key/element, independent of row count).

All tests written before the implementation (TDD red phase).
"""

from __future__ import annotations

import time

import pandas as pd
from _perf_helpers import perf_threshold

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


# ===========================================================================
# Category 1 — Map Literal correctness
# ===========================================================================


class TestMapLiteralCorrectness:
    """_eval_map_literal must produce correct dicts regardless of row count."""

    def test_empty_map_literal(self) -> None:
        """RETURN {} AS m — each row gets an empty dict."""
        result = _q("RETURN {} AS m")
        assert result["m"].iloc[0] == {}

    def test_single_key_constant(self) -> None:
        """RETURN {name: 'Alice'} AS m — single constant key."""
        result = _q("RETURN {name: 'Alice'} AS m")
        assert result["m"].iloc[0] == {"name": "Alice"}

    def test_multiple_keys_constants(self) -> None:
        """RETURN {a: 1, b: 2, c: 3} AS m — multiple constant keys."""
        result = _q("RETURN {a: 1, b: 2, c: 3} AS m")
        m = result["m"].iloc[0]
        assert m["a"] == 1
        assert m["b"] == 2
        assert m["c"] == 3

    def test_map_literal_with_property(self) -> None:
        """Map literal referencing a property — correct per-row values."""
        star = _star_with_rows(
            [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
            ],
        )
        result = star.execute_query(
            "MATCH (r:Row) RETURN {label: r.name, years: r.age} AS m ORDER BY id(r)",
        )
        assert result["m"].iloc[0] == {"label": "Alice", "years": 30}
        assert result["m"].iloc[1] == {"label": "Bob", "years": 25}

    def test_map_keys_present_for_all_rows(self) -> None:
        """All keys must appear in every row's dict, even with varying values."""
        star = _star_with_rows(
            [
                {"x": 1},
                {"x": 2},
                {"x": 3},
            ],
        )
        result = star.execute_query(
            "MATCH (r:Row) RETURN {val: r.x, doubled: r.x * 2} AS m",
        )
        for i, row in result["m"].items():
            assert "val" in row
            assert "doubled" in row
            assert row["doubled"] == row["val"] * 2

    def test_map_with_null_value(self) -> None:
        """A null property value must appear as null in the map, not absent."""
        star = _star_with_rows([{"a": None}, {"a": 5}])
        result = star.execute_query(
            "MATCH (r:Row) RETURN {key: r.a} AS m ORDER BY id(r)",
        )
        assert pd.isna(result["m"].iloc[0]["key"])
        assert result["m"].iloc[1]["key"] == 5


# ===========================================================================
# Category 2 — Map Projection correctness
# ===========================================================================


class TestMapProjectionCorrectness:
    """_eval_map_projection must produce correct dicts for all projection forms."""

    def test_single_property_selector(self) -> None:
        """p{.name} — single named property."""
        star = _star_with_rows([{"name": "Alice"}, {"name": "Bob"}])
        result = star.execute_query(
            "MATCH (r:Row) RETURN r{.name} AS m ORDER BY id(r)",
        )
        assert result["m"].iloc[0] == {"name": "Alice"}
        assert result["m"].iloc[1] == {"name": "Bob"}

    def test_multiple_property_selectors(self) -> None:
        """p{.name, .age} — multiple named properties."""
        star = _star_with_rows(
            [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
            ],
        )
        result = star.execute_query(
            "MATCH (r:Row) RETURN r{.name, .age} AS m ORDER BY id(r)",
        )
        assert result["m"].iloc[0] == {"name": "Alice", "age": 30}
        assert result["m"].iloc[1] == {"name": "Bob", "age": 25}

    def test_computed_key_expression(self) -> None:
        """p{.name, doubled: p.age * 2} — mix of property and computed."""
        star = _star_with_rows([{"name": "Carol", "age": 20}])
        result = star.execute_query(
            "MATCH (r:Row) RETURN r{.name, twice: r.age * 2} AS m",
        )
        m = result["m"].iloc[0]
        assert m["name"] == "Carol"
        assert m["twice"] == 40

    def test_all_rows_have_same_keys(self) -> None:
        """Every row's projected dict must contain the same set of keys."""
        star = _star_with_rows(
            [
                {"name": "A", "score": 1},
                {"name": "B", "score": 2},
                {"name": "C", "score": 3},
            ],
        )
        result = star.execute_query(
            "MATCH (r:Row) RETURN r{.name, .score} AS m",
        )
        for m in result["m"]:
            assert set(m.keys()) == {"name", "score"}

    def test_empty_projection(self) -> None:
        """A map projection with no elements returns an empty dict per row."""
        star = _star_with_rows([{"name": "X"}])
        result = star.execute_query("MATCH (r:Row) RETURN r{} AS m")
        assert result["m"].iloc[0] == {}


# ===========================================================================
# Category 3 — List Literal correctness
# ===========================================================================


class TestListLiteralCorrectness:
    """List literal assembly must produce correct per-row lists."""

    def test_constant_list(self) -> None:
        """RETURN [1, 2, 3] AS lst — all rows get the same list."""
        result = _q("RETURN [1, 2, 3] AS lst")
        assert list(result["lst"].iloc[0]) == [1, 2, 3]

    def test_list_with_expressions(self) -> None:
        """List literal with arithmetic expressions."""
        result = _q("RETURN [1 + 1, 2 * 3, 10 - 4] AS lst")
        assert list(result["lst"].iloc[0]) == [2, 6, 6]

    def test_list_with_property_values(self) -> None:
        """Per-row list assembly from property values."""
        star = _star_with_rows(
            [
                {"a": 10, "b": 20},
                {"a": 30, "b": 40},
            ],
        )
        result = star.execute_query(
            "MATCH (r:Row) RETURN [r.a, r.b] AS lst ORDER BY id(r)",
        )
        assert list(result["lst"].iloc[0]) == [10, 20]
        assert list(result["lst"].iloc[1]) == [30, 40]

    def test_single_element_list(self) -> None:
        """[expr] — single-element list."""
        result = _q("RETURN [42] AS lst")
        assert list(result["lst"].iloc[0]) == [42]

    def test_list_with_null_element(self) -> None:
        """[1, null, 3] — null element preserved in list."""
        star = _star_with_rows([{"x": None}])
        result = star.execute_query("MATCH (r:Row) RETURN [1, r.x, 3] AS lst")
        lst = result["lst"].iloc[0]
        assert lst[0] == 1
        assert pd.isna(lst[1])
        assert lst[2] == 3

    def test_multi_row_each_row_correct(self) -> None:
        """100 rows each with a 3-element list from properties."""
        rows = [{"a": i, "b": i * 2, "c": i * 3} for i in range(100)]
        star = _star_with_rows(rows)
        result = star.execute_query(
            "MATCH (r:Row) RETURN [r.a, r.b, r.c] AS lst",
        )
        for i, lst in enumerate(result["lst"]):
            assert lst[0] == i
            assert lst[1] == i * 2
            assert lst[2] == i * 3


# ===========================================================================
# Category 4 — Performance: vectorised path must beat iloc-loop baseline
# ===========================================================================


REPS = 20
N_ROWS = 500
N_KEYS = 10


def _make_wide_star(n_rows: int, n_cols: int) -> Star:
    """Create a Star with n_rows rows and n_cols named properties."""
    rows = [
        {f"c{j}": i * n_cols + j for j in range(n_cols)} for i in range(n_rows)
    ]
    return _star_with_rows(rows)


class TestMapLiteralPerformance:
    """Map literal assembly must complete within an absolute time threshold."""

    def test_map_literal_absolute_threshold(self) -> None:
        """20 × (500-row map literal with 10 keys) must complete in < 2s."""
        star = _make_wide_star(N_ROWS, N_KEYS)
        # Build a query: RETURN {c0: r.c0, c1: r.c1, ..., c9: r.c9} AS m
        keys_clause = ", ".join(f"c{j}: r.c{j}" for j in range(N_KEYS))
        query = f"MATCH (r:Row) RETURN {{{keys_clause}}} AS m"
        start = time.perf_counter()
        for _ in range(REPS):
            star.execute_query(query)
        elapsed = time.perf_counter() - start
        assert elapsed < perf_threshold(2.0), (
            f"20 × 500-row × {N_KEYS}-key map literal took {elapsed:.2f}s "
            "(threshold 2s). The iloc-loop is still in place."
        )

    def test_map_literal_speedup_vs_iloc_baseline(self) -> None:
        """Vectorised map literal must be ≥ 2× faster than a direct iloc loop."""
        import numpy as np

        star = _make_wide_star(N_ROWS, N_KEYS)
        keys_clause = ", ".join(f"c{j}: r.c{j}" for j in range(N_KEYS))
        query = f"MATCH (r:Row) RETURN {{{keys_clause}}} AS m"

        # Baseline: simulate iloc loop cost — one .iloc[i] call per row per key
        fake_series = [
            pd.Series(np.arange(N_ROWS), dtype=object) for _ in range(N_KEYS)
        ]
        keys = [f"c{j}" for j in range(N_KEYS)]
        start = time.perf_counter()
        for _ in range(REPS):
            [
                {k: fake_series[j].iloc[i] for j, k in enumerate(keys)}
                for i in range(N_ROWS)
            ]
        baseline = time.perf_counter() - start

        # Vectorised path
        start = time.perf_counter()
        for _ in range(REPS):
            star.execute_query(query)
        vectorised = time.perf_counter() - start

        speedup = baseline / vectorised if vectorised > 0 else float("inf")
        assert speedup >= 2.0, (
            f"Vectorised map literal ({vectorised:.3f}s) should be ≥ 2× faster "
            f"than iloc baseline ({baseline:.3f}s), got {speedup:.1f}×. "
            "The iloc-loop is still in place."
        )


class TestMapProjectionPerformance:
    """Map projection assembly must complete within an absolute time threshold."""

    def test_map_projection_absolute_threshold(self) -> None:
        """20 × (500-row map projection with 10 properties) must complete in < 2s."""
        star = _make_wide_star(N_ROWS, N_KEYS)
        props_clause = ", ".join(f".c{j}" for j in range(N_KEYS))
        query = f"MATCH (r:Row) RETURN r{{{props_clause}}} AS m"
        start = time.perf_counter()
        for _ in range(REPS):
            star.execute_query(query)
        elapsed = time.perf_counter() - start
        assert elapsed < perf_threshold(2.0), (
            f"20 × 500-row × {N_KEYS}-property map projection took {elapsed:.2f}s "
            "(threshold 2s). The iloc-loop is still in place."
        )


class TestListLiteralPerformance:
    """List literal assembly must complete within an absolute time threshold."""

    def test_list_literal_absolute_threshold(self) -> None:
        """20 × (500-row list literal with 10 elements) must complete in < 2s."""
        star = _make_wide_star(N_ROWS, N_KEYS)
        elems_clause = ", ".join(f"r.c{j}" for j in range(N_KEYS))
        query = f"MATCH (r:Row) RETURN [{elems_clause}] AS lst"
        start = time.perf_counter()
        for _ in range(REPS):
            star.execute_query(query)
        elapsed = time.perf_counter() - start
        assert elapsed < perf_threshold(2.0), (
            f"20 × 500-row × {N_KEYS}-element list literal took {elapsed:.2f}s "
            "(threshold 2s). The iloc-loop is still in place."
        )

    def test_list_literal_speedup_vs_iloc_baseline(self) -> None:
        """Vectorised list literal must be ≥ 2× faster than a direct iloc loop."""
        import numpy as np

        star = _make_wide_star(N_ROWS, N_KEYS)
        elems_clause = ", ".join(f"r.c{j}" for j in range(N_KEYS))
        query = f"MATCH (r:Row) RETURN [{elems_clause}] AS lst"

        fake_series = [
            pd.Series(np.arange(N_ROWS), dtype=object) for _ in range(N_KEYS)
        ]
        start = time.perf_counter()
        for _ in range(REPS):
            [[s.iloc[i] for s in fake_series] for i in range(N_ROWS)]
        baseline = time.perf_counter() - start

        start = time.perf_counter()
        for _ in range(REPS):
            star.execute_query(query)
        vectorised = time.perf_counter() - start

        speedup = baseline / vectorised if vectorised > 0 else float("inf")
        assert speedup >= 2.0, (
            f"Vectorised list literal ({vectorised:.3f}s) should be ≥ 2× faster "
            f"than iloc baseline ({baseline:.3f}s), got {speedup:.1f}×. "
            "The iloc-loop is still in place."
        )
