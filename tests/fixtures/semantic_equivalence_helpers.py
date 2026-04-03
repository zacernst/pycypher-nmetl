"""Helpers for semantic equivalence validation of multi-query composition.

Provides utilities to compare sequential execution of multiple Cypher queries
against a single combined query, asserting that the results are identical.

Architecture
------------

::

    SemanticValidator
    ├── execute_sequential()   — run queries one-by-one, feeding context forward
    ├── execute_combined()     — combine queries via QueryCombiner, run once
    ├── assert_equivalent()    — compare two DataFrames with helpful diffs
    └── build_context()        — construct test Context from node/edge dicts

    TestScenario (dataclass)
    ├── name                   — descriptive scenario label
    ├── context_nodes          — {label: DataFrame} for entities
    ├── context_edges          — {type: DataFrame} for relationships
    └── queries                — [(query_id, cypher_string)]

"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd
from pycypher.ingestion.context_builder import ContextBuilder
from pycypher.query_combiner import QueryCombiner
from pycypher.relational_models import Context
from pycypher.star import Star


@dataclass
class TestScenario:
    __test__ = False  # Prevent pytest collection (not a test class)
    """A complete test scenario for semantic equivalence validation.

    Attributes:
        name: Human-readable description of the scenario.
        context_nodes: Mapping of ``{label: DataFrame}`` for entity tables.
            Each DataFrame must include an ``__ID__`` column.
        context_edges: Mapping of ``{type: DataFrame}`` for relationship tables.
            Each DataFrame must include ``__SOURCE__`` and ``__TARGET__`` columns.
        queries: Ordered list of ``(query_id, cypher_string)`` pairs.
        expected_columns: If set, assert that the result contains exactly these columns.
        expected_row_count: If set, assert that the result has this many rows.

    """

    name: str
    context_nodes: dict[str, pd.DataFrame]
    context_edges: dict[str, pd.DataFrame] = field(default_factory=dict)
    queries: list[tuple[str, str]] = field(default_factory=list)
    expected_columns: list[str] | None = None
    expected_row_count: int | None = None


def build_context(
    nodes: dict[str, pd.DataFrame],
    edges: dict[str, pd.DataFrame] | None = None,
) -> Context:
    """Build a :class:`Context` from node and edge DataFrames.

    Uses :meth:`ContextBuilder.from_dict` which auto-detects entities vs
    relationships based on the presence of ``__SOURCE__``/``__TARGET__``
    columns, and properly normalizes all tables.

    Args:
        nodes: Mapping of ``{label: DataFrame}`` for entity tables.
        edges: Mapping of ``{type: DataFrame}`` for relationship tables.
            Each DataFrame must include ``__SOURCE__`` and ``__TARGET__`` columns.

    Returns:
        A fully-constructed :class:`Context` for query execution.

    """
    combined: dict[str, pd.DataFrame] = dict(nodes)
    if edges:
        combined.update(edges)
    return ContextBuilder.from_dict(combined)


def execute_sequential(
    context: Context,
    queries: list[tuple[str, str]],
) -> pd.DataFrame:
    """Execute queries sequentially, returning the last query's result.

    Each query is executed independently against the same context.
    This establishes the **baseline** for semantic equivalence: the result
    of the final query when run in isolation.

    Args:
        context: The data context to execute against.
        queries: Ordered list of ``(query_id, cypher_string)`` pairs.

    Returns:
        DataFrame result from the **last** query in the list.

    """
    star = Star(context=context)
    result = pd.DataFrame()
    for _query_id, cypher in queries:
        result = star.execute_query(cypher)
    return result


def execute_combined(
    context: Context,
    queries: list[tuple[str, str]],
) -> pd.DataFrame:
    """Combine queries via QueryCombiner and execute the combined query.

    Uses :class:`~pycypher.query_combiner.QueryCombiner` to merge the
    queries into a single Cypher string, then executes against the context.

    Args:
        context: The data context to execute against.
        queries: Ordered list of ``(query_id, cypher_string)`` pairs.

    Returns:
        DataFrame result from the combined query execution.

    """
    combiner = QueryCombiner()
    combined_cypher = combiner.combine(queries)
    star = Star(context=context)
    return star.execute_query(combined_cypher)


def assert_dataframes_equivalent(
    actual: pd.DataFrame,
    expected: pd.DataFrame,
    scenario_name: str = "",
    *,
    check_column_order: bool = False,
    check_row_order: bool = False,
) -> None:
    """Assert two DataFrames contain equivalent data with helpful diff messages.

    Args:
        actual: The DataFrame to validate (from combined execution).
        expected: The reference DataFrame (from sequential execution).
        scenario_name: Label for error messages.
        check_column_order: If True, assert columns are in the same order.
        check_row_order: If True, assert rows are in the same order.

    Raises:
        AssertionError: With detailed diff information on mismatch.

    """
    prefix = f"[{scenario_name}] " if scenario_name else ""

    # Column comparison — sort unless order matters
    actual_cols = list(actual.columns)
    expected_cols = list(expected.columns)

    if check_column_order:
        assert actual_cols == expected_cols, (
            f"{prefix}Column order mismatch.\n"
            f"  Actual:   {actual_cols}\n"
            f"  Expected: {expected_cols}"
        )
    else:
        assert set(actual_cols) == set(expected_cols), (
            f"{prefix}Column set mismatch.\n"
            f"  Extra in actual:   {set(actual_cols) - set(expected_cols)}\n"
            f"  Missing in actual: {set(expected_cols) - set(actual_cols)}"
        )

    # Row count comparison
    assert len(actual) == len(expected), (
        f"{prefix}Row count mismatch.\n"
        f"  Actual:   {len(actual)} rows\n"
        f"  Expected: {len(expected)} rows"
    )

    # Normalize column order for comparison
    cols = sorted(expected.columns)
    a = actual[cols].reset_index(drop=True)
    e = expected[cols].reset_index(drop=True)

    if not check_row_order:
        # Sort both DataFrames by all columns for order-independent comparison
        a = a.sort_values(by=cols, ignore_index=True)
        e = e.sort_values(by=cols, ignore_index=True)

    # Value comparison with detailed diff
    if not a.equals(e):
        # Find specific differences
        diff_mask = a != e
        diff_rows = diff_mask.any(axis=1)
        diff_count = diff_rows.sum()

        diff_details = []
        for idx in diff_rows[diff_rows].index[:5]:  # Show first 5 diffs
            for col in cols:
                if a.at[idx, col] != e.at[idx, col]:
                    diff_details.append(
                        f"  Row {idx}, col '{col}': "
                        f"actual={a.at[idx, col]!r}, expected={e.at[idx, col]!r}",
                    )

        msg = (
            f"{prefix}DataFrame values differ in {diff_count} row(s).\n"
            + "\n".join(
                diff_details,
            )
        )
        if diff_count > 5:
            msg += f"\n  ... and {diff_count - 5} more differences"

        raise AssertionError(msg)


def assert_semantic_equivalence(
    scenario: TestScenario,
    *,
    check_column_order: bool = False,
    check_row_order: bool = False,
) -> None:
    """Validate that combined execution produces identical results to sequential.

    This is the **primary validation entry point** for semantic equivalence.
    It builds the context, runs both sequential and combined execution paths,
    and asserts the results are equivalent.

    Args:
        scenario: A :class:`TestScenario` describing the test case.
        check_column_order: If True, assert column ordering matches.
        check_row_order: If True, assert row ordering matches.

    Raises:
        AssertionError: With detailed diff if results diverge.

    """
    context = build_context(scenario.context_nodes, scenario.context_edges)

    sequential_result = execute_sequential(context, scenario.queries)
    combined_result = execute_combined(context, scenario.queries)

    # Optional assertions on expected shape
    if scenario.expected_columns is not None:
        assert set(sequential_result.columns) >= set(
            scenario.expected_columns,
        ), (
            f"[{scenario.name}] Sequential result missing expected columns: "
            f"{set(scenario.expected_columns) - set(sequential_result.columns)}"
        )

    if scenario.expected_row_count is not None:
        assert len(sequential_result) == scenario.expected_row_count, (
            f"[{scenario.name}] Sequential result has {len(sequential_result)} rows, "
            f"expected {scenario.expected_row_count}"
        )

    assert_dataframes_equivalent(
        actual=combined_result,
        expected=sequential_result,
        scenario_name=scenario.name,
        check_column_order=check_column_order,
        check_row_order=check_row_order,
    )
