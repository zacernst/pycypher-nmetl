"""Tests for async query execution API (Task #27).

Verifies that ``Star.execute_query_async()`` provides:
1. Async/await interface wrapping sync execution
2. Proper result propagation (same DataFrame as sync)
3. Parameter forwarding (parameters, timeout, etc.)
4. Exception propagation through the async boundary
5. Concurrent async execution without blocking

Uses ``asyncio.run()`` to avoid requiring ``pytest-asyncio``.

Run with:
    uv run pytest tests/test_async_query_execution.py -v
"""

from __future__ import annotations

import asyncio
import inspect

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star


@pytest.fixture()
def simple_star() -> Star:
    """Three-person context: Alice (30), Bob (25), Carol (35)."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        }
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
    )


class TestAsyncMethodExists:
    """Verify execute_query_async is available on Star."""

    def test_star_has_async_method(self, simple_star: Star) -> None:
        assert hasattr(simple_star, "execute_query_async")

    def test_async_method_is_coroutine_function(
        self, simple_star: Star
    ) -> None:
        assert inspect.iscoroutinefunction(simple_star.execute_query_async)


class TestAsyncBasicExecution:
    """Verify async execution returns correct results."""

    def test_async_returns_dataframe(self, simple_star: Star) -> None:
        async def _run() -> pd.DataFrame:
            return await simple_star.execute_query_async(
                "MATCH (p:Person) RETURN p.name AS name"
            )

        result = asyncio.run(_run())
        assert isinstance(result, pd.DataFrame)

    def test_async_result_matches_sync(self, simple_star: Star) -> None:
        query = "MATCH (p:Person) RETURN p.name AS name ORDER BY name"

        async def _run() -> pd.DataFrame:
            return await simple_star.execute_query_async(query)

        sync_result = simple_star.execute_query(query)
        async_result = asyncio.run(_run())
        pd.testing.assert_frame_equal(sync_result, async_result)

    def test_async_returns_correct_rows(self, simple_star: Star) -> None:
        async def _run() -> pd.DataFrame:
            return await simple_star.execute_query_async(
                "MATCH (p:Person) RETURN p.name AS name"
            )

        result = asyncio.run(_run())
        assert len(result) == 3
        assert set(result["name"]) == {"Alice", "Bob", "Carol"}


class TestAsyncParameterForwarding:
    """Verify parameters are correctly forwarded."""

    def test_async_with_parameters(self, simple_star: Star) -> None:
        async def _run() -> pd.DataFrame:
            return await simple_star.execute_query_async(
                "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name AS name",
                parameters={"min_age": 28},
            )

        result = asyncio.run(_run())
        assert set(result["name"]) == {"Alice", "Carol"}

    def test_async_with_timeout(self, simple_star: Star) -> None:
        async def _run() -> pd.DataFrame:
            return await simple_star.execute_query_async(
                "MATCH (p:Person) RETURN p.name AS name",
                timeout_seconds=10.0,
            )

        result = asyncio.run(_run())
        assert len(result) == 3


class TestAsyncErrorPropagation:
    """Verify exceptions propagate through the async boundary."""

    def test_async_propagates_value_error(self, simple_star: Star) -> None:
        async def _run() -> pd.DataFrame:
            return await simple_star.execute_query_async("")

        with pytest.raises(ValueError, match="empty"):
            asyncio.run(_run())

    def test_async_propagates_type_error(self, simple_star: Star) -> None:
        async def _run() -> pd.DataFrame:
            return await simple_star.execute_query_async(
                "MATCH (p:Person) RETURN p.name AS name",
                parameters="not a dict",  # type: ignore[arg-type]
            )

        with pytest.raises(TypeError):
            asyncio.run(_run())


class TestAsyncConcurrency:
    """Verify multiple async queries can run concurrently."""

    def test_concurrent_async_queries(self, simple_star: Star) -> None:
        async def _run() -> list[pd.DataFrame]:
            queries = [
                "MATCH (p:Person) WHERE p.age = 30 RETURN p.name AS name",
                "MATCH (p:Person) WHERE p.age = 25 RETURN p.name AS name",
                "MATCH (p:Person) WHERE p.age = 35 RETURN p.name AS name",
            ]
            return await asyncio.gather(
                *(simple_star.execute_query_async(q) for q in queries)
            )

        results = asyncio.run(_run())
        assert len(results) == 3
        names = {r["name"].iloc[0] for r in results}
        assert names == {"Alice", "Bob", "Carol"}
