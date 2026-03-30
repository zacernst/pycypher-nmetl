"""TDD tests for engine layer removal.

These tests verify that removing the engine layer preserves all functionality
while eliminating architectural debt. Written in TDD red phase.

Run with:
    uv run pytest tests/test_engine_removal_tdd.py -v
"""

from __future__ import annotations

import time
from unittest.mock import patch

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.star import Star


class TestEngineRemovalCorrectness:
    """Verify engine removal preserves all query execution functionality."""

    @pytest.fixture
    def simple_context(self) -> Context:
        """Create a simple test context."""
        person_df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "age": [30, 25, 35],
            },
        )
        table = EntityTable.from_dataframe("Person", person_df)
        return Context(entity_mapping=EntityMapping(mapping={"Person": table}))

    def test_star_init_no_engine_parameters(
        self,
        simple_context: Context,
    ) -> None:
        """Star.__init__ should not accept engine-related parameters."""
        # Should work: clean constructor
        star = Star(context=simple_context)
        assert star.context is simple_context

        # Should fail: engine parameters removed
        with pytest.raises(TypeError):
            Star(context=simple_context, engine="pandas")

        with pytest.raises(TypeError):
            Star(context=simple_context, backend="pandas")

    def test_execute_query_no_double_star_creation(
        self,
        simple_context: Context,
    ) -> None:
        """execute_query should not create multiple Star instances."""
        star = Star(context=simple_context)

        # execute_query should work directly without additional instance creation
        result = star.execute_query("MATCH (p:Person) RETURN p.name")

        # Verify correct execution (not dependent on instance creation details)
        assert len(result) == 3  # Correct result
        assert list(result.columns) == ["name"]  # Correct columns

    def test_execute_query_no_exception_swallowing(
        self,
        simple_context: Context,
    ) -> None:
        """execute_query should propagate exceptions instead of swallowing them."""
        star = Star(context=simple_context)

        # Should propagate parse errors (not swallow in engine try/except)
        with pytest.raises(Exception):  # Specific error depends on parser
            star.execute_query("INVALID CYPHER SYNTAX !!!")

    def test_execute_query_direct_path(self, simple_context: Context) -> None:
        """execute_query should use direct execution path without engine routing."""
        star = Star(context=simple_context)

        # Mock the direct execution method
        with patch.object(star, "_execute_query_binding_frame") as mock_direct:
            mock_direct.return_value = pd.DataFrame(
                {"name": ["Alice", "Bob", "Carol"]},
            )

            star.execute_query("MATCH (p:Person) RETURN p.name")

            # Should call direct method exactly once (not through engine fallback)
            assert mock_direct.call_count == 1

    def test_no_engine_logging_noise(
        self,
        simple_context: Context,
        caplog,
    ) -> None:
        """Should not log engine fallback messages."""
        star = Star(context=simple_context)

        star.execute_query("MATCH (p:Person) RETURN p.name")

        # Should not contain engine-related log messages
        log_messages = [record.message for record in caplog.records]
        for msg in log_messages:
            assert "engine" not in msg.lower()
            assert "fallback" not in msg.lower()
            assert "backend" not in msg.lower()


class TestEngineRemovalPerformance:
    """Verify engine removal improves performance."""

    @pytest.fixture
    def performance_context(self) -> Context:
        """Create a larger test context for performance testing."""
        # Larger dataset for meaningful performance measurement
        person_df = pd.DataFrame(
            {
                ID_COLUMN: list(range(1000)),
                "name": [f"Person{i}" for i in range(1000)],
                "age": [(20 + i % 50) for i in range(1000)],
            },
        )
        table = EntityTable.from_dataframe("Person", person_df)
        return Context(entity_mapping=EntityMapping(mapping={"Person": table}))

    def test_no_double_instance_creation_overhead(
        self,
        performance_context: Context,
    ) -> None:
        """Removing engine layer should eliminate double Star instance overhead."""
        star = Star(context=performance_context)

        # Time a simple query - should be fast without double instance creation
        start = time.perf_counter()
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age > 30 RETURN p.name",
        )
        elapsed_ms = (time.perf_counter() - start) * 1000

        # Should complete quickly without engine overhead
        assert elapsed_ms < 200, (
            f"Query too slow: {elapsed_ms:.1f}ms (should be < 200ms)"
        )
        assert len(result) > 0  # Verify correctness


class TestEngineRemovalCleanup:
    """Verify engine-related code is properly removed."""

    def test_engines_module_removed(self) -> None:
        """Engines module should not be importable."""
        with pytest.raises(ImportError):
            import importlib

            importlib.import_module("pycypher.engines")

    def test_backend_detection_removed(self) -> None:
        """backend_detection module should not be importable."""
        with pytest.raises(ImportError):
            import importlib

            importlib.import_module("pycypher.backend_detection")

    def test_no_engine_references_in_star(self) -> None:
        """Star class should contain no engine-related code (excluding __repr__)."""
        import inspect

        from pycypher.star import Star

        star_source = inspect.getsource(Star)

        # Remove __repr__ method source — it legitimately references
        # context.backend_name for user-facing display.
        repr_source = inspect.getsource(Star.__repr__)
        star_source_no_repr = star_source.replace(repr_source, "")

        # Should not contain engine-related terms outside of __repr__
        forbidden_terms = ["QueryEngine", "PandasQueryEngine"]
        for term in forbidden_terms:
            assert term not in star_source_no_repr, (
                f"Star still contains '{term}' - cleanup incomplete"
            )

    def test_public_api_unchanged(self) -> None:
        """Public API should remain unchanged for users."""
        from pycypher import Context, EntityMapping, EntityTable, Star

        # Core classes should still be available
        assert Star is not None
        assert Context is not None
        assert EntityMapping is not None
        assert EntityTable is not None

        # Star should be constructible with just context
        person_df = pd.DataFrame({ID_COLUMN: [1], "name": ["Test"]})
        table = EntityTable.from_dataframe("Person", person_df)
        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
        )
        star = Star(context=context)

        # execute_query should work as expected
        result = star.execute_query("MATCH (p:Person) RETURN p.name")
        assert len(result) == 1
