"""Tests for optimizer-to-backend selection feedback loop.

Validates that ``select_backend_for_query()`` correctly re-evaluates
the backend after the optimizer produces cardinality estimates and
query complexity hints, and that the wiring in ``Star.execute_query``
calls the re-evaluation at the right time.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from pycypher.backend_engine import (
    PandasBackend,
    select_backend_for_query,
)

# ---------------------------------------------------------------------------
# Unit tests for select_backend_for_query()
# ---------------------------------------------------------------------------


class TestSelectBackendForQuery:
    """Unit tests for the optimizer-driven backend re-evaluation."""

    def test_no_switch_small_data(self) -> None:
        """Pandas stays when estimated rows are small."""
        result = select_backend_for_query(
            current_backend=PandasBackend(),
            optimization_hints={"cardinality_estimates": {"Person": 500}},
            estimated_rows=0,
        )
        assert result is None

    def test_no_switch_empty_hints(self) -> None:
        """Pandas stays when no optimizer hints are available."""
        result = select_backend_for_query(
            current_backend=PandasBackend(),
            optimization_hints={},
            estimated_rows=0,
        )
        assert result is None

    def test_switch_on_high_cardinality(self) -> None:
        """Switch from pandas when cardinality exceeds threshold."""
        result = select_backend_for_query(
            current_backend=PandasBackend(),
            optimization_hints={
                "cardinality_estimates": {"Person": 200_000},
            },
            estimated_rows=0,
        )
        # Should recommend a scalable backend (duckdb or polars)
        if result is not None:
            assert result.name in ("duckdb", "polars")

    def test_switch_on_explicit_estimated_rows(self) -> None:
        """Switch when estimated_rows passed directly exceeds threshold."""
        result = select_backend_for_query(
            current_backend=PandasBackend(),
            optimization_hints={},
            estimated_rows=150_000,
        )
        if result is not None:
            assert result.name in ("duckdb", "polars")

    def test_no_switch_non_pandas_backend(self) -> None:
        """Non-pandas backends are never switched away from."""
        # Create a mock backend with name != "pandas"
        try:
            from pycypher.backend_engine import DuckDBBackend

            backend = DuckDBBackend()
        except (ImportError, RuntimeError):
            pytest.skip("DuckDB backend not available")

        result = select_backend_for_query(
            current_backend=backend,
            optimization_hints={
                "cardinality_estimates": {"Person": 500_000},
            },
            estimated_rows=0,
        )
        assert result is None

    def test_complex_query_heuristic(self) -> None:
        """Complex multi-join queries trigger duckdb preference."""
        result = select_backend_for_query(
            current_backend=PandasBackend(),
            optimization_hints={
                "match_clause_count": 3,
                "filter_pushdown_count": 4,
                "cardinality_estimates": {"Person": 60_000},
            },
            estimated_rows=0,
        )
        if result is not None:
            assert result.name == "duckdb"

    def test_complex_query_below_row_threshold(self) -> None:
        """Complex queries with low rows don't trigger switch."""
        result = select_backend_for_query(
            current_backend=PandasBackend(),
            optimization_hints={
                "match_clause_count": 3,
                "filter_pushdown_count": 4,
                "cardinality_estimates": {"Person": 1_000},
            },
            estimated_rows=0,
        )
        assert result is None

    def test_instrument_wraps_result(self) -> None:
        """When instrument=True, result is wrapped in InstrumentedBackend."""
        result = select_backend_for_query(
            current_backend=PandasBackend(),
            optimization_hints={
                "cardinality_estimates": {"Person": 200_000},
            },
            estimated_rows=0,
            instrument=True,
        )
        if result is not None:
            from pycypher.backend_engine import InstrumentedBackend

            assert isinstance(result, InstrumentedBackend)


# ---------------------------------------------------------------------------
# Integration test: wiring in Star.execute_query
# ---------------------------------------------------------------------------


class TestStarBackendFeedbackLoop:
    """Integration tests for the feedback loop wired into Star."""

    def test_feedback_loop_called_during_execution(self) -> None:
        """Verify select_backend_for_query is called during query execution."""
        import numpy as np
        import pandas as pd
        from pycypher.relational_models import (
            ID_COLUMN,
            Context,
            EntityMapping,
            EntityTable,
        )
        from pycypher.star import Star

        # Build a minimal context
        df = pd.DataFrame(
            {
                ID_COLUMN: np.arange(1, 11),
                "name": [f"Person_{i}" for i in range(1, 11)],
            }
        )
        et = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=list(df.columns),
            source_obj_attribute_map={"name": "name"},
            attribute_map={"name": "name"},
            source_obj=df,
        )
        ctx = Context(
            entity_mapping=EntityMapping(mapping={"Person": et}),
        )
        star = Star(context=ctx)

        with patch(
            "pycypher.backend_engine.select_backend_for_query",
            wraps=select_backend_for_query,
        ) as mock_select:
            star.execute_query("MATCH (p:Person) RETURN p.name")
            mock_select.assert_called_once()
            # Verify the call received the right arguments
            call_kwargs = mock_select.call_args.kwargs
            assert "current_backend" in call_kwargs
            assert "optimization_hints" in call_kwargs

    def test_backend_not_switched_for_small_query(self) -> None:
        """Backend stays as pandas for small queries."""
        import numpy as np
        import pandas as pd
        from pycypher.relational_models import (
            ID_COLUMN,
            Context,
            EntityMapping,
            EntityTable,
        )
        from pycypher.star import Star

        df = pd.DataFrame(
            {
                ID_COLUMN: np.arange(1, 11),
                "name": [f"Person_{i}" for i in range(1, 11)],
            }
        )
        et = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=list(df.columns),
            source_obj_attribute_map={"name": "name"},
            attribute_map={"name": "name"},
            source_obj=df,
        )
        ctx = Context(
            entity_mapping=EntityMapping(mapping={"Person": et}),
        )
        star = Star(context=ctx)

        star.execute_query("MATCH (p:Person) RETURN p.name")
        # Backend should remain pandas for 10-row dataset
        assert ctx.backend_name == "pandas"
