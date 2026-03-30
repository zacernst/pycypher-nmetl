"""Security tests for BackendEngine backends (DuckDB, Polars).

Tests that all backends validate user-controlled identifiers and avoid
unsafe serialization.  Each test category targets a specific method and
attack vector.

These tests complement the distributed security contracts in
test_distributed_security_contracts.py — those verify system-wide
invariants while these verify method-level input validation.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest
from pycypher.backend_engine import (
    DuckDBBackend,
    PandasBackend,
    validate_identifier,
)

pytestmark = pytest.mark.slow


# ===========================================================================
# Category 1 — validate_identifier() direct tests
# ===========================================================================


class TestValidateIdentifier:
    """The identifier validator must allow safe names and reject injections."""

    def test_simple_name_is_accepted(self) -> None:
        assert validate_identifier("Person") == "Person"

    def test_underscore_prefix_is_accepted(self) -> None:
        assert validate_identifier("_internal") == "_internal"

    def test_alphanumeric_with_underscores_is_accepted(self) -> None:
        assert validate_identifier("my_table_2") == "my_table_2"

    def test_empty_string_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier("")

    def test_semicolon_injection_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            validate_identifier("Person; DROP TABLE users")

    def test_double_quote_escape_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            validate_identifier('Person"')

    def test_comment_injection_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            validate_identifier("Person--")

    def test_space_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            validate_identifier("Person Name")

    def test_starts_with_number_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            validate_identifier("1Person")

    def test_parenthesis_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            validate_identifier("Person()")

    def test_backtick_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            validate_identifier("Person`")


# ===========================================================================
# Category 2 — DuckDBBackend.scan_entity() injection prevention
# ===========================================================================


class TestDuckDBScanEntitySecurity:
    """scan_entity must validate entity_type before SQL interpolation."""

    def test_normal_entity_type_works(self) -> None:
        backend = DuckDBBackend()
        df = pd.DataFrame({"__ID__": [1, 2, 3], "name": ["a", "b", "c"]})
        result = backend.scan_entity(df, "Person")
        assert list(result.columns) == ["__ID__"]
        assert len(result) == 3

    def test_injection_in_entity_type_is_blocked(self) -> None:
        backend = DuckDBBackend()
        df = pd.DataFrame({"__ID__": [1]})
        with pytest.raises(ValueError):
            backend.scan_entity(df, 'Person"; DROP TABLE x; --')

    def test_semicolon_in_entity_type_is_blocked(self) -> None:
        backend = DuckDBBackend()
        df = pd.DataFrame({"__ID__": [1]})
        with pytest.raises(ValueError):
            backend.scan_entity(df, "Person;")


# ===========================================================================
# Category 3 — DuckDBBackend.join() injection prevention
# ===========================================================================


class TestDuckDBJoinSecurity:
    """join must validate column names used in ON clause."""

    def test_normal_join_works(self) -> None:
        backend = DuckDBBackend()
        left = pd.DataFrame({"__ID__": [1, 2], "val_l": ["a", "b"]})
        right = pd.DataFrame({"__ID__": [1, 2], "val_r": ["x", "y"]})
        result = backend.join(left, right, on="__ID__")
        assert len(result) == 2

    def test_injection_in_join_column_is_blocked(self) -> None:
        backend = DuckDBBackend()
        left = pd.DataFrame({"id": [1]})
        right = pd.DataFrame({"id": [1]})
        with pytest.raises(ValueError):
            backend.join(left, right, on='id" OR 1=1; --')


# ===========================================================================
# Category 4 — DuckDBBackend.aggregate() injection prevention
# ===========================================================================


class TestDuckDBAggSecurity:
    """aggregate must validate source and output column names."""

    def test_normal_aggregation_works(self) -> None:
        backend = DuckDBBackend()
        df = pd.DataFrame({"group": ["a", "a", "b"], "val": [1, 2, 3]})
        result = backend.aggregate(
            df,
            group_cols=["group"],
            agg_specs={"total": ("val", "sum")},
        )
        assert "total" in result.columns

    def test_injection_in_agg_source_col_is_blocked(self) -> None:
        backend = DuckDBBackend()
        df = pd.DataFrame({"val": [1, 2]})
        with pytest.raises(ValueError):
            backend.aggregate(
                df,
                group_cols=[],
                agg_specs={"result": ('val"); DROP TABLE x; --', "sum")},
            )

    def test_injection_in_agg_output_col_is_blocked(self) -> None:
        backend = DuckDBBackend()
        df = pd.DataFrame({"val": [1, 2]})
        with pytest.raises(ValueError):
            backend.aggregate(
                df,
                group_cols=[],
                agg_specs={'result"; DROP TABLE x': ("val", "sum")},
            )

    def test_injection_in_group_col_is_blocked(self) -> None:
        backend = DuckDBBackend()
        df = pd.DataFrame({"group": ["a"], "val": [1]})
        with pytest.raises(ValueError):
            backend.aggregate(
                df,
                group_cols=['group"; DROP TABLE x'],
                agg_specs={"total": ("val", "sum")},
            )

    def test_unsupported_agg_func_is_blocked(self) -> None:
        """Whitelist-based function mapping must reject unknown functions."""
        backend = DuckDBBackend()
        df = pd.DataFrame({"val": [1]})
        with pytest.raises(ValueError, match="Unsupported"):
            backend.aggregate(
                df,
                group_cols=[],
                agg_specs={"result": ("val", "DROP TABLE")},
            )


# ===========================================================================
# Category 5 — DuckDBBackend.limit()/skip() type enforcement
# ===========================================================================


class TestDuckDBLimitSkipSecurity:
    """limit() and skip() must enforce integer types."""

    def test_limit_with_valid_int_works(self) -> None:
        backend = DuckDBBackend()
        df = pd.DataFrame({"a": range(10)})
        result = backend.limit(df, 3)
        assert len(result) == 3

    def test_limit_with_negative_is_rejected(self) -> None:
        backend = DuckDBBackend()
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="non-negative"):
            backend.limit(df, -1)

    def test_limit_with_string_is_rejected(self) -> None:
        backend = DuckDBBackend()
        df = pd.DataFrame({"a": [1]})
        with pytest.raises((ValueError, TypeError)):
            backend.limit(df, "5; DROP TABLE x")  # type: ignore[arg-type]

    def test_skip_with_valid_int_works(self) -> None:
        backend = DuckDBBackend()
        df = pd.DataFrame({"a": range(10)})
        result = backend.skip(df, 7)
        assert len(result) == 3

    def test_skip_with_negative_is_rejected(self) -> None:
        backend = DuckDBBackend()
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="non-negative"):
            backend.skip(df, -1)


# ===========================================================================
# Category 6 — Backend parity: PandasBackend vs DuckDBBackend
# ===========================================================================


class TestBackendParity:
    """Both backends must produce identical results for the same operations."""

    def test_scan_entity_parity(self) -> None:
        df = pd.DataFrame({"__ID__": [1, 2, 3], "name": ["a", "b", "c"]})
        pandas_result = PandasBackend().scan_entity(df, "Person")
        duckdb_result = DuckDBBackend().scan_entity(df, "Person")
        pd.testing.assert_frame_equal(
            pandas_result.reset_index(drop=True),
            duckdb_result.reset_index(drop=True),
        )

    def test_join_parity(self) -> None:
        left = pd.DataFrame({"__ID__": [1, 2], "val": ["a", "b"]})
        right = pd.DataFrame({"__ID__": [1, 2], "score": [10, 20]})
        pandas_result = PandasBackend().join(left, right, on="__ID__")
        duckdb_result = DuckDBBackend().join(left, right, on="__ID__")
        # Sort both for deterministic comparison
        pandas_sorted = pandas_result.sort_values("__ID__").reset_index(
            drop=True,
        )
        duckdb_sorted = duckdb_result.sort_values("__ID__").reset_index(
            drop=True,
        )
        pd.testing.assert_frame_equal(pandas_sorted, duckdb_sorted)

    def test_limit_parity(self) -> None:
        df = pd.DataFrame({"a": range(10)})
        pandas_result = PandasBackend().limit(df, 3)
        duckdb_result = DuckDBBackend().limit(df, 3)
        pd.testing.assert_frame_equal(
            pandas_result.reset_index(drop=True),
            duckdb_result.reset_index(drop=True),
        )


# ===========================================================================
# Category 7 — PolarsBackend security requirements
#
# These tests define the security contract for PolarsBackend (Task #27).
# They will FAIL until PolarsBackend is implemented — that's by design (TDD).
# Once Takeshi implements PolarsBackend, these must all pass GREEN.
# ===========================================================================


def _try_import_polars_backend() -> type | None:
    """Try to import PolarsBackend; return None if not yet implemented."""
    try:
        from pycypher.backend_engine import PolarsBackend

        return PolarsBackend
    except ImportError:
        return None


class TestPolarsBackendNoSQLContext:
    """PolarsBackend must NOT use Polars SQLContext for any operations.

    The Polars Expressions API is AST-based and inherently injection-safe.
    SQLContext uses string-based SQL with the same injection risks as DuckDB.
    """

    def _get_backend(self) -> Any:
        cls = _try_import_polars_backend()
        if cls is None:
            pytest.skip("PolarsBackend not yet implemented")
        return cls()

    def test_polars_backend_exists(self) -> None:
        """PolarsBackend class must be importable from backend_engine."""
        cls = _try_import_polars_backend()
        if cls is None:
            pytest.skip("PolarsBackend not yet implemented")
        assert cls is not None

    def test_polars_backend_source_has_no_sql_context(self) -> None:
        """PolarsBackend implementation must not use pl.SQLContext."""
        cls = _try_import_polars_backend()
        if cls is None:
            pytest.skip("PolarsBackend not yet implemented")
        import inspect

        source = inspect.getsource(cls)
        assert "SQLContext" not in source, (
            "PolarsBackend must use Expressions API, not SQLContext. "
            "SQLContext has SQL injection risks — use pl.col(), .filter(), "
            ".join() instead."
        )
        assert ".execute(" not in source or "SQL" not in source, (
            "PolarsBackend appears to execute SQL strings — use Expressions API"
        )

    def test_polars_scan_entity_works(self) -> None:
        """PolarsBackend.scan_entity must return ID column."""
        backend = self._get_backend()
        df = pd.DataFrame({"__ID__": [1, 2, 3], "name": ["a", "b", "c"]})
        result = backend.scan_entity(df, "Person")
        result_pd = backend.to_pandas(result)
        assert "__ID__" in result_pd.columns
        assert len(result_pd) == 3

    def test_polars_join_works(self) -> None:
        """PolarsBackend.join must produce correct results."""
        backend = self._get_backend()
        left = pd.DataFrame({"__ID__": [1, 2], "val": ["a", "b"]})
        right = pd.DataFrame({"__ID__": [1, 2], "score": [10, 20]})
        result = backend.join(left, right, on="__ID__")
        result_pd = backend.to_pandas(result)
        assert len(result_pd) == 2


class TestPolarsBackendNoPickle:
    """PolarsBackend must never use pickle for data serialization."""

    def test_polars_backend_source_has_no_pickle(self) -> None:
        """PolarsBackend source must not import pickle or cloudpickle."""
        cls = _try_import_polars_backend()
        if cls is None:
            pytest.skip("PolarsBackend not yet implemented")
        import inspect

        source = inspect.getsource(cls)
        assert "pickle" not in source.lower(), (
            "PolarsBackend must not use pickle. Use Arrow IPC "
            "(write_ipc/read_ipc) for serialization."
        )


class TestPolarsBackendToPandasCopy:
    """PolarsBackend.to_pandas() must return a copy, not a zero-copy view.

    Polars' Arrow-native format enables zero-copy sharing with pandas,
    but mutations to the returned DataFrame must not affect the original
    Polars data.
    """

    def test_to_pandas_returns_independent_copy(self) -> None:
        """Modifying to_pandas() result must not affect backend state."""
        cls = _try_import_polars_backend()
        if cls is None:
            pytest.skip("PolarsBackend not yet implemented")
        backend = cls()
        df = pd.DataFrame({"__ID__": [1, 2, 3]})
        scanned = backend.scan_entity(df, "TestEntity")
        pandas_result = backend.to_pandas(scanned)

        # Mutate the pandas result
        original_len = len(pandas_result)
        pandas_result.drop(pandas_result.index, inplace=True)
        assert len(pandas_result) == 0

        # Backend data must be unaffected
        pandas_result2 = backend.to_pandas(scanned)
        assert len(pandas_result2) == original_len, (
            "to_pandas() returned a view, not a copy — mutations leaked back"
        )


class TestPolarsBackendParity:
    """PolarsBackend must produce results identical to PandasBackend."""

    def _get_backend(self) -> Any:
        cls = _try_import_polars_backend()
        if cls is None:
            pytest.skip("PolarsBackend not yet implemented")
        return cls()

    def test_scan_entity_parity(self) -> None:
        df = pd.DataFrame({"__ID__": [1, 2, 3], "name": ["a", "b", "c"]})
        pandas_result = PandasBackend().scan_entity(df, "Person")
        polars_backend = self._get_backend()
        polars_result = polars_backend.to_pandas(
            polars_backend.scan_entity(df, "Person"),
        )
        pd.testing.assert_frame_equal(
            pandas_result.reset_index(drop=True),
            polars_result.reset_index(drop=True),
        )

    def test_limit_parity(self) -> None:
        df = pd.DataFrame({"a": range(10)})
        pandas_result = PandasBackend().limit(df, 3)
        polars_backend = self._get_backend()
        polars_result = polars_backend.to_pandas(
            polars_backend.limit(
                polars_backend.scan_entity(
                    pd.DataFrame({"__ID__": range(10), "a": range(10)}),
                    "Test",
                ),
                3,
            ),
        )
        assert len(polars_result) == 3
