"""Unit tests for the Spark backend (Phases 1-5).

Each test carries ``@pytest.mark.spark`` and uses the session-scoped
``spark_session`` fixture (see ``conftest.py``), which skips cleanly when
PySpark is not installed.  The ``spark_session`` fixture is requested by every
test so a Spark session already exists when ``SparkBackend()`` is constructed —
this exercises the shared-session (``_owned is False``) code path.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.constants import ID_COLUMN

pytestmark = pytest.mark.spark


@pytest.fixture
def backend(spark_session):  # noqa: ARG001 — depended on so a session exists first
    """A SparkBackend bound to the session-scoped test SparkSession."""
    from pycypher.backends.spark_backend import SparkBackend

    return SparkBackend()


# ---------------------------------------------------------------------------
# Phase 1 — lifecycle + materialize/inspect + scan
# ---------------------------------------------------------------------------


class TestLifecycle:
    def test_name(self, backend) -> None:
        assert backend.name == "spark"

    def test_reuses_existing_session_not_owned(self, backend) -> None:
        # A session exists (fixture created it), so the backend must not own it.
        assert backend._owned is False

    def test_close_does_not_stop_shared_session(
        self, backend, spark_session,
    ) -> None:
        backend.close()
        # Shared session must still be usable after an unowned close().
        assert spark_session.createDataFrame(
            pd.DataFrame({ID_COLUMN: [1]}),
        ).count() == 1

    def test_close_is_idempotent(self, backend) -> None:
        backend.close()
        backend.close()  # must not raise

    def test_context_manager(self, spark_session) -> None:
        from pycypher.backends.spark_backend import SparkBackend

        with SparkBackend() as be:
            assert be.name == "spark"


class TestMaterializeAndScan:
    def test_to_pandas_roundtrip(self, backend) -> None:
        df = pd.DataFrame({ID_COLUMN: [1, 2, 3], "v": ["a", "b", "c"]})
        sframe = backend._to_spark(df)
        out = backend.to_pandas(sframe)
        assert isinstance(out, pd.DataFrame)
        assert sorted(out[ID_COLUMN].tolist()) == [1, 2, 3]

    def test_scan_entity_returns_id_only(self, backend) -> None:
        df = pd.DataFrame({ID_COLUMN: [1, 2, 3], "name": ["a", "b", "c"]})
        scanned = backend.scan_entity(df, "Person")
        out = backend.to_pandas(scanned)
        assert list(out.columns) == [ID_COLUMN]
        assert sorted(out[ID_COLUMN].tolist()) == [1, 2, 3]

    def test_row_count(self, backend) -> None:
        df = pd.DataFrame({ID_COLUMN: [1, 2, 3, 4]})
        assert backend.row_count(backend._to_spark(df)) == 4

    def test_is_empty_true(self, backend) -> None:
        df = pd.DataFrame({ID_COLUMN: pd.Series([], dtype="int64")})
        assert backend.is_empty(backend._to_spark(df)) is True

    def test_is_empty_false(self, backend) -> None:
        df = pd.DataFrame({ID_COLUMN: [1]})
        assert backend.is_empty(backend._to_spark(df)) is False

    def test_memory_estimate_positive(self, backend) -> None:
        df = pd.DataFrame({ID_COLUMN: [1, 2, 3], "v": ["a", "b", "c"]})
        est = backend.memory_estimate_bytes(backend._to_spark(df))
        assert isinstance(est, int)
        assert est >= 0

    def test_to_pandas_accepts_raw_pandas(self, backend) -> None:
        # Ops must coerce raw pandas inputs (health-check passes them).
        df = pd.DataFrame({ID_COLUMN: [7]})
        assert backend.to_pandas(backend._to_spark(df))[ID_COLUMN].iloc[0] == 7


# ---------------------------------------------------------------------------
# Phase 2 — filter
# ---------------------------------------------------------------------------


class TestFilter:
    def test_filter_boolean_mask(self, backend) -> None:
        df = pd.DataFrame({ID_COLUMN: [1, 2, 3], "age": [30, 25, 35]})
        mask = pd.Series([True, False, True])
        out = backend.to_pandas(backend.filter(df, mask.values))
        assert sorted(out[ID_COLUMN].tolist()) == [1, 3]

    def test_filter_all_true(self, backend) -> None:
        df = pd.DataFrame({ID_COLUMN: [1, 2]})
        out = backend.to_pandas(backend.filter(df, pd.Series([True, True]).values))
        assert len(out) == 2

    def test_filter_all_false_empty(self, backend) -> None:
        df = pd.DataFrame({ID_COLUMN: [1, 2]})
        out = backend.to_pandas(backend.filter(df, pd.Series([False, False]).values))
        assert len(out) == 0

    def test_filter_resets_index(self, backend) -> None:
        df = pd.DataFrame({ID_COLUMN: [1, 2, 3]})
        out = backend.filter(df, pd.Series([False, True, True]).values)
        assert list(out.index) == [0, 1]


# ---------------------------------------------------------------------------
# Phase 3 — reshaping ops
# ---------------------------------------------------------------------------


class TestJoin:
    def test_inner_join(self, backend) -> None:
        left = pd.DataFrame({ID_COLUMN: [1, 2, 3], "name": ["a", "b", "c"]})
        right = pd.DataFrame({ID_COLUMN: [2, 3, 4], "age": [25, 35, 40]})
        out = backend.to_pandas(backend.join(left, right, on=ID_COLUMN))
        assert sorted(out[ID_COLUMN].tolist()) == [2, 3]
        assert set(out.columns) == {ID_COLUMN, "name", "age"}

    def test_left_join(self, backend) -> None:
        left = pd.DataFrame({ID_COLUMN: [1, 2], "name": ["a", "b"]})
        right = pd.DataFrame({ID_COLUMN: [2], "age": [25]})
        out = backend.to_pandas(backend.join(left, right, on=ID_COLUMN, how="left"))
        assert sorted(out[ID_COLUMN].tolist()) == [1, 2]
        row1 = out[out[ID_COLUMN] == 1]
        assert pd.isna(row1["age"].iloc[0])

    def test_cross_join(self, backend) -> None:
        left = pd.DataFrame({"a": [1, 2]})
        right = pd.DataFrame({"b": [10, 20]})
        out = backend.to_pandas(backend.join(left, right, on=[], how="cross"))
        assert len(out) == 4

    def test_join_accepts_raw_pandas_right(self, backend) -> None:
        # Mirrors check_backend_health passing a raw pandas right side.
        left = pd.DataFrame({ID_COLUMN: [1, 2]})
        out = backend.to_pandas(
            backend.join(left, pd.DataFrame({ID_COLUMN: [1]}), on=ID_COLUMN),
        )
        assert out[ID_COLUMN].tolist() == [1]


class TestReshaping:
    def test_rename(self, backend) -> None:
        df = pd.DataFrame({ID_COLUMN: [1], "old": [5]})
        out = backend.to_pandas(backend.rename(df, {"old": "new"}))
        assert "new" in out.columns
        assert "old" not in out.columns

    def test_concat(self, backend) -> None:
        a = pd.DataFrame({ID_COLUMN: [1, 2]})
        b = pd.DataFrame({ID_COLUMN: [3]})
        out = backend.to_pandas(backend.concat([a, b]))
        assert sorted(out[ID_COLUMN].tolist()) == [1, 2, 3]

    def test_distinct(self, backend) -> None:
        df = pd.DataFrame({ID_COLUMN: [1, 1, 2, 2, 3]})
        out = backend.to_pandas(backend.distinct(df))
        assert sorted(out[ID_COLUMN].tolist()) == [1, 2, 3]

    def test_drop_columns(self, backend) -> None:
        df = pd.DataFrame({ID_COLUMN: [1], "x": [2], "y": [3]})
        out = backend.to_pandas(backend.drop_columns(df, ["x", "missing"]))
        assert set(out.columns) == {ID_COLUMN, "y"}

    def test_assign_scalar(self, backend) -> None:
        df = pd.DataFrame({ID_COLUMN: [1, 2]})
        out = backend.to_pandas(backend.assign_column(df, "k", 9))
        assert out["k"].tolist() == [9, 9]

    def test_assign_list(self, backend) -> None:
        df = pd.DataFrame({ID_COLUMN: [1, 2]})
        out = backend.to_pandas(backend.assign_column(df, "k", [7, 8]))
        assert out["k"].tolist() == [7, 8]


class TestAggregate:
    def test_grouped_sum(self, backend) -> None:
        df = pd.DataFrame(
            {"dept": ["eng", "eng", "mktg"], "salary": [100, 110, 80]},
        )
        out = backend.to_pandas(
            backend.aggregate(df, ["dept"], {"total": ("salary", "sum")}),
        ).sort_values("dept").reset_index(drop=True)
        assert out["dept"].tolist() == ["eng", "mktg"]
        assert out["total"].tolist() == [210, 80]

    def test_full_table_count(self, backend) -> None:
        df = pd.DataFrame({ID_COLUMN: [1, 2, 3]})
        out = backend.to_pandas(
            backend.aggregate(df, [], {"n": (ID_COLUMN, "count")}),
        )
        assert out["n"].iloc[0] == 3

    def test_grouped_mean_min_max(self, backend) -> None:
        df = pd.DataFrame({"g": ["a", "a", "b"], "v": [10.0, 20.0, 5.0]})
        out = backend.to_pandas(
            backend.aggregate(
                df,
                ["g"],
                {"avg_v": ("v", "mean"), "min_v": ("v", "min"), "max_v": ("v", "max")},
            ),
        ).sort_values("g").reset_index(drop=True)
        assert out["avg_v"].tolist() == [15.0, 5.0]
        assert out["min_v"].tolist() == [10.0, 5.0]
        assert out["max_v"].tolist() == [20.0, 5.0]

    def test_unsupported_func_raises(self, backend) -> None:
        df = pd.DataFrame({"g": ["a"], "v": [1]})
        with pytest.raises(ValueError, match="Unsupported aggregation"):
            backend.aggregate(df, ["g"], {"o": ("v", "median")})


class TestOrdering:
    def test_sort_single_ascending(self, backend) -> None:
        df = pd.DataFrame({ID_COLUMN: [3, 1, 2]})
        out = backend.to_pandas(backend.sort(df, [ID_COLUMN]))
        assert out[ID_COLUMN].tolist() == [1, 2, 3]

    def test_sort_descending(self, backend) -> None:
        df = pd.DataFrame({ID_COLUMN: [3, 1, 2]})
        out = backend.to_pandas(backend.sort(df, [ID_COLUMN], ascending=[False]))
        assert out[ID_COLUMN].tolist() == [3, 2, 1]

    def test_sort_multi_col_mixed(self, backend) -> None:
        df = pd.DataFrame(
            {"g": ["a", "a", "b"], "v": [2, 1, 3]},
        )
        out = backend.to_pandas(
            backend.sort(df, ["g", "v"], ascending=[True, False]),
        )
        assert out["g"].tolist() == ["a", "a", "b"]
        assert out["v"].tolist() == [2, 1, 3]

    def test_limit(self, backend) -> None:
        df = pd.DataFrame({ID_COLUMN: [1, 2, 3, 4, 5]})
        out = backend.to_pandas(backend.limit(df, 2))
        assert out[ID_COLUMN].tolist() == [1, 2]

    def test_skip(self, backend) -> None:
        df = pd.DataFrame({ID_COLUMN: [1, 2, 3, 4, 5]})
        out = backend.to_pandas(backend.skip(df, 2))
        assert out[ID_COLUMN].tolist() == [3, 4, 5]

    def test_sort_then_limit_deterministic(self, backend) -> None:
        df = pd.DataFrame({ID_COLUMN: [5, 3, 1, 4, 2]})
        ordered = backend.sort(df, [ID_COLUMN])
        out = backend.to_pandas(backend.limit(ordered, 3))
        assert out[ID_COLUMN].tolist() == [1, 2, 3]


class TestHealthCheck:
    def test_backend_passes_health_check(self, spark_session) -> None:
        from pycypher.backend_engine import check_backend_health
        from pycypher.backends.spark_backend import SparkBackend

        assert bool(check_backend_health(SparkBackend())) is True
