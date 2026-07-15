"""Spark-based backend for distributed / large-scale workloads.

Implements the :class:`~pycypher.backend_engine.BackendEngine` protocol on top
of PySpark DataFrames.  See ``docs/spark_backend_design.md`` for the design
decisions behind this backend, in particular:

- **Mask alignment** (``filter``): the protocol hands ``filter`` a *positional*
  boolean ``pd.Series``; Spark DataFrames have no positional index, so the
  current implementation materialises to pandas, applies the mask, and
  re-creates a Spark DataFrame (boundary materialisation).  Kept behind the
  :meth:`_filter_with_mask` seam so a future predicate-push-down design can
  replace it without touching call sites.
- **Session lifecycle**: obtained via ``getOrCreate``.  The backend only
  ``stop()``s a session it created itself (``_owned``), so closing a backend
  that reused an existing session (tests, a shared driver) is a no-op.
- **Ordering**: Spark has no inherent row order — ``limit`` is only
  deterministic after a ``sort``; ``skip`` is implemented with a window.

Correctness first: property resolution and aggregation still collect to the
driver via the pandas paths in the core engine, so this backend is correct but
not yet fully distributed.  See the design doc's "Non-goals".
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd
from shared.logger import LOGGER

from pycypher.backends._helpers import _spark_agg_func, _to_pandas
from pycypher.constants import ID_COLUMN

if TYPE_CHECKING:
    from pycypher.cypher_types import BackendMask, ColumnValues, SourceObject


def _spark_schema_from_pandas(pdf: pd.DataFrame) -> Any:
    """Derive an explicit Spark ``StructType`` from a pandas DataFrame.

    An explicit schema is required so that *empty* pandas frames (which Spark
    cannot infer a schema from) still produce a well-typed Spark DataFrame,
    and so column types are deterministic rather than sampled.
    """
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    dtype_map: dict[str, Any] = {
        "int64": LongType(),
        "int32": IntegerType(),
        "int16": IntegerType(),
        "int8": IntegerType(),
        "float64": DoubleType(),
        "float32": FloatType(),
        "bool": BooleanType(),
        "datetime64[ns]": TimestampType(),
    }
    fields = []
    for col in pdf.columns:
        dtype = str(pdf[col].dtype)
        spark_type = dtype_map.get(dtype, StringType())
        fields.append(StructField(str(col), spark_type, nullable=True))
    return StructType(fields)


class SparkBackend:
    """PySpark-based :class:`~pycypher.backend_engine.BackendEngine`.

    Operations accept either a Spark DataFrame or a raw pandas DataFrame
    (coerced via :meth:`_to_spark`) and return Spark DataFrames, except
    :meth:`to_pandas` which materialises to pandas.
    """

    def __init__(self) -> None:
        """Create or attach to a SparkSession.

        Uses ``getOrCreate``; if no session was active beforehand this backend
        is considered the owner and will ``stop()`` the session on
        :meth:`close`.  If a session already existed (e.g. a test fixture or a
        shared driver) the backend does not own it and :meth:`close` is a
        no-op for the session.
        """
        from pyspark.sql import SparkSession

        existing = SparkSession.getActiveSession()
        self._owned: bool = existing is None
        self._spark: Any = (
            SparkSession.builder.appName("pycypher").getOrCreate()
        )
        self._closed: bool = False

    # -- Context manager & cleanup -----------------------------------------

    def close(self) -> None:
        """Stop the SparkSession only if this backend created it.

        Idempotent and safe to call multiple times.  A backend that reused an
        existing session never stops it — this keeps the ``close()`` call in
        the pipeline run path from tearing down a shared session.
        """
        if self._closed:
            return
        self._closed = True
        if self._owned and self._spark is not None:
            try:
                self._spark.stop()
            except Exception:  # noqa: BLE001 — best-effort cleanup
                LOGGER.warning(
                    "SparkSession stop raised; ignoring",
                    exc_info=True,
                )

    def __enter__(self) -> SparkBackend:
        """Enter the context manager, returning this backend instance."""
        return self

    def __exit__(self, *_exc: object) -> None:
        """Exit the context manager, closing an owned session."""
        self.close()

    def __del__(self) -> None:
        """Release an owned session on garbage collection."""
        self.close()

    @property
    def name(self) -> str:
        """Return ``'spark'``."""
        return "spark"

    # -- Internal helpers --------------------------------------------------

    def _is_spark_df(self, obj: Any) -> bool:
        """Return True if *obj* is a PySpark DataFrame."""
        from pyspark.sql import DataFrame as SparkDataFrame

        return isinstance(obj, SparkDataFrame)

    def _to_spark(self, frame: Any) -> Any:
        """Coerce *frame* to a Spark DataFrame.

        Spark DataFrames pass through; pandas / Arrow inputs are converted with
        an explicit schema so empty frames do not fail schema inference.
        """
        if self._is_spark_df(frame):
            return frame
        pdf = frame if isinstance(frame, pd.DataFrame) else _to_pandas(frame)
        schema = _spark_schema_from_pandas(pdf)
        return self._spark.createDataFrame(pdf, schema=schema)

    def _materialize(self, frame: Any) -> pd.DataFrame:
        """Return *frame* as pandas without copying pandas inputs.

        Used by the pandas-delegated ops (``rename``/``concat``/``assign``/
        ``drop_columns``/``filter``) where a defensive copy is unnecessary.
        """
        if isinstance(frame, pd.DataFrame):
            return frame
        if self._is_spark_df(frame):
            return frame.toPandas()
        return _to_pandas(frame)

    # ------------------------------------------------------------------
    # Scan
    # ------------------------------------------------------------------

    def scan_entity(
        self,
        source_obj: SourceObject,
        entity_type: str,
    ) -> pd.DataFrame:
        """Load entity IDs, computed in Spark, returned as pandas."""
        return self._to_spark(source_obj).select(ID_COLUMN).toPandas()

    # ------------------------------------------------------------------
    # Transform
    # ------------------------------------------------------------------

    def filter(self, frame: Any, mask: BackendMask) -> pd.DataFrame:
        """Apply a positional boolean *mask* to *frame*.

        Delegates to pandas (boundary materialisation, strategy (c) in the
        design doc): the mask is an externally-computed positional array, which
        cannot be expressed against an unordered Spark DataFrame.  This mirrors
        ``DuckDBBackend.filter`` (``duckdb_backend.py:247``).
        """
        return self._filter_with_mask(frame, mask)

    def _filter_with_mask(self, frame: Any, mask: BackendMask) -> pd.DataFrame:
        """Seam for the filter strategy.

        Isolated so a future predicate-push-down design (Phase 9) can replace
        boundary materialisation without touching :meth:`filter`'s callers.
        """
        return self._materialize(frame).loc[mask].reset_index(drop=True)

    def join(
        self,
        left: Any,
        right: Any,
        on: str | list[str],
        how: str = "inner",
        strategy: str = "auto",
    ) -> pd.DataFrame:
        """Join two frames in Spark, returned as pandas.

        *strategy* is accepted for protocol compatibility but ignored — Spark's
        Catalyst optimiser selects the join algorithm.  For non-cross joins the
        key column(s) in *on* are coalesced to a single output column (Spark
        semantics), matching ``DuckDBBackend.join``.
        """
        left_sdf = self._to_spark(left)
        right_sdf = self._to_spark(right)
        if how == "cross":
            return left_sdf.crossJoin(right_sdf).toPandas()
        on_cols = [on] if isinstance(on, str) else list(on)
        return left_sdf.join(right_sdf, on=on_cols, how=how).toPandas()

    def rename(self, frame: Any, columns: dict[str, str]) -> pd.DataFrame:
        """Rename columns — delegates to pandas (no Spark benefit)."""
        return self._materialize(frame).rename(columns=columns)

    def concat(
        self,
        frames: list[Any],
        *,
        ignore_index: bool = True,
    ) -> pd.DataFrame:
        """Concatenate frames vertically via pandas.

        pandas handles heterogeneous/missing columns more predictably than
        Spark's ``unionByName`` for the mixed-schema cases the engine produces.
        """
        return pd.concat(
            [self._materialize(f) for f in frames],
            ignore_index=ignore_index,
        )

    def distinct(self, frame: Any) -> pd.DataFrame:
        """Remove duplicate rows in Spark, returned as pandas."""
        return self._to_spark(frame).dropDuplicates().toPandas()

    def assign_column(
        self,
        frame: Any,
        name: str,
        values: ColumnValues,
    ) -> pd.DataFrame:
        """Add or replace a column — delegates to pandas.

        *values* may be a positional list/Series/scalar, which has no
        well-defined mapping onto an unordered Spark DataFrame, so this is a
        boundary-materialised pandas operation.
        """
        return self._materialize(frame).assign(**{name: values})

    def drop_columns(self, frame: Any, columns: list[str]) -> pd.DataFrame:
        """Drop columns, ignoring names that are absent."""
        df = self._materialize(frame)
        existing = [c for c in columns if c in df.columns]
        if not existing:
            return df
        return df.drop(columns=existing)

    # ------------------------------------------------------------------
    # Aggregate
    # ------------------------------------------------------------------

    def aggregate(
        self,
        frame: Any,
        group_cols: list[str],
        agg_specs: dict[str, tuple[str, str]],
    ) -> pd.DataFrame:
        """Grouped or full-table aggregation in Spark, returned as pandas.

        Args:
            frame: Input frame.
            group_cols: Columns to group by (empty for full-table aggregation).
            agg_specs: ``{output_col: (source_col, agg_func)}``.

        """
        from pyspark.sql import functions as F  # noqa: N812

        sdf = self._to_spark(frame)
        agg_exprs = [
            _spark_agg_func(F.col(src_col), func).alias(out_col)
            for out_col, (src_col, func) in agg_specs.items()
        ]
        if group_cols:
            result = sdf.groupBy(*group_cols).agg(*agg_exprs)
        else:
            result = sdf.agg(*agg_exprs)
        return result.toPandas()

    # ------------------------------------------------------------------
    # Order
    # ------------------------------------------------------------------

    def sort(
        self,
        frame: Any,
        by: list[str],
        ascending: list[bool] | None = None,
    ) -> pd.DataFrame:
        """Sort in Spark, returned as pandas.

        ``orderBy`` produces a globally-ordered DataFrame; ``toPandas`` collects
        partitions in order, so the pandas result preserves the sort.
        """
        if ascending is None:
            ascending = [True] * len(by)
        sdf = self._to_spark(frame)
        return (
            sdf.orderBy(by, ascending=ascending)
            .toPandas()
            .reset_index(drop=True)
        )

    def limit(self, frame: Any, n: int) -> pd.DataFrame:
        """Return the first *n* rows.

        Delegated to pandas ``head`` on the already-materialised frame — Spark
        ``limit`` is nondeterministic without a preceding sort, whereas the
        engine has already applied any ORDER BY before LIMIT.
        """
        return self._materialize(frame).head(n).reset_index(drop=True)

    def skip(self, frame: Any, n: int) -> pd.DataFrame:
        """Skip the first *n* rows (pandas ``iloc`` on the ordered frame)."""
        return self._materialize(frame).iloc[n:].reset_index(drop=True)

    # ------------------------------------------------------------------
    # Materialise / inspect
    # ------------------------------------------------------------------

    def to_pandas(self, frame: Any) -> pd.DataFrame:
        """Materialise *frame* to a pandas DataFrame."""
        if isinstance(frame, pd.DataFrame):
            return frame.copy()
        return self._to_spark(frame).toPandas()

    def row_count(self, frame: Any) -> int:
        """Row count — pandas ``len`` for pandas inputs, else Spark ``count``."""
        if isinstance(frame, pd.DataFrame):
            return len(frame)
        return int(self._to_spark(frame).count())

    def is_empty(self, frame: Any) -> bool:
        """True if *frame* has no rows (pandas short-circuit)."""
        if isinstance(frame, pd.DataFrame):
            return len(frame) == 0
        return bool(self._to_spark(frame).isEmpty())

    def memory_estimate_bytes(self, frame: Any) -> int:
        """Rough size estimate: rows × columns × 8 bytes.

        Order-of-magnitude only, per the protocol contract.  Uses pandas'
        own accounting for pandas inputs to avoid a Spark round-trip.
        """
        if isinstance(frame, pd.DataFrame):
            return int(frame.memory_usage(deep=True).sum())
        sdf = self._to_spark(frame)
        n_cols = len(sdf.columns)
        return int(sdf.count() * max(n_cols, 1) * 8)
