"""BindingFrame — the core IR abstraction for the refactored query engine.

A BindingFrame is a DataFrame whose columns are named after Cypher variables
(plain strings such as ``"p"``, ``"q"``, ``"r"``).  Each row is one possible
assignment of entity IDs to those variables.  Attributes are **never** stored in
a BindingFrame; they are fetched on demand from the ``Context`` via
``get_property()``.

This eliminates three sources of fragility in the legacy pipeline:

1. The opaque 32-hex HASH_ID column names produced by ``Projection``.
2. The ``PREFIXED_ENTITY`` column names (``Person__name``) that bleed into
   operator logic via ``_ensure_full_entity_data`` and similar hacks.
3. The ``variable_map`` / ``variable_type_map`` metadata that must be threaded
   through every ``Relation`` subclass to recover which column belongs to which
   Cypher variable.

With BindingFrame the column *is* the variable — no lookup table required.

Usage::

    bf = BindingFrame(
        bindings=pd.DataFrame({"p": [1, 2, 3], "q": [4, 5, 6]}),
        type_registry={"p": "Person", "q": "Person"},
        context=ctx,
    )
    names = bf.get_property("p", "name")   # pd.Series of person names
    filtered = bf.filter(names == "Alice")  # BindingFrame with one row
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
from shared.helpers import suggest_close_match
from shared.logger import LOGGER

from pycypher.config import (
    CROSS_JOIN_WARN_THRESHOLDS as CROSS_JOIN_WARN_THRESHOLDS,
)
from pycypher.config import MAX_CROSS_JOIN_ROWS as MAX_CROSS_JOIN_ROWS
from pycypher.constants import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
)
from pycypher.cypher_types import FrameDataFrame, FrameSeries

# ---------------------------------------------------------------------------
# Performance: module-level debug check avoids per-call overhead
# ---------------------------------------------------------------------------

#: Cached debug-level check.  Evaluated once at import time so that
#: hot-path methods (get_property, join, filter) skip ``time.perf_counter()``
#: and ``QueryPlanner`` allocation when debug logging is disabled.
_DEBUG_ENABLED: bool = LOGGER.isEnabledFor(logging.DEBUG)

if TYPE_CHECKING:
    from pycypher.ast_models import Expression

# ---------------------------------------------------------------------------
# Column-name constants used across the BindingFrame execution path
# ---------------------------------------------------------------------------

#: Prefix for path-hop-count columns produced by variable-length path expansion.
#: ``star.py`` writes ``f"{PATH_HOP_COLUMN_PREFIX}{path_var}"`` and
#: ``binding_evaluator.py`` reads it when evaluating ``length(path_var)``.
PATH_HOP_COLUMN_PREFIX: str = "_path_hop_"

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

# Module-level pyarrow availability probe — evaluated once at import time
# to avoid repeated try/except in the hot path (_source_to_pandas).
try:
    import pyarrow as _pa

    _PYARROW_TABLE_TYPE: type | None = _pa.Table
except ImportError:
    _PYARROW_TABLE_TYPE = None

# Pre-built ufunc for ndarray→list normalisation.  Created once at module
# level so that get_property() doesn't re-create it on every call.
_ndarray_to_list = np.frompyfunc(
    lambda x: x.tolist() if isinstance(x, np.ndarray) else x,
    1,
    1,
)


def _null_series(n: int, index: Any = None) -> pd.Series:
    """Create a Series of *n* Python ``None`` values efficiently.

    Uses ``np.empty`` + fill instead of ``[None] * n`` to avoid building
    a Python list.  For n > ~100 this is measurably faster and uses less
    peak memory.
    """
    arr = np.empty(n, dtype=object)
    arr[:] = None
    return pd.Series(arr, index=index, dtype=object)


def _normalize_mapped_result(result: pd.Series) -> pd.Series:
    """Normalize a mapped Series: convert NaN→None, ndarray→list.

    Shared by :meth:`BindingFrame.get_property` and
    :meth:`BindingFrame.get_properties_batch` to avoid duplicated
    post-map normalization logic.

    Args:
        result: A ``pd.Series`` returned by ``id_series.map(lookup)``.

    Returns:
        A normalized ``pd.Series`` with NaN→None and ndarray→list conversions.

    """
    _vals = result.values
    _n = len(_vals)
    # Fast path: empty series needs no normalization.
    if _n == 0:
        return result
    if result.dtype == object:
        _needs_list = False
        # Sample first element for ndarray presence (common in list properties).
        _v0 = _vals[0]
        if isinstance(_v0, np.ndarray):
            _needs_list = True
        # Defer the expensive pd.isna() call: check the raw numpy array
        # directly with a cheaper dtype-aware test first.
        _na_mask = None
        _needs_none = False
        # For object arrays, use pd.isna which handles mixed types correctly.
        _na_mask = pd.isna(result)
        _needs_none = _na_mask.any()
        if _needs_none and _needs_list:
            _new = np.empty(_n, dtype=object)
            for _i in range(_n):
                _v = _vals[_i]
                if _v is None or (_v is not _v):
                    _new[_i] = None
                elif isinstance(_v, np.ndarray):
                    _new[_i] = _v.tolist()
                else:
                    _new[_i] = _v
            result = pd.Series(_new, dtype=object)
        elif _needs_none:
            result = result.astype(object)
            result[_na_mask] = None
        elif _needs_list:
            result = pd.Series(
                _ndarray_to_list(_vals),
                dtype=object,
            )
    else:
        _is_native_float = (
            hasattr(result.dtype, "kind") and result.dtype.kind == "f"
        )
        if _is_native_float:
            _na_mask = np.isnan(_vals)
            if _na_mask.any():
                result = pd.Series(
                    np.where(_na_mask, None, _vals),
                    dtype=object,
                )
        elif result.hasnans:
            _na_mask = result.isna()
            if _na_mask.any():
                result = result.astype(object)
                result[_na_mask] = None
    return result


# Re-export from dataframe_utils for backward compatibility.
# New code should import directly from pycypher.dataframe_utils.
from pycypher.dataframe_utils import source_to_pandas as _source_to_pandas


# Backward-compatible re-exports from scan_operators (extracted for SRP).
# New code should import directly from pycypher.scan_operators.
from pycypher.scan_operators import _coerce_pushdown_ids as _coerce_pushdown_ids
from pycypher.scan_operators import (
    _coerce_pushdown_series as _coerce_pushdown_series,
)


def _backend_merge(
    backend: Any,
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    left_col: str,
    right_col: str,
    how: str = "inner",
    strategy: str = "auto",
    suffixes: tuple[str, str] = ("", "_right"),
) -> pd.DataFrame:
    """Delegate a merge to the backend, handling column-name normalisation.

    The :class:`BackendEngine` protocol requires the join key to have the
    same name in both frames (``on``).  When *left_col* differs from
    *right_col*, this helper renames the right join key before delegating
    and resolves any non-join column collisions using *suffixes*.

    Args:
        backend: A :class:`~pycypher.backend_engine.BackendEngine` instance.
        left: Left DataFrame.
        right: Right DataFrame.
        left_col: Join key column name in *left*.
        right_col: Join key column name in *right*.
        how: Join type (``'inner'``, ``'left'``, ``'cross'``).
        strategy: Join strategy hint for the backend.
        suffixes: Tuple of suffixes for resolving non-join column collisions.

    Returns:
        The joined DataFrame with collision-suffixed columns.

    """
    # --- Normalise join key names ---
    if left_col != right_col:
        right = backend.rename(right, {right_col: left_col})

    # --- Resolve non-join column collisions ---
    left_suffix, right_suffix = suffixes
    join_key = left_col
    left_other = set(left.columns) - {join_key}
    right_other = set(right.columns) - {join_key}
    collisions = left_other & right_other

    if collisions:
        # Rename colliding columns in the right frame with the right suffix
        rename_map = {c: f"{c}{right_suffix}" for c in collisions}
        right = backend.rename(right, rename_map)
        # Also rename left collisions if left_suffix is non-empty
        if left_suffix:
            left_rename = {c: f"{c}{left_suffix}" for c in collisions}
            left = backend.rename(left, left_rename)

    # --- Delegate to backend ---
    return backend.join(left, right, on=join_key, how=how, strategy=strategy)


@dataclass
class BindingFrame:
    """A table of variable bindings where every column is a Cypher variable name.

    A BindingFrame is the core intermediate representation (IR) for the
    query engine.  Each column is named after a Cypher variable (``"p"``,
    ``"q"``, ``"r"``), and each row represents one candidate assignment of
    entity/relationship IDs to those variables.  **Attributes are never
    stored here** — they are fetched on-demand from the Context via
    :meth:`get_property`.

    Key operations
    ~~~~~~~~~~~~~~

    - :meth:`get_property` — fetch attribute values via ID-keyed join
    - :meth:`filter` — apply a boolean mask (WHERE clause)
    - :meth:`join` — inner join on shared variable names (multi-MATCH)
    - :meth:`cross_join` — Cartesian product (disjoint patterns)
    - :meth:`mutate` — write property values back through shadow layer

    Example::

        # After MATCH (p:Person)-[:KNOWS]->(q:Person), the BindingFrame
        # might look like:
        #   bindings = DataFrame({"p": [1, 1, 2], "q": [2, 3, 3]})
        #   type_registry = {"p": "Person", "q": "Person"}
        #
        # To evaluate WHERE p.age > 25:
        ages = bf.get_property("p", "age")   # Series: [30, 30, 25]
        filtered = bf.filter(ages > 25)      # keeps rows 0 and 1

    Attributes:
        bindings: DataFrame whose columns are variable names (strings) and whose
            values are entity or relationship IDs.
        type_registry: Maps each variable name to its entity or relationship type
            string (e.g. ``{"p": "Person", "r": "KNOWS"}``).
        context: The query context holding entity and relationship tables.

    """

    bindings: FrameDataFrame
    type_registry: dict[str, str]
    context: Any  # Context — typed as Any to avoid circular import at runtime
    _property_cache: dict[tuple[str, str], FrameSeries] = field(
        default_factory=dict,
        repr=False,
        compare=False,
    )

    # ------------------------------------------------------------------
    # Introspection helpers
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        """Number of binding rows."""
        return len(self.bindings)

    @property
    def var_names(self) -> list[str]:
        """Ordered list of variable names in this frame."""
        return list(self.bindings.columns)

    def entity_type(self, var_name: str) -> str:
        """Return the entity/relationship type for *var_name*.

        Raises:
            KeyError: If *var_name* is not in the type registry.

        """
        return self.type_registry[var_name]

    # ------------------------------------------------------------------
    # Context accessor helpers — consolidate repeated getattr calls
    # ------------------------------------------------------------------

    @property
    def _shadow(self) -> dict:
        """Entity shadow layer for uncommitted mutations."""
        return getattr(self.context, "_shadow", {})

    @property
    def _shadow_rels(self) -> dict:
        """Relationship shadow layer for uncommitted mutations."""
        return getattr(self.context, "_shadow_rels", {})

    @property
    def _backend(self) -> Any:
        """Backend engine (None when using default pandas path)."""
        return getattr(self.context, "backend", None)

    @property
    def _property_lookup_cache_ctx(self) -> dict:
        """Property lookup cache stored on the context."""
        return getattr(self.context, "_property_lookup_cache", {})

    # ------------------------------------------------------------------
    # Property access
    # ------------------------------------------------------------------

    @staticmethod
    def _coerce_ids(id_values, target_index: pd.Index):
        """Coerce binding ID values to match the entity table's index dtype.

        DuckDB backend may produce string IDs after merges even when entity
        tables use integer IDs.  This ensures lookups (map, reindex,
        searchsorted) match types correctly.

        Skips coercion when the target index has mixed types (e.g. after
        MERGE appends integer IDs to a string-ID table).
        """
        if len(id_values) == 0 or len(target_index) == 0:
            return id_values
        # Detect type mismatch: binding IDs vs entity table index
        sample_binding = (
            id_values.iloc[0] if hasattr(id_values, "iloc") else id_values[0]
        )
        sample_index = target_index[0]
        if type(sample_binding) is type(sample_index):
            return id_values
        # Check if target index has homogeneous types; skip coercion if mixed
        if target_index.dtype == object:
            index_types = {type(v) for v in target_index[:100]}
            if len(index_types) > 1:
                return id_values  # Mixed types — don't coerce
        try:
            if isinstance(sample_index, (int, np.integer)):
                if hasattr(id_values, "astype"):
                    return id_values.astype(int)
                return np.array(id_values, dtype=int)
            if isinstance(sample_index, str):
                if hasattr(id_values, "astype"):
                    return id_values.astype(str)
                return np.array(id_values, dtype=str)
        except (ValueError, TypeError):
            pass
        return id_values

    def _get_indexed_dataframe(self, entity_type: str) -> pd.DataFrame | None:
        """Resolve an ID-indexed DataFrame for *entity_type*.

        Shadow entries (from uncommitted CREATE/SET) take precedence over
        canonical data.  Canonical reads are cached on the Context so that
        Arrow-to-pandas conversion happens at most once per entity/relationship
        type per query epoch.

        Cache key convention:
          entity_mapping entries  ->  entity_type          (e.g. ``"Person"``)
          relationship_mapping    ->  ``"__rel__"`` + type (e.g. ``"__rel__KNOWS"``)

        Returns:
            A ``pd.DataFrame`` indexed by ``ID_COLUMN``, or ``None`` when
            *entity_type* is not found in any mapping.

        """
        shadow = self._shadow
        shadow_rels = self._shadow_rels
        cache = self._property_lookup_cache_ctx

        if entity_type in shadow:
            # Shadow bypasses cache — may have SET-added columns not in base
            return shadow[entity_type].set_index(ID_COLUMN)
        if entity_type in shadow_rels:
            return shadow_rels[entity_type].set_index(ID_COLUMN)
        if entity_type in self.context.entity_mapping.mapping:
            if entity_type not in cache:
                raw_df = _source_to_pandas(
                    self.context.entity_mapping[entity_type].source_obj,
                )
                cache[entity_type] = raw_df.set_index(ID_COLUMN)
            return cache[entity_type]
        if entity_type in self.context.relationship_mapping.mapping:
            cache_key = f"__rel__{entity_type}"
            if cache_key not in cache:
                raw_df = _source_to_pandas(
                    self.context.relationship_mapping[entity_type].source_obj,
                )
                cache[cache_key] = raw_df.set_index(ID_COLUMN)
            return cache[cache_key]
        return None

    def get_property(self, var_name: str, prop_name: str) -> FrameSeries:
        """Fetch entity attribute values for all rows in this frame.

        Looks up the entity IDs in ``bindings[var_name]``, then performs an
        ID-keyed lookup against the entity table stored in the context.  The
        result is a ``pd.Series`` aligned with ``self.bindings`` (same index,
        same length).

        Args:
            var_name: The Cypher variable name (e.g. ``"p"``).
            prop_name: The entity attribute name (e.g. ``"name"``).

        Returns:
            A ``pd.Series`` of property values, one per binding row.

        Raises:
            KeyError: If *var_name* is not in the type registry.
            ValueError: If *var_name* is not a column in ``bindings``.

        Note:
            If *prop_name* does not exist as a column in the entity table, a
            Series of ``pd.NA`` values is returned (per Cypher null semantics)
            rather than raising an exception.

        """
        # Fast path: return cached result if already resolved for this frame.
        _cache_key = (var_name, prop_name)
        if _cache_key in self._property_cache:
            return self._property_cache[_cache_key]

        if _DEBUG_ENABLED:
            _t0 = time.perf_counter()
            _rows_before = len(self.bindings)

        if var_name not in self.bindings.columns:
            available = self.var_names
            hint = suggest_close_match(var_name, available)
            from pycypher.exceptions import VariableNotFoundError

            raise VariableNotFoundError(var_name, available, hint)

        if var_name not in self.type_registry:
            # Auto-detect entity type: find which entity table contains the IDs
            # present in this variable's column.  This handles unlabeled nodes
            # like MATCH (p:Person)-[:KNOWS]->(q) where q inherits its type
            # implicitly from the relationship traversal.
            entity_type = self._infer_entity_type(var_name)
            if entity_type is None:
                # Empty frame: nothing to look up — return null Series.
                if len(self.bindings) == 0:
                    return pd.Series([], dtype=object, name=prop_name)
                from pycypher.exceptions import VariableTypeMismatchError

                registered_vars = list(self.type_registry.keys())
                suggestion = (
                    "Variable exists but has no registered entity type. Use WITH clause to define entity type"
                    if registered_vars
                    else "Variable exists but has no registered entity type. Define entity variables first"
                )
                raise VariableTypeMismatchError(
                    var_name,
                    "entity",
                    "scalar",
                    suggestion,
                )
        else:
            entity_type = self.type_registry[var_name]

        # Multi-type sentinel: variable spans all entity tables (e.g. MATCH (n)
        # with multiple entity types registered).  Delegate to per-row lookup.
        if entity_type == "__MULTI__":
            row_values = self._get_property_multitype(var_name, prop_name)
            if row_values is not None:
                self._property_cache[_cache_key] = row_values
                return row_values
            result = _null_series(
                len(self.bindings), index=self.bindings.index
            )
            self._property_cache[_cache_key] = result
            return result

        # --- Vectorized fast path ---
        # Use np.searchsorted-based bulk lookup when the graph index manager
        # has a VectorizedPropertyStore for this entity type.  This avoids
        # hash-based pd.Series.map() in favour of O(k log N) binary search.
        _used_vectorized = False
        index_mgr = getattr(self.context, "index_manager", None)
        shadow = self._shadow
        shadow_rels = self._shadow_rels
        if (
            index_mgr is not None
            and entity_type not in shadow
            and entity_type not in shadow_rels
        ):
            try:
                store = index_mgr.get_vectorized_store(entity_type)
                if store is not None:
                    id_values = self.bindings[var_name].values
                    raw = store.fetch(id_values, prop_name)
                    result = pd.Series(
                        raw, index=self.bindings.index, dtype=object
                    )
                    # Apply same NaN→None and ndarray→list normalization as standard path
                    result = _normalize_mapped_result(result)
                    result.index = self.bindings.index
                    _used_vectorized = True
            except (KeyError, ValueError, TypeError, IndexError, AttributeError):
                LOGGER.debug(
                    "Vectorized fetch failed for %s.%s, falling back",
                    entity_type,
                    prop_name,
                    exc_info=True,
                )

        if not _used_vectorized:
            # --- Standard path (hash-based map) ---
            indexed_df = self._get_indexed_dataframe(entity_type)
            if indexed_df is None:
                return _null_series(
                    len(self.bindings), index=self.bindings.index
                )

            if prop_name not in indexed_df.columns:
                # Per Cypher semantics, accessing a nonexistent property returns null.
                return _null_series(
                    len(self.bindings), index=self.bindings.index
                )
            backend = self._backend
            if backend is not None:
                # Backend-delegated single-property resolution via join.
                entity_subset = indexed_df[[prop_name]].reset_index()
                id_frame = pd.DataFrame(
                    {ID_COLUMN: self.bindings[var_name].values}
                )
                if id_frame[ID_COLUMN].dtype != entity_subset[ID_COLUMN].dtype:
                    id_frame[ID_COLUMN] = id_frame[ID_COLUMN].astype(
                        entity_subset[ID_COLUMN].dtype, errors="ignore"
                    )
                joined = backend.join(
                    id_frame, entity_subset, on=ID_COLUMN, how="left",
                )
                result = _normalize_mapped_result(joined[prop_name])
                result.index = self.bindings.index
            else:
                lookup: pd.Series = indexed_df[prop_name]

                # Map entity IDs in this frame to property values.
                id_series: pd.Series = self._coerce_ids(
                    self.bindings[var_name],
                    indexed_df.index,
                )
                result: pd.Series = id_series.map(lookup)

                result = _normalize_mapped_result(result)
                result.index = self.bindings.index
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "BindingFrame.get_property  var=%s  prop=%s  rows=%d  elapsed=%.4fs",
                var_name,
                prop_name,
                _rows_before,
                time.perf_counter() - _t0,
            )
        self._property_cache[_cache_key] = result
        return result

    def get_properties_batch(
        self,
        var_name: str,
        prop_names: list[str],
    ) -> dict[str, FrameSeries]:
        """Fetch multiple properties for *var_name* in a single indexed join.

        This is a performance optimization over calling :meth:`get_property`
        multiple times for the same variable.  Instead of N separate
        ``Series.map()`` calls, the IDs are joined against the entity table
        once and all requested columns are extracted simultaneously.

        Falls back to per-property :meth:`get_property` for multi-type
        variables or when the entity type cannot be resolved.

        Args:
            var_name: The Cypher variable name (e.g. ``"p"``).
            prop_names: List of property names to fetch.

        Returns:
            A dict mapping each property name to a ``pd.Series`` of values.

        """
        if not prop_names:
            return {}

        # Check cache first: return already-resolved properties, fetch only
        # the ones we haven't seen yet.
        results_from_cache: dict[str, FrameSeries] = {}
        uncached_props: list[str] = []
        for p in prop_names:
            cache_key = (var_name, p)
            if cache_key in self._property_cache:
                results_from_cache[p] = self._property_cache[cache_key]
            else:
                uncached_props.append(p)

        if not uncached_props:
            return results_from_cache

        # Fall back for edge cases: missing variable, multi-type, unresolvable
        if var_name not in self.bindings.columns:
            for p in uncached_props:
                results_from_cache[p] = self.get_property(var_name, p)
            return results_from_cache

        entity_type = self.type_registry.get(var_name)
        if entity_type is None or entity_type == "__MULTI__":
            for p in uncached_props:
                results_from_cache[p] = self.get_property(var_name, p)
            return results_from_cache

        # Replace prop_names with only uncached ones for the bulk fetch below
        prop_names = uncached_props

        # --- Vectorized fast path (np.searchsorted) ---
        index_mgr = getattr(self.context, "index_manager", None)
        shadow = self._shadow
        shadow_rels = self._shadow_rels
        if (
            index_mgr is not None
            and entity_type not in shadow
            and entity_type not in shadow_rels
        ):
            try:
                store = index_mgr.get_vectorized_store(entity_type)
                if store is not None:
                    id_values = self.bindings[var_name].values
                    raw_results = store.fetch_multi(id_values, prop_names)
                    results: dict[str, FrameSeries] = {}
                    for prop, raw in raw_results.items():
                        series = pd.Series(
                            raw, index=self.bindings.index, dtype=object
                        )
                        series = _normalize_mapped_result(series)
                        series.index = self.bindings.index
                        results[prop] = series
                        self._property_cache[(var_name, prop)] = series
                    results.update(results_from_cache)
                    return results
            except (KeyError, ValueError, TypeError, IndexError, AttributeError):
                LOGGER.debug(
                    "Vectorized batch fetch failed for %s, falling back",
                    entity_type,
                    exc_info=True,
                )

        # --- Standard path (reindex-based, with backend delegation) ---
        indexed_df = self._get_indexed_dataframe(entity_type)
        if indexed_df is None:
            null_results = {
                p: _null_series(len(self.bindings), index=self.bindings.index)
                for p in prop_names
            }
            for prop, series in null_results.items():
                self._property_cache[(var_name, prop)] = series
            null_results.update(results_from_cache)
            return null_results

        id_series: pd.Series = self.bindings[var_name]

        # When all IDs are NA (e.g. from OPTIONAL MATCH that matched nothing),
        # skip property resolution entirely and return null series.
        if id_series.isna().all():
            null_results = {
                p: _null_series(len(self.bindings), index=self.bindings.index)
                for p in prop_names
            }
            for prop, series in null_results.items():
                self._property_cache[(var_name, prop)] = series
            null_results.update(results_from_cache)
            return null_results

        # Identify which requested props actually exist as columns
        available = [p for p in prop_names if p in indexed_df.columns]
        missing = [p for p in prop_names if p not in indexed_df.columns]

        results: dict[str, FrameSeries] = {}

        if available:
            backend = self._backend
            if backend is not None:
                # Backend-delegated property resolution: use join to fetch
                # all properties in a single backend operation.
                # Prepare entity table with ID as a regular column.
                entity_subset = indexed_df[available].reset_index()
                id_frame = pd.DataFrame({ID_COLUMN: id_series.values})
                # Coerce ID column types to match before merge to avoid
                # pandas TypeError on mixed object/int64 columns.
                if id_frame[ID_COLUMN].dtype != entity_subset[ID_COLUMN].dtype:
                    id_frame[ID_COLUMN] = id_frame[ID_COLUMN].astype(
                        entity_subset[ID_COLUMN].dtype, errors="ignore"
                    )
                joined = backend.join(
                    id_frame, entity_subset, on=ID_COLUMN, how="left",
                )
                for prop in available:
                    series = joined[prop]
                    series.index = self.bindings.index
                    results[prop] = _normalize_mapped_result(series)
            else:
                # Reindex the entity table subset by IDs in one operation.
                id_values = self._coerce_ids(id_series, indexed_df.index)
                if hasattr(id_values, "values"):
                    id_values = id_values.values
                subset = indexed_df[available].reindex(id_values)
                subset.index = self.bindings.index
                for prop in available:
                    results[prop] = _normalize_mapped_result(subset[prop])

        # Missing properties → null Series
        for prop in missing:
            results[prop] = _null_series(
                len(self.bindings),
                index=self.bindings.index,
            )

        # Populate per-property cache for future single-property lookups
        for prop, series in results.items():
            self._property_cache[(var_name, prop)] = series

        # Merge with previously cached results
        results.update(results_from_cache)
        return results

    def _get_id_to_etype_map(self) -> dict:
        """Return a cached mapping from entity ID to entity type name.

        Built once per context and cached as ``_id_etype_map`` on the context
        object.  This allows ``_get_property_multitype`` to skip entity tables
        whose IDs are not present in the current bindings — reducing iterations
        from O(E) entity types to only the relevant ones.
        """
        ctx = self.context
        existing = getattr(ctx, "_id_etype_map", None)
        if existing is not None:
            return existing
        cache: dict = getattr(ctx, "_property_lookup_cache", {})
        id_etype: dict = {}
        for etype, table in ctx.entity_mapping.mapping.items():
            if etype not in cache:
                raw_df: pd.DataFrame = _source_to_pandas(table.source_obj)
                cache[etype] = raw_df.set_index(ID_COLUMN)
            ids = cache[etype].index
            for eid in ids:
                id_etype[eid] = etype
        ctx._id_etype_map = id_etype  # noqa: SLF001
        return id_etype

    def _get_property_multitype(
        self,
        var_name: str,
        prop_name: str,
    ) -> FrameSeries | None:
        """Look up *prop_name* for a variable whose IDs span multiple entity types.

        Used when a variable (e.g. from ``MATCH (n) WHERE n:A OR n:B``) has
        IDs from more than one entity table.  Builds an ID→value mapping from
        all entity tables that have the requested property column, then returns
        a Series aligned with ``self.bindings``.

        Only materialises the subset of each table that overlaps with the IDs
        present in ``self.bindings[var_name]``, avoiding full-table scans on
        large entity tables.

        Returns ``None`` if no entity table with the property could be found
        (caller should fall back to raising a descriptive error).
        """
        shadow = self._shadow
        cache = self._property_lookup_cache_ctx

        # Pre-compute the set of IDs we actually need.  This avoids
        # materialising entire entity tables when only a handful of IDs
        # are present in the current bindings.
        id_series: pd.Series = self.bindings[var_name]
        needed_ids = set(id_series.dropna().unique())
        if not needed_ids:
            return _null_series(len(self.bindings), index=self.bindings.index)

        # Use pre-computed ID→entity-type map to skip irrelevant tables.
        id_etype_map = self._get_id_to_etype_map()
        relevant_etypes = {
            id_etype_map[eid] for eid in needed_ids if eid in id_etype_map
        }

        id_to_value: dict = {}
        found_any = False
        for etype, table in self.context.entity_mapping.mapping.items():
            shadow_df = shadow.get(etype)
            # Skip entity types with no matching IDs (unless shadowed).
            if shadow_df is None and etype not in relevant_etypes:
                continue
            if shadow_df is not None:
                # Shadow data bypasses cache — may have SET-added columns
                if (
                    ID_COLUMN not in shadow_df.columns
                    or prop_name not in shadow_df.columns
                ):
                    continue
                # Filter to only needed IDs before materialising.
                mask = shadow_df[ID_COLUMN].isin(needed_ids)
                if not mask.any():
                    continue
                subset = shadow_df.loc[mask, [ID_COLUMN, prop_name]]
                id_arr = subset[ID_COLUMN].to_numpy(dtype=object)
                val_arr = subset[prop_name].to_numpy(dtype=object)
            else:
                if etype not in cache:
                    raw_df: pd.DataFrame = _source_to_pandas(table.source_obj)
                    cache[etype] = raw_df.set_index(ID_COLUMN)
                idx_df = cache[etype]
                if prop_name not in idx_df.columns:
                    continue
                # Use .reindex() to look up only the IDs we need — O(k) not O(N).
                relevant = idx_df.index.isin(needed_ids)
                if not relevant.any():
                    continue
                subset_idx = idx_df.loc[relevant, [prop_name]]
                id_arr = subset_idx.index.to_numpy(dtype=object)
                val_arr = subset_idx[prop_name].to_numpy(dtype=object)
            found_any = True
            id_to_value.update(zip(id_arr, val_arr, strict=False))
            # Early exit: if we've resolved all needed IDs, no point scanning more tables.
            needed_ids -= set(id_arr)
            if not needed_ids:
                break
        if not found_any:
            return None
        result = id_series.map(id_to_value)
        result = result.astype(object)
        nan_mask = result.isna() & id_series.notna()
        result[nan_mask] = None
        return result

    def _infer_entity_type(self, var_name: str) -> str | None:
        """Infer the entity type for *var_name* by scanning entity tables.

        When a node has no label in the MATCH pattern (e.g. ``(q)``), the
        variable is not added to ``type_registry``.  This method recovers the
        entity type by finding which entity table's ``__ID__`` column contains
        the set of IDs present in ``self.bindings[var_name]``.

        Returns the entity type string if found, otherwise ``None``.
        """
        if var_name not in self.bindings.columns:
            return None
        try:
            var_ids = set(self.bindings[var_name].dropna().unique())
        except TypeError:
            # Series contains unhashable values (e.g. dicts from UNWIND of a
            # list of maps) — these are never entity IDs, so no type can be
            # inferred.
            return None
        if not var_ids:
            return None
        shadow = self._shadow
        cache = self._property_lookup_cache_ctx
        n_needed = len(var_ids)
        for etype, entity_table in self.context.entity_mapping.mapping.items():
            shadow_df = shadow.get(etype)
            if shadow_df is not None:
                if ID_COLUMN not in shadow_df.columns:
                    continue
                # Use .isin() for O(k) check rather than materialising full set.
                hit_count = shadow_df[ID_COLUMN].isin(var_ids).sum()
            else:
                if etype not in cache:
                    raw_df: pd.DataFrame = _source_to_pandas(
                        entity_table.source_obj,
                    )
                    cache[etype] = raw_df.set_index(ID_COLUMN)
                idx_df = cache[etype]
                hit_count = idx_df.index.isin(var_ids).sum()
            if hit_count >= n_needed:
                return etype
        return None

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------

    def filter(self, mask: FrameSeries) -> BindingFrame:
        """Return a new BindingFrame containing only rows where *mask* is True.

        Args:
            mask: A boolean ``pd.Series`` aligned with ``self.bindings``.

        Returns:
            A new BindingFrame with the same type_registry and context.

        """
        if _DEBUG_ENABLED:
            _t0 = time.perf_counter()
            _rows_before = len(self.bindings)
        backend = self._backend
        if backend is not None:
            filtered = backend.filter(self.bindings, mask.values)
        else:
            filtered = self.bindings[mask.values]
            # Skip reset_index when already contiguous — avoids a full copy.
            idx = filtered.index
            if not (
                isinstance(idx, pd.RangeIndex)
                and idx.start == 0
                and idx.step == 1
                and idx.stop == len(filtered)
            ):
                filtered = filtered.reset_index(drop=True)
        if _DEBUG_ENABLED:
            _rows_after = len(filtered)
            LOGGER.debug(
                "BindingFrame.filter  rows_before=%d  rows_after=%d  selectivity=%.2f  elapsed=%.4fs",
                _rows_before,
                _rows_after,
                _rows_after / _rows_before if _rows_before else 0.0,
                time.perf_counter() - _t0,
            )
        # type_registry is shared (not copied) — filter does not change
        # the set of variables or their types, so a reference is safe.
        #
        # Carry forward property cache: re-align cached Series with the
        # filtered index.  This avoids re-fetching properties already
        # resolved during WHERE when they are accessed again in RETURN.
        carried_cache: dict[tuple[str, str], FrameSeries] = {}
        if self._property_cache:
            for cache_key, cached_series in self._property_cache.items():
                try:
                    aligned = cached_series.iloc[mask.values]
                    if not (
                        isinstance(aligned.index, pd.RangeIndex)
                        and aligned.index.start == 0
                        and aligned.index.step == 1
                        and aligned.index.stop == len(aligned)
                    ):
                        aligned = aligned.reset_index(drop=True)
                    carried_cache[cache_key] = aligned
                except (IndexError, KeyError, ValueError):
                    pass  # Cache entry incompatible — skip, will be re-fetched

        return BindingFrame(
            bindings=filtered,
            type_registry=self.type_registry,
            context=self.context,
            _property_cache=carried_cache,
        )

    # ------------------------------------------------------------------
    # Joining helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _cleanup_merged(
        merged: pd.DataFrame,
        left_col: str | None = None,
        right_col: str | None = None,
    ) -> pd.DataFrame:
        """Drop redundant join key and pandas ``_right`` collision suffixes.

        After a pandas ``merge()`` call, this helper performs the three
        post-merge cleanup steps shared by :meth:`join`, :meth:`left_join`,
        and :meth:`cross_join`:

        1. Drop the redundant right join key when it differs from the left key.
        2. Drop any columns pandas suffixed with ``_right`` (collision suffix).
        3. Reset the index to a clean 0-based ``RangeIndex``.

        Args:
            merged: The raw ``pd.DataFrame`` returned by ``pd.merge()``.
            left_col: Name of the left join key (or ``None`` for cross joins).
            right_col: Name of the right join key (or ``None`` for cross joins).

        Returns:
            A cleaned ``pd.DataFrame`` ready to wrap in a new
            :class:`BindingFrame`.

        """
        # Collect all columns to drop in a single pass (avoids multiple
        # DataFrame.drop() calls, each of which copies the frame).
        _drop: list[str] = []
        if (
            left_col is not None
            and right_col is not None
            and left_col != right_col
            and right_col in merged.columns
        ):
            _drop.append(right_col)
        # Only scan for "_right" suffixes — pandas adds these when column
        # names collide during merge (suffixes=("", "_right")).
        for _c in merged.columns:
            if _c.endswith("_right"):
                _drop.append(_c)
        if _drop:
            merged = merged.drop(columns=_drop)
        # Skip reset_index when the index is already a contiguous RangeIndex
        # starting at 0 — avoids an unnecessary full-frame copy.
        idx = merged.index
        if not (
            isinstance(idx, pd.RangeIndex)
            and idx.start == 0
            and idx.step == 1
            and idx.stop == len(merged)
        ):
            merged = merged.reset_index(drop=True)
        return merged

    # ------------------------------------------------------------------
    # Joining
    # ------------------------------------------------------------------

    def join(
        self,
        other: BindingFrame,
        left_col: str,
        right_col: str,
        *,
        join_plan: object | None = None,
    ) -> BindingFrame:
        """Inner-join two BindingFrames on a pair of columns.

        Used to connect a node scan to a relationship scan (or two scans of
        different entity types that share a structural constraint).

        After the join the redundant *right_col* is dropped when it differs
        from *left_col*, leaving a single unified column for the shared ID.

        Args:
            other: The right-hand BindingFrame.
            left_col: Column in this frame to join on.
            right_col: Column in *other* to join on.

        Returns:
            A new BindingFrame whose type_registry merges both sides.

        Raises:
            VariableNotFoundError: If either join column is absent from its frame.

        """
        if _DEBUG_ENABLED:
            _t0 = time.perf_counter()

        if left_col not in self.bindings.columns:
            from pycypher.exceptions import VariableNotFoundError

            available = self.var_names
            hint = suggest_close_match(left_col, available)
            raise VariableNotFoundError(left_col, available, hint)
        if right_col not in other.bindings.columns:
            from pycypher.exceptions import VariableNotFoundError

            available = other.var_names
            hint = suggest_close_match(right_col, available)
            raise VariableNotFoundError(right_col, available, hint)

        # --- Adaptive join strategy selection ---
        # Use the pre-computed join plan from QueryPlanAnalyzer when available,
        # falling back to on-the-fly planning for standalone calls.
        _left_len = len(self.bindings)
        _right_len = len(other.bindings)

        from pycypher.query_planner import JoinStrategy, get_default_planner

        if join_plan is not None:
            _plan = join_plan
        else:
            _plan = get_default_planner().plan_join(
                left_name=left_col,
                right_name=right_col,
                left_rows=_left_len,
                right_rows=_right_len,
                join_key=left_col,
            )

        backend = self._backend
        if _plan.strategy == JoinStrategy.BROADCAST and _right_len > _left_len:
            # Swap so smaller side is the build table for the hash join.
            if backend is not None:
                merged = _backend_merge(
                    backend,
                    other.bindings,
                    self.bindings,
                    left_col=right_col,
                    right_col=left_col,
                    how="inner",
                    strategy=_plan.strategy.value,
                    suffixes=("_right", ""),
                )
                # _backend_merge normalises both join keys to the same name
                # (right_col after the swap).  Restore the original left_col
                # so that downstream code still sees the caller's variable.
                if (
                    left_col != right_col
                    and left_col not in merged.columns
                    and right_col in merged.columns
                ):
                    merged = merged.rename(columns={right_col: left_col})
            else:
                merged = other.bindings.merge(
                    self.bindings,
                    left_on=right_col,
                    right_on=left_col,
                    how="inner",
                    suffixes=("_right", ""),
                )
        else:
            if backend is not None:
                merged = _backend_merge(
                    backend,
                    self.bindings,
                    other.bindings,
                    left_col=left_col,
                    right_col=right_col,
                    how="inner",
                    strategy=_plan.strategy.value,
                )
            else:
                merged = self.bindings.merge(
                    other.bindings,
                    left_on=left_col,
                    right_on=right_col,
                    how="inner",
                    suffixes=("", "_right"),
                )

        merged = BindingFrame._cleanup_merged(merged, left_col, right_col)
        # Build merged registry: when one side is empty (common for initial
        # scan→rel join), avoid dict unpacking overhead entirely.
        if not self.type_registry:
            merged_registry = other.type_registry
        elif not other.type_registry:
            merged_registry = self.type_registry
        else:
            merged_registry = {**self.type_registry, **other.type_registry}

        if _DEBUG_ENABLED:
            LOGGER.debug(
                "BindingFrame.join  left_col=%s  right_col=%s  left_rows=%d  right_rows=%d  result_rows=%d  strategy=%s  elapsed=%.4fs",
                left_col,
                right_col,
                _left_len,
                _right_len,
                len(merged),
                _plan.strategy.value,
                time.perf_counter() - _t0,
            )
        return BindingFrame(
            bindings=merged,
            type_registry=merged_registry,
            context=self.context,
        )

    def left_join(
        self,
        other: BindingFrame,
        left_col: str,
        right_col: str,
    ) -> BindingFrame:
        """Left-join two BindingFrames: all rows from *self* are preserved.

        Rows in *self* that have no matching row in *other* receive ``NaN``
        / ``None`` for all columns contributed by *other*.  This implements
        the semantics of ``OPTIONAL MATCH``.

        Args:
            other: The right-hand BindingFrame.
            left_col: Column in this frame to join on.
            right_col: Column in *other* to join on.

        Returns:
            A new BindingFrame whose type_registry merges both sides.

        Raises:
            VariableNotFoundError: If either join column is absent from its frame.

        """
        if _DEBUG_ENABLED:
            _t0 = time.perf_counter()
            _left_rows = len(self.bindings)
            _right_rows = len(other.bindings)

        if left_col not in self.bindings.columns:
            from pycypher.exceptions import VariableNotFoundError

            available = self.var_names
            hint = suggest_close_match(left_col, available)
            raise VariableNotFoundError(left_col, available, hint)
        if right_col not in other.bindings.columns:
            from pycypher.exceptions import VariableNotFoundError

            available = other.var_names
            hint = suggest_close_match(right_col, available)
            raise VariableNotFoundError(right_col, available, hint)

        backend = self._backend
        if backend is not None:
            merged: pd.DataFrame = _backend_merge(
                backend,
                self.bindings,
                other.bindings,
                left_col=left_col,
                right_col=right_col,
                how="left",
            )
        else:
            merged: pd.DataFrame = self.bindings.merge(
                other.bindings,
                left_on=left_col,
                right_on=right_col,
                how="left",
                suffixes=("", "_right"),
            )

        merged = BindingFrame._cleanup_merged(merged, left_col, right_col)
        if not self.type_registry:
            merged_registry = other.type_registry
        elif not other.type_registry:
            merged_registry = self.type_registry
        else:
            merged_registry = {**self.type_registry, **other.type_registry}

        if _DEBUG_ENABLED:
            LOGGER.debug(
                "BindingFrame.left_join  left_col=%s  right_col=%s  left_rows=%d  right_rows=%d  result_rows=%d  elapsed=%.4fs",
                left_col,
                right_col,
                _left_rows,
                _right_rows,
                len(merged),
                time.perf_counter() - _t0,
            )
        return BindingFrame(
            bindings=merged,
            type_registry=merged_registry,
            context=self.context,
        )

    def cross_join(self, other: BindingFrame) -> BindingFrame:
        """Cartesian-product two BindingFrames (no join key).

        Produces a frame with ``len(self) × len(other)`` rows and every column
        from both frames.  When column names collide, *other*'s columns are
        suffixed with ``_right`` and then dropped (same behaviour as
        :meth:`join`).

        Args:
            other: The right-hand BindingFrame.

        Returns:
            A new BindingFrame whose type_registry merges both sides.

        Raises:
            QueryMemoryBudgetError: If the projected result size exceeds
                :data:`MAX_CROSS_JOIN_ROWS`.

        """
        if _DEBUG_ENABLED:
            _t0 = time.perf_counter()
        _left_rows = len(self.bindings)
        _right_rows = len(other.bindings)
        result_size = _left_rows * _right_rows

        # Log cardinality estimate before execution for monitoring.
        LOGGER.info(
            "Cross-join cardinality estimate: %s rows (%d × %d)",
            f"{result_size:,}",
            _left_rows,
            _right_rows,
        )

        # Hard ceiling — refuse to execute.
        if result_size > MAX_CROSS_JOIN_ROWS:
            msg = (
                f"Cross-join would produce {result_size:,} rows "
                f"({_left_rows:,} × {_right_rows:,}), "
                f"exceeding the {MAX_CROSS_JOIN_ROWS:,}-row safety limit.\n"
                "To fix:\n"
                "  1. Add WHERE filters to reduce matched rows\n"
                "  2. Add LIMIT to cap the result size\n"
                "  3. Increase limit via PYCYPHER_MAX_CROSS_JOIN_ROWS env var"
            )
            from pycypher.exceptions import QueryMemoryBudgetError

            _BYTES_PER_ROW = 200
            raise QueryMemoryBudgetError(
                estimated_bytes=result_size * _BYTES_PER_ROW,
                budget_bytes=MAX_CROSS_JOIN_ROWS * _BYTES_PER_ROW,
                suggestion=msg,
            )

        # Progressive warnings at configured thresholds.
        for threshold in CROSS_JOIN_WARN_THRESHOLDS:
            if result_size > threshold:
                LOGGER.warning(
                    "Cross-join producing %s rows (%d × %d) exceeds %s-row"
                    " warning threshold — consider adding a WHERE clause",
                    f"{result_size:,}",
                    _left_rows,
                    _right_rows,
                    f"{threshold:,}",
                )
        backend = self._backend
        if backend is not None:
            # Resolve non-join column collisions before cross join
            left_other = set(self.bindings.columns)
            right_other = set(other.bindings.columns)
            collisions = left_other & right_other
            right_df = other.bindings
            if collisions:
                rename_map = {c: f"{c}_right" for c in collisions}
                right_df = backend.rename(right_df, rename_map)
            merged: pd.DataFrame = backend.join(
                self.bindings,
                right_df,
                on=[],
                how="cross",
            )
        else:
            merged: pd.DataFrame = self.bindings.merge(
                other.bindings,
                how="cross",
                suffixes=("", "_right"),
            )
        merged = BindingFrame._cleanup_merged(merged)
        if not self.type_registry:
            merged_registry = other.type_registry
        elif not other.type_registry:
            merged_registry = self.type_registry
        else:
            merged_registry = {**self.type_registry, **other.type_registry}
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "BindingFrame.cross_join  left_rows=%d  right_rows=%d  result_rows=%d  elapsed=%.4fs",
                _left_rows,
                _right_rows,
                len(merged),
                time.perf_counter() - _t0,
            )
        return BindingFrame(
            bindings=merged,
            type_registry=merged_registry,
            context=self.context,
        )

    # ------------------------------------------------------------------
    # Renaming
    # ------------------------------------------------------------------

    def rename(
        self,
        old_col: str,
        new_col: str,
        new_type: str | None = None,
    ) -> BindingFrame:
        """Return a new BindingFrame with one column renamed.

        Used by the pattern translator to promote structural join-key columns
        (e.g. ``_tgt_r``) to named Cypher variables (e.g. ``q``).

        Args:
            old_col: Existing column name.
            new_col: New column name.
            new_type: If given, registers *new_col* with this type in the
                type_registry.  If ``None`` and *old_col* was registered,
                the registry entry is moved from *old_col* to *new_col*.

        Returns:
            A new BindingFrame with the renamed column.

        Raises:
            VariableNotFoundError: If *old_col* is not in the bindings.

        """
        if old_col not in self.bindings.columns:
            from pycypher.exceptions import VariableNotFoundError

            available = self.var_names
            hint = suggest_close_match(old_col, available)
            raise VariableNotFoundError(old_col, available, hint)

        backend = self._backend
        if backend is not None:
            new_bindings = backend.rename(self.bindings, {old_col: new_col})
        else:
            new_bindings = self.bindings.rename(columns={old_col: new_col})
        # Build registry efficiently: only copy when we actually need to
        # mutate (i.e., when old_col is registered or new_type is given).
        if new_type is not None:
            new_registry = {**self.type_registry, new_col: new_type}
        elif old_col in self.type_registry:
            # Move old_col → new_col: rebuild without old_col, add new_col.
            new_registry = {
                (new_col if k == old_col else k): v
                for k, v in self.type_registry.items()
            }
        else:
            # No registry change needed — share reference.
            new_registry = self.type_registry

        return BindingFrame(
            bindings=new_bindings,
            type_registry=new_registry,
            context=self.context,
        )

    # ------------------------------------------------------------------
    # Projection (output production)
    # ------------------------------------------------------------------

    def project(self, computed: dict[str, FrameSeries]) -> FrameDataFrame:
        """Build an output DataFrame from pre-evaluated column Series.

        This is the final step before returning results to the caller.  Each
        key in *computed* becomes a column in the output DataFrame.

        In Phase 1 the caller is responsible for evaluating expressions into
        Series first (e.g. using ``get_property``).  Phase 4 will introduce
        ``BindingExpressionEvaluator`` which accepts ``Expression`` objects
        directly.

        Args:
            computed: Mapping from output alias to a ``pd.Series`` aligned
                with ``self.bindings``.

        Returns:
            A plain ``pd.DataFrame`` with one column per alias.

        """
        return pd.DataFrame(
            {alias: series.values for alias, series in computed.items()},
        )

    # ------------------------------------------------------------------
    # Mutation (SET clause write-back)
    # ------------------------------------------------------------------

    def mutate(
        self,
        var_name: str,
        prop_name: str,
        values: FrameSeries,
    ) -> None:
        """Write a new or updated property back to the entity table in context.

        Aligns *values* by entity ID so that only the rows present in this
        BindingFrame are updated; rows not present are left unchanged.

        This mutates ``context.entity_mapping[type].source_obj`` in place,
        which is the same object that ``get_property`` reads from.

        Args:
            var_name: The Cypher variable whose entity table is updated.
            prop_name: The attribute name to set or overwrite.
            values: A ``pd.Series`` of new values aligned with ``self.bindings``.

        Raises:
            VariableNotFoundError: If *var_name* is not in the frame.
            GraphTypeNotFoundError: If the variable's type is not in any mapping.

        """
        if _DEBUG_ENABLED:
            _t0 = time.perf_counter()
            _rows = len(self.bindings)

        if var_name not in self.bindings.columns:
            from pycypher.exceptions import VariableNotFoundError

            available = self.var_names
            hint = suggest_close_match(var_name, available)
            raise VariableNotFoundError(var_name, available, hint)

        entity_type = self.type_registry[var_name]
        # Check entity_mapping first; fall back to relationship_mapping for
        # relationship variables (e.g. SET r.weight = 1.0 where r : KNOWS).
        if entity_type in self.context.entity_mapping.mapping:
            entity_table = self.context.entity_mapping[entity_type]
        elif entity_type in self.context.relationship_mapping.mapping:
            entity_table = self.context.relationship_mapping.mapping[
                entity_type
            ]
        else:
            from pycypher.exceptions import GraphTypeNotFoundError

            raise GraphTypeNotFoundError(
                entity_type,
                f"Variable '{var_name}' has type {entity_type!r} which is not found "
                f"in entity_mapping or relationship_mapping. "
                f"Available entities: {list(self.context.entity_mapping.mapping.keys())}, "
                f"Available relationships: {list(self.context.relationship_mapping.mapping.keys())}",
            )

        # Choose the correct shadow layer: entities go to _shadow,
        # relationship properties go to _shadow_rels.  Using the wrong layer
        # causes commit_query() to store the mutated data in the wrong mapping
        # (entity vs relationship), silently corrupting context state.
        is_relationship = (
            entity_type in self.context.relationship_mapping.mapping
        )
        if is_relationship:
            shadow = getattr(self.context, "_shadow_rels", None)
        else:
            shadow = getattr(self.context, "_shadow", None)

        if shadow is not None:
            if entity_type not in shadow:
                shadow[entity_type] = _source_to_pandas(
                    entity_table.source_obj,
                ).copy()
            source_df: pd.DataFrame = shadow[entity_type]
        else:
            source_df = _source_to_pandas(entity_table.source_obj)

        id_series: pd.Series = self.bindings[var_name]

        # Build an ID → new value mapping.
        update_map: dict[Any, Any] = dict(
            zip(id_series.values, values.values, strict=False),
        )

        # Vectorised O(n) write-back.  Two cases:
        #
        # New property: build the full column in one pass via .map() — unmatched
        # IDs naturally become NaN (pd.Series.map returns NaN for missing keys).
        # This avoids dtype conflicts that arise from first setting None and then
        # trying to assign numeric values into the resulting string/object column.
        #
        # Existing property: use boolean .loc to only overwrite matched rows;
        # unmatched rows keep their current value.
        if prop_name not in source_df.columns:
            source_df[prop_name] = source_df[ID_COLUMN].map(update_map)
        else:
            # Use isin() to identify rows that have an update, then map()
            # only those rows.  This correctly handles None/NaN update values
            # (e.g. from REMOVE operations) which .map().notna() would miss.
            matched_mask: pd.Series = source_df[ID_COLUMN].isin(update_map)
            if matched_mask.any():
                mapped_vals = source_df.loc[
                    matched_mask, ID_COLUMN
                ].map(update_map)
                try:
                    source_df.loc[matched_mask, prop_name] = mapped_vals
                except (TypeError, ValueError):
                    # Arrow-backed or strictly-typed columns reject incompatible
                    # dtype assignments.  Fall back to object-dtype merge.
                    col = source_df[prop_name].astype(object).copy()
                    col.loc[matched_mask] = mapped_vals.values
                    source_df[prop_name] = col

        # If no shadow layer, write directly back (legacy path)
        if shadow is None:
            entity_table.source_obj = source_df

        # Register new property in attribute maps so get_property can resolve it
        if prop_name not in entity_table.attribute_map:
            entity_table.attribute_map[prop_name] = prop_name
        if prop_name not in entity_table.source_obj_attribute_map:
            entity_table.source_obj_attribute_map[prop_name] = prop_name
        if prop_name not in entity_table.column_names:
            entity_table.column_names.append(prop_name)

        if _DEBUG_ENABLED:
            LOGGER.debug(
                "BindingFrame.mutate  var=%s  prop=%s  entity_type=%s  rows=%d  elapsed=%.4fs",
                var_name,
                prop_name,
                entity_type,
                _rows,
                time.perf_counter() - _t0,
            )

    def mutate_batch(
        self,
        var_name: str,
        properties: dict[str, FrameSeries],
    ) -> None:
        """Write multiple properties back to the entity table in a single pass.

        This is a batch optimisation of :meth:`mutate` for the ``SET p = {…}``
        map-literal expansion pattern.  Instead of calling :meth:`mutate` once
        per key (each of which builds a separate dict and scans the ID column),
        this method builds a single updates DataFrame with all properties and
        applies them via a single ``merge`` + column assignment.

        For *k* properties and *n* rows, :meth:`mutate` called *k* times is
        O(n * k) with *k* dict constructions and *k* ID column scans.
        ``mutate_batch`` reduces this to O(n + k) — one merge pass plus per-column
        assignment from the merged result.

        Args:
            var_name: The Cypher variable whose entity table is updated.
            properties: Mapping from property name to a ``pd.Series`` of new
                values, each aligned with ``self.bindings``.

        Raises:
            VariableNotFoundError: If *var_name* is not in the frame.
            GraphTypeNotFoundError: If the variable's type is not in any mapping.

        """
        if not properties:
            return

        # For single property, delegate to mutate (no overhead from merge).
        if len(properties) == 1:
            prop_name, values = next(iter(properties.items()))
            self.mutate(var_name, prop_name, values)
            return

        if _DEBUG_ENABLED:
            _t0 = time.perf_counter()
            _rows = len(self.bindings)

        if var_name not in self.bindings.columns:
            from pycypher.exceptions import VariableNotFoundError

            available = self.var_names
            hint = suggest_close_match(var_name, available)
            raise VariableNotFoundError(var_name, available, hint)

        entity_type = self.type_registry[var_name]
        if entity_type in self.context.entity_mapping.mapping:
            entity_table = self.context.entity_mapping[entity_type]
        elif entity_type in self.context.relationship_mapping.mapping:
            entity_table = self.context.relationship_mapping.mapping[
                entity_type
            ]
        else:
            from pycypher.exceptions import GraphTypeNotFoundError

            raise GraphTypeNotFoundError(
                entity_type,
                f"Variable '{var_name}' has type {entity_type!r} which is not found "
                f"in entity_mapping or relationship_mapping.",
            )

        is_relationship = (
            entity_type in self.context.relationship_mapping.mapping
        )
        shadow = getattr(
            self.context,
            "_shadow_rels" if is_relationship else "_shadow",
            None,
        )

        if shadow is not None:
            if entity_type not in shadow:
                shadow[entity_type] = _source_to_pandas(
                    entity_table.source_obj,
                ).copy()
            source_df: pd.DataFrame = shadow[entity_type]
        else:
            source_df = _source_to_pandas(entity_table.source_obj)

        id_series: pd.Series = self.bindings[var_name]

        # Build a single updates DataFrame: one row per unique ID, one column
        # per property.  Deduplicates IDs so the merge is on unique keys.
        updates_data: dict[str, Any] = {ID_COLUMN: id_series.values}
        for prop_name, values in properties.items():
            updates_data[prop_name] = values.values
        updates_df = pd.DataFrame(updates_data).drop_duplicates(
            subset=[ID_COLUMN], keep="last",
        )

        # Single merge: left join source IDs against the updates.
        # This produces _prop_new columns for each property that has a match.
        suffixed_cols = {
            prop: f"_batch_{prop}" for prop in properties
        }
        updates_renamed = updates_df.rename(
            columns={p: s for p, s in suffixed_cols.items()},
        )

        merged = source_df[[ID_COLUMN]].merge(
            updates_renamed, on=ID_COLUMN, how="left",
        )

        # Apply each property from the merged result.
        for prop_name in properties:
            new_col = suffixed_cols[prop_name]
            new_values = merged[new_col]
            has_new = new_values.notna()

            if prop_name not in source_df.columns:
                # New property: assign directly from merge result.
                source_df[prop_name] = new_values.values
            elif has_new.any():
                try:
                    source_df[prop_name] = np.where(
                        has_new.values,
                        new_values.values,
                        source_df[prop_name].values,
                    )
                except (TypeError, ValueError):
                    combined = np.where(
                        has_new.values,
                        new_values.values,
                        source_df[prop_name].values,
                    )
                    source_df[prop_name] = pd.Series(
                        combined, index=source_df.index, dtype=object,
                    )

            # Register new property in attribute maps.
            if prop_name not in entity_table.attribute_map:
                entity_table.attribute_map[prop_name] = prop_name
            if prop_name not in entity_table.source_obj_attribute_map:
                entity_table.source_obj_attribute_map[prop_name] = prop_name
            if prop_name not in entity_table.column_names:
                entity_table.column_names.append(prop_name)

        if shadow is None:
            entity_table.source_obj = source_df

        if _DEBUG_ENABLED:
            LOGGER.debug(
                "BindingFrame.mutate_batch  var=%s  props=%d  entity_type=%s  "
                "rows=%d  elapsed=%.4fs",
                var_name,
                len(properties),
                entity_type,
                _rows,
                time.perf_counter() - _t0,
            )


# ---------------------------------------------------------------------------
# Scan and filter operators — re-exported for backward compatibility.
# New code should import directly from pycypher.scan_operators.
# ---------------------------------------------------------------------------
from pycypher.scan_operators import BindingFilter as BindingFilter
from pycypher.scan_operators import EntityScan as EntityScan
from pycypher.scan_operators import RelationshipScan as RelationshipScan
