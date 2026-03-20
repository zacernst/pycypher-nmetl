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
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
from shared.helpers import suggest_close_match
from shared.logger import LOGGER

from pycypher.config import MAX_CROSS_JOIN_ROWS as MAX_CROSS_JOIN_ROWS
from pycypher.constants import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
)
from pycypher.types import FrameDataFrame, FrameSeries

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
    if result.dtype == object:
        _needs_none = False
        _needs_list = False
        for _i in range(min(len(_vals), 1)):
            _v = _vals[_i]
            if isinstance(_v, np.ndarray):
                _needs_list = True
        if len(_vals) > 0:
            _na_mask = pd.isna(result)
            _needs_none = _na_mask.any()
        if _needs_none and _needs_list:
            _new = np.empty(len(_vals), dtype=object)
            for _i in range(len(_vals)):
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


def _source_to_pandas(obj: Any) -> pd.DataFrame:
    """Convert *obj* to a pandas DataFrame.

    If PyArrow is installed and *obj* is a ``pyarrow.Table``, it is converted
    via ``.to_pandas()``.  Otherwise *obj* is assumed to already be a
    ``pd.DataFrame`` and is returned unchanged.

    Args:
        obj: A ``pd.DataFrame`` or ``pyarrow.Table``.

    Returns:
        A ``pd.DataFrame``.

    """
    if _PYARROW_TABLE_TYPE is not None and isinstance(
        obj, _PYARROW_TABLE_TYPE
    ):
        return obj.to_pandas()
    return obj


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
    # Property access
    # ------------------------------------------------------------------

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
        shadow: dict[str, pd.DataFrame] = getattr(self.context, "_shadow", {})
        shadow_rels: dict[str, pd.DataFrame] = getattr(
            self.context, "_shadow_rels", {},
        )
        cache: dict[str, pd.DataFrame] = getattr(
            self.context, "_property_lookup_cache", {},
        )

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
                return row_values
            return _null_series(len(self.bindings), index=self.bindings.index)

        indexed_df = self._get_indexed_dataframe(entity_type)
        if indexed_df is None:
            return _null_series(len(self.bindings), index=self.bindings.index)

        if prop_name not in indexed_df.columns:
            # Per Cypher semantics, accessing a nonexistent property returns null.
            return _null_series(len(self.bindings), index=self.bindings.index)
        lookup: pd.Series = indexed_df[prop_name]

        # Map entity IDs in this frame to property values.
        # pd.Series.map() converts both "missing key" and null property values
        # (stored as NaN by pandas) to float NaN.  Cypher null semantics require
        # Python None so that downstream ``x is None`` guards in scalar functions
        # (isString, isFloat, isNaN, …) correctly propagate null rather than
        # misclassifying float('nan') as a valid non-null value.
        id_series: pd.Series = self.bindings[var_name]
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
        return result

    def get_properties_batch(
        self, var_name: str, prop_names: list[str]
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

        # Fall back for edge cases: missing variable, multi-type, unresolvable
        if var_name not in self.bindings.columns:
            return {p: self.get_property(var_name, p) for p in prop_names}

        entity_type = self.type_registry.get(var_name)
        if entity_type is None or entity_type == "__MULTI__":
            return {p: self.get_property(var_name, p) for p in prop_names}

        # Resolve the indexed DataFrame (shared cache logic with get_property)
        indexed_df = self._get_indexed_dataframe(entity_type)
        if indexed_df is None:
            return {
                p: _null_series(len(self.bindings), index=self.bindings.index)
                for p in prop_names
            }

        id_series: pd.Series = self.bindings[var_name]

        # Identify which requested props actually exist as columns
        available = [p for p in prop_names if p in indexed_df.columns]
        missing = [p for p in prop_names if p not in indexed_df.columns]

        results: dict[str, FrameSeries] = {}

        if available:
            # Map each property via the cached indexed_df — the entity type
            # resolution and cache lookup above are amortized across all props.
            for prop in available:
                lookup: pd.Series = indexed_df[prop]
                result: pd.Series = id_series.map(lookup)
                result = _normalize_mapped_result(result)
                result.index = self.bindings.index
                results[prop] = result

        # Missing properties → null Series
        for prop in missing:
            results[prop] = _null_series(
                len(self.bindings), index=self.bindings.index
            )

        return results

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
        shadow = getattr(self.context, "_shadow", {})
        cache: dict = getattr(self.context, "_property_lookup_cache", {})

        # Pre-compute the set of IDs we actually need.  This avoids
        # materialising entire entity tables when only a handful of IDs
        # are present in the current bindings.
        id_series: pd.Series = self.bindings[var_name]
        needed_ids = set(id_series.dropna().unique())
        if not needed_ids:
            return _null_series(len(self.bindings), index=self.bindings.index)

        id_to_value: dict = {}
        found_any = False
        for etype, table in self.context.entity_mapping.mapping.items():
            shadow_df = shadow.get(etype)
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
        shadow = getattr(self.context, "_shadow", {})
        cache: dict = getattr(self.context, "_property_lookup_cache", {})
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
        filtered = self.bindings[mask.values].reset_index(drop=True)
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
        return BindingFrame(
            bindings=filtered,
            type_registry=self.type_registry,
            context=self.context,
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
        return merged.reset_index(drop=True)

    # ------------------------------------------------------------------
    # Joining
    # ------------------------------------------------------------------

    def join(
        self,
        other: BindingFrame,
        left_col: str,
        right_col: str,
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

        # --- Adaptive join strategy selection ---
        # Use the QueryPlanner to classify the join (broadcast/hash/merge).
        # Today this is informational — pandas handles strategy internally.
        # The classification is logged and available for future backends
        # (DuckDB/Polars) that benefit from explicit strategy hints.
        if _DEBUG_ENABLED and len(self.bindings) + len(other.bindings) > 0:
            from pycypher.query_planner import get_default_planner

            _plan = get_default_planner().plan_join(
                left_name=left_col,
                right_name=right_col,
                left_rows=len(self.bindings),
                right_rows=len(other.bindings),
                join_key=left_col,
            )
            # Strategy is available at _plan.strategy for logging/monitoring.
            # Future: pass _plan.strategy.value to backend.join(strategy=...)
            # when BindingFrame fully delegates to the backend engine.

        merged: pd.DataFrame = self.bindings.merge(
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
                "BindingFrame.join  left_col=%s  right_col=%s  left_rows=%d  right_rows=%d  result_rows=%d  elapsed=%.4fs",
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
        if result_size > MAX_CROSS_JOIN_ROWS:
            msg = (
                f"Cross-join would produce {result_size:,} rows "
                f"({len(self.bindings):,} × {len(other.bindings):,}), "
                f"exceeding the {MAX_CROSS_JOIN_ROWS:,}-row safety limit.\n"
                "To fix:\n"
                "  1. Add WHERE filters to reduce matched rows\n"
                "  2. Add LIMIT to cap the result size\n"
                "  3. Increase limit via PYCYPHER_MAX_CROSS_JOIN_ROWS env var"
            )
            from pycypher.exceptions import QueryMemoryBudgetError

            # Estimate bytes: ~200 bytes per row is a conservative heuristic
            # for a DataFrame with a handful of ID/attribute columns.
            _BYTES_PER_ROW = 200
            raise QueryMemoryBudgetError(
                estimated_bytes=result_size * _BYTES_PER_ROW,
                budget_bytes=MAX_CROSS_JOIN_ROWS * _BYTES_PER_ROW,
                suggestion=msg,
            )
        if result_size > 1_000_000:
            LOGGER.warning(
                "Cross-join producing %s rows (%d x %d) — consider adding"
                " a WHERE clause to reduce result size",
                f"{result_size:,}",
                len(self.bindings),
                len(other.bindings),
            )
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
            matched_mask: pd.Series = source_df[ID_COLUMN].isin(update_map)
            try:
                source_df.loc[matched_mask, prop_name] = source_df.loc[
                    matched_mask,
                    ID_COLUMN,
                ].map(update_map)
            except (TypeError, ValueError):
                # Arrow-backed or strictly-typed columns reject incompatible
                # dtype assignments via .loc.  Fall back to replacing the entire
                # column so that type coercion (e.g. int→string or str→int)
                # is handled by pandas' column-assignment semantics.
                existing: dict[Any, Any] = dict(
                    zip(
                        source_df[ID_COLUMN],
                        source_df[prop_name],
                        strict=False,
                    ),
                )
                existing.update(update_map)
                source_df[prop_name] = source_df[ID_COLUMN].map(existing)

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


# ---------------------------------------------------------------------------
# Scan operators (Phase 2)
# ---------------------------------------------------------------------------


@dataclass
class EntityScan:
    """Produces a BindingFrame of all entity IDs for a given entity type.

    The resulting frame has a single column named *var_name* containing every
    ``__ID__`` value from the entity table.  Attributes are **not** included —
    they are fetched on demand via :meth:`BindingFrame.get_property`.

    Attributes:
        entity_type: The entity label (e.g. ``"Person"``).
        var_name: The Cypher variable name to bind the IDs to (e.g. ``"p"``).

    """

    entity_type: str
    var_name: str

    def scan(self, context: Any) -> BindingFrame:
        """Return a :class:`BindingFrame` containing all IDs for this entity type.

        Args:
            context: The query :class:`~pycypher.relational_models.Context`.

        Returns:
            A :class:`BindingFrame` with one column (*var_name*) of entity IDs.

        """
        if _DEBUG_ENABLED:
            _t0 = time.perf_counter()
        try:
            entity_table = context.entity_mapping[self.entity_type]
        except KeyError:
            from pycypher.exceptions import GraphTypeNotFoundError

            available = list(context.entity_mapping.mapping.keys())
            hint = suggest_close_match(self.entity_type, available)
            raise GraphTypeNotFoundError(
                self.entity_type,
                f"Entity type {self.entity_type!r} is not registered in the context. "
                f"Available entity types: {available or []}"
                f"{hint}",
            ) from None
        # Use the _property_lookup_cache to avoid repeated Arrow→pandas
        # conversions.  The cache stores indexed_df = raw_df.set_index(ID_COLUMN);
        # entity IDs are recovered from the index.
        cache: dict = getattr(context, "_property_lookup_cache", {})
        if self.entity_type not in cache:
            raw_df: pd.DataFrame = _source_to_pandas(entity_table.source_obj)
            cache[self.entity_type] = raw_df.set_index(ID_COLUMN)
        indexed_df = cache[self.entity_type]
        ids = pd.Series(
            indexed_df.index.to_numpy(dtype=object),
            name=ID_COLUMN,
        ).reset_index(drop=True)
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "EntityScan.scan  entity_type=%s  var=%s  rows=%d  elapsed=%.4fs",
                self.entity_type,
                self.var_name,
                len(ids),
                time.perf_counter() - _t0,
            )
        return BindingFrame(
            bindings=pd.DataFrame({self.var_name: ids}),
            type_registry={self.var_name: self.entity_type},
            context=context,
        )


@dataclass
class RelationshipScan:
    """Produces a BindingFrame of all relationship IDs for a given type.

    The resulting frame has **three** columns:

    * ``rel_var`` — the relationship's own ``__ID__``.
    * ``_src_{rel_var}`` — the source-node ``__SOURCE__`` ID.
    * ``_tgt_{rel_var}`` — the target-node ``__TARGET__`` ID.

    The source and target columns are structural join keys consumed by the
    pattern translator (Phase 5); they are **not** user-visible Cypher
    variables and are therefore absent from the ``type_registry``.

    Attributes:
        rel_type: The relationship type label (e.g. ``"KNOWS"``).
        rel_var: The Cypher variable name for the relationship (e.g. ``"r"``).
            Use a synthetic name such as ``"_anon_0"`` for anonymous
            relationships.

    """

    rel_type: str
    rel_var: str
    #: Cached column name for source-node IDs.
    src_col: str = ""
    #: Cached column name for target-node IDs.
    tgt_col: str = ""

    def __post_init__(self) -> None:
        """Cache derived column names to avoid repeated f-string creation."""
        self.src_col = f"_src_{self.rel_var}"
        self.tgt_col = f"_tgt_{self.rel_var}"

    def scan(
        self,
        context: Any,
        *,
        source_ids: FrameSeries | None = None,
        target_ids: FrameSeries | None = None,
    ) -> BindingFrame:
        """Return a :class:`BindingFrame` containing relationship IDs for this type.

        Supports **predicate pushdown**: when *source_ids* or *target_ids*
        are provided, only relationships whose ``__SOURCE__`` / ``__TARGET__``
        column values appear in the given ID set are materialised.  This
        avoids loading the full relationship table when the query pattern
        already constrains one endpoint.

        Args:
            context: The query :class:`~pycypher.relational_models.Context`.
            source_ids: If provided, only materialise relationships whose
                ``__SOURCE__`` is in this set.
            target_ids: If provided, only materialise relationships whose
                ``__TARGET__`` is in this set.

        Returns:
            A :class:`BindingFrame` with columns *rel_var*, ``_src_{rel_var}``,
            and ``_tgt_{rel_var}``.

        """
        if _DEBUG_ENABLED:
            _t0 = time.perf_counter()
        try:
            rel_table = context.relationship_mapping[self.rel_type]
        except KeyError:
            from pycypher.exceptions import GraphTypeNotFoundError

            available = list(context.relationship_mapping.mapping.keys())
            hint = suggest_close_match(self.rel_type, available)
            raise GraphTypeNotFoundError(
                self.rel_type,
                f"Relationship type {self.rel_type!r} is not registered in the context. "
                f"Available relationship types: {available or []}"
                f"{hint}",
            ) from None
        # Use the _property_lookup_cache to avoid repeated Arrow→pandas
        # conversions.  Cache key uses "__rel__" prefix to prevent collision
        # with entity type names (same convention as get_property).
        cache: dict = getattr(context, "_property_lookup_cache", {})
        cache_key = f"__rel__{self.rel_type}"
        if cache_key not in cache:
            raw_df: pd.DataFrame = _source_to_pandas(rel_table.source_obj)
            cache[cache_key] = raw_df.set_index(ID_COLUMN)
        indexed_df = cache[cache_key]

        # --- Predicate pushdown: filter at scan level ---
        # Apply endpoint filters BEFORE materialising the full DataFrame.
        # This ensures memory usage is proportional to the result set,
        # not the source table — like only transferring the consciousness
        # data that fits the target sleeve.
        mask: pd.Series | None = None
        if source_ids is not None:
            # pd.Index for O(1) hash-based isin() — avoids Python set copy.
            source_set = pd.Index(source_ids.dropna().unique())
            src_mask = indexed_df[RELATIONSHIP_SOURCE_COLUMN].isin(source_set)
            mask = src_mask if mask is None else mask & src_mask
        if target_ids is not None:
            target_set = pd.Index(target_ids.dropna().unique())
            tgt_mask = indexed_df[RELATIONSHIP_TARGET_COLUMN].isin(target_set)
            mask = tgt_mask if mask is None else mask & tgt_mask

        if mask is not None:
            filtered = indexed_df[mask]
        else:
            filtered = indexed_df

        # Recover the three columns from the (possibly filtered) DataFrame.
        ids = pd.Series(
            filtered.index.to_numpy(dtype=object),
            name=ID_COLUMN,
        ).reset_index(drop=True)
        bindings = pd.DataFrame(
            {
                self.rel_var: ids,
                self.src_col: filtered[RELATIONSHIP_SOURCE_COLUMN].to_numpy(
                    dtype=object,
                ),
                self.tgt_col: filtered[RELATIONSHIP_TARGET_COLUMN].to_numpy(
                    dtype=object,
                ),
            },
        )
        if _DEBUG_ENABLED:
            _pushdown = source_ids is not None or target_ids is not None
            LOGGER.debug(
                "RelationshipScan.scan  rel_type=%s  var=%s  rows=%d  pushdown=%s  elapsed=%.4fs",
                self.rel_type,
                self.rel_var,
                len(ids),
                _pushdown,
                time.perf_counter() - _t0,
            )
        return BindingFrame(
            bindings=bindings,
            type_registry={self.rel_var: self.rel_type},
            context=context,
        )


# ---------------------------------------------------------------------------
# Filter operator (Phase 3)
# ---------------------------------------------------------------------------


@dataclass
class BindingFilter:
    """Filters a BindingFrame by evaluating a boolean AST expression.

    This is the Phase-3 analogue of the legacy ``FilterRows`` operator.
    Unlike ``FilterRows``, it never touches prefixed column names — it
    delegates directly to :class:`~pycypher.binding_evaluator.BindingExpressionEvaluator`.

    Attributes:
        predicate: The Cypher AST boolean expression to evaluate (e.g. a
            ``Comparison``, ``And``, ``NullCheck``, etc.).

    """

    predicate: Expression

    def apply(self, frame: BindingFrame) -> BindingFrame:
        """Return a new BindingFrame containing only rows where *predicate* is True.

        Args:
            frame: The input :class:`BindingFrame`.

        Returns:
            A filtered :class:`BindingFrame`.

        """
        if _DEBUG_ENABLED:
            _t0 = time.perf_counter()
            _rows_before = len(frame)
        # Import here to avoid circular dependency at module load time
        from pycypher.binding_evaluator import BindingExpressionEvaluator

        evaluator = BindingExpressionEvaluator(frame)
        mask: FrameSeries = evaluator.evaluate(self.predicate).fillna(False)
        result = frame.filter(mask)
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "BindingFilter.apply  predicate=%s  rows_before=%d  rows_after=%d  elapsed=%.4fs",
                type(self.predicate).__name__,
                _rows_before,
                len(result),
                time.perf_counter() - _t0,
            )
        return result
