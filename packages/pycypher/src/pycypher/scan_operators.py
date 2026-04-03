"""Scan and filter operators for the BindingFrame execution path.

Extracted from :mod:`pycypher.binding_frame` to separate scan-level
concerns (entity/relationship table scanning, predicate pushdown,
dtype coercion) from the core BindingFrame data container.

Classes:
    EntityScan — produces a BindingFrame of entity IDs for a given type.
    RelationshipScan — produces a BindingFrame of relationship IDs.
    BindingFilter — filters a BindingFrame by a boolean AST predicate.

Helpers:
    _coerce_pushdown_ids — coerce pushdown IDs to match target column dtype.
    _coerce_pushdown_series — coerce a pushdown Series to match target dtype.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pandas as pd
from shared.helpers import suggest_close_match
from shared.logger import LOGGER

from pycypher.constants import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
)
from pycypher.cypher_types import FrameSeries
from pycypher.dataframe_utils import source_to_pandas as _source_to_pandas

if TYPE_CHECKING:
    from pycypher.ast_models import Expression
    from pycypher.binding_frame import BindingFrame

# ---------------------------------------------------------------------------
# Performance: module-level debug check avoids per-call overhead
# ---------------------------------------------------------------------------
_DEBUG_ENABLED: bool = LOGGER.isEnabledFor(logging.DEBUG)


# ---------------------------------------------------------------------------
# Dtype coercion helpers for predicate pushdown
# ---------------------------------------------------------------------------


def _coerce_pushdown_ids(
    ids: FrameSeries,
    target_col: pd.Series,
) -> pd.Index:
    """Build a ``pd.Index`` of unique pushdown IDs, coercing dtype to match *target_col*.

    When the DuckDB backend materialises join results via ``fetchdf()``,
    originally-integer columns may come back as ``StringDtype``.  A naive
    ``isin()`` then fails because ``'2' != 2``.  This helper converts the
    pushdown IDs to the target column's dtype so the comparison succeeds.
    """
    unique_ids = ids.dropna().unique()
    pushdown_idx = pd.Index(unique_ids)
    target_dtype = target_col.dtype

    # Fast path: dtypes already compatible.
    if pushdown_idx.dtype == target_dtype:
        return pushdown_idx

    # String-like pushdown IDs vs numeric target — try numeric conversion.
    if pd.api.types.is_string_dtype(
        pushdown_idx
    ) and pd.api.types.is_numeric_dtype(target_dtype):
        try:
            return pd.Index(pd.to_numeric(pushdown_idx))
        except (ValueError, TypeError):
            return pushdown_idx

    # Numeric pushdown IDs vs object/string target — cast to object.
    if pd.api.types.is_numeric_dtype(pushdown_idx) and (
        target_dtype == object or pd.api.types.is_string_dtype(target_dtype)
    ):
        return pushdown_idx.astype(object)

    return pushdown_idx


def _coerce_pushdown_series(
    ids: FrameSeries,
    target_col: pd.Series,
) -> pd.Series:
    """Coerce a pushdown ID Series to match *target_col*'s dtype.

    Used in :meth:`RelationshipScan.scan` to normalise pushdown IDs
    before both the adjacency-index path and the table-scan path so that
    dict lookups and ``isin()`` comparisons succeed across dtype boundaries.
    """
    target_dtype = target_col.dtype

    # Fast path: already compatible.
    if ids.dtype == target_dtype:
        return ids

    # String-like IDs vs numeric target — try numeric conversion.
    if pd.api.types.is_string_dtype(
        ids.dtype
    ) and pd.api.types.is_numeric_dtype(target_dtype):
        try:
            return pd.to_numeric(ids)
        except (ValueError, TypeError):
            return ids

    # Numeric IDs vs object/string target — cast to object.
    if pd.api.types.is_numeric_dtype(ids.dtype) and (
        target_dtype == object or pd.api.types.is_string_dtype(target_dtype)
    ):
        return ids.astype(object)

    return ids


# ---------------------------------------------------------------------------
# Scan operators
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

    def scan(
        self,
        context: Any,
        property_filters: dict[str, Any] | None = None,
    ) -> BindingFrame:
        """Return a :class:`BindingFrame` containing all IDs for this entity type.

        Args:
            context: The query :class:`~pycypher.relational_models.Context`.
            property_filters: Optional dict of ``{prop_name: value}`` equality
                predicates.  When provided and a :class:`PropertyValueIndex`
                is available, the scan returns only IDs matching **all**
                predicates instead of the full entity table — O(1) per
                predicate instead of O(N) post-scan filtering.

        Returns:
            A :class:`BindingFrame` with one column (*var_name*) of entity IDs.

        """
        from pycypher.binding_frame import BindingFrame

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

        # --- Predicate pushdown via property index ---
        _pushed_down = False
        if property_filters:
            shadow: dict = getattr(context, "_shadow", {})
            if self.entity_type not in shadow:
                index_mgr = getattr(context, "index_manager", None)
                if index_mgr is not None:
                    try:
                        candidate_ids: frozenset | None = None
                        for prop_name, value in property_filters.items():
                            matching = index_mgr.indexed_property_lookup(
                                self.entity_type,
                                prop_name,
                                value,
                            )
                            if matching is None:
                                # No index for this property — skip pushdown
                                candidate_ids = None
                                break
                            if candidate_ids is None:
                                candidate_ids = matching
                            else:
                                candidate_ids = candidate_ids & matching
                        if candidate_ids is not None:
                            ids = pd.Series(
                                list(candidate_ids),
                                name=ID_COLUMN,
                            )
                            _pushed_down = True
                            if _DEBUG_ENABLED:
                                LOGGER.debug(
                                    "EntityScan.scan  PUSHDOWN  entity_type=%s  var=%s  "
                                    "filters=%s  matched=%d  elapsed=%.4fs",
                                    self.entity_type,
                                    self.var_name,
                                    property_filters,
                                    len(ids),
                                    time.perf_counter() - _t0,
                                )
                    except (
                        KeyError,
                        ValueError,
                        TypeError,
                        IndexError,
                        AttributeError,
                    ):
                        LOGGER.debug(
                            "EntityScan: predicate pushdown failed for %s, "
                            "falling back to full scan",
                            self.entity_type,
                            exc_info=True,
                        )

        if not _pushed_down:
            # --- Standard full scan ---
            cache: dict = getattr(context, "_property_lookup_cache", {})
            if self.entity_type not in cache:
                raw_df: pd.DataFrame = _source_to_pandas(
                    entity_table.source_obj
                )
                cache[self.entity_type] = raw_df.set_index(ID_COLUMN)
            indexed_df = cache[self.entity_type]
            ids = pd.Series(
                indexed_df.index.to_numpy(dtype=object),
                name=ID_COLUMN,
            )
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
        from pycypher.binding_frame import BindingFrame

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

        # --- Coerce pushdown IDs to match relationship table dtypes ---
        if source_ids is not None or target_ids is not None:
            raw_df_for_dtype: pd.DataFrame = _source_to_pandas(
                rel_table.source_obj
            )
            if source_ids is not None:
                source_ids = _coerce_pushdown_series(
                    source_ids,
                    raw_df_for_dtype[RELATIONSHIP_SOURCE_COLUMN],
                )
            if target_ids is not None:
                target_ids = _coerce_pushdown_series(
                    target_ids,
                    raw_df_for_dtype[RELATIONSHIP_TARGET_COLUMN],
                )

        # --- Fast path: adjacency index for O(degree) pushdown ---
        _shadow_rels: dict = getattr(context, "_shadow_rels", {})
        if (
            source_ids is not None or target_ids is not None
        ) and self.rel_type not in _shadow_rels:
            try:
                index_mgr = getattr(context, "index_manager", None)
                if index_mgr is not None:
                    idx_result = index_mgr.indexed_relationship_scan(
                        self.rel_type,
                        source_ids=source_ids,
                        target_ids=target_ids,
                    )
                    if idx_result is not None:
                        bindings = pd.DataFrame(
                            {
                                self.rel_var: idx_result[ID_COLUMN].values,
                                self.src_col: idx_result[
                                    RELATIONSHIP_SOURCE_COLUMN
                                ].values,
                                self.tgt_col: idx_result[
                                    RELATIONSHIP_TARGET_COLUMN
                                ].values,
                            },
                        )
                        if _DEBUG_ENABLED:
                            LOGGER.debug(
                                "RelationshipScan.scan  rel_type=%s  var=%s  rows=%d  pushdown=index  elapsed=%.4fs",
                                self.rel_type,
                                self.rel_var,
                                len(bindings),
                                time.perf_counter() - _t0,
                            )
                        return BindingFrame(
                            bindings=bindings,
                            type_registry={self.rel_var: self.rel_type},
                            context=context,
                        )
            except (KeyError, ValueError, TypeError, IndexError, AttributeError):
                # Fall through to table-scan path on any index error
                LOGGER.debug(
                    "RelationshipScan: index scan failed for %s, falling back to table scan",
                    self.rel_type,
                    exc_info=True,
                )

        # --- Fallback: table scan with isin() pushdown ---
        cache: dict = getattr(context, "_property_lookup_cache", {})
        cache_key = f"__rel__{self.rel_type}"
        if cache_key not in cache:
            raw_df: pd.DataFrame = _source_to_pandas(rel_table.source_obj)
            cache[cache_key] = raw_df.set_index(ID_COLUMN)
        indexed_df = cache[cache_key]

        # --- Predicate pushdown: filter at scan level ---
        mask: pd.Series | None = None
        if source_ids is not None:
            source_set = _coerce_pushdown_ids(
                source_ids, indexed_df[RELATIONSHIP_SOURCE_COLUMN]
            )
            src_mask = indexed_df[RELATIONSHIP_SOURCE_COLUMN].isin(source_set)
            mask = src_mask if mask is None else mask & src_mask
        if target_ids is not None:
            target_set = _coerce_pushdown_ids(
                target_ids, indexed_df[RELATIONSHIP_TARGET_COLUMN]
            )
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
        )
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
# Filter operator
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
