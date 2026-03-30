"""Graph-native index structures for accelerating pattern matching.

Provides adjacency indexes, property value indexes, and label-partitioned
indexes that transform pattern matching from O(E) full table scans to
O(degree) neighbor lookups.

Architecture
------------

::

    GraphIndexManager
    ├── AdjacencyIndex         — per-relationship-type adjacency lists
    │   ├── outgoing[src_id]   → list of (rel_id, tgt_id)
    │   └── incoming[tgt_id]   → list of (rel_id, src_id)
    ├── PropertyValueIndex     — per-(entity_type, property) hash index
    │   └── value_to_ids[val]  → set of entity IDs
    └── EntityLabelIndex       — per-label sorted ID arrays for fast membership
        └── ids: np.ndarray    — sorted entity IDs for O(log n) lookup

Indexes are built lazily on first access and invalidated when the Context
commits mutations (``commit_query()`` increments ``_data_epoch``).

Usage::

    # Indexes are managed through the Context:
    ctx = Context(entity_mapping=..., relationship_mapping=...)
    manager = ctx.index_manager  # lazily created

    # Neighbor lookup — O(degree) instead of O(E)
    neighbors = manager.get_neighbors("KNOWS", source_id=42, direction="outgoing")

    # Property index — O(1) instead of O(N)
    ids = manager.lookup_property("Person", "name", "Alice")
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
from shared.logger import LOGGER

from pycypher.constants import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
)

if TYPE_CHECKING:
    from pycypher.relational_models import Context


@dataclass(slots=True)
class AdjacencyIndex:
    """Adjacency list index for a single relationship type.

    Stores both outgoing (source → targets) and incoming (target → sources)
    adjacency lists for O(degree) neighbor lookups instead of O(E) table scans.

    Attributes:
        rel_type: The relationship type this index covers.
        outgoing: Maps source node ID → list of (relationship_id, target_id).
        incoming: Maps target node ID → list of (relationship_id, source_id).
        size: Total number of relationships indexed.

    """

    rel_type: str
    outgoing: dict[Any, tuple[tuple[Any, Any], ...]] = field(
        default_factory=dict
    )
    incoming: dict[Any, tuple[tuple[Any, Any], ...]] = field(
        default_factory=dict
    )
    size: int = 0

    @classmethod
    def build(cls, rel_type: str, source_df: pd.DataFrame) -> AdjacencyIndex:
        """Build adjacency index from a relationship DataFrame.

        Args:
            rel_type: Relationship type label.
            source_df: DataFrame with __ID__, __SOURCE__, __TARGET__ columns.

        Returns:
            Populated AdjacencyIndex.  All adjacency lists are frozen tuples
            for thread-safe read access after build.

        """
        t0 = time.perf_counter()

        if ID_COLUMN not in source_df.columns:
            return cls(rel_type=rel_type)

        rel_ids = source_df[ID_COLUMN].values
        src_ids = source_df[RELATIONSHIP_SOURCE_COLUMN].values
        tgt_ids = source_df[RELATIONSHIP_TARGET_COLUMN].values

        # Build into mutable lists first, then freeze
        out_tmp: dict[Any, list[tuple[Any, Any]]] = defaultdict(list)
        in_tmp: dict[Any, list[tuple[Any, Any]]] = defaultdict(list)

        for i in range(len(rel_ids)):
            rid = rel_ids[i]
            sid = src_ids[i]
            tid = tgt_ids[i]
            out_tmp[sid].append((rid, tid))
            in_tmp[tid].append((rid, sid))

        # Freeze into tuples for thread-safe reads
        outgoing = {k: tuple(v) for k, v in out_tmp.items()}
        incoming = {k: tuple(v) for k, v in in_tmp.items()}
        size = len(rel_ids)

        idx = cls(
            rel_type=rel_type, outgoing=outgoing, incoming=incoming, size=size
        )

        elapsed = time.perf_counter() - t0
        LOGGER.debug(
            "AdjacencyIndex.build  rel_type=%s  edges=%d  elapsed=%.4fs",
            rel_type,
            idx.size,
            elapsed,
        )
        return idx

    def neighbors_outgoing(
        self,
        source_id: Any,
    ) -> tuple[tuple[Any, Any], ...]:
        """Return outgoing neighbors: tuple of (rel_id, target_id)."""
        return self.outgoing.get(source_id, ())

    def neighbors_incoming(
        self,
        target_id: Any,
    ) -> tuple[tuple[Any, Any], ...]:
        """Return incoming neighbors: tuple of (rel_id, source_id)."""
        return self.incoming.get(target_id, ())

    def neighbors_outgoing_batch(
        self,
        source_ids: np.ndarray | pd.Series,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Batch lookup outgoing neighbors for multiple source IDs.

        Returns:
            Tuple of (rel_ids, src_ids, tgt_ids) arrays — the subset of
            relationships where __SOURCE__ is in source_ids.

        """
        if isinstance(source_ids, pd.Series):
            source_ids = source_ids.values

        unique_ids = set(source_ids)
        result_rel: list[Any] = []
        result_src: list[Any] = []
        result_tgt: list[Any] = []

        outgoing = self.outgoing
        for sid in unique_ids:
            entries = outgoing.get(sid)
            if entries:
                for rid, tid in entries:
                    result_rel.append(rid)
                    result_src.append(sid)
                    result_tgt.append(tid)

        return (
            np.array(result_rel, dtype=object),
            np.array(result_src, dtype=object),
            np.array(result_tgt, dtype=object),
        )

    def neighbors_incoming_batch(
        self,
        target_ids: np.ndarray | pd.Series,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Batch lookup incoming neighbors for multiple target IDs.

        Returns:
            Tuple of (rel_ids, src_ids, tgt_ids) arrays — the subset of
            relationships where __TARGET__ is in target_ids.

        """
        if isinstance(target_ids, pd.Series):
            target_ids = target_ids.values

        unique_ids = set(target_ids)
        result_rel: list[Any] = []
        result_src: list[Any] = []
        result_tgt: list[Any] = []

        incoming = self.incoming
        for tid in unique_ids:
            entries = incoming.get(tid)
            if entries:
                for rid, sid in entries:
                    result_rel.append(rid)
                    result_src.append(sid)
                    result_tgt.append(tid)

        return (
            np.array(result_rel, dtype=object),
            np.array(result_src, dtype=object),
            np.array(result_tgt, dtype=object),
        )


@dataclass(slots=True)
class PropertyValueIndex:
    """Hash index on a single property of an entity type.

    Maps property values to sets of entity IDs for O(1) equality lookups.

    Attributes:
        entity_type: Entity type label.
        property_name: Property being indexed.
        value_to_ids: Maps property value → frozenset of entity IDs.
        size: Total number of indexed entries.

    """

    entity_type: str
    property_name: str
    value_to_ids: dict[Any, frozenset] = field(default_factory=dict)
    size: int = 0

    @classmethod
    def build(
        cls,
        entity_type: str,
        property_name: str,
        source_df: pd.DataFrame,
    ) -> PropertyValueIndex:
        """Build property value index from an entity DataFrame.

        Args:
            entity_type: Entity type label.
            property_name: Column name to index.
            source_df: DataFrame with __ID__ and property columns.

        Returns:
            Populated PropertyValueIndex.

        """
        t0 = time.perf_counter()

        idx = cls(entity_type=entity_type, property_name=property_name)

        if (
            property_name not in source_df.columns
            or ID_COLUMN not in source_df.columns
        ):
            return idx

        ids = source_df[ID_COLUMN].values
        vals = source_df[property_name].values

        # Build value → set of IDs mapping
        temp: dict[Any, set] = defaultdict(set)
        for i in range(len(ids)):
            v = vals[i]
            if v is not None and v == v:  # skip None and NaN
                temp[v].add(ids[i])

        idx.value_to_ids = {k: frozenset(v) for k, v in temp.items()}
        idx.size = len(ids)

        elapsed = time.perf_counter() - t0
        LOGGER.debug(
            "PropertyValueIndex.build  type=%s  prop=%s  entries=%d  distinct=%d  elapsed=%.4fs",
            entity_type,
            property_name,
            idx.size,
            len(idx.value_to_ids),
            elapsed,
        )
        return idx

    def lookup(self, value: Any) -> frozenset:
        """Return entity IDs where property equals value. O(1)."""
        return self.value_to_ids.get(value, frozenset())

    @property
    def distinct_values(self) -> int:
        """Number of distinct indexed values."""
        return len(self.value_to_ids)


@dataclass(slots=True)
class EntityLabelIndex:
    """Sorted array index for entity IDs of a single label.

    Provides O(log n) membership testing via binary search and O(1)
    count retrieval.

    Attributes:
        entity_type: Entity type label.
        ids: Sorted numpy array of entity IDs.

    """

    entity_type: str
    ids: np.ndarray = field(default_factory=lambda: np.array([], dtype=object))

    @classmethod
    def build(
        cls, entity_type: str, source_df: pd.DataFrame
    ) -> EntityLabelIndex:
        """Build label index from an entity DataFrame.

        Args:
            entity_type: Entity type label.
            source_df: DataFrame with __ID__ column.

        Returns:
            Populated EntityLabelIndex.

        """
        if ID_COLUMN not in source_df.columns:
            return cls(entity_type=entity_type)

        ids = np.sort(np.array(source_df[ID_COLUMN].tolist(), dtype=object))
        return cls(entity_type=entity_type, ids=ids)

    def contains(self, entity_id: Any) -> bool:
        """Check if entity_id exists in this label. O(log n)."""
        idx = np.searchsorted(self.ids, entity_id)
        return bool(idx < len(self.ids) and self.ids[idx] == entity_id)

    def count(self) -> int:
        """Return total number of entities with this label."""
        return len(self.ids)


def _coerce_query_ids(
    sorted_ids: np.ndarray, query_ids: np.ndarray
) -> np.ndarray:
    """Coerce query_ids element types to match sorted_ids for correct comparison.

    Both arrays may be object dtype but contain different Python types
    (e.g. int vs str after DuckDB backend merges).
    """
    if len(sorted_ids) == 0 or len(query_ids) == 0:
        return query_ids
    sample_sorted = sorted_ids[0]
    sample_query = query_ids[0]
    if type(sample_sorted) is type(sample_query):
        return query_ids
    try:
        if isinstance(sample_sorted, (int, np.integer)):
            return np.array([int(x) for x in query_ids], dtype=object)
        if isinstance(sample_sorted, (float, np.floating)):
            return np.array([float(x) for x in query_ids], dtype=object)
        if isinstance(sample_sorted, str):
            return np.array([str(x) for x in query_ids], dtype=object)
    except (ValueError, TypeError):
        pass
    return query_ids


@dataclass(frozen=True)
class VectorizedPropertyStore:
    """Pre-sorted entity ID array with aligned property columns for O(n log n) bulk lookups.

    Instead of hash-based ``pd.Series.map()`` (O(n) amortised but with high constant
    factor due to Python-level hashing) or ``pd.DataFrame.reindex()`` (creates intermediate
    DataFrames), this store uses ``np.searchsorted`` on a pre-sorted ID array to resolve
    entity IDs to row positions, then fancy-indexes directly into numpy property arrays.

    Build cost: O(N log N) for the sort (one-time, cached).
    Lookup cost: O(k log N) for k query IDs against N stored entities.
    """

    entity_type: str
    sorted_ids: np.ndarray  # dtype=object, sorted
    property_arrays: dict[
        str, np.ndarray
    ]  # prop_name → values aligned with sorted_ids

    @classmethod
    def build(
        cls, entity_type: str, source_df: pd.DataFrame
    ) -> VectorizedPropertyStore:
        """Build a vectorized store from an entity DataFrame.

        Args:
            entity_type: The entity type label.
            source_df: DataFrame with ID_COLUMN and property columns.

        Returns:
            A new VectorizedPropertyStore.

        """
        if source_df is None or len(source_df) == 0:
            return cls(
                entity_type=entity_type,
                sorted_ids=np.array([], dtype=object),
                property_arrays={},
            )

        # Convert to numpy for sorting (handles Arrow-backed DFs)
        ids = np.array(source_df[ID_COLUMN].tolist(), dtype=object)
        sort_order = np.argsort(ids, kind="mergesort")
        sorted_ids = ids[sort_order]

        # Build aligned property arrays
        prop_arrays: dict[str, np.ndarray] = {}
        for col in source_df.columns:
            if col == ID_COLUMN:
                continue
            values = source_df[col].values
            if hasattr(values, "to_numpy"):
                # Arrow-backed columns
                values = values.to_numpy(dtype=object, na_value=None)
            else:
                values = np.asarray(values, dtype=object)
            prop_arrays[col] = values[sort_order]

        return cls(
            entity_type=entity_type,
            sorted_ids=sorted_ids,
            property_arrays=prop_arrays,
        )

    @property
    def size(self) -> int:
        """Number of entities in this store."""
        return len(self.sorted_ids)

    @property
    def properties(self) -> list[str]:
        """List of available property names."""
        return list(self.property_arrays.keys())

    def fetch(self, query_ids: np.ndarray, prop_name: str) -> np.ndarray:
        """Fetch property values for a batch of entity IDs.

        Uses np.searchsorted for O(k log N) bulk resolution instead of
        hash-based lookups.

        Args:
            query_ids: Array of entity IDs to look up.
            prop_name: Property name to fetch.

        Returns:
            Array of property values aligned with query_ids.
            Missing IDs get None.

        """
        if prop_name not in self.property_arrays:
            result = np.empty(len(query_ids), dtype=object)
            result[:] = None
            return result

        if len(self.sorted_ids) == 0:
            result = np.empty(len(query_ids), dtype=object)
            result[:] = None
            return result

        prop_values = self.property_arrays[prop_name]

        # Coerce query IDs to match sorted_ids element types for correct comparison.
        # Both arrays may be object dtype but contain different Python types
        # (e.g. int vs str after DuckDB merges).
        query_ids = _coerce_query_ids(self.sorted_ids, query_ids)

        # Binary search: find insertion positions for each query ID
        positions = np.searchsorted(self.sorted_ids, query_ids)

        # Clamp to valid range for comparison
        n = len(self.sorted_ids)
        safe_positions = np.clip(positions, 0, n - 1)

        # Check which positions actually matched (ID equality)
        matched = self.sorted_ids[safe_positions] == query_ids

        # Build result: matched positions get values, unmatched get None
        result = np.empty(len(query_ids), dtype=object)
        result[:] = None
        if matched.any():
            result[matched] = prop_values[safe_positions[matched]]

        return result

    def fetch_multi(
        self,
        query_ids: np.ndarray,
        prop_names: list[str],
    ) -> dict[str, np.ndarray]:
        """Fetch multiple properties in a single searchsorted pass.

        The binary search is done once and reused for all properties,
        making this significantly faster than N separate fetch() calls.

        Args:
            query_ids: Array of entity IDs to look up.
            prop_names: List of property names to fetch.

        Returns:
            Dict mapping property names to value arrays aligned with query_ids.

        """
        if not prop_names or len(query_ids) == 0:
            return {
                p: np.empty(len(query_ids), dtype=object) for p in prop_names
            }

        if len(self.sorted_ids) == 0:
            results = {}
            for p in prop_names:
                arr = np.empty(len(query_ids), dtype=object)
                arr[:] = None
                results[p] = arr
            return results

        # Coerce query IDs to match sorted_ids element types
        query_ids = _coerce_query_ids(self.sorted_ids, query_ids)

        # Single searchsorted pass — O(k log N)
        positions = np.searchsorted(self.sorted_ids, query_ids)
        n = len(self.sorted_ids)
        safe_positions = np.clip(positions, 0, n - 1)
        matched = self.sorted_ids[safe_positions] == query_ids
        match_mask = matched  # reuse for all properties

        results: dict[str, np.ndarray] = {}
        for prop in prop_names:
            result = np.empty(len(query_ids), dtype=object)
            result[:] = None
            if prop in self.property_arrays and match_mask.any():
                result[match_mask] = self.property_arrays[prop][
                    safe_positions[match_mask]
                ]
            results[prop] = result
        return results


class GraphIndexManager:
    """Manages all graph-native indexes for a Context.

    Indexes are built lazily on first access and invalidated when the
    data epoch changes (i.e., after mutation commits).

    Attributes:
        context: The query Context whose data is being indexed.

    """

    def __init__(self, context: Context) -> None:
        self._context = context
        self._adjacency: dict[str, AdjacencyIndex] = {}
        self._property: dict[tuple[str, str], PropertyValueIndex] = {}
        self._label: dict[str, EntityLabelIndex] = {}
        self._vectorized: dict[str, VectorizedPropertyStore] = {}
        self._epoch: int = getattr(context, "_data_epoch", 0)
        self._lock = threading.Lock()

    def _check_epoch(self) -> None:
        """Invalidate all indexes if Context data has changed."""
        current_epoch = getattr(self._context, "_data_epoch", 0)
        if current_epoch != self._epoch:
            LOGGER.debug(
                "GraphIndexManager: epoch changed %d → %d, invalidating indexes",
                self._epoch,
                current_epoch,
            )
            self._adjacency.clear()
            self._property.clear()
            self._label.clear()
            self._vectorized.clear()
            self._epoch = current_epoch

    def get_adjacency_index(self, rel_type: str) -> AdjacencyIndex | None:
        """Get or build adjacency index for a relationship type.

        Returns None if the relationship type doesn't exist in the context.
        Thread-safe: uses a lock to prevent concurrent builds.
        """
        with self._lock:
            self._check_epoch()

            if rel_type in self._adjacency:
                return self._adjacency[rel_type]

            rel_mapping = self._context.relationship_mapping.mapping
            if rel_type not in rel_mapping:
                return None

            rel_table = rel_mapping[rel_type]
            source_df = rel_table.source_obj
            if hasattr(source_df, "to_pandas"):
                source_df = source_df.to_pandas()

            index = AdjacencyIndex.build(rel_type, source_df)
            self._adjacency[rel_type] = index
            return index

    def get_property_index(
        self,
        entity_type: str,
        property_name: str,
    ) -> PropertyValueIndex | None:
        """Get or build property value index.

        Returns None if the entity type or property doesn't exist.
        Thread-safe: uses a lock to prevent concurrent builds.
        """
        with self._lock:
            self._check_epoch()

            key = (entity_type, property_name)
            if key in self._property:
                return self._property[key]

            ent_mapping = self._context.entity_mapping.mapping
            if entity_type not in ent_mapping:
                return None

            ent_table = ent_mapping[entity_type]
            source_df = ent_table.source_obj
            if hasattr(source_df, "to_pandas"):
                source_df = source_df.to_pandas()

            if property_name not in source_df.columns:
                return None

            index = PropertyValueIndex.build(
                entity_type, property_name, source_df
            )
            self._property[key] = index
            return index

    def get_label_index(self, entity_type: str) -> EntityLabelIndex | None:
        """Get or build label index for an entity type.

        Returns None if the entity type doesn't exist.
        Thread-safe: uses a lock to prevent concurrent builds.
        """
        with self._lock:
            self._check_epoch()

            if entity_type in self._label:
                return self._label[entity_type]

            ent_mapping = self._context.entity_mapping.mapping
            if entity_type not in ent_mapping:
                return None

            ent_table = ent_mapping[entity_type]
            source_df = ent_table.source_obj
            if hasattr(source_df, "to_pandas"):
                source_df = source_df.to_pandas()

            index = EntityLabelIndex.build(entity_type, source_df)
            self._label[entity_type] = index
            return index

    def indexed_relationship_scan(
        self,
        rel_type: str,
        *,
        source_ids: pd.Series | None = None,
        target_ids: pd.Series | None = None,
    ) -> pd.DataFrame | None:
        """Use adjacency index for relationship scan with endpoint pushdown.

        When source_ids or target_ids are provided, uses O(degree) adjacency
        lookup instead of O(E) table scan + isin() filter.

        Returns:
            DataFrame with columns (rel_id, source_id, target_id), or None
            if no index is available (caller should fall back to table scan).

        """
        if source_ids is None and target_ids is None:
            return None  # No pushdown — full scan needed, no index benefit

        adj = self.get_adjacency_index(rel_type)
        if adj is None:
            return None

        if source_ids is not None and target_ids is not None:
            # Both endpoints constrained — use source pushdown, then filter
            rel_ids, src_ids, tgt_ids = adj.neighbors_outgoing_batch(
                source_ids
            )
            if len(rel_ids) == 0:
                return pd.DataFrame(
                    {
                        ID_COLUMN: pd.Series(dtype=object),
                        RELATIONSHIP_SOURCE_COLUMN: pd.Series(dtype=object),
                        RELATIONSHIP_TARGET_COLUMN: pd.Series(dtype=object),
                    },
                )
            target_set = set(target_ids.dropna().unique())
            mask = np.array([t in target_set for t in tgt_ids])
            return pd.DataFrame(
                {
                    ID_COLUMN: rel_ids[mask],
                    RELATIONSHIP_SOURCE_COLUMN: src_ids[mask],
                    RELATIONSHIP_TARGET_COLUMN: tgt_ids[mask],
                },
            )

        if source_ids is not None:
            rel_ids, src_ids, tgt_ids = adj.neighbors_outgoing_batch(
                source_ids
            )
        else:
            assert target_ids is not None
            rel_ids, src_ids, tgt_ids = adj.neighbors_incoming_batch(
                target_ids
            )

        if len(rel_ids) == 0:
            return pd.DataFrame(
                {
                    ID_COLUMN: pd.Series(dtype=object),
                    RELATIONSHIP_SOURCE_COLUMN: pd.Series(dtype=object),
                    RELATIONSHIP_TARGET_COLUMN: pd.Series(dtype=object),
                },
            )

        return pd.DataFrame(
            {
                ID_COLUMN: rel_ids,
                RELATIONSHIP_SOURCE_COLUMN: src_ids,
                RELATIONSHIP_TARGET_COLUMN: tgt_ids,
            },
        )

    def indexed_property_lookup(
        self,
        entity_type: str,
        property_name: str,
        value: Any,
    ) -> frozenset | None:
        """Use property index for O(1) equality lookup.

        Returns:
            Frozenset of matching entity IDs, or None if no index available.

        """
        idx = self.get_property_index(entity_type, property_name)
        if idx is None:
            return None
        return idx.lookup(value)

    def get_vectorized_store(
        self, entity_type: str
    ) -> VectorizedPropertyStore | None:
        """Get or build a VectorizedPropertyStore for an entity type.

        The store pre-sorts entity IDs and aligns property columns for
        O(k log N) bulk property resolution via np.searchsorted.

        Returns None if the entity type doesn't exist in the context.
        Thread-safe: uses a lock to prevent concurrent builds.
        """
        with self._lock:
            self._check_epoch()

            if entity_type in self._vectorized:
                return self._vectorized[entity_type]

            # Check entity mapping
            entity_mapping = self._context.entity_mapping.mapping
            rel_mapping = self._context.relationship_mapping.mapping

            source_df = None
            if entity_type in entity_mapping:
                raw = entity_mapping[entity_type].source_obj
                if hasattr(raw, "to_pandas"):
                    source_df = raw.to_pandas()
                elif isinstance(raw, pd.DataFrame):
                    source_df = raw
            elif entity_type in rel_mapping:
                raw = rel_mapping[entity_type].source_obj
                if hasattr(raw, "to_pandas"):
                    source_df = raw.to_pandas()
                elif isinstance(raw, pd.DataFrame):
                    source_df = raw

            if source_df is None:
                return None

            t0 = time.perf_counter()
            store = VectorizedPropertyStore.build(entity_type, source_df)
            LOGGER.debug(
                "VectorizedPropertyStore built for %s: %d entities, %d properties in %.4fs",
                entity_type,
                store.size,
                len(store.properties),
                time.perf_counter() - t0,
            )
            self._vectorized[entity_type] = store
            return store

    def stats(self) -> dict[str, Any]:
        """Return index statistics for diagnostics."""
        return {
            "epoch": self._epoch,
            "adjacency_indexes": {
                rt: {
                    "edges": idx.size,
                    "sources": len(idx.outgoing),
                    "targets": len(idx.incoming),
                }
                for rt, idx in self._adjacency.items()
            },
            "property_indexes": {
                f"{et}.{prop}": {
                    "entries": idx.size,
                    "distinct": idx.distinct_values,
                }
                for (et, prop), idx in self._property.items()
            },
            "label_indexes": {
                et: {"count": idx.count()} for et, idx in self._label.items()
            },
            "vectorized_stores": {
                et: {
                    "entities": store.size,
                    "properties": len(store.properties),
                }
                for et, store in self._vectorized.items()
            },
        }

    def eager_build_vectorized_stores(self) -> None:
        """Pre-build VectorizedPropertyStores for all entity and relationship types.

        Call this at query start to ensure the fast O(k log N) path in
        ``BindingFrame.get_property()`` is always available, avoiding fallback
        to the slower hash-based ``pd.Series.map()`` path.

        Build cost is O(N log N) per entity type (one-time sort), amortised
        across all subsequent property lookups in the query.
        """
        t0 = time.perf_counter()
        built = 0
        for etype in self._context.entity_mapping.mapping:
            if etype not in self._vectorized:
                self.get_vectorized_store(etype)
                built += 1
        for rtype in self._context.relationship_mapping.mapping:
            if rtype not in self._vectorized:
                self.get_vectorized_store(rtype)
                built += 1
        if built > 0:
            LOGGER.debug(
                "eager_build_vectorized_stores: built %d stores in %.4fs",
                built,
                time.perf_counter() - t0,
            )

    def invalidate(self) -> None:
        """Force invalidation of all indexes."""
        self._adjacency.clear()
        self._property.clear()
        self._label.clear()
        self._vectorized.clear()
        self._epoch = getattr(self._context, "_data_epoch", 0)
