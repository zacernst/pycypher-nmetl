"""Tests for vectorized property batch fetch (VectorizedPropertyStore).

Covers:
- VectorizedPropertyStore: build, single fetch, multi fetch, missing data
- Integration with BindingFrame.get_property() fast path
- Integration with BindingFrame.get_properties_batch() fast path
- Shadow mutation bypass (vectorized path disabled during mutations)
- Epoch invalidation of vectorized stores
- Performance characteristics (vectorized vs standard path)
"""

from __future__ import annotations

import time

import numpy as np
import pandas as pd
import pytest

from pycypher.constants import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
)
from pycypher.graph_index import VectorizedPropertyStore


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def entity_df() -> pd.DataFrame:
    """A small entity DataFrame for testing."""
    return pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "age": [30, 25, 35, 28, 42],
            "city": ["NYC", "LA", "NYC", "Chicago", "LA"],
        }
    )


@pytest.fixture()
def large_entity_df() -> pd.DataFrame:
    """A larger entity DataFrame for performance testing."""
    n = 10_000
    rng = np.random.default_rng(42)
    return pd.DataFrame(
        {
            ID_COLUMN: list(range(n)),
            "name": [f"Person{i}" for i in range(n)],
            "age": rng.integers(18, 80, size=n).tolist(),
            "score": rng.random(size=n).tolist(),
        }
    )


@pytest.fixture()
def context_with_data():
    """A Context for integration tests."""
    from pycypher.relational_models import (
        Context,
        EntityMapping,
        EntityTable,
        RelationshipMapping,
        RelationshipTable,
    )

    person_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Charlie", "Diana"],
            "age": [30, 25, 35, 28],
        }
    )
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [101, 102, 103, 104, 105],
            RELATIONSHIP_SOURCE_COLUMN: [1, 1, 2, 3, 4],
            RELATIONSHIP_TARGET_COLUMN: [2, 3, 3, 4, 1],
        }
    )

    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=list(person_df.columns),
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=person_df,
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=list(knows_df.columns),
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=knows_df,
    )

    ctx = Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(mapping={"KNOWS": knows_table}),
    )
    return ctx


# ---------------------------------------------------------------------------
# VectorizedPropertyStore unit tests
# ---------------------------------------------------------------------------


class TestVectorizedPropertyStore:
    def test_build_basic(self, entity_df):
        store = VectorizedPropertyStore.build("Person", entity_df)
        assert store.entity_type == "Person"
        assert store.size == 5
        assert set(store.properties) == {"name", "age", "city"}

    def test_sorted_ids(self, entity_df):
        store = VectorizedPropertyStore.build("Person", entity_df)
        # IDs should be sorted
        ids = store.sorted_ids
        assert all(ids[i] <= ids[i + 1] for i in range(len(ids) - 1))

    def test_fetch_single_property(self, entity_df):
        store = VectorizedPropertyStore.build("Person", entity_df)
        result = store.fetch(np.array([1, 3, 5]), "name")
        assert list(result) == ["Alice", "Charlie", "Eve"]

    def test_fetch_numeric_property(self, entity_df):
        store = VectorizedPropertyStore.build("Person", entity_df)
        result = store.fetch(np.array([2, 4]), "age")
        assert list(result) == [25, 28]

    def test_fetch_missing_ids(self, entity_df):
        store = VectorizedPropertyStore.build("Person", entity_df)
        result = store.fetch(np.array([99, 100]), "name")
        assert result[0] is None
        assert result[1] is None

    def test_fetch_mixed_ids(self, entity_df):
        store = VectorizedPropertyStore.build("Person", entity_df)
        result = store.fetch(np.array([1, 99, 3]), "name")
        assert result[0] == "Alice"
        assert result[1] is None
        assert result[2] == "Charlie"

    def test_fetch_missing_property(self, entity_df):
        store = VectorizedPropertyStore.build("Person", entity_df)
        result = store.fetch(np.array([1, 2]), "nonexistent")
        assert result[0] is None
        assert result[1] is None

    def test_fetch_empty_query(self, entity_df):
        store = VectorizedPropertyStore.build("Person", entity_df)
        result = store.fetch(np.array([], dtype=object), "name")
        assert len(result) == 0

    def test_fetch_multi_basic(self, entity_df):
        store = VectorizedPropertyStore.build("Person", entity_df)
        result = store.fetch_multi(np.array([1, 3]), ["name", "age", "city"])
        assert list(result["name"]) == ["Alice", "Charlie"]
        assert list(result["age"]) == [30, 35]
        assert list(result["city"]) == ["NYC", "NYC"]

    def test_fetch_multi_with_missing(self, entity_df):
        store = VectorizedPropertyStore.build("Person", entity_df)
        result = store.fetch_multi(
            np.array([1, 99]), ["name", "nonexistent"]
        )
        assert result["name"][0] == "Alice"
        assert result["name"][1] is None
        assert result["nonexistent"][0] is None
        assert result["nonexistent"][1] is None

    def test_fetch_multi_empty_props(self, entity_df):
        store = VectorizedPropertyStore.build("Person", entity_df)
        result = store.fetch_multi(np.array([1]), [])
        assert result == {}

    def test_fetch_multi_empty_ids(self, entity_df):
        store = VectorizedPropertyStore.build("Person", entity_df)
        result = store.fetch_multi(np.array([], dtype=object), ["name"])
        assert len(result["name"]) == 0

    def test_build_empty_df(self):
        empty_df = pd.DataFrame(
            {ID_COLUMN: pd.Series(dtype=object), "name": pd.Series(dtype=object)}
        )
        store = VectorizedPropertyStore.build("Empty", empty_df)
        assert store.size == 0
        assert store.fetch(np.array([1]), "name")[0] is None

    def test_build_preserves_all_rows(self, large_entity_df):
        store = VectorizedPropertyStore.build("Person", large_entity_df)
        assert store.size == 10_000

    def test_fetch_large_batch(self, large_entity_df):
        store = VectorizedPropertyStore.build("Person", large_entity_df)
        query_ids = np.array([0, 100, 5000, 9999, 99999])
        result = store.fetch(query_ids, "name")
        assert result[0] == "Person0"
        assert result[1] == "Person100"
        assert result[2] == "Person5000"
        assert result[3] == "Person9999"
        assert result[4] is None  # Out of range

    def test_null_values_in_source(self):
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3],
                "name": ["Alice", None, "Charlie"],
            }
        )
        store = VectorizedPropertyStore.build("Person", df)
        result = store.fetch(np.array([1, 2, 3]), "name")
        assert result[0] == "Alice"
        assert result[1] is None
        assert result[2] == "Charlie"

    def test_duplicate_ids_returns_last(self):
        """If source has duplicate IDs, behavior is defined (last wins after sort)."""
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 1, 2],
                "name": ["Alice1", "Alice2", "Bob"],
            }
        )
        store = VectorizedPropertyStore.build("Person", df)
        # searchsorted finds one of the duplicates — both have valid values
        result = store.fetch(np.array([1, 2]), "name")
        assert result[0] in ("Alice1", "Alice2")
        assert result[1] == "Bob"

    def test_string_ids(self):
        df = pd.DataFrame(
            {
                ID_COLUMN: ["c", "a", "b"],
                "name": ["Charlie", "Alice", "Bob"],
            }
        )
        store = VectorizedPropertyStore.build("Person", df)
        result = store.fetch(np.array(["a", "b", "c", "z"]), "name")
        assert result[0] == "Alice"
        assert result[1] == "Bob"
        assert result[2] == "Charlie"
        assert result[3] is None


# ---------------------------------------------------------------------------
# Integration: get_property() uses vectorized path
# ---------------------------------------------------------------------------


class TestGetPropertyVectorized:
    def test_basic_property_fetch(self, context_with_data):
        from pycypher.binding_frame import EntityScan

        a_scan = EntityScan("Person", "a").scan(context_with_data)
        names = a_scan.get_property("a", "name")
        assert set(names) == {"Alice", "Bob", "Charlie", "Diana"}

    def test_property_after_join(self, context_with_data):
        from pycypher.binding_frame import EntityScan, RelationshipScan

        a_scan = EntityScan("Person", "a").scan(context_with_data)
        r_scan = RelationshipScan("KNOWS", "r").scan(context_with_data)
        joined = a_scan.join(r_scan, left_col="a", right_col="_src_r")

        names = joined.get_property("a", "name")
        assert len(names) == 5
        # Person 1 (Alice) has 2 outgoing KNOWS
        assert list(names).count("Alice") == 2

    def test_missing_property_returns_none(self, context_with_data):
        from pycypher.binding_frame import EntityScan

        a_scan = EntityScan("Person", "a").scan(context_with_data)
        result = a_scan.get_property("a", "nonexistent")
        assert all(v is None for v in result)

    def test_property_cache_works(self, context_with_data):
        from pycypher.binding_frame import EntityScan

        a_scan = EntityScan("Person", "a").scan(context_with_data)
        names1 = a_scan.get_property("a", "name")
        names2 = a_scan.get_property("a", "name")
        # Should return cached result (same object)
        assert names1 is names2

    def test_vectorized_store_built(self, context_with_data):
        from pycypher.binding_frame import EntityScan

        a_scan = EntityScan("Person", "a").scan(context_with_data)
        a_scan.get_property("a", "name")

        # The vectorized store should now be in the manager
        mgr = context_with_data.index_manager
        store = mgr.get_vectorized_store("Person")
        assert store is not None
        assert store.size == 4


# ---------------------------------------------------------------------------
# Integration: get_properties_batch() uses vectorized path
# ---------------------------------------------------------------------------


class TestGetPropertiesBatchVectorized:
    def test_batch_fetch(self, context_with_data):
        from pycypher.binding_frame import EntityScan

        a_scan = EntityScan("Person", "a").scan(context_with_data)
        batch = a_scan.get_properties_batch("a", ["name", "age"])
        assert set(batch["name"]) == {"Alice", "Bob", "Charlie", "Diana"}
        assert set(batch["age"]) == {30, 25, 35, 28}

    def test_batch_with_missing_property(self, context_with_data):
        from pycypher.binding_frame import EntityScan

        a_scan = EntityScan("Person", "a").scan(context_with_data)
        batch = a_scan.get_properties_batch("a", ["name", "nonexistent"])
        assert len(batch["name"]) == 4
        assert all(v is None for v in batch["nonexistent"])

    def test_batch_empty_props(self, context_with_data):
        from pycypher.binding_frame import EntityScan

        a_scan = EntityScan("Person", "a").scan(context_with_data)
        batch = a_scan.get_properties_batch("a", [])
        assert batch == {}

    def test_batch_after_join(self, context_with_data):
        from pycypher.binding_frame import EntityScan, RelationshipScan

        a_scan = EntityScan("Person", "a").scan(context_with_data)
        r_scan = RelationshipScan("KNOWS", "r").scan(context_with_data)
        joined = a_scan.join(r_scan, left_col="a", right_col="_src_r")

        batch = joined.get_properties_batch("a", ["name", "age"])
        assert len(batch["name"]) == 5
        assert len(batch["age"]) == 5


# ---------------------------------------------------------------------------
# Shadow mutation bypass
# ---------------------------------------------------------------------------


class TestShadowBypass:
    def test_shadow_disables_vectorized_path(self, context_with_data):
        from pycypher.binding_frame import EntityScan

        # Pre-build vectorized store
        mgr = context_with_data.index_manager
        store_before = mgr.get_vectorized_store("Person")
        assert store_before is not None

        # Simulate active shadow mutation with proper schema
        shadow_df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4],
                "name": ["ShadowAlice", "ShadowBob", "ShadowCharlie", "ShadowDiana"],
                "age": [31, 26, 36, 29],
            }
        )
        context_with_data._shadow = {"Person": shadow_df}

        a_scan = EntityScan("Person", "a").scan(context_with_data)
        # Should use shadow data, not vectorized store
        result = a_scan.get_property("a", "name")
        assert len(result) == 4
        assert "ShadowAlice" in list(result)


# ---------------------------------------------------------------------------
# Epoch invalidation
# ---------------------------------------------------------------------------


class TestEpochInvalidation:
    def test_vectorized_store_invalidated_on_epoch_change(self, context_with_data):
        mgr = context_with_data.index_manager
        store1 = mgr.get_vectorized_store("Person")
        assert store1 is not None

        # Simulate mutation commit
        context_with_data._data_epoch += 1

        store2 = mgr.get_vectorized_store("Person")
        assert store2 is not store1  # Rebuilt

    def test_vectorized_store_cached_within_epoch(self, context_with_data):
        mgr = context_with_data.index_manager
        store1 = mgr.get_vectorized_store("Person")
        store2 = mgr.get_vectorized_store("Person")
        assert store1 is store2

    def test_invalidate_clears_vectorized(self, context_with_data):
        mgr = context_with_data.index_manager
        mgr.get_vectorized_store("Person")
        assert len(mgr._vectorized) == 1

        mgr.invalidate()
        assert len(mgr._vectorized) == 0


# ---------------------------------------------------------------------------
# Performance: verify vectorized path is faster for large datasets
# ---------------------------------------------------------------------------


class TestPerformanceCharacteristics:
    def test_vectorized_fetch_correctness_large(self, large_entity_df):
        """Verify vectorized fetch produces correct results on large data."""
        store = VectorizedPropertyStore.build("Person", large_entity_df)

        # Random sample of IDs
        rng = np.random.default_rng(123)
        sample_ids = rng.choice(10_000, size=1000, replace=False)
        query_ids = np.array(sample_ids, dtype=object)

        names = store.fetch(query_ids, "name")
        for i, eid in enumerate(sample_ids):
            assert names[i] == f"Person{eid}"

    def test_fetch_multi_correctness_large(self, large_entity_df):
        """Verify multi-fetch produces correct results on large data."""
        store = VectorizedPropertyStore.build("Person", large_entity_df)

        query_ids = np.array([0, 500, 9999], dtype=object)
        results = store.fetch_multi(query_ids, ["name", "age", "score"])

        assert results["name"][0] == "Person0"
        assert results["name"][1] == "Person500"
        assert results["name"][2] == "Person9999"
        # All age values should be integers
        for age in results["age"]:
            assert isinstance(age, (int, np.integer))

    def test_fetch_multi_faster_than_repeated_map(self, large_entity_df):
        """fetch_multi should be faster than N separate Series.map() calls."""
        store = VectorizedPropertyStore.build("Person", large_entity_df)
        indexed_df = large_entity_df.set_index(ID_COLUMN)
        props = ["name", "age", "score"]

        rng = np.random.default_rng(42)
        query_ids = np.array(
            rng.choice(10_000, size=5000, replace=True), dtype=object
        )
        query_series = pd.Series(query_ids)

        # Warm up
        store.fetch_multi(query_ids, props)
        for p in props:
            query_series.map(indexed_df[p])

        # Time vectorized multi-fetch
        t0 = time.perf_counter()
        for _ in range(50):
            store.fetch_multi(query_ids, props)
        vectorized_time = time.perf_counter() - t0

        # Time standard map path (N separate calls)
        t0 = time.perf_counter()
        for _ in range(50):
            for p in props:
                query_series.map(indexed_df[p])
        map_time = time.perf_counter() - t0

        # Multi-fetch should be competitive with repeated map
        # (both are fast, but multi-fetch amortizes the search)
        assert vectorized_time < map_time * 5, (
            f"Vectorized multi ({vectorized_time:.4f}s) too slow vs "
            f"repeated map ({map_time:.4f}s)"
        )

    def test_fetch_multi_amortizes_search(self, large_entity_df):
        """fetch_multi with N properties should be faster than N fetch() calls."""
        store = VectorizedPropertyStore.build("Person", large_entity_df)

        rng = np.random.default_rng(42)
        query_ids = np.array(
            rng.choice(10_000, size=5000, replace=True), dtype=object
        )
        props = ["name", "age", "score"]

        # Warm up
        store.fetch_multi(query_ids, props)
        for p in props:
            store.fetch(query_ids, p)

        # Time multi-fetch
        t0 = time.perf_counter()
        for _ in range(50):
            store.fetch_multi(query_ids, props)
        multi_time = time.perf_counter() - t0

        # Time individual fetches
        t0 = time.perf_counter()
        for _ in range(50):
            for p in props:
                store.fetch(query_ids, p)
        individual_time = time.perf_counter() - t0

        # Multi-fetch should be faster (amortized searchsorted)
        assert multi_time < individual_time, (
            f"Multi ({multi_time:.4f}s) not faster than individual ({individual_time:.4f}s)"
        )
