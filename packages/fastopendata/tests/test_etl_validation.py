"""Comprehensive ETL pipeline validation and performance benchmarking.

Tests cover:
- Data quality validation across ETL transformations
- Multi-source join query accuracy
- Stress testing with larger synthetic datasets
- Performance benchmarking for loading and queries
- Error handling and edge cases
- Incremental update patterns
"""

from __future__ import annotations

import csv
import time
from pathlib import Path

import pandas as pd
import pytest

from fastopendata.etl.georgia_pipeline import build_georgia_pipeline
from fastopendata.etl.relationship_derivation import (
    derive_contract_state_relationships,
    derive_tract_block_group_relationships,
    derive_tract_puma_relationships,
    derive_tract_state_relationships,
)
from fastopendata.pipeline import GraphPipeline
from fastopendata.schema_evolution.registry import SchemaRegistry


# ---------------------------------------------------------------------------
# Fixtures — synthetic data at various scales
# ---------------------------------------------------------------------------


def _write_contracts(path: Path, n: int, state_fips: str = "13") -> None:
    """Generate n synthetic contract rows."""
    fieldnames = [
        "contract_transaction_unique_key",
        "federal_action_obligation",
        "prime_award_transaction_recipient_state_fips_code",
        "recipient_state_code",
        "prime_award_transaction_place_of_performance_state_fips_code",
        "primary_place_of_performance_state_code",
        "recipient_name",
        "naics_code",
        "award_type",
        "action_date",
    ]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for i in range(n):
            w.writerow({
                "contract_transaction_unique_key": f"C-{i:06d}",
                "federal_action_obligation": str(1000 + i * 100),
                "prime_award_transaction_recipient_state_fips_code": state_fips,
                "recipient_state_code": "GA",
                "prime_award_transaction_place_of_performance_state_fips_code": state_fips,
                "primary_place_of_performance_state_code": "GA",
                "recipient_name": f"Vendor-{i % 50}",
                "naics_code": str(541000 + (i % 20) * 100),
                "award_type": ["A", "B", "C", "D"][i % 4],
                "action_date": f"2025-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
            })


def _write_crosswalk(path: Path, n_tracts: int = 100) -> None:
    """Generate crosswalk with n_tracts Georgia rows."""
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["STATEFP", "COUNTYFP", "TRACTCE", "PUMA5CE"])
        w.writeheader()
        for i in range(n_tracts):
            county = f"{(i // 20) + 1:03d}"
            tract = f"{(i % 20) * 100 + 100:06d}"
            puma = f"{(i // 10) + 1:05d}"
            w.writerow({
                "STATEFP": "13",
                "COUNTYFP": county,
                "TRACTCE": tract,
                "PUMA5CE": puma,
            })


@pytest.fixture
def small_dataset(tmp_path: Path) -> Path:
    """Small dataset: 10 contracts, 5 crosswalk rows."""
    d = tmp_path / "small"
    d.mkdir()
    _write_contracts(d / "contracts_state_13.csv", 10)
    _write_crosswalk(d / "state_county_tract_puma.csv", 5)
    return d


@pytest.fixture
def medium_dataset(tmp_path: Path) -> Path:
    """Medium dataset: 1000 contracts, 100 crosswalk rows."""
    d = tmp_path / "medium"
    d.mkdir()
    _write_contracts(d / "contracts_state_13.csv", 1000)
    _write_crosswalk(d / "state_county_tract_puma.csv", 100)
    return d


@pytest.fixture
def large_dataset(tmp_path: Path) -> Path:
    """Large dataset: 10000 contracts, 500 crosswalk rows."""
    d = tmp_path / "large"
    d.mkdir()
    _write_contracts(d / "contracts_state_13.csv", 10_000)
    _write_crosswalk(d / "state_county_tract_puma.csv", 500)
    return d


# ---------------------------------------------------------------------------
# Data quality validation
# ---------------------------------------------------------------------------


class TestDataQualityValidation:
    """Validate data integrity across ETL transformations."""

    def test_contract_count_preserved(self, small_dataset: Path) -> None:
        pipeline = build_georgia_pipeline(small_dataset)
        assert pipeline.entity_count("Contract") == 10

    def test_obligation_numeric_conversion(self, small_dataset: Path) -> None:
        star = build_georgia_pipeline(small_dataset).build_star()
        result = star.execute_query(
            "MATCH (c:Contract) RETURN sum(c.federal_action_obligation) AS total"
        )
        total = result.iloc[0]["total"]
        # Sum of 1000 + 1100 + ... + 1900 = 10*1000 + 100*(0+1+...+9) = 14500
        assert total == 14500.0

    def test_no_duplicate_contracts(self, medium_dataset: Path) -> None:
        star = build_georgia_pipeline(medium_dataset).build_star()
        result = star.execute_query(
            "MATCH (c:Contract) RETURN count(c) AS total"
        )
        assert result.iloc[0]["total"] == 1000

    def test_relationship_referential_integrity(self, small_dataset: Path) -> None:
        """All relationship targets must exist as entities."""
        pipeline = build_georgia_pipeline(small_dataset)
        # PERFORMED_IN_STATE targets State entity
        assert pipeline.entity_count("State") == 1
        assert pipeline.relationship_count("PERFORMED_IN_STATE") > 0

    def test_crosswalk_state_filtering(self, small_dataset: Path) -> None:
        """Only Georgia crosswalk rows should create MAPS_TO_PUMA edges."""
        pipeline = build_georgia_pipeline(small_dataset)
        assert pipeline.relationship_count("MAPS_TO_PUMA") == 5

    def test_empty_contract_file_handled(self, tmp_path: Path) -> None:
        """Pipeline works with zero-row contract file (header only)."""
        d = tmp_path / "empty"
        d.mkdir()
        _write_contracts(d / "contracts_state_13.csv", 0)
        _write_crosswalk(d / "state_county_tract_puma.csv", 3)
        pipeline = build_georgia_pipeline(d)
        assert pipeline.entity_count("Contract") == 0
        assert pipeline.entity_count("State") == 1


# ---------------------------------------------------------------------------
# Multi-source join query validation
# ---------------------------------------------------------------------------


class TestMultiSourceQueries:
    """Validate Cypher queries spanning multiple data sources."""

    def test_count_aggregation(self, medium_dataset: Path) -> None:
        star = build_georgia_pipeline(medium_dataset).build_star()
        result = star.execute_query(
            "MATCH (c:Contract) RETURN count(c) AS n"
        )
        assert result.iloc[0]["n"] == 1000

    def test_sum_aggregation(self, small_dataset: Path) -> None:
        star = build_georgia_pipeline(small_dataset).build_star()
        result = star.execute_query(
            "MATCH (c:Contract) RETURN sum(c.federal_action_obligation) AS total"
        )
        assert result.iloc[0]["total"] > 0

    def test_where_filter_string(self, medium_dataset: Path) -> None:
        star = build_georgia_pipeline(medium_dataset).build_star()
        result = star.execute_query(
            "MATCH (c:Contract) WHERE c.award_type = 'A' "
            "RETURN count(c) AS n"
        )
        # 1000 contracts, 4 award types → 250 of type A
        assert result.iloc[0]["n"] == 250

    def test_state_entity_query(self, small_dataset: Path) -> None:
        star = build_georgia_pipeline(small_dataset).build_star()
        result = star.execute_query(
            "MATCH (s:State) RETURN s.NAME AS name"
        )
        assert result.iloc[0]["name"] == "Georgia"

    def test_distinct_vendors(self, medium_dataset: Path) -> None:
        star = build_georgia_pipeline(medium_dataset).build_star()
        result = star.execute_query(
            "MATCH (c:Contract) RETURN DISTINCT c.recipient_name AS vendor"
        )
        # 50 unique vendors (i % 50)
        assert len(result) == 50

    def test_order_by_limit(self, medium_dataset: Path) -> None:
        star = build_georgia_pipeline(medium_dataset).build_star()
        result = star.execute_query(
            "MATCH (c:Contract) "
            "RETURN c.federal_action_obligation AS amount "
            "ORDER BY amount DESC LIMIT 5"
        )
        assert len(result) == 5
        # Verify descending order
        amounts = result["amount"].tolist()
        assert amounts == sorted(amounts, reverse=True)


# ---------------------------------------------------------------------------
# Performance benchmarking
# ---------------------------------------------------------------------------


class TestPerformanceBenchmarks:
    """Benchmark loading and query performance at different scales."""

    def test_small_pipeline_build_under_1s(self, small_dataset: Path) -> None:
        t0 = time.monotonic()
        pipeline = build_georgia_pipeline(small_dataset)
        pipeline.build_star()
        elapsed = time.monotonic() - t0
        assert elapsed < 1.0, f"Small pipeline took {elapsed:.2f}s"

    def test_medium_pipeline_build_under_3s(self, medium_dataset: Path) -> None:
        t0 = time.monotonic()
        pipeline = build_georgia_pipeline(medium_dataset)
        pipeline.build_star()
        elapsed = time.monotonic() - t0
        assert elapsed < 3.0, f"Medium pipeline took {elapsed:.2f}s"

    def test_large_pipeline_build_under_10s(self, large_dataset: Path) -> None:
        t0 = time.monotonic()
        pipeline = build_georgia_pipeline(large_dataset)
        pipeline.build_star()
        elapsed = time.monotonic() - t0
        assert elapsed < 10.0, f"Large pipeline took {elapsed:.2f}s"

    def test_count_query_under_100ms(self, medium_dataset: Path) -> None:
        star = build_georgia_pipeline(medium_dataset).build_star()
        t0 = time.monotonic()
        star.execute_query("MATCH (c:Contract) RETURN count(c) AS n")
        elapsed_ms = (time.monotonic() - t0) * 1000
        assert elapsed_ms < 100, f"Count query took {elapsed_ms:.1f}ms"

    def test_filter_query_under_200ms(self, medium_dataset: Path) -> None:
        star = build_georgia_pipeline(medium_dataset).build_star()
        t0 = time.monotonic()
        star.execute_query(
            "MATCH (c:Contract) WHERE c.award_type = 'D' "
            "RETURN count(c) AS n"
        )
        elapsed_ms = (time.monotonic() - t0) * 1000
        assert elapsed_ms < 200, f"Filter query took {elapsed_ms:.1f}ms"

    def test_aggregation_query_under_500ms(self, large_dataset: Path) -> None:
        star = build_georgia_pipeline(large_dataset).build_star()
        t0 = time.monotonic()
        star.execute_query(
            "MATCH (c:Contract) "
            "RETURN sum(c.federal_action_obligation) AS total"
        )
        elapsed_ms = (time.monotonic() - t0) * 1000
        assert elapsed_ms < 500, f"Aggregation query took {elapsed_ms:.1f}ms"


# ---------------------------------------------------------------------------
# Error handling and edge cases
# ---------------------------------------------------------------------------


class TestErrorHandling:
    def test_missing_contracts_file(self, tmp_path: Path) -> None:
        d = tmp_path / "no_contracts"
        d.mkdir()
        _write_crosswalk(d / "state_county_tract_puma.csv", 3)
        with pytest.raises(FileNotFoundError, match="contracts"):
            build_georgia_pipeline(d)

    def test_missing_crosswalk_file(self, tmp_path: Path) -> None:
        d = tmp_path / "no_crosswalk"
        d.mkdir()
        _write_contracts(d / "contracts_state_13.csv", 5)
        with pytest.raises(FileNotFoundError, match="crosswalk"):
            build_georgia_pipeline(d)

    def test_max_rows_zero(self, small_dataset: Path) -> None:
        pipeline = build_georgia_pipeline(small_dataset, max_contract_rows=0)
        assert pipeline.entity_count("Contract") == 0

    def test_schema_registry_registers_entities(self, small_dataset: Path) -> None:
        registry = SchemaRegistry()
        build_georgia_pipeline(small_dataset, schema_registry=registry)
        assert registry.get_latest("Contract") is not None
        assert registry.get_latest("State") is not None


# ---------------------------------------------------------------------------
# Incremental update patterns
# ---------------------------------------------------------------------------


class TestIncrementalUpdates:
    """Test rebuilding pipeline with updated data (full-replace pattern)."""

    def test_rebuild_with_new_contracts(self, tmp_path: Path) -> None:
        """Simulate incremental update by rebuilding with more data."""
        d = tmp_path / "incremental"
        d.mkdir()
        _write_crosswalk(d / "state_county_tract_puma.csv", 5)

        # Initial load: 10 contracts
        _write_contracts(d / "contracts_state_13.csv", 10)
        star1 = build_georgia_pipeline(d).build_star()
        r1 = star1.execute_query("MATCH (c:Contract) RETURN count(c) AS n")
        assert r1.iloc[0]["n"] == 10

        # Updated load: 20 contracts (full replace)
        _write_contracts(d / "contracts_state_13.csv", 20)
        star2 = build_georgia_pipeline(d).build_star()
        r2 = star2.execute_query("MATCH (c:Contract) RETURN count(c) AS n")
        assert r2.iloc[0]["n"] == 20

    def test_rebuild_preserves_relationships(self, tmp_path: Path) -> None:
        d = tmp_path / "rebuild"
        d.mkdir()
        _write_crosswalk(d / "state_county_tract_puma.csv", 5)

        _write_contracts(d / "contracts_state_13.csv", 10)
        p1 = build_georgia_pipeline(d)
        r1_puma = p1.relationship_count("MAPS_TO_PUMA")

        _write_contracts(d / "contracts_state_13.csv", 50)
        p2 = build_georgia_pipeline(d)
        r2_puma = p2.relationship_count("MAPS_TO_PUMA")

        # MAPS_TO_PUMA depends on crosswalk, not contracts — count unchanged
        assert r1_puma == r2_puma == 5


# ---------------------------------------------------------------------------
# Relationship derivation edge cases
# ---------------------------------------------------------------------------


class TestRelationshipEdgeCases:
    def test_empty_tracts_no_edges(self) -> None:
        tracts = pd.DataFrame(columns=["GEOID", "STATEFP"])
        states = pd.DataFrame({"STATEFP": ["13"]})
        edges = derive_tract_state_relationships(tracts, states)
        assert len(edges) == 0

    def test_empty_crosswalk_no_edges(self) -> None:
        crosswalk = pd.DataFrame(columns=["STATEFP", "COUNTYFP", "TRACTCE", "PUMA5CE"])
        edges = derive_tract_puma_relationships(crosswalk)
        assert len(edges) == 0

    def test_empty_block_groups_no_edges(self) -> None:
        tracts = pd.DataFrame({"GEOID": ["13001000100"]})
        bgs = pd.DataFrame(columns=["GEOID"])
        edges = derive_tract_block_group_relationships(tracts, bgs)
        assert len(edges) == 0

    def test_contract_with_all_null_fips(self) -> None:
        contracts = pd.DataFrame({
            "contract_transaction_unique_key": ["X-001"],
            "prime_award_transaction_place_of_performance_state_fips_code": [None],
        })
        states = pd.DataFrame({"STATEFP": ["13"]})
        edges = derive_contract_state_relationships(contracts, states)
        assert len(edges) == 0

    def test_large_crosswalk_derivation(self) -> None:
        """Verify derivation scales to realistic crosswalk sizes."""
        rows = []
        for i in range(5000):
            rows.append({
                "STATEFP": "13",
                "COUNTYFP": f"{(i // 100) + 1:03d}",
                "TRACTCE": f"{i:06d}",
                "PUMA5CE": f"{(i // 50):05d}",
            })
        crosswalk = pd.DataFrame(rows)
        edges = derive_tract_puma_relationships(crosswalk, state_fips="13")
        assert len(edges) == 5000
