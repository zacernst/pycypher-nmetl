"""Tests for the ETL pipeline implementation.

Uses synthetic data files that mimic Snakefile outputs to test the
complete pipeline without requiring actual data downloads.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd
import pytest
from fastopendata.etl.georgia_pipeline import build_georgia_pipeline
from fastopendata.etl.state_pipeline import build_state_pipeline


def _write_contracts_csv(path: Path, rows: list[dict[str, str]]) -> None:
    """Write a synthetic contracts CSV."""
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
        w.writerows(rows)


def _write_crosswalk_csv(path: Path) -> None:
    """Write a synthetic tract-PUMA crosswalk CSV."""
    rows = [
        {"STATEFP": "13", "COUNTYFP": "001", "TRACTCE": "000100", "PUMA5CE": "03700"},
        {"STATEFP": "13", "COUNTYFP": "001", "TRACTCE": "000200", "PUMA5CE": "03700"},
        {"STATEFP": "13", "COUNTYFP": "089", "TRACTCE": "010100", "PUMA5CE": "04000"},
        {"STATEFP": "06", "COUNTYFP": "037", "TRACTCE": "123400", "PUMA5CE": "06500"},
    ]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["STATEFP", "COUNTYFP", "TRACTCE", "PUMA5CE"])
        w.writeheader()
        w.writerows(rows)


@pytest.fixture
def georgia_data_dir(tmp_path: Path) -> Path:
    """Create synthetic Georgia data files in a temp directory."""
    data_dir = tmp_path / "raw_data"
    data_dir.mkdir()

    # Contracts
    contracts = [
        {
            "contract_transaction_unique_key": "GA-001",
            "federal_action_obligation": "1500000",
            "prime_award_transaction_recipient_state_fips_code": "13",
            "recipient_state_code": "GA",
            "prime_award_transaction_place_of_performance_state_fips_code": "13",
            "primary_place_of_performance_state_code": "GA",
            "recipient_name": "Acme Corp",
            "naics_code": "541512",
            "award_type": "D",
            "action_date": "2025-01-15",
        },
        {
            "contract_transaction_unique_key": "GA-002",
            "federal_action_obligation": "750000",
            "prime_award_transaction_recipient_state_fips_code": "13",
            "recipient_state_code": "GA",
            "prime_award_transaction_place_of_performance_state_fips_code": "13",
            "primary_place_of_performance_state_code": "GA",
            "recipient_name": "Beta Inc",
            "naics_code": "236220",
            "award_type": "C",
            "action_date": "2025-02-20",
        },
        {
            "contract_transaction_unique_key": "GA-003",
            "federal_action_obligation": "250000",
            "prime_award_transaction_recipient_state_fips_code": "48",
            "recipient_state_code": "TX",
            "prime_award_transaction_place_of_performance_state_fips_code": "13",
            "primary_place_of_performance_state_code": "GA",
            "recipient_name": "Gamma LLC",
            "naics_code": "541330",
            "award_type": "D",
            "action_date": "2025-03-10",
        },
    ]
    _write_contracts_csv(data_dir / "contracts_state_13.csv", contracts)

    # Crosswalk
    _write_crosswalk_csv(data_dir / "state_county_tract_puma.csv")

    return data_dir


class TestGeorgiaPipeline:
    def test_builds_successfully(self, georgia_data_dir: Path) -> None:
        pipeline = build_georgia_pipeline(georgia_data_dir)
        assert "Contract" in pipeline.entity_types
        assert "State" in pipeline.entity_types

    def test_loads_contracts(self, georgia_data_dir: Path) -> None:
        pipeline = build_georgia_pipeline(georgia_data_dir)
        assert pipeline.entity_count("Contract") == 3

    def test_creates_state_entity(self, georgia_data_dir: Path) -> None:
        pipeline = build_georgia_pipeline(georgia_data_dir)
        assert pipeline.entity_count("State") == 1

    def test_derives_performed_in_state(self, georgia_data_dir: Path) -> None:
        pipeline = build_georgia_pipeline(georgia_data_dir)
        assert "PERFORMED_IN_STATE" in pipeline.relationship_types
        # All 3 contracts have POP in Georgia
        assert pipeline.relationship_count("PERFORMED_IN_STATE") == 3

    def test_derives_awarded_in_state(self, georgia_data_dir: Path) -> None:
        pipeline = build_georgia_pipeline(georgia_data_dir)
        assert "AWARDED_IN_STATE" in pipeline.relationship_types
        # GA-001 and GA-002 have recipient in GA; GA-003 is TX recipient
        assert pipeline.relationship_count("AWARDED_IN_STATE") == 2

    def test_derives_maps_to_puma(self, georgia_data_dir: Path) -> None:
        pipeline = build_georgia_pipeline(georgia_data_dir)
        assert "MAPS_TO_PUMA" in pipeline.relationship_types
        # 3 Georgia crosswalk rows
        assert pipeline.relationship_count("MAPS_TO_PUMA") == 3

    def test_max_contract_rows(self, georgia_data_dir: Path) -> None:
        pipeline = build_georgia_pipeline(
            georgia_data_dir, max_contract_rows=2
        )
        assert pipeline.entity_count("Contract") == 2

    def test_missing_data_raises_error(self, tmp_path: Path) -> None:
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        with pytest.raises(FileNotFoundError, match="snakemake"):
            build_georgia_pipeline(empty_dir)

    def test_builds_context(self, georgia_data_dir: Path) -> None:
        pipeline = build_georgia_pipeline(georgia_data_dir)
        context = pipeline.build_context()
        assert context is not None

    def test_builds_star(self, georgia_data_dir: Path) -> None:
        pipeline = build_georgia_pipeline(georgia_data_dir)
        star = pipeline.build_star()
        assert star is not None

    def test_query_contract_count(self, georgia_data_dir: Path) -> None:
        star = build_georgia_pipeline(georgia_data_dir).build_star()
        result = star.execute_query(
            "MATCH (c:Contract) RETURN count(c) AS total"
        )
        assert result.iloc[0]["total"] == 3

    def test_query_state_entity(self, georgia_data_dir: Path) -> None:
        star = build_georgia_pipeline(georgia_data_dir).build_star()
        result = star.execute_query(
            "MATCH (s:State) RETURN s.NAME AS name, s.STUSPS AS abbrev"
        )
        assert result.iloc[0]["name"] == "Georgia"
        assert result.iloc[0]["abbrev"] == "GA"

    def test_query_contract_properties(self, georgia_data_dir: Path) -> None:
        star = build_georgia_pipeline(georgia_data_dir).build_star()
        result = star.execute_query(
            "MATCH (c:Contract) "
            "WHERE c.recipient_name = 'Acme Corp' "
            "RETURN c.naics_code AS naics"
        )
        assert len(result) == 1
        assert result.iloc[0]["naics"] == "541512"

    def test_lineage_tracked(self, georgia_data_dir: Path) -> None:
        pipeline = build_georgia_pipeline(georgia_data_dir)
        lineage = pipeline.lineage
        # Should have nodes for entities and relationships
        assert lineage.node_count > 0

    def test_with_schema_registry(self, georgia_data_dir: Path) -> None:
        from fastopendata.schema_evolution.registry import SchemaRegistry

        registry = SchemaRegistry()
        pipeline = build_georgia_pipeline(
            georgia_data_dir, schema_registry=registry
        )
        # Schema should be registered for Contract entity
        assert registry.get_latest("Contract") is not None


class TestStatePipeline:
    def test_georgia_delegates(self, georgia_data_dir: Path) -> None:
        pipeline = build_state_pipeline(
            georgia_data_dir, state_fips="13"
        )
        assert "Contract" in pipeline.entity_types

    def test_invalid_fips_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Invalid state FIPS"):
            build_state_pipeline(tmp_path, state_fips="abc")

    def test_missing_data_for_other_state_raises_file_not_found(
        self, tmp_path: Path,
    ) -> None:
        """California now works — but only when its data files exist.

        Without contracts/crosswalk on disk we get a clear FileNotFoundError
        naming the state and the snakemake command to fetch the data.
        """
        with pytest.raises(FileNotFoundError) as excinfo:
            build_state_pipeline(tmp_path, state_fips="06")
        msg = str(excinfo.value)
        assert "California" in msg
        assert "STATE_FIPS=06" in msg

    def test_unknown_fips_uses_fallback_label(self, tmp_path: Path) -> None:
        """A FIPS not in _STATE_INFO still works — error names the FIPS."""
        with pytest.raises(FileNotFoundError) as excinfo:
            build_state_pipeline(tmp_path, state_fips="99")
        assert "FIPS 99" in str(excinfo.value)

    def test_california_pipeline_with_synthetic_data(self, tmp_path: Path) -> None:
        """End-to-end: synthetic CA data produces a full graph."""
        data_dir = tmp_path / "ca_raw"
        data_dir.mkdir()

        # CA contracts (FIPS 06)
        ca_contracts = [
            {
                "contract_transaction_unique_key": "CA-001",
                "federal_action_obligation": "1000000",
                "prime_award_transaction_recipient_state_fips_code": "06",
                "recipient_state_code": "CA",
                "prime_award_transaction_place_of_performance_state_fips_code": "06",
                "primary_place_of_performance_state_code": "CA",
                "recipient_name": "BigCo",
                "naics_code": "541512",
                "award_type": "D",
                "action_date": "2025-04-01",
            },
            {
                "contract_transaction_unique_key": "CA-002",
                "federal_action_obligation": "500000",
                "prime_award_transaction_recipient_state_fips_code": "13",  # GA recipient
                "recipient_state_code": "GA",
                "prime_award_transaction_place_of_performance_state_fips_code": "06",
                "primary_place_of_performance_state_code": "CA",
                "recipient_name": "OutOfStateCo",
                "naics_code": "541330",
                "award_type": "D",
                "action_date": "2025-05-15",
            },
        ]
        _write_contracts_csv(data_dir / "contracts_state_06.csv", ca_contracts)

        # Crosswalk with both GA and CA rows (CA rows must filter through)
        _write_crosswalk_csv(data_dir / "state_county_tract_puma.csv")

        pipeline = build_state_pipeline(data_dir, state_fips="06")

        # Entities: Contract + State (no shapefiles in this test)
        assert "Contract" in pipeline.entity_types
        assert "State" in pipeline.entity_types
        assert pipeline.entity_count("Contract") == 2
        assert pipeline.entity_count("State") == 1

        # Relationships:
        # PERFORMED_IN_STATE: both CA contracts (POP = CA)
        assert pipeline.relationship_count("PERFORMED_IN_STATE") == 2
        # AWARDED_IN_STATE: only CA-001 has recipient in CA; CA-002 → GA
        # (and GA isn't in this State entity), so 1.
        assert pipeline.relationship_count("AWARDED_IN_STATE") == 1
        # MAPS_TO_PUMA: only the 1 CA crosswalk row
        # (sample crosswalk has 3 GA rows + 1 CA row).
        assert pipeline.relationship_count("MAPS_TO_PUMA") == 1

    def test_state_entity_uses_correct_label(self, tmp_path: Path) -> None:
        """Synthetic State entity uses the right (abbrev, name) for the FIPS."""
        data_dir = tmp_path / "tx_raw"
        data_dir.mkdir()
        _write_contracts_csv(data_dir / "contracts_state_48.csv", [])
        _write_crosswalk_csv(data_dir / "state_county_tract_puma.csv")

        pipeline = build_state_pipeline(data_dir, state_fips="48")
        star = pipeline.build_star()
        result = star.execute_query(
            "MATCH (s:State) RETURN s.NAME AS name, s.STUSPS AS abbrev"
        )
        assert result.iloc[0]["name"] == "Texas"
        assert result.iloc[0]["abbrev"] == "TX"
