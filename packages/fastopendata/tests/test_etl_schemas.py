"""Tests for ETL schema definitions and relationship derivation."""

from __future__ import annotations

import pandas as pd
import pytest

from fastopendata.etl.schemas import (
    ALL_ENTITY_SCHEMAS,
    ALL_RELATIONSHIP_SCHEMAS,
    CENSUS_TRACT_SCHEMA,
    CONTRACT_SCHEMA,
    EntitySchema,
    RelationshipSchema,
)
from fastopendata.etl.relationship_derivation import (
    derive_contract_state_relationships,
    derive_tract_block_group_relationships,
    derive_tract_puma_relationships,
    derive_tract_state_relationships,
)


# ---------------------------------------------------------------------------
# Schema definition tests
# ---------------------------------------------------------------------------


class TestEntitySchemas:
    def test_all_entity_schemas_have_required_fields(self) -> None:
        for schema in ALL_ENTITY_SCHEMAS:
            assert schema.entity_type, f"Missing entity_type: {schema}"
            assert schema.id_col, f"Missing id_col: {schema}"
            assert len(schema.required_columns) > 0, f"No columns: {schema}"
            assert schema.source_pattern, f"Missing source_pattern: {schema}"

    def test_id_col_in_required_columns(self) -> None:
        for schema in ALL_ENTITY_SCHEMAS:
            assert schema.id_col in schema.required_columns, (
                f"{schema.entity_type}: id_col '{schema.id_col}' "
                f"not in required_columns"
            )

    def test_entity_types_unique(self) -> None:
        types = [s.entity_type for s in ALL_ENTITY_SCHEMAS]
        assert len(types) == len(set(types)), "Duplicate entity types"

    def test_census_tract_schema(self) -> None:
        assert CENSUS_TRACT_SCHEMA.entity_type == "CensusTract"
        assert CENSUS_TRACT_SCHEMA.id_col == "GEOID"
        assert "STATEFP" in CENSUS_TRACT_SCHEMA.required_columns

    def test_contract_schema(self) -> None:
        assert CONTRACT_SCHEMA.entity_type == "Contract"
        assert CONTRACT_SCHEMA.id_col == "contract_transaction_unique_key"


class TestRelationshipSchemas:
    def test_all_relationship_schemas_have_required_fields(self) -> None:
        for schema in ALL_RELATIONSHIP_SCHEMAS:
            assert schema.relationship_type, f"Missing type: {schema}"
            assert schema.source_entity, f"Missing source_entity: {schema}"
            assert schema.target_entity, f"Missing target_entity: {schema}"

    def test_relationship_types_unique(self) -> None:
        types = [s.relationship_type for s in ALL_RELATIONSHIP_SCHEMAS]
        assert len(types) == len(set(types)), "Duplicate relationship types"

    def test_source_target_entities_exist(self) -> None:
        entity_types = {s.entity_type for s in ALL_ENTITY_SCHEMAS}
        for schema in ALL_RELATIONSHIP_SCHEMAS:
            assert schema.source_entity in entity_types, (
                f"{schema.relationship_type}: source '{schema.source_entity}' "
                f"not in entity schemas"
            )
            assert schema.target_entity in entity_types, (
                f"{schema.relationship_type}: target '{schema.target_entity}' "
                f"not in entity schemas"
            )


# ---------------------------------------------------------------------------
# Relationship derivation tests
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_tracts() -> pd.DataFrame:
    return pd.DataFrame({
        "GEOID": ["13001000100", "13001000200", "13003000100"],
        "STATEFP": ["13", "13", "13"],
        "COUNTYFP": ["001", "001", "003"],
        "TRACTCE": ["000100", "000200", "000100"],
        "ALAND": [1000000, 2000000, 3000000],
    })


@pytest.fixture
def sample_states() -> pd.DataFrame:
    return pd.DataFrame({
        "STATEFP": ["13", "06", "48"],
        "STUSPS": ["GA", "CA", "TX"],
        "NAME": ["Georgia", "California", "Texas"],
    })


@pytest.fixture
def sample_block_groups() -> pd.DataFrame:
    return pd.DataFrame({
        "GEOID": ["130010001001", "130010001002", "130010002001"],
        "STATEFP": ["13", "13", "13"],
        "COUNTYFP": ["001", "001", "001"],
        "TRACTCE": ["000100", "000100", "000200"],
        "BLKGRPCE": ["1", "2", "1"],
    })


@pytest.fixture
def sample_crosswalk() -> pd.DataFrame:
    return pd.DataFrame({
        "STATEFP": ["13", "13", "06"],
        "COUNTYFP": ["001", "001", "037"],
        "TRACTCE": ["000100", "000200", "123400"],
        "PUMA5CE": ["03700", "03700", "06500"],
    })


@pytest.fixture
def sample_contracts() -> pd.DataFrame:
    return pd.DataFrame({
        "contract_transaction_unique_key": ["GA-001", "TX-001", "GA-002"],
        "federal_action_obligation": [1000.0, 2000.0, 3000.0],
        "prime_award_transaction_place_of_performance_state_fips_code": [
            "13", "48", None,
        ],
        "prime_award_transaction_recipient_state_fips_code": [
            "13", "48", "13",
        ],
    })


class TestTractStateRelationships:
    def test_basic_derivation(
        self, sample_tracts: pd.DataFrame, sample_states: pd.DataFrame
    ) -> None:
        edges = derive_tract_state_relationships(sample_tracts, sample_states)
        assert "__SOURCE__" in edges.columns
        assert "__TARGET__" in edges.columns
        assert len(edges) == 3
        assert set(edges["__TARGET__"]) == {"13"}

    def test_filters_invalid_states(self, sample_tracts: pd.DataFrame) -> None:
        # Only state "99" — no tracts match
        states = pd.DataFrame({"STATEFP": ["99"]})
        edges = derive_tract_state_relationships(sample_tracts, states)
        assert len(edges) == 0

    def test_source_is_geoid(
        self, sample_tracts: pd.DataFrame, sample_states: pd.DataFrame
    ) -> None:
        edges = derive_tract_state_relationships(sample_tracts, sample_states)
        assert set(edges["__SOURCE__"]) == set(sample_tracts["GEOID"])


class TestTractPumaRelationships:
    def test_basic_derivation(self, sample_crosswalk: pd.DataFrame) -> None:
        edges = derive_tract_puma_relationships(sample_crosswalk)
        assert len(edges) == 3
        assert "__SOURCE__" in edges.columns
        assert "__TARGET__" in edges.columns

    def test_state_filter(self, sample_crosswalk: pd.DataFrame) -> None:
        edges = derive_tract_puma_relationships(
            sample_crosswalk, state_fips="13"
        )
        assert len(edges) == 2
        # All sources should be Georgia tracts
        assert all(s.startswith("13") for s in edges["__SOURCE__"])

    def test_geoid_construction(self, sample_crosswalk: pd.DataFrame) -> None:
        edges = derive_tract_puma_relationships(
            sample_crosswalk, state_fips="13"
        )
        # STATEFP(13) + COUNTYFP(001) + TRACTCE(000100) = 13001000100
        assert "13001000100" in edges["__SOURCE__"].values

    def test_empty_state_filter(self, sample_crosswalk: pd.DataFrame) -> None:
        edges = derive_tract_puma_relationships(
            sample_crosswalk, state_fips="99"
        )
        assert len(edges) == 0


class TestTractBlockGroupRelationships:
    def test_basic_derivation(
        self,
        sample_tracts: pd.DataFrame,
        sample_block_groups: pd.DataFrame,
    ) -> None:
        edges = derive_tract_block_group_relationships(
            sample_tracts, sample_block_groups
        )
        assert len(edges) == 3
        assert "__SOURCE__" in edges.columns
        assert "__TARGET__" in edges.columns

    def test_tract_geoid_prefix_match(
        self,
        sample_tracts: pd.DataFrame,
        sample_block_groups: pd.DataFrame,
    ) -> None:
        edges = derive_tract_block_group_relationships(
            sample_tracts, sample_block_groups
        )
        # Block groups 130010001001 and 130010001002 → tract 13001000100
        tract_100_bgs = edges[edges["__SOURCE__"] == "13001000100"]
        assert len(tract_100_bgs) == 2

    def test_filters_orphan_block_groups(
        self, sample_block_groups: pd.DataFrame
    ) -> None:
        # Only one tract exists — block groups for other tracts are excluded
        tracts = pd.DataFrame({"GEOID": ["13001000100"]})
        edges = derive_tract_block_group_relationships(tracts, sample_block_groups)
        assert len(edges) == 2  # Only block groups in tract 000100


class TestContractStateRelationships:
    def test_pop_fips_derivation(
        self,
        sample_contracts: pd.DataFrame,
        sample_states: pd.DataFrame,
    ) -> None:
        edges = derive_contract_state_relationships(
            sample_contracts, sample_states
        )
        # GA-001 → 13, TX-001 → 48, GA-002 has None POP FIPS
        assert len(edges) == 2
        assert set(edges["__SOURCE__"]) == {"GA-001", "TX-001"}

    def test_recipient_fips_derivation(
        self,
        sample_contracts: pd.DataFrame,
        sample_states: pd.DataFrame,
    ) -> None:
        edges = derive_contract_state_relationships(
            sample_contracts,
            sample_states,
            fips_column="prime_award_transaction_recipient_state_fips_code",
        )
        # All 3 have recipient FIPS
        assert len(edges) == 3

    def test_filters_invalid_states(
        self, sample_contracts: pd.DataFrame
    ) -> None:
        states = pd.DataFrame({"STATEFP": ["99"]})
        edges = derive_contract_state_relationships(
            sample_contracts, states
        )
        assert len(edges) == 0

    def test_drops_null_fips(
        self,
        sample_contracts: pd.DataFrame,
        sample_states: pd.DataFrame,
    ) -> None:
        edges = derive_contract_state_relationships(
            sample_contracts, sample_states
        )
        # GA-002 has None POP FIPS → excluded
        assert "GA-002" not in edges["__SOURCE__"].values
