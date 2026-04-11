"""Entity and relationship schema definitions for fastopendata ETL pipelines.

Defines the data model mapping Snakefile-produced data sources to pycypher
entity types and relationship types. Each schema specifies:

- Entity/relationship type name (Cypher label)
- ID column used for node/edge identity
- Required columns that must exist in source data
- Source file pattern for traceability

These schemas drive validation during pipeline construction and serve as
the canonical reference for the fastopendata graph data model.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class EntitySchema:
    """Schema definition for a pycypher entity type (graph node)."""

    entity_type: str
    id_col: str
    required_columns: tuple[str, ...]
    source_pattern: str
    description: str = ""


@dataclass(frozen=True, slots=True)
class RelationshipSchema:
    """Schema definition for a pycypher relationship type (graph edge)."""

    relationship_type: str
    source_entity: str
    target_entity: str
    source_col: str
    target_col: str
    derivation: str = ""
    description: str = ""


# ---------------------------------------------------------------------------
# Column name constants — canonical names used across the pipeline
# ---------------------------------------------------------------------------

# Census TIGER tract shapefiles (tl_2025_{fips}_tract.shp)
CENSUS_TRACT_COLUMNS = (
    "STATEFP",    # State FIPS code (2 digits)
    "COUNTYFP",   # County FIPS code (3 digits)
    "TRACTCE",    # Census tract code (6 digits)
    "GEOID",      # Full GEOID: STATEFP + COUNTYFP + TRACTCE (11 digits)
    "NAME",       # Tract name
    "ALAND",      # Land area (square meters)
    "AWATER",     # Water area (square meters)
)

# Census TIGER block group shapefiles (tl_2024_{fips}_bg.shp)
BLOCK_GROUP_COLUMNS = (
    "STATEFP",
    "COUNTYFP",
    "TRACTCE",
    "BLKGRPCE",   # Block group code (1 digit)
    "GEOID",      # Full GEOID: STATEFP + COUNTYFP + TRACTCE + BLKGRPCE (12 digits)
    "ALAND",
    "AWATER",
)

# Census TIGER PUMA shapefiles (tl_2024_{fips}_puma20.shp)
PUMA_COLUMNS = (
    "STATEFP20",  # State FIPS code
    "PUMACE20",   # PUMA code (5 digits)
    "GEOID20",    # Full GEOID: STATEFP + PUMACE
    "NAMELSAD20", # PUMA name
    "ALAND20",
    "AWATER20",
)

# Census TIGER state boundaries (tl_2024_us_state.shp)
STATE_COLUMNS = (
    "STATEFP",    # State FIPS code
    "STUSPS",     # State postal abbreviation (e.g. "GA")
    "NAME",       # State name (e.g. "Georgia")
    "ALAND",
    "AWATER",
)

# USAspending.gov federal contracts (contracts_state_{fips}.csv)
# Only the columns most relevant for graph queries; full file has ~297 columns
CONTRACT_COLUMNS = (
    "contract_transaction_unique_key",
    "federal_action_obligation",
    "prime_award_transaction_recipient_state_fips_code",
    "prime_award_transaction_place_of_performance_state_fips_code",
    "recipient_name",
    "naics_code",
    "award_type",
    "action_date",
)

# OSM node extract (united_states_nodes.csv)
OSM_NODE_COLUMNS = (
    "id",
    "longitude",
    "latitude",
    "encoded_tags",
)

# Tract-PUMA crosswalk (state_county_tract_puma.csv)
CROSSWALK_COLUMNS = (
    "STATEFP",
    "COUNTYFP",
    "TRACTCE",
    "PUMA5CE",
)


# ---------------------------------------------------------------------------
# Entity schemas — one per data source
# ---------------------------------------------------------------------------

CENSUS_TRACT_SCHEMA = EntitySchema(
    entity_type="CensusTract",
    id_col="GEOID",
    required_columns=CENSUS_TRACT_COLUMNS,
    source_pattern="tl_2025_{fips}_tract.shp",
    description="Census tract geographic boundaries from TIGER/Line",
)

BLOCK_GROUP_SCHEMA = EntitySchema(
    entity_type="BlockGroup",
    id_col="GEOID",
    required_columns=BLOCK_GROUP_COLUMNS,
    source_pattern="tl_2024_{fips}_bg.shp",
    description="Census block group geographic boundaries from TIGER/Line",
)

PUMA_SCHEMA = EntitySchema(
    entity_type="Puma",
    id_col="PUMACE20",
    required_columns=PUMA_COLUMNS,
    source_pattern="tl_2024_{fips}_puma20.shp",
    description="Public Use Microdata Areas from TIGER/Line",
)

STATE_SCHEMA = EntitySchema(
    entity_type="State",
    id_col="STATEFP",
    required_columns=STATE_COLUMNS,
    source_pattern="tl_2024_us_state.shp",
    description="State boundaries from TIGER/Line",
)

CONTRACT_SCHEMA = EntitySchema(
    entity_type="Contract",
    id_col="contract_transaction_unique_key",
    required_columns=CONTRACT_COLUMNS,
    source_pattern="contracts_state_{fips}.csv",
    description="Federal contract transactions from USAspending.gov",
)

OSM_NODE_SCHEMA = EntitySchema(
    entity_type="OsmNode",
    id_col="id",
    required_columns=OSM_NODE_COLUMNS,
    source_pattern="united_states_nodes.csv",
    description="OpenStreetMap nodes with geographic coordinates and tags",
)


# ---------------------------------------------------------------------------
# Relationship schemas — derived from FIPS code joins
# ---------------------------------------------------------------------------

TRACT_IN_STATE_SCHEMA = RelationshipSchema(
    relationship_type="IN_STATE",
    source_entity="CensusTract",
    target_entity="State",
    source_col="__SOURCE__",
    target_col="__TARGET__",
    derivation="CensusTract.STATEFP = State.STATEFP",
    description="Census tract belongs to a state",
)

TRACT_MAPS_TO_PUMA_SCHEMA = RelationshipSchema(
    relationship_type="MAPS_TO_PUMA",
    source_entity="CensusTract",
    target_entity="Puma",
    source_col="__SOURCE__",
    target_col="__TARGET__",
    derivation="Via state_county_tract_puma.csv crosswalk",
    description="Census tract maps to a Public Use Microdata Area",
)

TRACT_CONTAINS_BLOCK_GROUP_SCHEMA = RelationshipSchema(
    relationship_type="CONTAINS_BLOCK_GROUP",
    source_entity="CensusTract",
    target_entity="BlockGroup",
    source_col="__SOURCE__",
    target_col="__TARGET__",
    derivation="BlockGroup.GEOID[:11] = CensusTract.GEOID",
    description="Census tract contains block groups",
)

CONTRACT_PERFORMED_IN_STATE_SCHEMA = RelationshipSchema(
    relationship_type="PERFORMED_IN_STATE",
    source_entity="Contract",
    target_entity="State",
    source_col="__SOURCE__",
    target_col="__TARGET__",
    derivation="Contract.pop_state_fips = State.STATEFP",
    description="Contract work performed in a state",
)

CONTRACT_AWARDED_IN_STATE_SCHEMA = RelationshipSchema(
    relationship_type="AWARDED_IN_STATE",
    source_entity="Contract",
    target_entity="State",
    source_col="__SOURCE__",
    target_col="__TARGET__",
    derivation="Contract.recipient_state_fips = State.STATEFP",
    description="Contract awarded to recipient in a state",
)


# ---------------------------------------------------------------------------
# Schema collections for pipeline construction
# ---------------------------------------------------------------------------

ALL_ENTITY_SCHEMAS: tuple[EntitySchema, ...] = (
    CENSUS_TRACT_SCHEMA,
    BLOCK_GROUP_SCHEMA,
    PUMA_SCHEMA,
    STATE_SCHEMA,
    CONTRACT_SCHEMA,
    OSM_NODE_SCHEMA,
)

ALL_RELATIONSHIP_SCHEMAS: tuple[RelationshipSchema, ...] = (
    TRACT_IN_STATE_SCHEMA,
    TRACT_MAPS_TO_PUMA_SCHEMA,
    TRACT_CONTAINS_BLOCK_GROUP_SCHEMA,
    CONTRACT_PERFORMED_IN_STATE_SCHEMA,
    CONTRACT_AWARDED_IN_STATE_SCHEMA,
)
