"""ETL pipeline implementations for fastopendata → pycypher integration.

Provides pipeline builders that load Snakefile-processed data sources
(Census TIGER, OSM, state contracts) into pycypher graph contexts
for Cypher query execution.

Modules
-------
schemas
    Entity and relationship schema constants for all data sources.
relationship_derivation
    Functions to derive graph edges from FIPS code joins.
shapefile_loader
    Shapefile → DataFrame conversion utilities.
georgia_pipeline
    Georgia development dataset pipeline (Phase 1).
"""

from fastopendata.etl.relationship_derivation import (
    derive_contract_state_relationships,
    derive_tract_block_group_relationships,
    derive_tract_puma_relationships,
    derive_tract_state_relationships,
)
from fastopendata.etl.schemas import (
    BLOCK_GROUP_COLUMNS,
    CENSUS_TRACT_COLUMNS,
    CONTRACT_COLUMNS,
    EntitySchema,
    OSM_NODE_COLUMNS,
    PUMA_COLUMNS,
    RelationshipSchema,
    STATE_COLUMNS,
)

__all__ = [
    # Schemas
    "EntitySchema",
    "RelationshipSchema",
    "CENSUS_TRACT_COLUMNS",
    "BLOCK_GROUP_COLUMNS",
    "PUMA_COLUMNS",
    "STATE_COLUMNS",
    "CONTRACT_COLUMNS",
    "OSM_NODE_COLUMNS",
    # Relationship derivation
    "derive_tract_state_relationships",
    "derive_tract_puma_relationships",
    "derive_tract_block_group_relationships",
    "derive_contract_state_relationships",
]
