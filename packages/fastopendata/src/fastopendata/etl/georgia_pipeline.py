"""Georgia development dataset ETL pipeline (Phase 1).

Loads Snakefile-processed Georgia data into a pycypher graph context,
deriving relationships from FIPS code joins. This is the reference
implementation for single-state ETL pipelines.

Usage::

    from fastopendata.etl.georgia_pipeline import build_georgia_pipeline

    pipeline = build_georgia_pipeline(Path("raw_data"))
    star = pipeline.build_star()
    result = star.execute_query(
        "MATCH (c:Contract) RETURN count(c) AS total"
    )

Prerequisites:
    Run ``snakemake --cores 4 georgia_dev`` from ``packages/fastopendata/``
    to download and process the Georgia development dataset.

.. versionadded:: 0.0.20
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from fastopendata.etl.relationship_derivation import (
    derive_contract_state_relationships,
    derive_tract_block_group_relationships,
    derive_tract_puma_relationships,
    derive_tract_state_relationships,
)
from fastopendata.etl.schemas import (
    CONTRACT_COLUMNS,
    CROSSWALK_COLUMNS,
)
from fastopendata.pipeline import GraphPipeline
from fastopendata.schema_evolution.registry import SchemaRegistry

_logger = logging.getLogger(__name__)

GEORGIA_FIPS = "13"

# Columns to load from contracts CSV (subset of ~297 total for efficiency)
_CONTRACT_USECOLS: list[str] = list(CONTRACT_COLUMNS)


def _read_shapefile_as_dataframe(path: Path) -> pd.DataFrame:
    """Read a shapefile and return a plain DataFrame (geometry dropped).

    Uses geopandas if available; raises ImportError with guidance if not.
    """
    try:
        import geopandas as gpd
    except ImportError as exc:
        raise ImportError(
            "geopandas is required to load shapefiles. "
            "Install with: pip install geopandas"
        ) from exc
    gdf = gpd.read_file(path)
    return pd.DataFrame(gdf.drop(columns="geometry", errors="ignore"))


def build_georgia_pipeline(
    data_dir: Path,
    *,
    schema_registry: SchemaRegistry | None = None,
    max_contract_rows: int | None = None,
) -> GraphPipeline:
    """Build a complete Georgia graph from Snakefile outputs.

    Parameters
    ----------
    data_dir:
        Directory containing Snakefile outputs (e.g. ``raw_data/``).
    schema_registry:
        Optional schema registry for compatibility validation.
    max_contract_rows:
        Limit contract rows loaded (useful for development/testing).

    Returns
    -------
    GraphPipeline ready to call :meth:`build_context` or :meth:`build_star`.

    Raises
    ------
    FileNotFoundError
        If required data files are missing. Run ``snakemake georgia_dev``
        to download them.
    """
    data_dir = Path(data_dir)
    pipeline = GraphPipeline(schema_registry=schema_registry)

    # --- Validate required files ---
    required_files = {
        "contracts": data_dir / f"contracts_state_{GEORGIA_FIPS}.csv",
        "crosswalk": data_dir / "state_county_tract_puma.csv",
    }
    # Shapefiles are optional — pipeline works with just CSV data
    optional_shapefiles = {
        "tracts": data_dir / f"tl_2025_{GEORGIA_FIPS}_tract.shp",
        "block_groups": data_dir / f"tl_2024_{GEORGIA_FIPS}_bg.shp",
        "pumas": data_dir / f"tl_2024_{GEORGIA_FIPS}_puma20.shp",
    }

    missing = [name for name, path in required_files.items() if not path.exists()]
    if missing:
        raise FileNotFoundError(
            f"Missing required data files: {missing}. "
            f"Run 'snakemake --cores 4 georgia_dev' from packages/fastopendata/ "
            f"to download them."
        )

    # --- Load entities ---
    _logger.info("Loading Georgia contracts...")
    read_kwargs: dict[str, Any] = {"dtype": str}
    if max_contract_rows is not None:
        read_kwargs["nrows"] = max_contract_rows
    # Only load columns we need if they exist in the file
    contracts = pd.read_csv(required_files["contracts"], **read_kwargs)
    # Convert obligation to float for aggregation queries
    if "federal_action_obligation" in contracts.columns:
        contracts["federal_action_obligation"] = pd.to_numeric(
            contracts["federal_action_obligation"], errors="coerce"
        )
    pipeline.add_entity_dataframe(
        "Contract", contracts, id_col="contract_transaction_unique_key"
    )
    _logger.info("Loaded %d contracts", len(contracts))

    _logger.info("Loading tract-PUMA crosswalk...")
    crosswalk = pd.read_csv(
        required_files["crosswalk"],
        dtype=str,
        usecols=lambda c: c in CROSSWALK_COLUMNS,
    )
    ga_crosswalk = crosswalk[crosswalk["STATEFP"] == GEORGIA_FIPS]
    _logger.info("Filtered crosswalk to %d Georgia rows", len(ga_crosswalk))

    # Create a synthetic State entity for Georgia (from crosswalk data)
    state_df = pd.DataFrame({
        "STATEFP": [GEORGIA_FIPS],
        "STUSPS": ["GA"],
        "NAME": ["Georgia"],
    })
    pipeline.add_entity_dataframe("State", state_df, id_col="STATEFP")

    # --- Load shapefiles (optional) ---
    tracts_df = None
    bg_df = None

    if optional_shapefiles["tracts"].exists():
        _logger.info("Loading tract shapefiles...")
        tracts_df = _read_shapefile_as_dataframe(optional_shapefiles["tracts"])
        pipeline.add_entity_dataframe("CensusTract", tracts_df, id_col="GEOID")
        _logger.info("Loaded %d census tracts", len(tracts_df))

    if optional_shapefiles["block_groups"].exists():
        _logger.info("Loading block group shapefiles...")
        bg_df = _read_shapefile_as_dataframe(optional_shapefiles["block_groups"])
        pipeline.add_entity_dataframe("BlockGroup", bg_df, id_col="GEOID")
        _logger.info("Loaded %d block groups", len(bg_df))

    if optional_shapefiles["pumas"].exists():
        _logger.info("Loading PUMA shapefiles...")
        puma_df = _read_shapefile_as_dataframe(optional_shapefiles["pumas"])
        pipeline.add_entity_dataframe("Puma", puma_df, id_col="PUMACE20")
        _logger.info("Loaded %d PUMAs", len(puma_df))

    # --- Derive relationships ---
    _logger.info("Deriving relationships...")

    # Contract → State (place of performance)
    pop_edges = derive_contract_state_relationships(
        contracts, state_df,
        fips_column="prime_award_transaction_place_of_performance_state_fips_code",
    )
    if not pop_edges.empty:
        pipeline.add_relationship_dataframe(
            "PERFORMED_IN_STATE", pop_edges,
            source_col="__SOURCE__", target_col="__TARGET__",
        )
        _logger.info("Derived %d PERFORMED_IN_STATE edges", len(pop_edges))

    # Contract → State (recipient)
    recip_edges = derive_contract_state_relationships(
        contracts, state_df,
        fips_column="prime_award_transaction_recipient_state_fips_code",
    )
    if not recip_edges.empty:
        pipeline.add_relationship_dataframe(
            "AWARDED_IN_STATE", recip_edges,
            source_col="__SOURCE__", target_col="__TARGET__",
        )
        _logger.info("Derived %d AWARDED_IN_STATE edges", len(recip_edges))

    # Tract → Puma (via crosswalk)
    tract_puma_edges = derive_tract_puma_relationships(
        ga_crosswalk, state_fips=GEORGIA_FIPS
    )
    if not tract_puma_edges.empty:
        pipeline.add_relationship_dataframe(
            "MAPS_TO_PUMA", tract_puma_edges,
            source_col="__SOURCE__", target_col="__TARGET__",
        )
        _logger.info("Derived %d MAPS_TO_PUMA edges", len(tract_puma_edges))

    # Tract → State
    if tracts_df is not None:
        ts_edges = derive_tract_state_relationships(tracts_df, state_df)
        if not ts_edges.empty:
            pipeline.add_relationship_dataframe(
                "IN_STATE", ts_edges,
                source_col="__SOURCE__", target_col="__TARGET__",
            )
            _logger.info("Derived %d IN_STATE edges", len(ts_edges))

    # Tract → BlockGroup
    if tracts_df is not None and bg_df is not None:
        tbg_edges = derive_tract_block_group_relationships(tracts_df, bg_df)
        if not tbg_edges.empty:
            pipeline.add_relationship_dataframe(
                "CONTAINS_BLOCK_GROUP", tbg_edges,
                source_col="__SOURCE__", target_col="__TARGET__",
            )
            _logger.info("Derived %d CONTAINS_BLOCK_GROUP edges", len(tbg_edges))

    _logger.info(
        "Georgia pipeline complete: %d entity types, %d relationship types",
        len(pipeline.entity_types),
        len(pipeline.relationship_types),
    )
    return pipeline
