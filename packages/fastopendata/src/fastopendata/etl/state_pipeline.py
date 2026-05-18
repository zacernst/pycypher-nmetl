"""Parameterized single-state ETL pipeline.

Builds a complete pycypher graph (entities + relationships) for any US
state by FIPS code. The Snakemake outputs already use ``{state_fips}``
in their file names; this module wires the Python side to match so any
state with downloaded data can flow into a graph without per-state
hardcoding.

Usage::

    from fastopendata.etl.state_pipeline import build_state_pipeline

    # Georgia
    pipeline = build_state_pipeline(Path("raw_data"), state_fips="13")

    # California
    pipeline = build_state_pipeline(Path("raw_data"), state_fips="06")

The ``build_georgia_pipeline`` shim in
:mod:`fastopendata.etl.georgia_pipeline` is preserved as a backwards-
compatible alias and forwards to this function.

Required files (per state)
--------------------------
- ``contracts_state_{fips}.csv`` — extracted via Snakemake
- ``state_county_tract_puma.csv`` — shared across all states

Optional files (loaded if present)
----------------------------------
- ``tl_2025_{fips}_tract.shp`` — TIGER census tracts
- ``tl_2024_{fips}_bg.shp`` — TIGER block groups
- ``tl_2024_{fips}_puma20.shp`` — TIGER PUMAs

.. versionadded:: 0.0.20
.. versionchanged:: 0.0.21
   Generalized to any FIPS — no longer special-cases Georgia.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd

from fastopendata.etl.relationship_derivation import (
    derive_contract_state_relationships,
    derive_tract_block_group_relationships,
    derive_tract_puma_relationships,
    derive_tract_state_relationships,
)
from fastopendata.etl.schemas import CROSSWALK_COLUMNS
from fastopendata.pipeline import GraphPipeline
from fastopendata.schema_evolution.registry import SchemaRegistry

_logger = logging.getLogger(__name__)

_FIPS_RE = re.compile(r"^\d{2}\Z")

# State FIPS → (abbreviation, name) for synthetic State entity creation.
# Source: https://www.census.gov/library/reference/code-lists/ansi.html
_STATE_INFO: dict[str, tuple[str, str]] = {
    "01": ("AL", "Alabama"),
    "02": ("AK", "Alaska"),
    "04": ("AZ", "Arizona"),
    "05": ("AR", "Arkansas"),
    "06": ("CA", "California"),
    "08": ("CO", "Colorado"),
    "09": ("CT", "Connecticut"),
    "10": ("DE", "Delaware"),
    "11": ("DC", "District of Columbia"),
    "12": ("FL", "Florida"),
    "13": ("GA", "Georgia"),
    "15": ("HI", "Hawaii"),
    "16": ("ID", "Idaho"),
    "17": ("IL", "Illinois"),
    "18": ("IN", "Indiana"),
    "19": ("IA", "Iowa"),
    "20": ("KS", "Kansas"),
    "21": ("KY", "Kentucky"),
    "22": ("LA", "Louisiana"),
    "23": ("ME", "Maine"),
    "24": ("MD", "Maryland"),
    "25": ("MA", "Massachusetts"),
    "26": ("MI", "Michigan"),
    "27": ("MN", "Minnesota"),
    "28": ("MS", "Mississippi"),
    "29": ("MO", "Missouri"),
    "30": ("MT", "Montana"),
    "31": ("NE", "Nebraska"),
    "32": ("NV", "Nevada"),
    "33": ("NH", "New Hampshire"),
    "34": ("NJ", "New Jersey"),
    "35": ("NM", "New Mexico"),
    "36": ("NY", "New York"),
    "37": ("NC", "North Carolina"),
    "38": ("ND", "North Dakota"),
    "39": ("OH", "Ohio"),
    "40": ("OK", "Oklahoma"),
    "41": ("OR", "Oregon"),
    "42": ("PA", "Pennsylvania"),
    "44": ("RI", "Rhode Island"),
    "45": ("SC", "South Carolina"),
    "46": ("SD", "South Dakota"),
    "47": ("TN", "Tennessee"),
    "48": ("TX", "Texas"),
    "49": ("UT", "Utah"),
    "50": ("VT", "Vermont"),
    "51": ("VA", "Virginia"),
    "53": ("WA", "Washington"),
    "54": ("WV", "West Virginia"),
    "55": ("WI", "Wisconsin"),
    "56": ("WY", "Wyoming"),
}


def state_label(state_fips: str) -> tuple[str, str]:
    """Return ``(abbrev, name)`` for the given FIPS, with sensible fallbacks.

    Unknown FIPS codes get ``("FIPS{xx}", "FIPS {xx}")`` rather than raising —
    we'd rather build a graph with a synthetic label than refuse work.

    Examples
    --------
    >>> state_label("13")
    ('GA', 'Georgia')
    >>> state_label("06")
    ('CA', 'California')
    >>> state_label("99")  # not a real FIPS
    ('FIPS99', 'FIPS 99')
    """
    return _STATE_INFO.get(
        state_fips,
        (f"FIPS{state_fips}", f"FIPS {state_fips}"),
    )


# Backwards-compatible private alias (kept in case anything inside the
# package imported the underscored name).
_state_label = state_label


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


def build_state_pipeline(
    data_dir: Path,
    *,
    state_fips: str,
    schema_registry: SchemaRegistry | None = None,
    max_contract_rows: int | None = None,
) -> GraphPipeline:
    """Build a complete graph pipeline for any state by FIPS code.

    Loads contracts + crosswalk (required) and tract / block-group / PUMA
    shapefiles (optional), then derives all available relationships:

    - ``PERFORMED_IN_STATE`` — Contract → State (place of performance)
    - ``AWARDED_IN_STATE`` — Contract → State (recipient)
    - ``MAPS_TO_PUMA`` — CensusTract → Puma (via crosswalk)
    - ``IN_STATE`` — CensusTract → State (when shapefile present)
    - ``CONTAINS_BLOCK_GROUP`` — CensusTract → BlockGroup (when shapefiles present)

    Parameters
    ----------
    data_dir:
        Directory containing Snakefile outputs.
    state_fips:
        Two-digit state FIPS code (e.g. ``"13"`` for Georgia, ``"06"`` for
        California).
    schema_registry:
        Optional schema registry for compatibility validation.
    max_contract_rows:
        Limit contract rows loaded (useful for development/testing).

    Returns
    -------
    GraphPipeline ready for :meth:`build_context` or :meth:`build_star`.

    Raises
    ------
    ValueError
        If ``state_fips`` is not a valid 2-digit code.
    FileNotFoundError
        If required data files are missing for this state. The error
        message names the FIPS code so the caller can run
        ``STATE_FIPS={fips} snakemake --cores 4 raw_data/contracts_state_{fips}.csv``.
    """
    if not _FIPS_RE.match(state_fips):
        raise ValueError(
            f"Invalid state FIPS code {state_fips!r}: must be exactly "
            f"two digits (e.g. '13' for Georgia, '06' for California)"
        )

    data_dir = Path(data_dir)
    abbrev, state_name = state_label(state_fips)
    pipeline = GraphPipeline(schema_registry=schema_registry)

    # --- Validate required files ---
    required_files = {
        "contracts": data_dir / f"contracts_state_{state_fips}.csv",
        "crosswalk": data_dir / "state_county_tract_puma.csv",
    }
    optional_shapefiles = {
        "tracts":       data_dir / f"tl_2025_{state_fips}_tract.shp",
        "block_groups": data_dir / f"tl_2024_{state_fips}_bg.shp",
        "pumas":        data_dir / f"tl_2024_{state_fips}_puma20.shp",
    }

    missing = [name for name, path in required_files.items() if not path.exists()]
    if missing:
        raise FileNotFoundError(
            f"Missing required data files for {state_name} (FIPS {state_fips}): "
            f"{missing}. Run "
            f"'STATE_FIPS={state_fips} snakemake --cores 4 "
            f"raw_data/contracts_state_{state_fips}.csv "
            f"raw_data/state_county_tract_puma.csv' from "
            f"packages/fastopendata/ to download them."
        )

    # --- Load entities ---
    _logger.info("Loading %s contracts...", state_name)
    read_kwargs: dict[str, Any] = {"dtype": str}
    if max_contract_rows is not None:
        read_kwargs["nrows"] = max_contract_rows
    contracts = pd.read_csv(required_files["contracts"], **read_kwargs)
    # Convert obligation to float for aggregation queries
    if "federal_action_obligation" in contracts.columns:
        contracts["federal_action_obligation"] = pd.to_numeric(
            contracts["federal_action_obligation"], errors="coerce"
        )
    pipeline.add_entity_dataframe(
        "Contract", contracts, id_col="contract_transaction_unique_key"
    )
    _logger.info("Loaded %d contracts for %s", len(contracts), state_name)

    _logger.info("Loading tract-PUMA crosswalk...")
    crosswalk = pd.read_csv(
        required_files["crosswalk"],
        dtype=str,
        usecols=lambda c: c in CROSSWALK_COLUMNS,
    )
    state_crosswalk = crosswalk[crosswalk["STATEFP"] == state_fips]
    _logger.info(
        "Filtered crosswalk to %d %s rows", len(state_crosswalk), state_name,
    )

    # Synthetic State entity from _STATE_INFO (or fallback label).
    state_df = pd.DataFrame({
        "STATEFP": [state_fips],
        "STUSPS":  [abbrev],
        "NAME":    [state_name],
    })
    pipeline.add_entity_dataframe("State", state_df, id_col="STATEFP")

    # --- Load shapefiles (optional) ---
    tracts_df: pd.DataFrame | None = None
    bg_df: pd.DataFrame | None = None

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
    _logger.info("Deriving relationships for %s...", state_name)

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

    # Tract → Puma (via crosswalk; per-state filter handled by the helper)
    tract_puma_edges = derive_tract_puma_relationships(
        state_crosswalk, state_fips=state_fips,
    )
    if not tract_puma_edges.empty:
        pipeline.add_relationship_dataframe(
            "MAPS_TO_PUMA", tract_puma_edges,
            source_col="__SOURCE__", target_col="__TARGET__",
        )
        _logger.info("Derived %d MAPS_TO_PUMA edges", len(tract_puma_edges))

    # Tract → State (only if tracts shapefile loaded)
    if tracts_df is not None:
        ts_edges = derive_tract_state_relationships(tracts_df, state_df)
        if not ts_edges.empty:
            pipeline.add_relationship_dataframe(
                "IN_STATE", ts_edges,
                source_col="__SOURCE__", target_col="__TARGET__",
            )
            _logger.info("Derived %d IN_STATE edges", len(ts_edges))

    # Tract → BlockGroup (only if both shapefiles loaded)
    if tracts_df is not None and bg_df is not None:
        tbg_edges = derive_tract_block_group_relationships(tracts_df, bg_df)
        if not tbg_edges.empty:
            pipeline.add_relationship_dataframe(
                "CONTAINS_BLOCK_GROUP", tbg_edges,
                source_col="__SOURCE__", target_col="__TARGET__",
            )
            _logger.info(
                "Derived %d CONTAINS_BLOCK_GROUP edges", len(tbg_edges),
            )

    _logger.info(
        "%s pipeline complete: %d entity types, %d relationship types",
        state_name,
        len(pipeline.entity_types),
        len(pipeline.relationship_types),
    )
    return pipeline
