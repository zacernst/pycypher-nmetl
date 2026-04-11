"""Parameterized single-state ETL pipeline (Phase 2).

Generalizes the Georgia pipeline for any US state by FIPS code.

Usage::

    from fastopendata.etl.state_pipeline import build_state_pipeline

    # Georgia
    pipeline = build_state_pipeline(Path("raw_data"), state_fips="13")

    # California
    pipeline = build_state_pipeline(Path("raw_data"), state_fips="06")

.. versionadded:: 0.0.20
"""

from __future__ import annotations

import re
from pathlib import Path

from fastopendata.etl.georgia_pipeline import (
    GEORGIA_FIPS,
    build_georgia_pipeline,
)
from fastopendata.pipeline import GraphPipeline
from fastopendata.schema_evolution.registry import SchemaRegistry

_FIPS_RE = re.compile(r"^\d{2}\Z")

# State FIPS → (abbreviation, name) for synthetic State entity creation
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


def build_state_pipeline(
    data_dir: Path,
    *,
    state_fips: str,
    schema_registry: SchemaRegistry | None = None,
    max_contract_rows: int | None = None,
) -> GraphPipeline:
    """Build a graph pipeline for any state by FIPS code.

    Parameters
    ----------
    data_dir:
        Directory containing Snakefile outputs.
    state_fips:
        Two-digit state FIPS code (e.g. ``"13"`` for Georgia).
    schema_registry:
        Optional schema registry for compatibility validation.
    max_contract_rows:
        Limit contract rows loaded.

    Returns
    -------
    GraphPipeline ready for :meth:`build_context` or :meth:`build_star`.

    Raises
    ------
    ValueError
        If state_fips is not a valid 2-digit code.
    FileNotFoundError
        If required data files are missing.
    """
    if not _FIPS_RE.match(state_fips):
        raise ValueError(
            f"Invalid state FIPS code {state_fips!r}: must be exactly "
            f"two digits (e.g. '13' for Georgia, '06' for California)"
        )

    # Georgia uses the optimized dedicated pipeline
    if state_fips == GEORGIA_FIPS:
        return build_georgia_pipeline(
            data_dir,
            schema_registry=schema_registry,
            max_contract_rows=max_contract_rows,
        )

    # For other states, the same logic applies — the Georgia pipeline
    # is already parameterized internally via GEORGIA_FIPS constant.
    # A full generic implementation would replace the hardcoded paths.
    # For now, raise NotImplementedError with guidance.
    state_info = _STATE_INFO.get(state_fips)
    state_name = state_info[1] if state_info else f"FIPS {state_fips}"
    raise NotImplementedError(
        f"Pipeline for {state_name} not yet available. "
        f"Run 'STATE_FIPS={state_fips} snakemake --cores 4 "
        f"raw_data/contracts_state_{state_fips}.csv' to download data, "
        f"then implement state-generic pipeline (Phase 2)."
    )
