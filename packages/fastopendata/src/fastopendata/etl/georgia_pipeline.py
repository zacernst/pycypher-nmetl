"""Georgia development dataset ETL pipeline (compatibility shim).

The full single-state implementation now lives in
:mod:`fastopendata.etl.state_pipeline` and works for any state by FIPS
code. This module is preserved as a thin alias so existing callers and
tests that imported ``build_georgia_pipeline`` keep working.

Usage::

    from fastopendata.etl.georgia_pipeline import build_georgia_pipeline

    pipeline = build_georgia_pipeline(Path("raw_data"))

    # Equivalent to:
    from fastopendata.etl.state_pipeline import build_state_pipeline
    pipeline = build_state_pipeline(Path("raw_data"), state_fips="13")

Prerequisites:
    Run ``make fod-data-sample`` (or ``snakemake --cores 4 sample_data``
    from ``packages/fastopendata/``) to download and process the sample
    dataset, which includes Georgia by default.

.. versionadded:: 0.0.20
.. versionchanged:: 0.0.21
   ``build_georgia_pipeline`` now delegates to
   :func:`fastopendata.etl.state_pipeline.build_state_pipeline`.
"""

from __future__ import annotations

from pathlib import Path

from fastopendata.etl.state_pipeline import build_state_pipeline
from fastopendata.pipeline import GraphPipeline
from fastopendata.schema_evolution.registry import SchemaRegistry

GEORGIA_FIPS = "13"


def build_georgia_pipeline(
    data_dir: Path,
    *,
    schema_registry: SchemaRegistry | None = None,
    max_contract_rows: int | None = None,
) -> GraphPipeline:
    """Build a complete Georgia graph from Snakefile outputs.

    Thin compatibility wrapper around
    :func:`fastopendata.etl.state_pipeline.build_state_pipeline` with
    ``state_fips=GEORGIA_FIPS``. Preserved for backwards compatibility
    with callers that imported ``build_georgia_pipeline`` directly.

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
        If required data files are missing. Run ``make fod-data-sample``
        to download them.
    """
    return build_state_pipeline(
        data_dir,
        state_fips=GEORGIA_FIPS,
        schema_registry=schema_registry,
        max_contract_rows=max_contract_rows,
    )
