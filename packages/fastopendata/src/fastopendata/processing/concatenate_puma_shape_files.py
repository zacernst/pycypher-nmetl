"""Concatenate per-state PUMA shapefiles into a single national file.

Reads all *.shp files whose name contains "puma" from the configured data directory
and writes puma_combined.shp.

Columns retained: PUMA20, GEOID, geometry.

Usage:
    uv run python concatenate_puma_shape_files.py

Configuration:
    Uses centralized configuration from config.toml.
    Data directory can be overridden with DATA_DIR environment variable.
"""

from shared.logger import LOGGER

from fastopendata.config import config
from fastopendata.processing.concatenate_shape_files import (
    concatenate_shapefiles,
)

if __name__ == "__main__":
    # Use centralized configuration (supports DATA_DIR environment override)
    data_dir = config.data_path

    LOGGER.info("Using data directory: %s", data_dir)

    puma_files = [
        f.name
        for f in data_dir.iterdir()
        if "puma" in f.name.lower() and f.suffix == ".shp"
    ]
    LOGGER.info("Found %d PUMA shapefiles.", len(puma_files))

    output_path = config.get_dataset_path("tiger_puma")
    LOGGER.info("Output file: %s", output_path)

    concatenate_shapefiles(
        puma_files,
        data_dir,
        output_path,
        columns=["PUMA20", "GEOID", "geometry"],
    )
