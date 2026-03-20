"""Concatenate per-state PUMA shapefiles into a single national file.

Reads all *.shp files whose name contains "puma" from $DATA_DIR and
writes $DATA_DIR/puma_combined.shp.

Columns retained: PUMA20, GEOID, geometry.

Usage:
    DATA_DIR=/path/to/raw_data uv run python concatenate_puma_shape_files.py
"""

import os

from shared.logger import LOGGER

from fastopendata.processing.concatenate_shape_files import (
    concatenate_shapefiles,
)

if __name__ == "__main__":
    DATA_DIR: str = os.environ["DATA_DIR"]
    puma_files = [
        f
        for f in os.listdir(DATA_DIR)
        if "puma" in f.lower() and f.endswith(".shp")
    ]
    LOGGER.info("Found %d PUMA shapefiles.", len(puma_files))
    concatenate_shapefiles(
        puma_files,
        DATA_DIR,
        os.path.join(DATA_DIR, "puma_combined.shp"),
        columns=["PUMA20", "GEOID", "geometry"],
    )
