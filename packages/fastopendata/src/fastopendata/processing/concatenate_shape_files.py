"""Concatenate a set of shapefiles into a single output shapefile.

Used to merge the per-state TIGER/Line block-group shapefiles
(tl_2024_*_bg.shp) into one national file: $DATA_DIR/combined.shp.

Columns retained: BLKGRPCE, GEOID, geometry.

Usage:
    DATA_DIR=/path/to/raw_data uv run python concatenate_shape_files.py
"""

import os

import pandas as pd
import pyogrio
from shared.logger import LOGGER


def concatenate_shapefiles(
    file_list: list[str],
    file_directory: str,
    output_file: str,
    columns: list[str] | None = None,
) -> None:
    """Read *file_list* from *file_directory* and write a merged shapefile.

    Args:
        file_list: Basenames of the shapefiles to merge.
        file_directory: Directory that contains them.
        output_file: Absolute path for the merged output shapefile.
        columns: Optional list of columns to retain (geometry is always kept).

    """
    LOGGER.info("Concatenating %d shapefiles...", len(file_list))
    frames = []
    for name in file_list:
        path = os.path.join(file_directory, name)
        LOGGER.info("Reading %s ...", path)
        frames.append(
            pyogrio.read_dataframe(path, columns=columns)
            if columns
            else pyogrio.read_dataframe(path),
        )
    merged = pd.concat(frames, ignore_index=True)
    LOGGER.info("Writing merged shapefile to %s ...", output_file)
    pyogrio.write_dataframe(merged, output_file)
    LOGGER.info("Done.")


if __name__ == "__main__":
    DATA_DIR: str = os.environ["DATA_DIR"]
    shapefiles = [f for f in os.listdir(DATA_DIR) if f.endswith(".shp")]
    concatenate_shapefiles(
        shapefiles,
        DATA_DIR,
        os.path.join(DATA_DIR, "combined.shp"),
        columns=["BLKGRPCE", "GEOID", "geometry"],
    )
