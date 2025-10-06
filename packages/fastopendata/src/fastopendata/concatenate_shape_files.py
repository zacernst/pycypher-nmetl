"""
Concatenate shapefiles into one file.
"""

import os
from typing import List, Optional

import geopandas
import pandas as pd
import pyogrio
from shared.logger import LOGGER


def concatenate_shapefiles(
    file_list: List[str],
    file_directory: str,
    output_file: str,
    columns: Optional[List[str]] = None,
):
    LOGGER.info("Concatenating shapefiles...")
    columns = columns or []
    df_list = []
    for shapefile in file_list:
        path = f"{file_directory}/{shapefile}"
        LOGGER.info("Reading %s...", path)
        if columns:
            tmp_df = pyogrio.read_dataframe(path, columns=columns)
        else:
            tmp_df = pyogrio.read_dataframe(path)
        df_list.append(tmp_df)
    LOGGER.info("Concatenating in memory...")
    df = pd.concat(df_list)
    LOGGER.info(f"Writing concatenated shapefile... {output_file}")
    pyogrio.write_dataframe(df, f"{output_file}")
    LOGGER.info("Done.")


if __name__ == "__main__":
    DATA_DIR = (
        "/Users/zernst/git/pycypher-nmetl/packages/fastopendata/raw_data"
    )
    SHAPE_FILES = [
        filename
        for filename in os.listdir(DATA_DIR)
        if filename.endswith(".shp")
    ]
    LOGGER.info("Concatenating shapefiles...")
    concatenate_shapefiles(
        SHAPE_FILES,
        DATA_DIR,
        DATA_DIR + "/combined.shp",
        columns=["BLKGRPCE", "GEOID", "geometry"],
    )
