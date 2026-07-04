import sys

import geopandas as gpd
import pandas as pd
from shared.logger import LOGGER

LOGGER.setLevel('INFO')

data_dir = sys.argv[1]


tract_file = f'{data_dir}/tract_combined.shp'
united_states_nodes_file = f'{data_dir}/united_states_nodes.parquet'


LOGGER.info('Reading tract table...')
tract_table = gpd.read_file(tract_file)

LOGGER.info('Reading US nodes Parquet file...')
us_nodes_df = pd.read_parquet(united_states_nodes_file)

LOGGER.info('Converting to GeoPandas...')
# 2. Convert to a GeoDataFrame
us_nodes_gdf = gpd.GeoDataFrame(
    us_nodes_df,
    geometry=gpd.points_from_xy(us_nodes_df.longitude, us_nodes_df.latitude),
    crs="EPSG:4269",
)

LOGGER.info('Spatial join...')
joined_df = gpd.sjoin(us_nodes_gdf, tract_table)

LOGGER.info('Saving...')
joined_df.drop(columns="geometry").to_parquet(
    f"{data_dir}/united_states_nodes_tract_crosswalk.parquet",
    index=False,
    row_group_size=5_000
)

sys.exit(0)

