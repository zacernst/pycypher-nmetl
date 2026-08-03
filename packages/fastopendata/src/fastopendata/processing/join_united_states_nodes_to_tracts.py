import sys
import os

import geopandas as gpd
import pandas as pd
from shared.logger import LOGGER

import pyarrow.parquet as pq
import pyarrow as pa

LOGGER.setLevel('INFO')


def batches():
    DATA_DIR = os.environ['DATA_DIR']
    united_states_nodes_file = f'{DATA_DIR}/united_states_nodes.parquet'
    tract_file = f'{DATA_DIR}/tract_combined.shp'

    LOGGER.info('Reading tract table...')
    tract_table = gpd.read_file(tract_file)


    nodes_parquet_file = pq.ParquetFile(united_states_nodes_file)
    for batch in nodes_parquet_file.iter_batches(batch_size=1_000):
        df = batch.to_pandas()
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df.longitude, df.latitude),
            crs="EPSG:4269",
        )
        joined_gdf = gdf.sjoin(tract_table)
        df = pd.DataFrame(joined_gdf)
        yield df

first = True
row_counter = 0
for df in batches():
    df = df.drop(['longitude', 'latitude', 'geometry', 'index_right'], axis=1)
    df.reset_index(drop=True, inplace=True)
    record_batch = pa.record_batch(df)
    row_counter += len(df)
    LOGGER.info(f'join to tract row: {row_counter}')
    if first:
        schema = record_batch.schema
        writer = pq.ParquetWriter('united_states_nodes_tract_crosswalk.parquet', schema)
        writer.write_table(pa.table(df))
        writer.close()
        first = False
        writer = pq.ParquetWriter('united_states_nodes_tract_crosswalk.parquet', schema)
        continue
    writer.write_batch(record_batch)
writer.close()

sys.exit(0)

