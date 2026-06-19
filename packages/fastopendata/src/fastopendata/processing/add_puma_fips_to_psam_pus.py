import pandas as pd
import sys
# from shared import LOGGER

DATA_DIR = sys.argv[1]

print('reading...')
df = pd.read_parquet(f'{DATA_DIR}/psam_pus_tmp.parquet')
print('applying...')
df['PUMA_FIPS'] = df.apply(lambda row: row['STATE'] + row['PUMA'], axis=1)
print('writing...')
df.to_parquet(f'{DATA_DIR}/psam_pus.parquet')

