import sys

import pandas as pd

input_file = sys.argv[1]
output_file = sys.argv[2]

df = pd.read_csv(input_file, dtype={'STATEFP': str, 'COUNTYFP': str,
                                     'TRACTCE': str, 'PUMA5CE': str})
df['STATE_FIPS'] = df['STATEFP']
df['COUNTY_FIPS'] = df.apply(lambda row: row['STATE_FIPS'] + row['COUNTYFP'],
                             axis=1)
df['TRACT_FIPS'] = df.apply(lambda row: row['COUNTY_FIPS'] + row['TRACTCE'],
                             axis=1)
df['PUMA_FIPS'] = df.apply(lambda row: row['STATE_FIPS'] + row['PUMA5CE'],
                             axis=1)

df.to_csv(output_file, index=False)
