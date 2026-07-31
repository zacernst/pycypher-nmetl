import sys

import pandas as pd

input_file = sys.argv[1]
output_file = sys.argv[2]

df = pd.read_csv(input_file,
                  dtype={'STATE': str, 'COUNTY': str, 'TRACT': str, 'CBSA': str},
                  encoding='latin-1')
df['TRACT_FIPS'] = df.apply(lambda row: row['STATE'] + row['COUNTY'] + row['TRACT'],
                             axis=1)

df.to_csv(output_file, index=False)
