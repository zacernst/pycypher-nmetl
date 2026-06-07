import json
import base64
import collections
import pandas as pd

df = pd.read_parquet(
    "/Users/zernst/scratch/united_states_nodes_tract_crosswalk.parquet"
)

tally = collections.defaultdict(int)
counter = 0
for v in df["encoded_tags"]:
    counter += 1
    if counter % 1000 == 0:
        print(f"Counter: {counter}")
    d = json.loads(base64.b64decode(v))
    for key, value in d.items():
        tally[
            (
                key,
                value,
            )
        ] += 1
