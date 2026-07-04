import csv
import base64
import json
import collections
import rich

entities = collections.defaultdict(int)
with open('/run/media/zac/2tb/fastopendata/data/united_states_nodes.csv', 'r') as f:

    for row in csv.DictReader(f):

        tags = row['encoded_tags']
        d = json.loads(base64.b64decode(tags))
        # print(d)
        if 'amenity' in d:
            entities[d['amenity']] += 1

rich.print(entities)
d = dict(sorted([(key, value,) for key, value in entities.items()], key=lambda x: x[1], reverse=True))
print(d)
