import base64
import csv
import json


def node_tags():
    with open("./raw_data/united_states_nodes.csv") as f:
        reader = csv.DictReader(f)
        for row in reader:
            encoded_tags = row["encoded_tags"]
            row["decoded_tags"] = json.loads(base64.b64decode(encoded_tags))
            yield row

counter = 0
vet_counter = 0
for row in node_tags():
    counter += 1
    tags = row["decoded_tags"]
    if any("veterinar" in key_value for key_value in tags.values()):
        vet_counter += 1
        print(counter, vet_counter, tags)
