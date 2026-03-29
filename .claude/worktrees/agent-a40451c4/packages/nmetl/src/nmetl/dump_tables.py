# Dump tables from FoundationDB back end

import sys
from collections import defaultdict

from pycypher.fact import FactNodeHasAttributeWithValue, FactNodeHasLabel
from pycypher.foundationdb import FoundationDBFactCollection
from rich.progress import Progress

fact_collection: FoundationDBFactCollection = FoundationDBFactCollection()

counter: int = 0
for thing in fact_collection.parallel_read(
    max_keys=None, num_threads=64, increment=5000
):
    counter += 1
    if counter % 1 == 0:
        print(counter, thing)

print(counter, thing)
sys.exit(0)

label_attributes = defaultdict(set)

key = fact_collection.first_key_after(b"", offset=1000)

approx_length = fact_collection.approx_len()

with Progress() as progress:
    task = progress.add_task("iterating", total=approx_length)
    for i, j in fact_collection.enumerate_in_batches():
        if isinstance(j, FactNodeHasLabel):
            pass
        elif isinstance(j, FactNodeHasAttributeWithValue):
            label = j.node_id.split("::")[0]
            label_attributes[label].add(j.attribute)
        else:
            pass
        progress.update(task, advance=1)

print(label_attributes)

# for i in fact_collection.node_has_attribute_with_value_facts():
#     label_set.add(i.attribute)
#     print(i.attribute)
#
#
# print(label_set)

# X = tr.get_range(begin, fdb.KeySelector.first_greater_than(end))
