"""Extract OSM point data to CSV file."""

import base64
import csv
import datetime
import os
import pickle

import osmium
from shared.logger import LOGGER

MAX_NODES = -1
FOD_UNITED_STATES_NODES_FILE = os.environ["DATA_DIR"] + '/us-latest.osm.pbf'


class NodeHandler(osmium.SimpleHandler):
    def __init__(self):
        super(NodeHandler, self).__init__()
        self.start_time = datetime.datetime.now()
        self.node_count = 0
        self.filtered_node_count = 0
        self.file = open(os.environ['DATA_DIR'] + '/united_states_nodes.csv', "w", encoding="utf8")
        self.writer = csv.DictWriter(
            self.file,
            fieldnames=[
                "longitude",
                "latitude",
                "encoded_tags",
                "id",
            ],
        )
        self.trivial_tags = set(
            [
                "access",
                "access_ref",
                "alt_name",
                "attribution",
                "barrier",
                "brand",
                "created_by",
                "direction",
                "ele",
                "fixme",
                "FIXME",
                "gnis",
                "highway",
                "image_direction",
                "is_in",
                "kerb",
                "name",
                "noexit",
                "noref",
                "note",
                "odbl",
                "official_name",
                "old_ref",
                "old_name",
                "project",
                "railway",
                "ref",
                "short_name",
                "source",
                "source_ref",
                "survey",
                "tiger",
                "was",
            ]
        )
        self.writer.writeheader()

    def node(self, o):
        """node handler"""
        self.node_count += 1
        if self.node_count % 100000 == 0:
            LOGGER.info(  # pylint: disable=logging-fstring-interpolation
                f"Node count: {self.node_count}"
                f"  Filtered: {self.filtered_node_count}"
                f"  Nodes per second: {round(self.node_count / (datetime.datetime.now() - self.start_time).seconds, 3)}"
            )
        tag_dict = dict(o.tags)
        for one_tag in list(tag_dict.keys()):
            for tag in self.trivial_tags:
                if one_tag == tag or one_tag.startswith(tag + ":"):
                    del tag_dict[one_tag]
                    break
            else:
                break
        if tag_dict:
            self.filtered_node_count += 1
            if self.filtered_node_count == MAX_NODES:
                os._exit(0)
            encoded = base64.b64encode(pickle.dumps(dict(o.tags))).decode()
            self.writer.writerow(
                {
                    "longitude": o.location.lon,
                    "latitude": o.location.lat,
                    "encoded_tags": encoded,
                    "id": o.id,
                }
            )


if __name__ == "__main__":
    LOGGER.info("Extracting point data from OSM...")
    h = NodeHandler()
    h.apply_file(
        os.environ['DATA_DIR'] + "/us-latest.osm.pbf",
        locations=True,
    )
    h.file.close()
    LOGGER.info("Done. Total nodes processed: %s", h.node_count)
