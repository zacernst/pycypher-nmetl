"""Filter a compressed Wikidata entity file to points inside the United States.

Reads $DATA_DIR/location_entities.json.bz2 (produced upstream from the
Wikidata dump), checks each entity's P625 coordinate claim against U.S.
state boundary polygons, and writes matching entities to
$DATA_DIR/wikidata_us_points.json.

The point-in-polygon test uses geopandas + shapely against
$DATA_DIR/tl_2024_us_state.shp (TIGER/Line 2024 state boundaries).

The workload is distributed across multiple worker processes via
multiprocessing queues; the writer process collects results and serialises
them to disk.

Usage:
    DATA_DIR=/path/to/raw_data uv run python filter_us_nodes.py
"""

import bz2
import json
import multiprocessing as mp
import os
from typing import Any

import geopandas as gpd
from rich.progress import Progress
from shapely.geometry import Point

DATA_DIR: str = os.environ["DATA_DIR"]
INPUT_FILE: str = os.path.join(DATA_DIR, "location_entities.json.bz2")
STATE_SHAPEFILE: str = os.path.join(DATA_DIR, "tl_2024_us_state.shp")
OUTPUT_FILE: str = os.path.join(DATA_DIR, "wikidata_us_points.json")

# Number of worker processes for the point-in-polygon tests.
_NUM_WORKERS: int = 10

# Separator used to pack counter into the queue message without a second
# queue slot; avoids serialisation overhead of a tuple.
_SEP: bytes = b"::::::::::::::::"


def _load_state_gdf() -> gpd.GeoDataFrame:
    return gpd.read_file(STATE_SHAPEFILE)


def _in_us(gdf: gpd.GeoDataFrame, longitude: float, latitude: float) -> bool:
    point = Point(longitude, latitude)
    return any(row.geometry.contains(point) for _, row in gdf.iterrows())


def _reader(jobs_queue: mp.Queue[bytes | None]) -> None:
    """Read bz2-compressed entities and push them onto the jobs queue."""
    with bz2.open(INPUT_FILE) as f:
        for counter, raw in enumerate(f):
            jobs_queue.put(raw.rstrip(b",\n") + _SEP + str(counter).encode())


def _worker(
    jobs_queue: mp.Queue[bytes | None],
    write_queue: mp.Queue[dict[str, Any] | None],
) -> None:
    """Test each entity's coordinates against U.S. boundaries."""
    gdf = _load_state_gdf()
    while True:
        item = jobs_queue.get()
        if item is None:
            write_queue.put(None)
            return
        raw, counter_bytes = item.split(_SEP, 1)
        counter = int(counter_bytes)
        entity: dict[str, Any] = json.loads(raw)
        try:
            loc = entity["claims"]["P625"][0]["mainsnak"]["datavalue"]["value"]
        except (KeyError, IndexError):
            continue
        entity["_counter"] = counter
        if _in_us(gdf, loc["longitude"], loc["latitude"]):
            write_queue.put(entity)


def _writer(write_queue: mp.Queue[dict[str, Any] | None]) -> None:
    """Collect results and write them to the output file."""
    finished_workers = 0
    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        with Progress() as progress:
            task = progress.add_task(
                "[cyan]Filtering to U.S. points...",
                total=11_514_545,
            )
            while finished_workers < _NUM_WORKERS:
                item = write_queue.get()
                if item is None:
                    finished_workers += 1
                    continue
                out.write(json.dumps(item) + "\n")
                progress.update(task, completed=item["_counter"])


if __name__ == "__main__":
    jobs: mp.Queue[bytes | None] = mp.Queue()
    results: mp.Queue[dict[str, Any] | None] = mp.Queue()

    reader_proc = mp.Process(target=_reader, args=(jobs,))
    worker_procs = [
        mp.Process(target=_worker, args=(jobs, results))
        for _ in range(_NUM_WORKERS)
    ]
    writer_proc = mp.Process(target=_writer, args=(results,))

    reader_proc.start()
    for wp in worker_procs:
        wp.start()
    writer_proc.start()

    reader_proc.join()
    # Signal each worker to stop.
    for _ in worker_procs:
        jobs.put(None)
    for wp in worker_procs:
        wp.join()
    writer_proc.join()
