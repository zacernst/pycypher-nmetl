"""Filter a compressed Wikidata entity file to points inside the United States.

Reads $DATA_DIR/wikidata_compressed.json.bz2 (produced upstream from the
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
import signal
import sys
from pathlib import Path
from typing import Any

import geopandas as gpd
from rich.progress import Progress
from shapely.geometry import Point

from fastopendata.config import config

DATA_DIR: Path = config.data_path
INPUT_FILE: str = str(DATA_DIR / "wikidata_compressed.json.bz2")
STATE_SHAPEFILE: str = str(DATA_DIR / "tl_2024_us_state.shp")
OUTPUT_FILE: Path = DATA_DIR / "wikidata_us_points.json"

# Number of worker processes for the point-in-polygon tests.
_NUM_WORKERS: int = 10

# Separator used to pack counter into the queue message without a second
# queue slot; avoids serialisation overhead of a tuple.
_SEP: bytes = b"::::::::::::::::"


def _load_state_gdf() -> gpd.GeoDataFrame:
    return gpd.read_file(STATE_SHAPEFILE)


def _in_us(gdf: gpd.GeoDataFrame, longitude: float, latitude: float) -> bool:
    point = Point(longitude, latitude)
    candidates = gdf.sindex.query(point, predicate="intersects")
    return len(candidates) > 0


def _reader(jobs_queue: mp.Queue) -> None:
    """Read bz2-compressed entities and push them onto the jobs queue."""
    with bz2.open(INPUT_FILE) as f:
        for counter, raw in enumerate(f):
            jobs_queue.put(raw.rstrip(b",\n") + _SEP + str(counter).encode())


def _worker(
    jobs_queue: mp.Queue,
    write_queue: mp.Queue,
) -> None:
    """Test each entity's coordinates against U.S. boundaries."""
    gdf = _load_state_gdf()
    while True:
        item = jobs_queue.get()
        if item is None:
            write_queue.put(None)
            return
        try:
            raw, counter_bytes = item.split(_SEP, 1)
            counter = int(counter_bytes)
            entity: dict[str, Any] = json.loads(raw)
        except (ValueError, json.JSONDecodeError):
            # Skip malformed lines rather than crashing the worker
            continue
        try:
            loc = entity["claims"]["P625"][0]["mainsnak"]["datavalue"]["value"]
        except (KeyError, IndexError):
            continue
        entity["_counter"] = counter
        if _in_us(gdf, loc["longitude"], loc["latitude"]):
            write_queue.put(entity)


def _writer(write_queue: mp.Queue) -> None:
    """Collect results and write them to the output file."""
    finished_workers = 0
    with (
        OUTPUT_FILE.open("w", encoding="utf-8") as out,
        Progress() as progress,
    ):
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
    for required, label in [
        (INPUT_FILE, "Wikidata compressed input"),
        (STATE_SHAPEFILE, "State boundaries shapefile"),
    ]:
        if not os.path.isfile(required):
            msg = f"{label} not found: {required}"
            raise SystemExit(msg)

    jobs: mp.Queue = mp.Queue()
    results: mp.Queue = mp.Queue()

    reader_proc = mp.Process(
        target=_reader, args=(jobs,), name="wikidata-reader"
    )
    worker_procs = [
        mp.Process(
            target=_worker, args=(jobs, results), name=f"wikidata-worker-{i}"
        )
        for i in range(_NUM_WORKERS)
    ]
    writer_proc = mp.Process(
        target=_writer, args=(results,), name="wikidata-writer"
    )

    all_procs = [reader_proc, *worker_procs, writer_proc]

    def _terminate_all(_signum: int, _frame: Any) -> None:
        """Clean up child processes on SIGINT/SIGTERM."""
        for p in all_procs:
            if p.is_alive():
                p.terminate()
        sys.exit(1)

    signal.signal(signal.SIGINT, _terminate_all)
    signal.signal(signal.SIGTERM, _terminate_all)

    try:
        reader_proc.start()
        for wp in worker_procs:
            wp.start()
        writer_proc.start()

        reader_proc.join()
        if reader_proc.exitcode != 0:
            msg = (
                f"Reader process failed with exit code {reader_proc.exitcode}"
            )
            raise RuntimeError(msg)

        # Signal each worker to stop.
        for _ in worker_procs:
            jobs.put(None)
        for wp in worker_procs:
            wp.join()
            if wp.exitcode != 0:
                msg = f"Worker {wp.name} failed with exit code {wp.exitcode}"
                raise RuntimeError(msg)

        writer_proc.join()
        if writer_proc.exitcode != 0:
            msg = (
                f"Writer process failed with exit code {writer_proc.exitcode}"
            )
            raise RuntimeError(msg)

    except Exception:
        for p in all_procs:
            if p.is_alive():
                p.terminate()
                p.join(timeout=5)
        raise

    # Validate output is non-empty.
    if not OUTPUT_FILE.exists() or OUTPUT_FILE.stat().st_size == 0:
        msg = f"Output file is empty or missing: {OUTPUT_FILE}"
        raise SystemExit(msg)
