"""Check lat/long against map of the US"""

import bz2
import logging
import multiprocessing as mp

import geopandas as gpd
import json
from rich.progress import Progress
from shapely import Point

LOGGER: logging.Logger = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

gdf = gpd.read_file(
    "/Users/zernst/git/pycypher-nmetl/packages/fastopendata/raw_data/tl_2024_us_state.shp"
)


def in_us(longitude, latitude):
    point = Point(longitude, latitude)

    inside = False
    for index, row in gdf.iterrows():
        is_inside = row.geometry.contains(point)
        if is_inside:
            inside = True
            break
    return inside


def gen_us_points(jobs_queue):
    f: bz2.BZ2File = bz2.open(
        "/Users/zernst/git/pycypher-nmetl/packages/fastopendata/raw_data/location_entities.json.bz2"
    )
    counter = 0
    for line in f:
        line = line.rstrip(b",\n")
        jobs_queue.put(line + b":::::::::::::::: " + str(counter).encode())
        counter += 1


def worker(jobs_queue, write_queue):
    print("starting...")
    while True:
        line = jobs_queue.get()
        if line is None:
            print("ending worker...")
            break
        line, counter = line.split(b":::::::::::::::: ")
        counter = int(counter)
        d = json.loads(line)  # pylint: disable=no-member
        try:
            location = d["claims"]["P625"][0]["mainsnak"]["datavalue"]["value"]
        except:
            continue
        latitude = location["latitude"]
        longitude = location["longitude"]
        d["_counter"] = counter
        if in_us(longitude, latitude):
            write_queue.put(d)
    write_queue.put(None)


def writer(write_queue):
    with open(
        "/Users/zernst/git/pycypher-nmetl/packages/fastopendata/raw_data/wikidata_us_points.json",
        "wb",
    ) as f:
        with Progress() as progress:
            task = progress.add_task(
                "[cyan]Filtering to US points...", total=11514545
            )
            while True:
                line = write_queue.get()
                if line is None:
                    print("ending writer...")
                    break
                counter = line["_counter"]
                serialized = json.dumps(line)  # pylint: disable=no-member
                f.write(serialized + b"\n")
                progress.update(task, completed=counter)


if __name__ == "__main__":
    jobs_queue = mp.Queue()
    write_queue = mp.Queue()
    reader_process = mp.Process(target=gen_us_points, args=(jobs_queue,))
    worker_processes = [
        mp.Process(target=worker, args=(jobs_queue, write_queue))
        for _ in range(10)
    ]
    reader_process.start()
    for worker_process in worker_processes:
        worker_process.start()
    writer_process = mp.Process(target=writer, args=(write_queue,))
    writer_process.start()
    reader_process.join()
    print("done reading...")
    for worker_process in worker_processes:
        worker_process.join()
    print("done workers...")
    writer_process.join()
    print("done writing...")
