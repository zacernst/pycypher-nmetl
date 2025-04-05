import bz2

import geopandas as gpd
import orjson as json
import rich
from rich.progress import Progress
from shapely import Point

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


def gen_us_points():
    f = bz2.open("/Users/zernst/git/pycypher-nmetl/out.json.bz2")
    counter = 0
    us_points = 0
    with Progress() as pbar:
        task = pbar.add_task("Points in US", total=10000)
        for line in f:
            pbar.update(task, advance=1)
            counter += 1
            if 0 and counter > 10000:
                break
            line = line.rstrip(b",")
            try:
                d = json.loads(line)
            except:
                continue
            try:
                location = d["claims"]["P625"][0]["mainsnak"]["datavalue"][
                    "value"
                ]
            except:
                continue
            latitude = location["latitude"]
            longitude = location["longitude"]
            if in_us(longitude, latitude):
                us_points += 1
                yield line


if __name__ == "__main__":
    with open(
        "/Users/zernst/git/pycypher-nmetl/wikidata_us_points.json", "wb"
    ) as f:
        for us_point in gen_us_points():
            f.write(us_point + b"\n")
