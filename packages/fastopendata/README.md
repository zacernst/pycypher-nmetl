# fastopendata

Open data ingestion and processing for the PyCypher ecosystem.

## What is this?

`fastopendata` provides tools for loading public datasets (Census shapefiles, OpenStreetMap extracts, Wikidata, and more) and transforming them into PyCypher-compatible graph structures for Cypher query execution.

## Installation

Installed automatically as a workspace dependency when you run ``uv sync``
from the monorepo root. See the [root README](../../README.md) for setup instructions.

## Quick Start

```python
from fastopendata import GraphPipeline, config

# Configure data paths
config.raw_data_dir = "raw_data/"
config.output_dir = "output/"

# Build a processing pipeline
pipeline = GraphPipeline()
pipeline.run()
```

## Features

- **Data processing scripts** -- download, extract, compress, and concatenate open datasets
- **Geospatial support** -- shapefiles, PUMA boundaries, Census blocks via GeoPandas
- **OpenStreetMap** -- node extraction and US region filtering via Osmium
- **Wikidata** -- compressed entity ingestion
- **Pipeline orchestration** -- Snakemake workflow for reproducible builds
- **FastAPI server** -- optional API layer for serving processed data

## Processing Scripts

The `processing/` directory contains standalone scripts for each data source:

| Script | Purpose |
|--------|---------|
| `download_block_shape_files.sh` | Fetch Census block shapefiles |
| `concatenate_shape_files.py` | Merge shapefiles into unified datasets |
| `extract_osm_nodes.py` | Extract nodes from OSM PBF files |
| `filter_us_nodes.py` | Filter to US geographic bounds |
| `compress_wikidata.py` | Compress Wikidata JSON dumps |

## Dependencies

Requires Python 3.12+ and includes GeoPandas, Shapely, Osmium, and Snakemake for geospatial processing and workflow orchestration.

## Documentation

Full docs: [https://zacernst.github.io/pycypher/](https://zacernst.github.io/pycypher/)

## License

MIT
