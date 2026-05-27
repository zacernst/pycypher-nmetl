# fastopendata

Open data ingestion and processing for the PyCypher ecosystem.

## What is this?

`fastopendata` provides tools for loading public datasets (Census shapefiles, OpenStreetMap extracts, Wikidata, and more) and transforming them into PyCypher-compatible graph structures for Cypher query execution.

## Installation

Installed automatically as a workspace dependency when you run ``uv sync``
from the monorepo root. See the [root README](../../README.md) for setup instructions.

## Quick Start

`GraphPipeline` collects entities and relationships from multiple sources
and produces a `pycypher.Context` (or fully-built `Star`) ready for queries:

```python
import pandas as pd
from fastopendata import GraphPipeline

people = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
knows = pd.DataFrame({"src": [1], "dst": [2]})

pipeline = GraphPipeline()
pipeline.add_entity_dataframe("Person", people, id_col="id")
pipeline.add_relationship_dataframe(
    "KNOWS", knows, source_col="src", target_col="dst"
)
star = pipeline.build_star()  # or .build_context() for a raw Context
result = star.execute_query("MATCH (p:Person) RETURN p.name")
```

Path configuration (data dir, scripts dir, etc.) is read from the
`Config` singleton and the `DATA_DIR` environment variable; see
`fastopendata.config` for the full surface.

## Features

- **Data processing scripts** -- download, extract, compress, and concatenate open datasets
- **Geospatial support** -- shapefiles, PUMA boundaries, Census blocks via GeoPandas
- **OpenStreetMap** -- node extraction and US region filtering via Osmium
- **Wikidata** -- compressed entity ingestion
- **Pipeline orchestration** -- Snakemake workflow for reproducible builds
- **FastAPI server** -- optional API layer for serving processed data

## Processing Scripts

`src/fastopendata/processing/` contains standalone scripts for each data
source. See `src/fastopendata/processing/PROCESSING_SCRIPTS.md` for the
authoritative list; the most-used entries:

| Script | Purpose |
|--------|---------|
| `download_block_shape_files.sh` | Fetch Census block shapefiles |
| `concatenate_shape_files.py` | Merge shapefiles into unified datasets |
| `concatenate_puma_shape_files.py` | Merge PUMA shapefiles |
| `extract_osm_nodes.py` | Extract nodes from OSM PBF files |
| `filter_us_nodes.py` | Filter to US geographic bounds |
| `compress_wikidata.py` | Compress Wikidata JSON dumps |
| `wikidata_to_csv.py` | Convert Wikidata dumps to CSV format |
| `extract_pums_5year.sh` | Pull Census PUMS 5-year microdata |
| `extract_state_contracts.py` | Extract state-level contract records |

## Dependencies

Requires Python 3.12+ and includes GeoPandas, Shapely, Osmium, and Snakemake for geospatial processing and workflow orchestration.

## Documentation

Full docs: [https://zacernst.github.io/pycypher/](https://zacernst.github.io/pycypher/)

## License

MIT
