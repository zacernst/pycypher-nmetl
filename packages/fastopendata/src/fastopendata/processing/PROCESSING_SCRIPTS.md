# Data Processing Scripts

All scripts live in `packages/fastopendata/src/fastopendata/processing/`.
Python scripts read paths from the `DATA_DIR` environment variable; shell
scripts default `DATA_DIR` to `raw_data/` relative to the repo root when
the variable is not set.  Inside Docker, `DATA_DIR` is set to
`/workspace/packages/fastopendata/raw_data`.

---

## Python scripts

### `extract_osm_nodes.py`

**Input:** `$DATA_DIR/us-latest.osm.pbf` (~10 GB)
**Output:** `$DATA_DIR/united_states_nodes.csv`

Parses the full U.S. OpenStreetMap binary extract using the `osmium`
library.  For each node it strips a hard-coded list of "trivial" tag keys
(highway, name, barrier, tiger:*, source, ref, etc.) that carry no
semantic value for our purposes.  Any node that still has at least one
remaining tag is written to the CSV with columns:

| Column | Description |
|--------|-------------|
| `longitude` | WGS-84 longitude |
| `latitude` | WGS-84 latitude |
| `encoded_tags` | `base64(json(dict(all_original_tags)))` |
| `id` | OSM node ID |

The full tag dict is JSON-encoded so downstream consumers can recover
it without re-reading the PBF.  Progress is logged every 100 k nodes.
`MAX_NODES` can be set in source to stop early during development.

**Notable issue in origin:** Tag-stripping used a `break/else` construct
that exited the outer loop on the *first non-trivial tag found*, silently
discarding the rest of the tag dict.  The rewrite fixes this by building
a comprehension over all tags before deciding whether to write the row.

---

### `compress_wikidata.py`

**Input:** stdin — decompressed Wikidata newline-JSON (pipe from `bunzip2`)
**Output:** stdout — filtered JSON, one entity per line (pipe to `bzip2`)

Streams the full Wikidata entity dump and keeps only entities whose raw
line contains the string `"latitude"` (cheap pre-filter).  For survivors
it:

1. Parses the JSON.
2. Keeps only the English facet of `descriptions`, `labels`, and `aliases`;
   drops the entire field if no English entry exists.
3. Removes the `sitelinks` block (large and not needed).
4. Writes the slimmed entity as a single JSON line to stdout.

Typical invocation (full pipeline):

```bash
wget -O - https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2 \
  | bunzip2 -c \
  | uv run python compress_wikidata.py \
  | bzip2 -c \
  > $DATA_DIR/wikidata_compressed.json.bz2
```

A rich progress bar is written to stderr.  The total-line estimate
(~98 billion) is calibrated to the 2024 dump; adjust `_APPROX_TOTAL_LINES`
for other vintages.

---

### `filter_us_nodes.py`

**Input:** `$DATA_DIR/location_entities.json.bz2`
**Also reads:** `$DATA_DIR/tl_2024_us_state.shp` (TIGER/Line state boundaries)
**Output:** `$DATA_DIR/wikidata_us_points.json`

Takes the compressed Wikidata entity file (produced by `compress_wikidata.py`
or a similar upstream step) and performs a point-in-polygon test for each
entity's P625 (coordinate location) Wikidata property against U.S. state
boundary polygons.

Uses a multiprocessing pipeline with three roles:

- **Reader process** — streams the bz2 file and pushes encoded lines onto a
  jobs queue.
- **Worker processes** (×10) — each loads the state shapefile independently,
  parses the entity JSON, extracts lat/lon from `claims.P625`, and puts
  matching entities on a results queue.
- **Writer process** — drains the results queue and writes one JSON line per
  entity to the output file.

**Notable issues in origin:** hardcoded absolute paths throughout; the
writer expected `f.write(serialized + b"\n")` but `json.dumps` returns `str`
not `bytes` — would have raised `TypeError` at runtime.  Both are fixed in
the rewrite (paths via `DATA_DIR`; writer uses text mode).

---

### `concatenate_shape_files.py`

**Input:** all `*.shp` files in `$DATA_DIR`
**Output:** `$DATA_DIR/combined.shp`
**Columns retained:** `BLKGRPCE`, `GEOID`, `geometry`

General-purpose shapefile concatenation utility used to merge the
per-state TIGER/Line block-group shapefiles into a single national file.
Delegates the actual read/write to `pyogrio` (fast C-backed I/O) and
concatenates with `pandas.concat`.  The `concatenate_shapefiles` function
is reusable — imported by `concatenate_puma_shape_files.py`.

---

### `concatenate_puma_shape_files.py`

**Input:** all `*.shp` files in `$DATA_DIR` whose name contains "puma"
**Output:** `$DATA_DIR/puma_combined.shp`
**Columns retained:** `PUMA20`, `GEOID`, `geometry`

Thin wrapper around `concatenate_shapefiles()` that selects only PUMA
shapefiles (from the TIGER/Line 2024 PUMA20 download) and writes the
merged result.  Previously a near-duplicate of `concatenate_shape_files.py`;
now it simply imports and calls the shared function.

---

## Shell scripts

### `download_block_shape_files.sh`

Downloads TIGER/Line 2024 Census Block Group shapefiles recursively from
`https://www2.census.gov/geo/tiger/TIGER2024/BG/`, extracts every zip into
`$DATA_DIR`, and then calls `concatenate_shape_files.py` to produce the
merged `combined.shp`.  This is the single entry-point for the entire
block-group pipeline (download → extract → merge).

---

### `extract_puma_shape_files.sh`

Extracts already-downloaded TIGER/Line 2024 PUMA20 zip files from
`$DATA_DIR/geo/tiger/TIGER2024/PUMA20/` into `$DATA_DIR`.  Intentionally
only handles extraction — the download is performed separately by the
Makefile (`wget` recursive mirror of the PUMA20 directory).  Run
`concatenate_puma_shape_files.py` afterwards to merge.

---

### `extract_pums_5year.sh`

Extracts the ACS PUMS 5-year person and housing zip files from
`$DATA_DIR/programs-surveys/acs/data/pums/2023/5-Year/`, then merges the
per-state CSVs into two national files:

- `$DATA_DIR/psam_p.csv` — all person records (header from `psam_p01.csv`,
  data rows from every `psam_p*.csv` except the pre-merged `psam_pus.csv`)
- `$DATA_DIR/psam_h.csv` — all housing records (same pattern)

This replaces the original `pums_5_year.sh` which had the same logic but
hardcoded absolute paths.

---

## Pipeline diagram

```
TIGER/Line BG zips ──▶ download_block_shape_files.sh ──▶ combined.shp
TIGER/Line PUMA zips ─▶ extract_puma_shape_files.sh
                         + concatenate_puma_shape_files.py ──▶ puma_combined.shp
ACS PUMS 5-yr zips ──▶ extract_pums_5year.sh ──▶ psam_p.csv, psam_h.csv
us-latest.osm.pbf ───▶ extract_osm_nodes.py ──▶ united_states_nodes.csv
Wikidata dump ────────▶ compress_wikidata.py (stdin/stdout pipeline)
                         ──▶ wikidata_compressed.json.bz2
wikidata_compressed + tl_2024_us_state.shp
                    ──▶ filter_us_nodes.py ──▶ wikidata_us_points.json
```
