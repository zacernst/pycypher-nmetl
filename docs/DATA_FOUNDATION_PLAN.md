# FastOpenData — Data Foundation: Current State and Plan

Date: 2026-06-09. Snapshot of the data pipeline's state, problems found during
review, and a proposed sequence of work toward a production dataset backing
the FastOpenData API.

---

## 1. Current state

### 1.1 Pipeline (Snakemake)

`packages/fastopendata/Snakefile` orchestrates download + processing of the
source datasets. Driven by `make fod-data` from the repo root, which sets
`DATA_DIR=/run/media/zac/2tb/fastopendata/data` (external 2 TB drive, 1.7 TB
free as of this writing).

Datasets currently wired into `rule all`:

| Dataset | Terminal artifact | Status notes |
|---|---|---|
| ACS PUMS 2024 1-yr person | `psam_pus.parquet` (with derived `PUMA_FIPS`) | conversion + FIPS derivation added in uncommitted work |
| ACS PUMS 2024 1-yr housing | `psam_hus.parquet` | no `PUMA_FIPS` derivation (see §2.4) |
| TIGER 2024 PUMA shapes | `puma_combined.shp` | |
| TIGER 2025 tracts (per state) | `tl_2025_{fips}_tract.shp`, `tract_combined.shp` | |
| TIGER 2025 address ranges | `tl_2025_{fips}_addr.dbf` | ~10 GB total |
| TIGER 2024 block groups | `combined_block_groups.shp` | |
| TIGER 2024 state boundaries | `tl_2024_us_state.shp` | present on disk |
| Tract→PUMA crosswalk | `state_county_tract_puma_crosswalk.parquet` | full FIPS codes derived by `combine_digits_to_fips.py` |
| OSM US extract (Geofabrik) | `united_states_nodes_tract_crosswalk.parquet` | node extraction + spatial join to tracts |
| Wikidata geopoints | `wikidata_us_points.json` | **download in progress; rule wiring bug — see §2.1** |
| CJARS 2022 county | `output/cjars_joe_2022_co.parquet` | |
| USAspending FY2025 contracts | `FY2025_All_Contracts_Full_20260506.csv` | full file only; per-state extraction not wired (§2.5) |

The data directory was recently moved to a fresh drive: as of 2026-06-09 it
contains only the state-boundary shapefile and a partial (2.4 GB) Wikidata
download. The pipeline is effectively restarting from scratch there; an active
`snakemake ... wikidata_us_points.json --cores 3` run is downloading the dump.

Recent commits (`1ff07f4`, `8f3e7ba`, `9b6b6eb`) converted CSV outputs to
ZSTD parquet via DuckDB and added the OSM tag tally extraction.

Uncommitted work in flight (as of this snapshot):

- `Snakefile`: split `download_filter_wikidata` into `download_wikidata` +
  `filter_wikidata` (buggy — §2.1); inserted `add_puma_fips_to_psam_pus` step;
  `DATA_DIR` trailing-slash strip.
- `Makefile`: `SNAKEMAKE_CORES` 50 → 8.
- New untracked: `src/fastopendata/processing/add_puma_fips_to_psam_pus.py`,
  `src/fastopendata/output/osm.csv` (data artifact in source tree — §2.6),
  `http-redirect.yaml` at repo root (GCP load-balancer HTTPS redirect map —
  deployment config, currently homeless).

### 1.2 Consumption layer

- **API** (`src/fastopendata/api.py`): FastAPI app exposing `/query` (Cypher
  via `Star`), `/datasets`, `/health`, analytics + audit endpoints, API-key
  auth, rate limiting. On startup it loads a single state selected by
  `STATE_FIPS` (default `13` / Georgia) **if** `contracts_state_{fips}.csv`
  and the crosswalk CSV exist; otherwise it falls back to generic CSV
  discovery of whatever is in `DATA_DIR`.
- **ETL → graph** (`src/fastopendata/etl/`): `state_pipeline.py` builds
  entities + relationships (contracts + geography) for one state.
  `ETL_PIPELINE_ARCHITECTURE.md` documents the target entity/relationship
  model (CensusTract, BlockGroup, Puma, State, Contract, OsmNode;
  IN_STATE, MAPS_TO_PUMA, PERFORMED_IN, NEAR, …).
- **nmetl path** (`fod_input_configs.yaml.template` + `make nmetl-go`):
  declares OSMNode/State/County/Tract/PUMA entities from the crosswalk CSV
  and the OSM-tract parquet. This is a second, parallel ingestion route into
  pycypher alongside the `GraphPipeline` route — they should converge (§3,
  Phase 4).

---

## 2. Problems found in review

### 2.1 Wikidata rule wiring bug (affects the run in progress)

Two rules now declare `wikidata_compressed.json.bz2` as output:

- `download_wikidata` downloads the **raw** dump
  (`latest-all.json.bz2`, ~90+ GB) directly to
  `wikidata_compressed.json.bz2` — the file is misnamed, not filtered.
- `filter_wikidata` (the rule that actually runs `compress_wikidata.py`)
  takes `latest-all.json.bz2` as input — which **no rule produces** — so it is
  unreachable dead code.

Snakemake resolved the ambiguity by choosing `download_wikidata`; that is the
wget currently running. Consequence: `filter_wikidata_us_points`
(`filter_us_nodes.py`) will consume the full unfiltered dump under the
"compressed" name. Fix: point `download_wikidata`'s output at
`latest-all.json.bz2` and let `filter_wikidata` produce
`wikidata_compressed.json.bz2`. After the in-progress download finishes,
rename the file to `latest-all.json.bz2` so the filter step can run without
re-downloading.

### 2.2 `DATASETS.md` is stale

It documents ACS PUMS **2023** (1-yr and 5-yr), SIPP 2023, and AHS 2023 —
none of which are in the current Snakefile — and omits USAspending FY2025
contracts, which is. The Snakefile header says "17 source datasets"; the
actual count in `rule all` is 12 logical datasets. Regenerate or hand-fix
when the pipeline stabilizes (Phase 5).

### 2.3 Row-wise pandas in FIPS derivation scripts

`add_puma_fips_to_psam_pus.py` and `combine_digits_to_fips.py` use
`df.apply(..., axis=1)` for string concatenation — O(rows) Python-level calls
over multi-GB tables. Use vectorized `df['STATE'] + df['PUMA']`, or fold the
derivation into the DuckDB `COPY` step that already exists
(`SELECT *, STATE || PUMA AS PUMA_FIPS FROM ...`), which would also remove a
full pandas materialization of the PUMS table.

### 2.4 Housing PUMS lacks the FIPS derivation

`psam_pus` gets `PUMA_FIPS`; `psam_hus` does not.
`combine_digis_puma_fips_psam_hus.py` (note the filename typo) exists in
`processing/` but is not referenced by any Snakefile rule, and it actually
operates on crosswalk-style columns (`STATEFP`/`COUNTYFP`/…), not the
PUMS housing schema. Decide whether housing records need the same join key
(they do, if households are to be located) and wire one correct script for
both.

### 2.5 Per-state contract extraction not wired

The API's state pipeline wants `contracts_state_{fips}.csv`;
`extract_state_contracts.py` produces it, but no Snakemake rule invokes it.
The pipeline stops at the 60+ GB national CSV. Add a rule (wildcard over
state FIPS) — or better, convert the national file to parquet once and have
the API/ETL filter at load time with DuckDB instead of materializing
per-state CSVs.

### 2.6 Repo hygiene

- `src/fastopendata/output/osm.csv` — a data artifact inside the package
  source tree. Given the historical 33 GB-wheel hatchling incident, data must
  never live under `src/`. Delete and gitignore.
- Root-level `wikidata_to_csv.py` is byte-identical to
  `processing/wikidata_to_csv.py`. Delete the root copy.
- `Snakefile.bak` — delete (git history preserves it).
- Snakefile line 1 contains a typo (`===oto===`).
- `http-redirect.yaml` should move into a `deploy/` directory (alongside
  whatever the `fix-deploy-auth` worktree produces) rather than sit untracked
  at repo root.

---

## 3. Proposed plan

The goal: a versioned, geography-keyed dataset that the API can load
deterministically — every record reachable from a geographic key
(state → county → tract → block group, plus PUMA via crosswalk), so that an
address or lat/lon can be enriched with all sources in one query.

### Phase 1 — Stabilize the pipeline (current blocker)

1. Fix the Wikidata rule split (§2.1); rename the in-progress download when
   it completes instead of re-fetching ~90 GB.
2. Vectorize / DuckDB-ify the FIPS derivations (§2.3) and add the housing
   equivalent (§2.4).
3. Wire contract state-extraction or parquet-convert the national file (§2.5).
4. Clean up §2.6 items; commit the in-flight Snakefile/Makefile changes on a
   feature branch.
5. Run `make fod-data` to completion on the 2 TB drive; record wall-clock
   time and final disk usage per dataset.

### Phase 2 — Canonical formats and keys

1. Everything terminal becomes ZSTD parquet (shapefiles → GeoParquet via
   geopandas; `wikidata_us_points.json` → parquet). CSV survives only as an
   intermediate.
2. Enforce one join-key convention across all tables: zero-padded string
   FIPS — `state_fips` (2), `county_fips` (5), `tract_fips` (11),
   `block_group_fips` (12), `puma_fips` (7). The crosswalk parquet is the
   single source of truth for the hierarchy.
3. Add a schema-validation step per artifact (column names, dtypes, key
   coverage) that fails the Snakemake run, not the API at load time.
   `tests/test_etl_schemas.py` is a starting point — promote those checks
   into the pipeline itself.

### Phase 3 — Spatial joins to geography

All point-like sources get a `tract_fips` (and where feasible
`block_group_fips`) column via point-in-polygon against the combined
shapefiles:

1. OSM nodes → tract (exists: `join_to_tracts.py`) — extend to block group.
2. Wikidata US points → tract (new rule; same machinery as OSM).
3. TIGER address ranges → block group, producing an address-range lookup
   table (street name + number range → block group) for address-based
   enrichment without a geocoder.

### Phase 4 — Aggregate feature tables

Produce per-geography "wide" tables — these are the actual product the API
serves:

1. **Tract features**: OSM tag tallies per tract (the tag-tally work from
   commit `9b6b6eb` feeds this), Wikidata entity counts/types per tract,
   land/water area from TIGER.
2. **County features**: CJARS justice-system measures, contract dollars
   (obligations by place-of-performance), establishment counts.
3. **PUMA features**: ACS PUMS person/housing aggregates (median income,
   education distribution, etc.), allocated down to tracts via the crosswalk
   where appropriate.
4. Unify the two ingestion routes (GraphPipeline vs. nmetl YAML) on these
   tables — one manifest of entities/relationships consumed by both.

### Phase 5 — Dataset releases and API loading

1. A `manifest.json` per pipeline run: artifact list, row counts, schema
   hashes, source URLs + retrieval dates, file checksums. The API loads only
   from a manifest, replacing directory-scan discovery.
2. Version the releases (e.g. `fod-2026.06`); keep the previous release until
   the new one passes validation (national FIPS coverage: 50 states + DC +
   PR; null-rate thresholds; row-count deltas vs. previous release).
3. Regenerate `DATASETS.md` from the manifest so docs cannot drift again.
4. Multi-state / national serving: replace the single-`STATE_FIPS` startup
   load with lazy per-state loading or DuckDB-backed predicate pushdown on
   the parquet files.

### Phase 6 — Refresh automation

ACS, TIGER, and USAspending are annual; OSM is daily; Wikidata is weekly.
Parameterize vintages in `config.py` (no hard-coded `2025`/`FY2025_..._20260506`
filenames in rules) and schedule a quarterly pipeline re-run with the
validation gate from Phase 5.

---

## 4. Candidate data sources not yet in the codebase

All public/open unless noted. Roughly ordered by fit with the
geography-enrichment model:

| Source | Geography | What it adds |
|---|---|---|
| **ACS 5-yr Summary Tables** (Census API / summary files) | tract, block group | Pre-aggregated income, education, housing, commute — far better small-area coverage than 1-yr PUMS microdata; arguably the single most valuable addition |
| **CDC PLACES** | tract | Model-based health outcomes (obesity, diabetes, insurance coverage) |
| **EPA EJSCREEN** | block group | Environmental indicators (air toxics, proximity to hazards) |
| **FEMA National Risk Index** | tract | Natural-hazard risk scores |
| **LEHD LODES** (Census) | block | Employment origin–destination, jobs by sector |
| **USDA Food Access Research Atlas** | tract | Food-desert indicators |
| **FCC Broadband Data Collection** | location/block | Broadband availability and speeds |
| **HUD datasets** | tract/ZIP | Fair Market Rents, USPS vacancy, ZIP↔tract crosswalk (the ZIP crosswalk fills a real gap — nothing currently maps ZIP codes) |
| **BLS QCEW + LAUS** | county | Employment, wages, unemployment rates |
| **IRS SOI** | ZIP/county | Income statistics, county-to-county migration flows |
| **NCES EDGE** | point/district | School locations, district boundaries |
| **FBI NIBRS / Crime Data Explorer** | agency/county | Crime incidence (coverage caveats by state) |
| **MIT Election Data Lab** | county/precinct | Election results |
| **NOAA Climate Normals** | station → interpolated | Temperature/precipitation normals |
| **OpenAddresses** | point | Address points complementing TIGER ADDR (license: mixed, mostly permissive — check per-source) |
| **GTFS feeds (Mobility Database)** | point/route | Transit stop density per tract |

Licensing note: everything above is US-government public domain or
permissively licensed except OSM (ODbL, attribution + share-alike for derived
databases — already in use) and OpenAddresses (per-source). Zillow/CoreLogic
style housing-price data is *not* open and should stay out.

---

## 5. Open questions

1. Is the API's unit of delivery a graph query surface (`/query` Cypher), a
   "give me everything for this address/point" enrichment endpoint, or both?
   Phase 4's wide tables serve the latter; the graph route serves the former.
   The answer determines how much of the relationship-derivation machinery is
   load-bearing.
2. 1-yr vs 5-yr PUMS: the Snakefile dropped 5-yr. For PUMA-level aggregates
   the 5-yr file is the defensible choice (sample size); revisit before
   Phase 4.3.
3. Should the per-state serving model (`STATE_FIPS`) survive, or is the
   target a national dataset with lazy loading? Affects Phase 5.4 design and
   the GCP deployment footprint (`http-redirect.yaml`, fix-deploy-auth work).
