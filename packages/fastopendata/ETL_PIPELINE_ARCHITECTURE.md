# FastOpenData ETL Pipeline Architecture

**Author**: Christopher (Architecture Specialist)
**Date**: 2026-04-10
**Status**: Design Complete — Ready for Implementation

---

## 1. Overview

This document defines the ETL pipeline architecture that connects Snakefile-processed data sources to pycypher's graph query engine via the `GraphPipeline` and `ContextBuilder` APIs. The design enables complex multi-source queries across Census, TIGER, OSM, and federal contracts data using Cypher graph patterns.

### Design Principles

1. **Snakefile produces files; pycypher consumes them** — clear boundary between data acquisition and query
2. **Graph relationships encode geographic hierarchy** — tracts ∈ counties ∈ states, linked via FIPS codes
3. **Incremental state expansion** — Georgia-first development, then parameterized for any state
4. **Batch-first, streaming-ready** — initial implementation uses batch DataFrames; streaming views added later for live data

---

## 2. Data Model

### Entity Types (Nodes)

| Entity Type | Source File | ID Column | Key Properties |
|---|---|---|---|
| `CensusTract` | `tl_2025_{fips}_tract.shp` → CSV | `GEOID` | `STATEFP`, `COUNTYFP`, `TRACTCE`, `ALAND`, `AWATER`, geometry |
| `BlockGroup` | `tl_2024_{fips}_bg.shp` → CSV | `GEOID` | `STATEFP`, `COUNTYFP`, `TRACTCE`, `BLKGRPCE`, `ALAND` |
| `Puma` | `tl_2024_{fips}_puma20.shp` → CSV | `PUMACE20` | `STATEFP20`, `NAMELSAD20`, `ALAND20` |
| `State` | `tl_2024_us_state.shp` → CSV | `STATEFP` | `STUSPS`, `NAME`, `ALAND`, `AWATER` |
| `Contract` | `contracts_state_{fips}.csv` | `contract_transaction_unique_key` | `federal_action_obligation`, `recipient_name`, `naics_code`, `award_type` |
| `OsmNode` | `united_states_nodes.csv` | `id` | `longitude`, `latitude`, `encoded_tags` |
| `TractPumaCrosswalk` | `state_county_tract_puma.csv` | composite | `STATEFP`, `COUNTYFP`, `TRACTCE`, `PUMA5CE` |

### Relationship Types (Edges)

| Relationship | Source → Target | Join Strategy | Source |
|---|---|---|---|
| `IN_COUNTY` | `CensusTract` → `State` | `CensusTract.STATEFP + COUNTYFP` → county-level grouping | Derived from TIGER tract GEOID |
| `IN_STATE` | `CensusTract` → `State` | `CensusTract.STATEFP` = `State.STATEFP` | FIPS code match |
| `CONTAINS_BLOCK_GROUP` | `CensusTract` → `BlockGroup` | `BlockGroup.TRACTCE` within `CensusTract.TRACTCE` | GEOID prefix match |
| `MAPS_TO_PUMA` | `CensusTract` → `Puma` | Via `TractPumaCrosswalk.PUMA5CE` | Crosswalk CSV |
| `PERFORMED_IN` | `Contract` → `State` | `Contract.pop_state_fips` = `State.STATEFP` | Contracts CSV column |
| `AWARDED_IN` | `Contract` → `State` | `Contract.recipient_state_fips` = `State.STATEFP` | Contracts CSV column |
| `NEAR` | `OsmNode` → `CensusTract` | Spatial join (point-in-polygon) | Computed at ingestion |

---

## 3. Pipeline Architecture

### 3.1 Layer Diagram

```
┌─────────────────────────────────────────────────────────┐
│                    Cypher Queries                        │
│  MATCH (c:Contract)-[:PERFORMED_IN]->(s:State)          │
│  WHERE s.STUSPS = 'GA' RETURN sum(c.obligation)         │
├─────────────────────────────────────────────────────────┤
│                   Star Query Engine                      │
│                  (pycypher.star.Star)                    │
├─────────────────────────────────────────────────────────┤
│                     Context                              │
│              (pycypher.relational_models)                │
├─────────────────────────────────────────────────────────┤
│                  GraphPipeline                           │
│     add_entity_dataframe() / add_relationship_dataframe()│
│     Schema validation via SchemaRegistry                 │
│     Lineage tracking via LineageGraph                    │
├─────────────────────────────────────────────────────────┤
│               Data Loading Layer                         │
│     pd.read_csv() / gpd.read_file() → DataFrame         │
│     Column renaming, type coercion, ID generation        │
├─────────────────────────────────────────────────────────┤
│              Snakefile Data Acquisition                   │
│     Census Bureau downloads, OSM extraction,             │
│     State contracts filtering, shapefile processing      │
└─────────────────────────────────────────────────────────┘
```

### 3.2 Data Flow

```
Snakefile rules          Data Loading             GraphPipeline           Star
─────────────           ────────────             ─────────────          ─────
download_tiger_tract → read_shapefile_as_csv() → add_entity("CensusTract") ─┐
download_tiger_state → read_shapefile_as_csv() → add_entity("State")        │
extract_state_contracts → pd.read_csv()        → add_entity("Contract")     ├→ build_context() → Star
download_tract_puma  → pd.read_csv()           → add_entity("Crosswalk")    │
extract_osm_nodes    → pd.read_csv()           → add_entity("OsmNode")      │
                                                                             │
                       derive_relationships()  → add_relationship("IN_STATE")┘
                       derive_relationships()  → add_relationship("MAPS_TO_PUMA")
                       derive_relationships()  → add_relationship("PERFORMED_IN")
```

### 3.3 Relationship Derivation

Relationships are **not stored in source files** — they are derived at ingestion time from shared FIPS codes:

```python
def derive_tract_state_relationships(
    tracts_df: pd.DataFrame,
    states_df: pd.DataFrame,
) -> pd.DataFrame:
    """Create IN_STATE edges from CensusTract → State via STATEFP match."""
    edges = tracts_df[["GEOID", "STATEFP"]].rename(
        columns={"GEOID": "__SOURCE__", "STATEFP": "__TARGET__"}
    )
    # Filter to only states that exist in our State entity table
    valid_states = set(states_df["STATEFP"])
    return edges[edges["__TARGET__"].isin(valid_states)]
```

```python
def derive_tract_puma_relationships(
    crosswalk_df: pd.DataFrame,
) -> pd.DataFrame:
    """Create MAPS_TO_PUMA edges from CensusTract → Puma via crosswalk."""
    # Crosswalk has STATEFP, COUNTYFP, TRACTCE, PUMA5CE
    # Build tract GEOID: STATEFP + COUNTYFP + TRACTCE
    crosswalk_df["tract_geoid"] = (
        crosswalk_df["STATEFP"] + crosswalk_df["COUNTYFP"] + crosswalk_df["TRACTCE"]
    )
    return crosswalk_df[["tract_geoid", "PUMA5CE"]].rename(
        columns={"tract_geoid": "__SOURCE__", "PUMA5CE": "__TARGET__"}
    )
```

---

## 4. Implementation Plan

### Phase 1: Georgia Development Dataset (Single State)

**Goal**: End-to-end pipeline with Georgia data, demonstrating all query patterns.

**Input files** (from `snakemake georgia_dev`):
- `contracts_state_13.csv`
- `tl_2025_13_tract.shp`
- `tl_2024_13_bg.shp`
- `tl_2024_13_puma20.shp`
- `state_county_tract_puma.csv`

**Implementation**:

```python
# packages/fastopendata/src/fastopendata/etl/georgia_pipeline.py

from pathlib import Path
import pandas as pd
import geopandas as gpd
from fastopendata.pipeline import GraphPipeline
from fastopendata.schema_evolution.registry import SchemaRegistry

def build_georgia_pipeline(data_dir: Path) -> GraphPipeline:
    """Build complete Georgia development graph from Snakefile outputs."""
    registry = SchemaRegistry()
    pipeline = GraphPipeline(schema_registry=registry)

    # --- Entities ---
    tracts = gpd.read_file(data_dir / "tl_2025_13_tract.shp")
    tracts_df = pd.DataFrame(tracts.drop(columns="geometry"))
    pipeline.add_entity_dataframe("CensusTract", tracts_df, id_col="GEOID")

    block_groups = gpd.read_file(data_dir / "tl_2024_13_bg.shp")
    bg_df = pd.DataFrame(block_groups.drop(columns="geometry"))
    pipeline.add_entity_dataframe("BlockGroup", bg_df, id_col="GEOID")

    pumas = gpd.read_file(data_dir / "tl_2024_13_puma20.shp")
    puma_df = pd.DataFrame(pumas.drop(columns="geometry"))
    pipeline.add_entity_dataframe("Puma", puma_df, id_col="PUMACE20")

    contracts = pd.read_csv(data_dir / "contracts_state_13.csv")
    pipeline.add_entity_dataframe(
        "Contract", contracts,
        id_col="contract_transaction_unique_key",
    )

    crosswalk = pd.read_csv(data_dir / "state_county_tract_puma.csv", dtype=str)
    # Filter to Georgia
    ga_crosswalk = crosswalk[crosswalk["STATEFP"] == "13"]

    # --- Relationships ---
    # Tract → Puma via crosswalk
    tract_puma_edges = ga_crosswalk.copy()
    tract_puma_edges["__SOURCE__"] = (
        tract_puma_edges["STATEFP"]
        + tract_puma_edges["COUNTYFP"]
        + tract_puma_edges["TRACTCE"]
    )
    tract_puma_edges["__TARGET__"] = tract_puma_edges["PUMA5CE"]
    pipeline.add_relationship_dataframe(
        "MAPS_TO_PUMA",
        tract_puma_edges[["__SOURCE__", "__TARGET__"]],
        source_col="__SOURCE__",
        target_col="__TARGET__",
    )

    # Contract → CensusTract (place of performance, if tract-level data available)
    # For now: Contract → State-level relationship
    contracts_with_fips = contracts[
        contracts["prime_award_transaction_place_of_performance_state_fips_code"].notna()
    ].copy()
    contracts_with_fips["__SOURCE__"] = contracts_with_fips[
        "contract_transaction_unique_key"
    ]
    contracts_with_fips["__TARGET__"] = "13"  # Georgia state FIPS
    pipeline.add_relationship_dataframe(
        "PERFORMED_IN_STATE",
        contracts_with_fips[["__SOURCE__", "__TARGET__"]],
        source_col="__SOURCE__",
        target_col="__TARGET__",
    )

    return pipeline
```

### Phase 2: Multi-State Expansion

**Goal**: Parameterize pipeline for any state FIPS code.

```python
def build_state_pipeline(data_dir: Path, state_fips: str) -> GraphPipeline:
    """Build graph pipeline for any state by FIPS code."""
    # Same structure as Georgia, parameterized with state_fips
    ...
```

### Phase 3: Full National Dataset

**Goal**: All 56 states/territories with cross-state relationship queries.

---

## 5. Query Patterns

### 5.1 Georgia Contract Analysis

```cypher
-- Total contract obligations in Georgia
MATCH (c:Contract)
RETURN count(c) AS total_contracts,
       sum(c.federal_action_obligation) AS total_obligation

-- Contracts by NAICS sector
MATCH (c:Contract)
RETURN c.naics_code AS sector,
       count(c) AS contract_count,
       sum(c.federal_action_obligation) AS total_value
ORDER BY total_value DESC
LIMIT 20
```

### 5.2 Geographic Hierarchy Traversal

```cypher
-- Census tracts that map to a specific PUMA
MATCH (t:CensusTract)-[:MAPS_TO_PUMA]->(p:Puma)
WHERE p.PUMACE20 = '03700'
RETURN t.GEOID AS tract, t.ALAND AS land_area

-- Block groups within a tract
MATCH (bg:BlockGroup)
WHERE bg.TRACTCE = '000100'
RETURN bg.GEOID, bg.ALAND
```

### 5.3 Cross-Domain Spatial Queries

```cypher
-- Contract density per PUMA region
MATCH (c:Contract)-[:PERFORMED_IN_STATE]->(s:State),
      (t:CensusTract)-[:MAPS_TO_PUMA]->(p:Puma)
WHERE s.STATEFP = '13'
RETURN p.NAMELSAD20 AS puma_name,
       count(c) AS contracts_in_region
ORDER BY contracts_in_region DESC
```

### 5.4 OSM Integration (Phase 2+)

```cypher
-- OSM amenities near census tracts with high contract activity
MATCH (n:OsmNode)-[:NEAR]->(t:CensusTract)
WHERE t.contract_count > 100
RETURN n.encoded_tags, t.GEOID
```

---

## 6. Shapefile-to-DataFrame Conversion

Shapefiles require conversion before pycypher ingestion. Two strategies:

### Strategy A: Pre-convert in Snakefile (Recommended)

Add Snakefile rules that convert shapefiles to CSV/Parquet at download time:

```python
rule convert_tract_shapefile:
    input: f"{DATA_DIR}/tl_2025_{{fips}}_tract.shp"
    output: f"{DATA_DIR}/tl_2025_{{fips}}_tract.parquet"
    shell:
        f"uv run python '{SCRIPTS}/shapefile_to_parquet.py' {{input}} {{output}}"
```

**Advantages**: Downstream consumers don't need geopandas; faster loading; smaller files.

### Strategy B: Load at pipeline time with geopandas

```python
gdf = gpd.read_file("tl_2025_13_tract.shp")
df = pd.DataFrame(gdf.drop(columns="geometry"))
```

**Advantages**: Simpler pipeline; no extra Snakefile rules.

**Recommendation**: Start with Strategy B for development. Add Strategy A when scaling to national dataset (56 states of shapefiles).

---

## 7. Schema Evolution Strategy

The `SchemaRegistry` (already integrated into `GraphPipeline`) handles schema changes:

- **Census data**: Schemas are stable across TIGER vintages but column names may change between years. Registry catches incompatible changes at ingestion.
- **Contracts data**: USAspending column set is stable within a fiscal year but may add columns across years. Backward-compatible additions are safe.
- **OSM data**: Schema is fixed (id, longitude, latitude, encoded_tags).

**Configuration**:
```python
# Strict for production (default)
registry = SchemaRegistry(compatibility_level=CompatibilityLevel.BACKWARD)

# Permissive for development
registry = SchemaRegistry(compatibility_level=CompatibilityLevel.NONE)
```

---

## 8. Incremental Update Strategy

### Contracts (Monthly USAspending releases)
1. Download new fiscal quarter data via Snakefile
2. Re-run `extract_state_contracts` with same FIPS
3. Rebuild `Contract` entity in pipeline (full replace per state)

### TIGER (Annual Census releases)
1. Update Snakefile URL year parameters
2. Re-download affected shapefiles
3. Schema registry validates compatibility with previous year
4. Full replace of geographic entities

### OSM (Continuous updates)
1. Download fresh PBF extract
2. Re-run extraction pipeline
3. Full replace of `OsmNode` entities

**Future**: Streaming incremental updates via `StreamEngine` for OSM changesets.

---

## 9. Performance Considerations

| Dataset | Rows (Georgia) | Rows (National) | Memory (Georgia) | Memory (National) |
|---|---|---|---|---|
| CensusTract | ~1,700 | ~85,000 | ~2 MB | ~100 MB |
| BlockGroup | ~5,500 | ~240,000 | ~5 MB | ~250 MB |
| Contract | ~34,000 | ~12M+ | ~50 MB | ~15 GB |
| OsmNode | N/A | ~500M | N/A | ~60 GB |

**Georgia development** fits entirely in memory (~60 MB). **National** requires:
- Lazy evaluation (REQ-P006) for Contract and OsmNode entities
- State-level partitioning for parallel processing
- DuckDB backend for OLAP queries on large datasets

**Recommended backend by dataset size**:
- Georgia dev: Pandas (default)
- Multi-state: Pandas with `max_rows` sampling
- National: DuckDB backend via `Star(context, backend="duckdb")`

---

## 10. File Structure

```
packages/fastopendata/src/fastopendata/
├── etl/                          # NEW: ETL pipeline implementations
│   ├── __init__.py
│   ├── georgia_pipeline.py       # Phase 1: Georgia development dataset
│   ├── state_pipeline.py         # Phase 2: Parameterized single-state
│   ├── national_pipeline.py      # Phase 3: Full national dataset
│   ├── relationship_derivation.py # Shared edge-building functions
│   └── shapefile_loader.py       # Shapefile → DataFrame conversion
├── pipeline.py                   # Existing GraphPipeline (unchanged)
├── schema_evolution/             # Existing (unchanged)
└── ...
```

---

## 11. Integration with Existing Infrastructure

| Component | Role | Changes Needed |
|---|---|---|
| `GraphPipeline` | Collects entities + relationships → Context | None (used as-is) |
| `SchemaRegistry` | Validates schema compatibility at ingestion | None (used as-is) |
| `LineageGraph` | Tracks data flow from source → context | None (automatic) |
| `Snakefile` | Produces raw data files | Add shapefile→parquet rules (Phase 2) |
| `config.toml` | Dataset metadata | Add ETL pipeline configuration section |
| `api.py` | REST query interface | Wire to ETL-built Star instance |
| `load_available_datasets()` | Auto-discovery of CSV datasets | Extend to load ETL-built contexts |

---

## 12. Implementation Priority

1. **`relationship_derivation.py`** — Edge-building functions (FIPS-based joins)
2. **`shapefile_loader.py`** — Shapefile → DataFrame conversion utility
3. **`georgia_pipeline.py`** — End-to-end Georgia dev pipeline
4. **Tests** — Pipeline builds, relationships correct, queries return expected results
5. **`state_pipeline.py`** — Parameterized version
6. **API integration** — Wire ETL pipeline into FastAPI startup
