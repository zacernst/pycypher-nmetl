# FastOpenData — Dataset Inventory

All datasets listed here are publicly available and free to download. Each
entry includes the source URL, a brief description of the content, the local
filename produced after download, and any special download notes.

---

## 1. Census Tract-to-PUMA Crosswalk (2020)

| Field | Value |
|-------|-------|
| **Source** | U.S. Census Bureau — Geography Division |
| **URL** | `https://www2.census.gov/geo/docs/maps-data/data/rel2020/2020_Census_Tract_to_2020_PUMA.txt` |
| **Local file** | `raw_data/state_county_tract_puma.csv` |
| **Format** | Pipe-delimited text (needs BOM strip + `dos2unix`) |
| **License** | U.S. Government open data (public domain) |

Crosswalk table mapping every 2020 census tract FIPS code to its containing
PUMA (Public Use Microdata Area), county, and state.  Used to derive
`tract_fips` and `county_fips` computed columns.

```bash
wget --no-check-certificate \
  https://www2.census.gov/geo/docs/maps-data/data/rel2020/2020_Census_Tract_to_2020_PUMA.txt \
  -O raw_data/state_county_unedited.txt
cat raw_data/state_county_unedited.txt | sed 's/^\xEF\xBB\xBF//' > raw_data/state_county_tract_puma.csv
dos2unix raw_data/state_county_tract_puma.csv
```

---

## 2. ACS PUMS 2023 — 1-Year, Person Records

| Field | Value |
|-------|-------|
| **Source** | U.S. Census Bureau — American Community Survey |
| **URL** | `https://www2.census.gov/programs-surveys/acs/data/pums/2023/1-Year/csv_pus.zip` |
| **Local file** | `raw_data/csv_pus_1_year.zip` → extracted to `raw_data/psam_p*.csv` → merged into `raw_data/psam_pus.csv` |
| **Format** | ZIP containing multiple CSV files |
| **License** | U.S. Government open data (public domain) |

Individual-level microdata for the 2023 1-year ACS survey.  Contains
demographic, socioeconomic, and housing-related person attributes
(`MIL`, `PUMA`, `STATE`, etc.).

```bash
wget --no-check-certificate \
  --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' \
  --no-cache \
  --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' \
  https://www2.census.gov/programs-surveys/acs/data/pums/2023/1-Year/csv_pus.zip \
  -O raw_data/csv_pus_1_year.zip
```

---

## 3. ACS PUMS 2023 — 5-Year, Person Records

| Field | Value |
|-------|-------|
| **Source** | U.S. Census Bureau — American Community Survey |
| **URL** | `https://www2.census.gov/programs-surveys/acs/data/pums/2023/5-Year/csv_pus.zip` |
| **Local file** | `raw_data/csv_pus_5_year.zip` → extracted to `raw_data/psam_p*.csv` → merged into `raw_data/psam_pus.csv` |
| **Format** | ZIP containing multiple CSV files (split by state) |
| **License** | U.S. Government open data (public domain) |

Five-year pooled person microdata — larger sample than the 1-year file,
better for small-area analysis.

```bash
wget --no-check-certificate \
  --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' \
  --no-cache \
  --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' \
  https://www2.census.gov/programs-surveys/acs/data/pums/2023/5-Year/csv_pus.zip \
  -O raw_data/csv_pus_5_year.zip
```

---

## 4. ACS PUMS 2023 — 1-Year, Housing Records

| Field | Value |
|-------|-------|
| **Source** | U.S. Census Bureau — American Community Survey |
| **URL** | `https://www2.census.gov/programs-surveys/acs/data/pums/2023/1-Year/csv_hus.zip` |
| **Local file** | `raw_data/csv_hus_1_year.zip` → merged into `raw_data/psam_hus.csv` |
| **Format** | ZIP containing multiple CSV files |
| **License** | U.S. Government open data (public domain) |

Housing-unit microdata for the 2023 1-year ACS.  Fields include
`SERIALNO`, `DIVISION`, `PUMA`, `REGION`, `ADJINC`, `PWGTP`, etc.

```bash
wget --no-check-certificate \
  --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' \
  --no-cache \
  --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' \
  https://www2.census.gov/programs-surveys/acs/data/pums/2023/1-Year/csv_hus.zip \
  -O raw_data/csv_hus_1_year.zip
```

---

## 5. ACS PUMS 2023 — 5-Year, Housing Records

| Field | Value |
|-------|-------|
| **Source** | U.S. Census Bureau — American Community Survey |
| **URL** | `https://www2.census.gov/programs-surveys/acs/data/pums/2023/5-Year/csv_hus.zip` |
| **Local file** | `raw_data/csv_hus_5_year.zip` → merged into `raw_data/psam_hus.csv` |
| **Format** | ZIP containing multiple CSV files (split by state) |
| **License** | U.S. Government open data (public domain) |

Five-year pooled housing microdata.

```bash
wget --no-check-certificate \
  --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' \
  --no-cache \
  --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' \
  https://www2.census.gov/programs-surveys/acs/data/pums/2023/5-Year/csv_hus.zip \
  -O raw_data/csv_hus_5_year.zip
```

---

## 6. SIPP 2023 — Public Use File (Person-level)

| Field | Value |
|-------|-------|
| **Source** | U.S. Census Bureau — Survey of Income and Program Participation |
| **URL** | `https://www2.census.gov/programs-surveys/sipp/data/datasets/2023/pu2023_csv.zip` |
| **Local file** | `raw_data/pu2023_csv.zip` → `raw_data/pu2023.csv` |
| **Format** | ZIP containing CSV |
| **License** | U.S. Government open data (public domain) |

Longitudinal panel survey covering income, poverty, program participation,
and wealth in the United States.

```bash
wget --no-check-certificate \
  --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' \
  --no-cache \
  --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' \
  https://www2.census.gov/programs-surveys/sipp/data/datasets/2023/pu2023_csv.zip \
  -O raw_data/pu2023_csv.zip
```

---

## 7. SIPP 2023 — Replicate Weights

| Field | Value |
|-------|-------|
| **Source** | U.S. Census Bureau — SIPP |
| **URL** | `https://www2.census.gov/programs-surveys/sipp/data/datasets/2023/rw2023_csv.zip` |
| **Local file** | `raw_data/rw2023_csv.zip` → `raw_data/rw2023.csv` |
| **Format** | ZIP containing CSV |
| **License** | U.S. Government open data (public domain) |

Replicate weight file for variance estimation from the 2023 SIPP.

```bash
wget --no-check-certificate \
  https://www2.census.gov/programs-surveys/sipp/data/datasets/2023/rw2023_csv.zip \
  -O raw_data/rw2023_csv.zip
unzip -o raw_data/rw2023_csv.zip -d raw_data/
```

---

## 8. SIPP 2023 — Data Dictionary / Schema

| Field | Value |
|-------|-------|
| **Source** | U.S. Census Bureau — SIPP |
| **URL** | `https://www2.census.gov/programs-surveys/sipp/data/datasets/2023/pu2023_schema.json` |
| **Local file** | `raw_data/pu2023_schema.json` (also checked in at `data_assets/pu2023_schema.json`) |
| **Format** | JSON |
| **License** | U.S. Government open data (public domain) |

Machine-readable schema describing the fields, types, and value labels in
the SIPP 2023 public use file.  Used at runtime as a `DataAsset`.

```bash
wget --no-check-certificate \
  https://www2.census.gov/programs-surveys/sipp/data/datasets/2023/pu2023_schema.json \
  -O raw_data/pu2023_schema.json
```

---

## 9. American Housing Survey (AHS) 2023 — National PUF

| Field | Value |
|-------|-------|
| **Source** | U.S. Census Bureau — American Housing Survey |
| **URL (data)** | `https://www2.census.gov/programs-surveys/ahs/2023/AHS%202023%20National%20PUF%20v1.1%20Flat%20CSV.zip` |
| **URL (labels)** | `https://www2.census.gov/programs-surveys/ahs/2023/AHS%202023%20Value%20Labels%20Package.zip` |
| **Local files** | `raw_data/ahs_2023_csv.zip`, `raw_data/ahs_2023/` |
| **Format** | ZIP containing flat CSV |
| **License** | U.S. Government open data (public domain) |

National-level microdata on housing units from the 2023 American Housing
Survey.  Flat CSV format (`v1.1`).

```bash
wget --no-check-certificate \
  "https://www2.census.gov/programs-surveys/ahs/2023/AHS%202023%20Value%20Labels%20Package.zip" \
  -O raw_data/ahs_2023_labels.zip
unzip -o raw_data/ahs_2023_labels.zip -d raw_data/ahs_2023/

wget --no-check-certificate \
  "https://www2.census.gov/programs-surveys/ahs/2023/AHS%202023%20National%20PUF%20v1.1%20Flat%20CSV.zip" \
  -O raw_data/ahs_2023_csv.zip
unzip -o raw_data/ahs_2023_csv.zip -d raw_data/ahs_2023/
```

---

## 10. CJARS 2022 — County-Level Job Offers

| Field | Value |
|-------|-------|
| **Source** | U.S. Census Bureau — Criminal Justice Administrative Records System |
| **URL** | `https://www2.census.gov/programs-surveys/cjars/datasets/2022/cjars_joe_2022_co.csv.zip` |
| **Local file** | `raw_data/cjars_joe_2022_co.csv.zip` → `raw_data/cjars_joe_2022_co.csv` |
| **Format** | ZIP containing CSV |
| **License** | U.S. Government open data (public domain) |

County-level job offer statistics from the CJARS 2022 dataset.

```bash
wget --no-check-certificate \
  https://www2.census.gov/programs-surveys/cjars/datasets/2022/cjars_joe_2022_co.csv.zip \
  -O raw_data/cjars_joe_2022_co.csv.zip
unzip -o raw_data/cjars_joe_2022_co.csv.zip -d raw_data/
```

---

## 11. TIGER/Line 2024 — PUMA Shapefiles

| Field | Value |
|-------|-------|
| **Source** | U.S. Census Bureau — TIGER/Line |
| **URL (index)** | `https://www2.census.gov/geo/tiger/TIGER2024/PUMA20/` |
| **Local files** | `raw_data/geo/tiger/TIGER2024/PUMA20/*.zip` → extracted shapefiles |
| **Format** | ZIP per state containing Shapefile (`.shp`, `.dbf`, `.prj`, etc.) |
| **License** | U.S. Government open data (public domain) |

Public Use Microdata Area boundary shapefiles (2020 definition) for all
U.S. states and territories.  The full directory is mirrored recursively
and the individual ZIPs are extracted and concatenated.

```bash
wget --no-check-certificate -e robots=off -w 3 \
  --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' \
  --no-cache \
  --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' \
  -P raw_data/ -nH --recursive --no-parent \
  https://www2.census.gov/geo/tiger/TIGER2024/PUMA20/
```

---

## 12. TIGER/Line 2024 — U.S. State Boundaries

| Field | Value |
|-------|-------|
| **Source** | U.S. Census Bureau — TIGER/Line |
| **URL** | `https://www2.census.gov/geo/tiger/TIGER2024/STATE/tl_2024_us_state.zip` |
| **Local file** | `raw_data/us_state_boundaries.zip` → `raw_data/tl_2024_us_state.shp` (+ siblings) |
| **Format** | ZIP containing Shapefile |
| **License** | U.S. Government open data (public domain) |

National cartographic boundary file for U.S. states and equivalent
entities (2024 vintage).

```bash
wget --no-check-certificate \
  --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' \
  --no-cache \
  --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' \
  https://www2.census.gov/geo/tiger/TIGER2024/STATE/tl_2024_us_state.zip \
  -O raw_data/us_state_boundaries.zip
unzip raw_data/us_state_boundaries.zip -d raw_data/
```

---

## 13. TIGER/Line 2025 — Census Tracts (per state)

| Field | Value |
|-------|-------|
| **Source** | U.S. Census Bureau — TIGER/Line |
| **URL pattern** | `https://www2.census.gov/geo/tiger/TIGER2025/TRACT/tl_2025_{SS}_tract.zip` (one per state FIPS) |
| **Local files** | `raw_data/tl_2025_*_tract.zip` → extracted shapefiles |
| **Format** | ZIP per state containing Shapefile |
| **License** | U.S. Government open data (public domain) |

Census tract boundary shapefiles (2025 vintage) downloaded per state.
State FIPS codes run from `01` (Alabama) to `72` (Puerto Rico).

```bash
# Example for a single state (Alabama = 01):
wget https://www2.census.gov/geo/tiger/TIGER2025/TRACT/tl_2025_01_tract.zip \
  -O raw_data/tl_2025_01_tract.zip
unzip -o raw_data/tl_2025_01_tract.zip -d raw_data/
```

---

## 14. TIGER/Line 2025 — Address Features (per state)

| Field | Value |
|-------|-------|
| **Source** | U.S. Census Bureau — TIGER/Line |
| **URL pattern** | `https://www2.census.gov/geo/tiger/TIGER2025/ADDR/tl_2025_{SS}_addr.zip` (one per state FIPS) |
| **Local files** | `raw_data/tl_2025_*_addr.zip` → extracted shapefiles |
| **Format** | ZIP per state containing Shapefile |
| **License** | U.S. Government open data (public domain) |

Address range shapefiles (2025 vintage) downloaded per state.

```bash
# Example for a single state (Alabama = 01):
wget https://www2.census.gov/geo/tiger/TIGER2025/ADDR/tl_2025_01_addr.zip \
  -O raw_data/tl_2025_01_addr.zip
unzip -o raw_data/tl_2025_01_addr.zip -d raw_data/
```

---

## 15. TIGER/Line 2024 — Census Block Groups

| Field | Value |
|-------|-------|
| **Source** | U.S. Census Bureau — TIGER/Line |
| **URL (index)** | `https://www2.census.gov/geo/tiger/TIGER2024/BG/` |
| **Local files** | `raw_data/geo/tiger/TIGER2024/BG/*.zip` → extracted shapefiles → concatenated |
| **Format** | ZIP per state containing Shapefile |
| **License** | U.S. Government open data (public domain) |

Block group boundary shapefiles (2024 vintage).  The full directory is
mirrored recursively, ZIPs extracted, and individual shapefiles are
concatenated using `concatenate_shape_files.py`.

```bash
wget --no-check-certificate -e robots=off \
  --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' \
  -P raw_data/ -nH --recursive --no-parent \
  https://www2.census.gov/geo/tiger/TIGER2024/BG/
```

---

## 16. OpenStreetMap — U.S. Extract (`.osm.pbf`)

| Field | Value |
|-------|-------|
| **Source** | Geofabrik GmbH |
| **URL** | `https://download.geofabrik.de/north-america/us-latest.osm.pbf` |
| **Local file** | `raw_data/us-latest.osm.pbf` → processed to `raw_data/united_states_nodes.csv` |
| **Format** | OSM PBF (binary Protocol Buffer) |
| **License** | [Open Database License (ODbL) 1.0](https://opendatacommons.org/licenses/odbl/) — attribution required |
| **Update frequency** | Daily snapshots at Geofabrik |

Full OSM node dump for the United States.  Parsed with `osmium` via
`extract_osm_nodes.py` to produce a CSV of point geometries
(`longitude`, `latitude`, `encoded_tags`, `id`).

> **Note:** This file is several GB.  The `.osm.pbf` is consumed once to
> produce `united_states_nodes.csv` and can then be deleted to reclaim
> disk space.

```bash
wget https://download.geofabrik.de/north-america/us-latest.osm.pbf \
  -O raw_data/us-latest.osm.pbf
DATA_DIR=raw_data uv run python src/fastopendata/extract_osm_nodes.py
```

---

## 17. Wikidata — Full Entity Dump (filtered to U.S. geopoints)

| Field | Value |
|-------|-------|
| **Source** | Wikimedia Foundation |
| **URL** | `https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2` |
| **Local file** | Streamed, filtered, recompressed to `raw_data/wikidata_compressed.json.bz2` |
| **Format** | bzip2-compressed newline-delimited JSON |
| **License** | [Creative Commons CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/) — public domain |
| **Update frequency** | Weekly full dumps |

Full Wikidata entity dump filtered to entities that have English labels,
descriptions, or aliases **and** contain a `latitude` field — i.e.,
geographic point entities.  The `compress_wikidata.py` script streams the
dump through `bunzip2`, drops non-geographic / non-English entries, strips
`sitelinks`, and re-compresses with `bzip2`.

> **Note:** The uncompressed dump is ~100 GB.  The pipeline streams it
> without materialising the full decompressed file on disk.

```bash
wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2 \
  -O - \
  | bunzip2 -c \
  | uv run python src/fastopendata/compress_wikidata.py \
  | bzip2 -c \
  > raw_data/wikidata_compressed.json.bz2
```

---

## Summary Table

| # | Dataset | Source | Approx. size | Local filename |
|---|---------|--------|-------------|----------------|
| 1 | Census Tract-to-PUMA crosswalk | Census | < 10 MB | `state_county_tract_puma.csv` |
| 2 | ACS PUMS 2023 1-yr persons | Census | ~300 MB | `psam_pus.csv` |
| 3 | ACS PUMS 2023 5-yr persons | Census | ~1.5 GB | `psam_pus.csv` |
| 4 | ACS PUMS 2023 1-yr housing | Census | ~200 MB | `psam_hus.csv` |
| 5 | ACS PUMS 2023 5-yr housing | Census | ~1 GB | `psam_hus.csv` |
| 6 | SIPP 2023 PUF | Census | ~500 MB | `pu2023.csv` |
| 7 | SIPP 2023 replicate weights | Census | ~200 MB | `rw2023.csv` |
| 8 | SIPP 2023 schema | Census | < 1 MB | `pu2023_schema.json` |
| 9 | AHS 2023 national PUF | Census | ~100 MB | `ahs_2023/` |
| 10 | CJARS 2022 county job offers | Census | < 10 MB | `cjars_joe_2022_co.csv` |
| 11 | TIGER/Line 2024 PUMA shapes | Census | ~300 MB | `geo/tiger/TIGER2024/PUMA20/` |
| 12 | TIGER/Line 2024 state boundaries | Census | < 5 MB | `tl_2024_us_state.shp` |
| 13 | TIGER/Line 2025 census tracts | Census | ~1 GB total | `tl_2025_*_tract.*` |
| 14 | TIGER/Line 2025 address features | Census | ~10 GB total | `tl_2025_*_addr.*` |
| 15 | TIGER/Line 2024 block groups | Census | ~500 MB | `geo/tiger/TIGER2024/BG/` |
| 16 | OpenStreetMap US extract | Geofabrik | ~10 GB | `us-latest.osm.pbf` |
| 17 | Wikidata geopoint entities | Wikimedia | ~5 GB filtered | `wikidata_compressed.json.bz2` |
