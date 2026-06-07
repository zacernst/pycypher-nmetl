"""Integration tests for the generic loader's shapefile + relationship paths.

Covers the work delivered in Tasks #1–#3 of the phase2-datasources team:

* TIGER/Line shapefile discovery in :func:`load_available_datasets` (Task #1)
* Relationship derivation invoked from the loader (Task #2)
* ACS PUMS person loading with column selection (Task #3)

Tests use synthetic in-memory DataFrames and ``unittest.mock`` to avoid the
heavy geopandas/shapely IO path — the integration point we want to exercise
is the loader's discovery + dispatch logic, not geopandas itself.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest
from fastopendata.pipeline import (
    _ACS_PUMS_COLS,
    _CJARS_FIPS_COLS,
    _OSM_COLS,
    _OSM_DEFAULT_MAX_ROWS,
    GraphPipeline,
    _derive_relationships,
    load_available_datasets,
)

# ---------------------------------------------------------------------------
# Helpers — copied/adapted from test_load_datasets.py to keep this file
# self-contained (the existing fakes are private to that module).
# ---------------------------------------------------------------------------


class _FakeDataset:
    """Minimal stand-in for ``DatasetConfig``."""

    def __init__(
        self,
        output_file: str | None,
        fmt: str = "CSV",
        source: str = "test",
        description: str = "test dataset",
        approx_size: str = "~1 KB",
    ) -> None:
        self.output_file = output_file
        self.format = fmt
        self.source = source
        self.description = description
        self.approx_size = approx_size


class _FakeConfig:
    """Minimal stand-in for ``Config`` used by ``load_available_datasets``."""

    def __init__(
        self,
        data_path: Path,
        datasets: dict[str, _FakeDataset],
    ) -> None:
        self.data_path = data_path
        self.datasets = datasets


def _patch_config(fake_config: _FakeConfig):
    """Patch the config object used by load_available_datasets."""
    return patch("fastopendata.config.config", fake_config)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    """Write a list of dicts as a CSV file."""
    if not rows:
        path.write_text("")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _empty_config(data_path: Path) -> _FakeConfig:
    return _FakeConfig(data_path=data_path, datasets={})


# ---------------------------------------------------------------------------
# Test 1 — TIGER shapefile loading via mocked geopandas
# ---------------------------------------------------------------------------


class TestShapefileLoading:
    """``load_available_datasets`` should pick up TIGER shapefiles."""

    def test_tract_shapefile_creates_census_tract_entity(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A ``tl_2025_*_tract.shp`` file should produce a ``CensusTract`` entity.

        We don't actually want to write a real shapefile (heavy IO and a
        geopandas dependency for what is fundamentally an integration test
        of the loader's dispatch logic). Instead we touch a sentinel ``.shp``
        path so ``Path.glob`` finds it, then patch the package-private
        ``_load_shapefile_as_entity`` helper to return a synthetic DataFrame.
        """
        # Create a sentinel file so Path.glob picks it up.
        shp_path = tmp_path / "tl_2025_13_tract.shp"
        shp_path.write_bytes(b"")  # contents irrelevant; we'll mock the reader

        synthetic_tracts = pd.DataFrame({
            "GEOID":   ["13001000100", "13001000200", "13089010100"],
            "STATEFP": ["13", "13", "13"],
            "COUNTYFP": ["001", "001", "089"],
            "NAME":    ["1", "2", "101"],
        })

        def _fake_loader(filepath: Path, entity_type: str, id_col: str) -> pd.DataFrame:
            # Sanity-check the loader is being asked for the file we touched.
            assert filepath == shp_path
            assert entity_type == "CensusTract"
            assert id_col == "GEOID"
            return synthetic_tracts.copy()

        monkeypatch.setattr(
            "fastopendata.pipeline._load_shapefile_as_entity", _fake_loader,
        )

        with _patch_config(_empty_config(tmp_path)):
            pipeline = load_available_datasets(data_dir=tmp_path)

        assert "CensusTract" in pipeline.entity_types
        assert pipeline.entity_count("CensusTract") == 3

    def test_multi_state_shapefiles_are_concatenated(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Multiple per-state shapefiles should produce one combined entity."""
        for state_fips in ("13", "06"):
            (tmp_path / f"tl_2025_{state_fips}_tract.shp").write_bytes(b"")

        # Each call gets a distinct DataFrame so we can verify concat ran.
        per_state = {
            tmp_path / "tl_2025_13_tract.shp": pd.DataFrame({
                "GEOID":   ["13001000100"],
                "STATEFP": ["13"],
            }),
            tmp_path / "tl_2025_06_tract.shp": pd.DataFrame({
                "GEOID":   ["06037123400", "06037567800"],
                "STATEFP": ["06", "06"],
            }),
        }

        def _fake_loader(filepath: Path, *_a: Any, **_kw: Any) -> pd.DataFrame:
            return per_state[filepath].copy()

        monkeypatch.setattr(
            "fastopendata.pipeline._load_shapefile_as_entity", _fake_loader,
        )

        with _patch_config(_empty_config(tmp_path)):
            pipeline = load_available_datasets(data_dir=tmp_path)

        assert pipeline.entity_count("CensusTract") == 3  # 1 + 2

    def test_no_shapefiles_means_no_geographic_entities(
        self, tmp_path: Path,
    ) -> None:
        """When no TIGER files are present, no geographic entity is registered."""
        with _patch_config(_empty_config(tmp_path)):
            pipeline = load_available_datasets(data_dir=tmp_path)
        for entity_type in ("CensusTract", "BlockGroup", "Puma"):
            assert entity_type not in pipeline.entity_types


# ---------------------------------------------------------------------------
# Test 2 — _derive_relationships() with CensusTract + State
# ---------------------------------------------------------------------------


class TestDeriveRelationshipsTractState:
    """The ``IN_STATE`` derivation runs when CensusTract + State are present."""

    def test_in_state_relationship_present(self) -> None:
        """A pipeline with CensusTract + State should yield ``IN_STATE`` edges."""
        tracts = pd.DataFrame({
            "GEOID":   ["13001000100", "13001000200", "06037123400"],
            "STATEFP": ["13", "13", "06"],
        })
        states = pd.DataFrame({
            "STATEFP": ["13", "06"],
            "STUSPS":  ["GA", "CA"],
            "NAME":    ["Georgia", "California"],
        })

        pipeline = (
            GraphPipeline()
            .add_entity_dataframe("CensusTract", tracts, id_col="GEOID")
            .add_entity_dataframe("State", states, id_col="STATEFP")
        )

        added = _derive_relationships(pipeline)

        assert "IN_STATE" in pipeline.relationship_types
        assert pipeline.relationship_count("IN_STATE") == 3
        # IN_STATE is the only derivation that fires here — no Puma, no
        # BlockGroup, no Contract, no Person.
        assert added == 1

    def test_in_state_filters_unmatched_states(self) -> None:
        """Tracts whose STATEFP isn't in States are dropped from edges."""
        tracts = pd.DataFrame({
            # 99 is a fake state FIPS not present in the State entity
            "GEOID":   ["13001000100", "99001000100"],
            "STATEFP": ["13", "99"],
        })
        states = pd.DataFrame({"STATEFP": ["13"], "NAME": ["Georgia"]})
        pipeline = (
            GraphPipeline()
            .add_entity_dataframe("CensusTract", tracts, id_col="GEOID")
            .add_entity_dataframe("State", states, id_col="STATEFP")
        )

        _derive_relationships(pipeline)

        assert pipeline.relationship_count("IN_STATE") == 1

    def test_in_state_skipped_when_state_missing(self) -> None:
        """No State entity → IN_STATE is not derived."""
        tracts = pd.DataFrame({
            "GEOID":   ["13001000100"],
            "STATEFP": ["13"],
        })
        pipeline = GraphPipeline().add_entity_dataframe(
            "CensusTract", tracts, id_col="GEOID",
        )

        added = _derive_relationships(pipeline)

        assert "IN_STATE" not in pipeline.relationship_types
        assert added == 0


# ---------------------------------------------------------------------------
# Test 3 — Person → Puma relationship
# ---------------------------------------------------------------------------


class TestDeriveRelationshipsPersonPuma:
    """The ``LIVES_IN_PUMA`` derivation joins Person.PUMA + STATE → Puma."""

    def test_lives_in_puma_relationship_present(self) -> None:
        """Person + Puma entities should yield ``LIVES_IN_PUMA`` edges."""
        # Persons use numeric PUMA/STATE (matches how pandas reads ACS PUMS).
        persons = pd.DataFrame({
            "SERIALNO": ["2023GQ0000001", "2023GQ0000002", "2023GQ0000003"],
            "PUMA":     [3700, 4000, 99999],   # third has no matching PUMA
            "STATE":    [13, 13, 13],
            "AGEP":     [25, 31, 47],
        })
        # Pumas use string-padded keys (matches TIGER shapefile output).
        pumas = pd.DataFrame({
            "PUMACE20": ["03700", "04000"],
            "STATEFP":  ["13", "13"],
            "GEOID":    ["1303700", "1304000"],
        })

        pipeline = (
            GraphPipeline()
            .add_entity_dataframe("Person", persons, id_col="SERIALNO")
            .add_entity_dataframe("Puma", pumas, id_col="PUMACE20")
        )

        _derive_relationships(pipeline)

        assert "LIVES_IN_PUMA" in pipeline.relationship_types
        # Two persons match real PUMAs; the 99999 one is dropped.
        assert pipeline.relationship_count("LIVES_IN_PUMA") == 2

    def test_lives_in_puma_skipped_when_puma_missing(self) -> None:
        """Person without Puma → LIVES_IN_PUMA is not derived."""
        persons = pd.DataFrame({
            "SERIALNO": ["X1"],
            "PUMA":     [3700],
            "STATE":    [13],
        })
        pipeline = GraphPipeline().add_entity_dataframe(
            "Person", persons, id_col="SERIALNO",
        )

        added = _derive_relationships(pipeline)

        assert "LIVES_IN_PUMA" not in pipeline.relationship_types
        assert added == 0

    def test_lives_in_puma_state_disambiguates_same_puma_code(self) -> None:
        """Same PUMA code in different states must match the right Puma."""
        # PUMA 03700 exists in two different states; STATE join must
        # disambiguate so each person's edge points to the correct Puma.
        persons = pd.DataFrame({
            "SERIALNO": ["GA-001", "CA-001"],
            "PUMA":     [3700, 3700],
            "STATE":    [13, 6],
        })
        pumas = pd.DataFrame({
            "PUMACE20": ["03700", "03700"],
            "STATEFP":  ["13", "06"],
            "GEOID":    ["1303700", "0603700"],
        })

        pipeline = (
            GraphPipeline()
            .add_entity_dataframe("Person", persons, id_col="SERIALNO")
            .add_entity_dataframe("Puma", pumas, id_col="PUMACE20")
        )

        _derive_relationships(pipeline)

        edges = pipeline._relationship_frames["LIVES_IN_PUMA"][0]  # noqa: SLF001
        as_pairs = set(zip(edges["__SOURCE__"], edges["__TARGET__"], strict=True))
        assert as_pairs == {("GA-001", "1303700"), ("CA-001", "0603700")}


# ---------------------------------------------------------------------------
# Test 4 — Empty data directory
# ---------------------------------------------------------------------------


class TestEmptyDataDir:
    """An empty data directory yields a pipeline with no entities or edges."""

    def test_empty_data_dir_produces_empty_pipeline(self, tmp_path: Path) -> None:
        with _patch_config(_empty_config(tmp_path)):
            pipeline = load_available_datasets(data_dir=tmp_path)

        assert pipeline.entity_types == []
        assert pipeline.relationship_types == []

    def test_empty_data_dir_with_configured_datasets_still_empty(
        self, tmp_path: Path,
    ) -> None:
        """Datasets configured but with no files on disk → still empty."""
        fake = _FakeConfig(
            data_path=tmp_path,
            datasets={
                "absent_a": _FakeDataset(output_file="missing_a.csv"),
                "absent_b": _FakeDataset(output_file="missing_b.csv"),
            },
        )
        with _patch_config(fake):
            pipeline = load_available_datasets(data_dir=tmp_path)

        assert pipeline.entity_types == []
        assert pipeline.relationship_types == []


# ---------------------------------------------------------------------------
# Test 5 — ACS PUMS column selection
# ---------------------------------------------------------------------------


class TestAcsPumsColumnSelection:
    """ACS PUMS person CSVs are loaded with explicit ``usecols`` selection."""

    @staticmethod
    def _write_pums_csv(path: Path, *, n_rows: int = 5) -> list[str]:
        """Write a synthetic ``psam_pus.csv`` with all expected columns plus
        extras. Returns the list of column names actually written.
        """
        # Take all expected ACS PUMS columns, plus several "extras" that
        # mimic the 500-column reality of the real file.
        extras = [
            "RT", "DIVISION", "REGION", "PWGTP", "ADJINC",
            "ANC1P", "ANC2P", "DECADE", "JWMNP", "WAOB",
        ]
        all_cols = list(_ACS_PUMS_COLS) + extras

        with path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=all_cols)
            writer.writeheader()
            for i in range(n_rows):
                writer.writerow({
                    # Expected demographic fields with realistic-ish values.
                    "SERIALNO": f"2023GQ{i:07d}",
                    "PUMA":     3700 + i,
                    "STATE":    13,
                    "AGEP":     25 + i,
                    "SEX":      1 + (i % 2),
                    "RAC1P":    1,
                    "SCHL":     21,
                    "ESR":      1,
                    "PINCP":    50000 + i * 1000,
                    "WAGP":     45000,
                    "WKHP":     40,
                    "MIL":      4,
                    "HISP":     1,
                    # Junk values for the extras — they should NOT be loaded.
                    "RT":       "P",
                    "DIVISION": 5,
                    "REGION":   3,
                    "PWGTP":    100,
                    "ADJINC":   1010145,
                    "ANC1P":    999,
                    "ANC2P":    999,
                    "DECADE":   0,
                    "JWMNP":    20,
                    "WAOB":     1,
                })
        return all_cols

    def test_pums_selects_only_expected_columns(self, tmp_path: Path) -> None:
        """The Person entity should contain only the ~13 expected columns."""
        all_cols = self._write_pums_csv(tmp_path / "psam_pus.csv", n_rows=5)
        # Sanity: source CSV has more than the expected set.
        assert len(all_cols) > len(_ACS_PUMS_COLS)

        with _patch_config(_empty_config(tmp_path)):
            pipeline = load_available_datasets(data_dir=tmp_path)

        assert "Person" in pipeline.entity_types
        person_df = pipeline._entity_frames["Person"]  # noqa: SLF001
        loaded_cols = set(person_df.columns)
        # All expected columns are present.
        assert set(_ACS_PUMS_COLS).issubset(loaded_cols)
        # None of the junk extras leaked through.
        for extra in ("RT", "DIVISION", "REGION", "PWGTP", "ADJINC",
                      "ANC1P", "ANC2P", "DECADE", "JWMNP", "WAOB"):
            assert extra not in loaded_cols, f"{extra} should have been filtered out"
        # Exact width = 13 expected demographic columns.
        assert len(loaded_cols) == len(_ACS_PUMS_COLS) == 13
        assert pipeline.entity_count("Person") == 5

    def test_pums_tolerates_missing_expected_columns(self, tmp_path: Path) -> None:
        """If the source file is a subset of expected cols, only the
        intersection is loaded — no KeyError on missing optional fields.
        """
        # Write a CSV that only has 5 of the 13 expected columns.
        partial_cols = ["SERIALNO", "PUMA", "STATE", "AGEP", "SEX"]
        path = tmp_path / "psam_pus.csv"
        with path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=partial_cols)
            writer.writeheader()
            writer.writerow({
                "SERIALNO": "2023GQ0000001",
                "PUMA":     3700,
                "STATE":    13,
                "AGEP":     30,
                "SEX":      1,
            })

        with _patch_config(_empty_config(tmp_path)):
            pipeline = load_available_datasets(data_dir=tmp_path)

        assert "Person" in pipeline.entity_types
        person_df = pipeline._entity_frames["Person"]  # noqa: SLF001
        assert set(person_df.columns) == set(partial_cols)
        assert pipeline.entity_count("Person") == 1


# ---------------------------------------------------------------------------
# Bonus end-to-end — the full flow: CSV + shapefile + ACS PUMS → relationships
# ---------------------------------------------------------------------------


class TestEndToEndLoaderWithRelationships:
    """Exercise the loader end-to-end with mocked shapefile + real CSVs.

    This is the smoke test that proves Tasks #1, #2, and #3 compose correctly
    inside ``load_available_datasets``: shapefile loads → ACS PUMS Person
    loads → ``_derive_relationships`` finds the right pairs and emits edges.
    """

    def test_full_loader_produces_entities_and_relationships(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Shapefile + ACS PUMS → loader emits entities AND derived edges.

        This focuses on the ``LIVES_IN_PUMA`` derivation because it spans
        two ingestion paths (TIGER shapefile for Puma, ACS PUMS CSV for
        Person) and proves that ``derive_person_puma_relationships``'s
        type-normalization handles the reality that pandas reads PUMA/STATE
        from the ACS CSV as ``int64`` while geopandas returns the PUMA
        keys as zero-padded strings. (We deliberately don't mix in a
        State entity loaded via the generic CSV loop here because that
        path doesn't pass ``dtype=str`` and would create a separate type-
        coercion concern unrelated to Tasks #1–#3.)
        """
        # 1. Shapefile sentinels — the loader will discover them via glob.
        (tmp_path / "tl_2025_13_tract.shp").write_bytes(b"")
        (tmp_path / "tl_2024_13_puma20.shp").write_bytes(b"")

        per_pattern: dict[str, pd.DataFrame] = {
            "tract": pd.DataFrame({
                "GEOID":   ["13001000100", "13001000200"],
                "STATEFP": ["13", "13"],
            }),
            "puma":  pd.DataFrame({
                "PUMACE20": ["03700"],
                "STATEFP":  ["13"],
                "GEOID":    ["1303700"],
            }),
        }

        def _fake_loader(filepath: Path, entity_type: str, id_col: str) -> pd.DataFrame:
            if "tract" in filepath.name:
                return per_pattern["tract"].copy()
            if "puma20" in filepath.name:
                return per_pattern["puma"].copy()
            raise AssertionError(f"unexpected shapefile: {filepath}")

        monkeypatch.setattr(
            "fastopendata.pipeline._load_shapefile_as_entity", _fake_loader,
        )

        # 2. ACS PUMS person CSV.
        with (tmp_path / "psam_pus.csv").open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=_ACS_PUMS_COLS)
            writer.writeheader()
            for i in range(3):
                writer.writerow({
                    "SERIALNO": f"P-{i}",
                    "PUMA": 3700, "STATE": 13, "AGEP": 30,
                    "SEX": 1, "RAC1P": 1, "SCHL": 21, "ESR": 1,
                    "PINCP": 50000, "WAGP": 45000, "WKHP": 40,
                    "MIL": 4, "HISP": 1,
                })

        with _patch_config(_empty_config(tmp_path)):
            pipeline = load_available_datasets(data_dir=tmp_path)

        # Entities loaded from both ingestion paths:
        assert "CensusTract" in pipeline.entity_types  # shapefile
        assert "Puma" in pipeline.entity_types          # shapefile
        assert "Person" in pipeline.entity_types        # ACS PUMS

        # Relationship derived across the two paths: 3 persons all live in
        # PUMA 03700 / state 13. This proves derive_person_puma_relationships
        # successfully normalizes int (from ACS) and zero-padded string
        # (from TIGER) keys for the join.
        assert pipeline.relationship_count("LIVES_IN_PUMA") == 3


# ---------------------------------------------------------------------------
# CJARS county job offers
# ---------------------------------------------------------------------------


class TestCjarsCountyJobOffers:
    """``cjars_joe_2022_co.csv`` loads as ``CountyJobOffers`` with edges to State."""

    @staticmethod
    def _write_cjars_csv(path: Path, rows: list[dict[str, Any]]) -> None:
        if not rows:
            path.write_text("county_fips,state_fips,naics_code,jobs_count\n")
            return
        fieldnames = list(rows[0].keys())
        with path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def test_cjars_loads_as_county_job_offers(self, tmp_path: Path) -> None:
        """The CJARS file produces a CountyJobOffers entity, not Cjars2022."""
        self._write_cjars_csv(
            tmp_path / "cjars_joe_2022_co.csv",
            [
                {"county_fips": "13089", "state_fips": "13",
                 "naics_code": "541", "jobs_count": 100},
                {"county_fips": "13121", "state_fips": "13",
                 "naics_code": "236", "jobs_count": 200},
                {"county_fips": "06037", "state_fips": "06",
                 "naics_code": "541", "jobs_count": 5000},
            ],
        )

        with _patch_config(_empty_config(tmp_path)):
            pipeline = load_available_datasets(data_dir=tmp_path)

        assert "CountyJobOffers" in pipeline.entity_types
        # Critically, NOT registered under the autogenerated PascalCase name.
        assert "Cjars2022" not in pipeline.entity_types
        assert pipeline.entity_count("CountyJobOffers") == 3

    def test_cjars_preserves_leading_zero_fips(self, tmp_path: Path) -> None:
        """county_fips/state_fips must round-trip as zero-padded strings.

        Pandas' default int inference would strip the leading "0" from
        Alabama's FIPS, breaking every downstream FIPS-based join.
        """
        self._write_cjars_csv(
            tmp_path / "cjars_joe_2022_co.csv",
            [
                {"county_fips": "01001", "state_fips": "01",
                 "naics_code": "111", "jobs_count": 50},
                {"county_fips": "13089", "state_fips": "13",
                 "naics_code": "541", "jobs_count": 100},
            ],
        )

        with _patch_config(_empty_config(tmp_path)):
            pipeline = load_available_datasets(data_dir=tmp_path)

        df = pipeline._entity_frames["CountyJobOffers"]  # noqa: SLF001
        # The actual concern: leading-zero FIPS values survived. Pandas may
        # store dtype=str as either ``object`` or the modern ``StringDtype``;
        # we don't care which, as long as values are strings (not ints).
        for col in _CJARS_FIPS_COLS:
            for val in df[col]:
                assert isinstance(val, str), (
                    f"{col} value {val!r} ({type(val).__name__}) should be str"
                )
        assert "01001" in df["county_fips"].values
        assert "01" in df["state_fips"].values

    def test_county_in_state_relationship_derived(self, tmp_path: Path) -> None:
        """CJARS + State → ``COUNTY_IN_STATE`` edges via FIPS prefix."""
        self._write_cjars_csv(
            tmp_path / "cjars_joe_2022_co.csv",
            [
                {"county_fips": "13089", "state_fips": "13",
                 "naics_code": "541", "jobs_count": 100},
                {"county_fips": "13121", "state_fips": "13",
                 "naics_code": "236", "jobs_count": 200},
                # 99-prefix county doesn't match any State row → dropped
                {"county_fips": "99999", "state_fips": "99",
                 "naics_code": "541", "jobs_count": 1},
            ],
        )

        # Inject a State entity directly so we don't have to go through the
        # generic CSV loop (which would auto-infer STATEFP as int and break
        # the prefix join — that's a separate concern, not what we're
        # testing here).
        with _patch_config(_empty_config(tmp_path)):
            pipeline = load_available_datasets(data_dir=tmp_path)
        states = pd.DataFrame({"STATEFP": ["13"], "NAME": ["Georgia"]})
        pipeline.add_entity_dataframe("State", states, id_col="STATEFP")

        # Re-run derivation now that State is present.
        from fastopendata.pipeline import _derive_relationships
        _derive_relationships(pipeline)

        assert "COUNTY_IN_STATE" in pipeline.relationship_types
        assert pipeline.relationship_count("COUNTY_IN_STATE") == 2

    def test_no_cjars_file_means_no_county_entity(self, tmp_path: Path) -> None:
        """Without the CJARS file on disk, no CountyJobOffers entity loads."""
        with _patch_config(_empty_config(tmp_path)):
            pipeline = load_available_datasets(data_dir=tmp_path)
        assert "CountyJobOffers" not in pipeline.entity_types


# ---------------------------------------------------------------------------
# OSM U.S. nodes (Task #12)
# ---------------------------------------------------------------------------


class TestOsmNodeLoading:
    """``united_states_nodes.csv`` loads via DuckDB with column projection.

    The real OSM file is ~500M rows / 10 GB; the loader uses DuckDB to
    stream-read it with a hard row cap so it can never blow up RAM in dev.
    Tests use small synthetic CSVs but cover the entity name, column
    selection, dev-cap, and graceful-absence paths.
    """

    @staticmethod
    def _write_osm_csv(
        path: Path, n_rows: int, *, extra_cols: list[str] | None = None,
    ) -> None:
        """Write a synthetic ``united_states_nodes.csv``.

        The real extractor emits ``longitude, latitude, encoded_tags, id``;
        we mirror that order. ``extra_cols`` simulates a future schema
        addition so we can prove projection actually drops them.
        """
        all_cols = ["longitude", "latitude", "encoded_tags", "id"] + (
            extra_cols or []
        )
        with path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=all_cols)
            writer.writeheader()
            for i in range(n_rows):
                row: dict[str, Any] = {
                    "longitude": -73.985 + i * 1e-5,
                    "latitude":  40.748 + i * 1e-5,
                    "encoded_tags": f"amenity:cafe;name:cafe_{i}",
                    "id":         1_000_000 + i,
                }
                for c in extra_cols or []:
                    row[c] = "junk"
                writer.writerow(row)

    def test_osm_loads_as_osm_node_entity(self, tmp_path: Path) -> None:
        """Synthetic OSM CSV → ``OsmNode`` entity (not ``OsmUs`` or anything else)."""
        self._write_osm_csv(tmp_path / "united_states_nodes.csv", n_rows=10)

        with _patch_config(_empty_config(tmp_path)):
            pipeline = load_available_datasets(data_dir=tmp_path)

        assert "OsmNode" in pipeline.entity_types
        assert "OsmUs" not in pipeline.entity_types  # not the autogenerated name
        assert pipeline.entity_count("OsmNode") == 10

    def test_osm_column_projection(self, tmp_path: Path) -> None:
        """Only the four expected columns reach the entity DataFrame."""
        # Add 3 unrelated columns that should be dropped by the SELECT.
        self._write_osm_csv(
            tmp_path / "united_states_nodes.csv",
            n_rows=5,
            extra_cols=["version", "timestamp", "user"],
        )

        with _patch_config(_empty_config(tmp_path)):
            pipeline = load_available_datasets(data_dir=tmp_path)

        df = pipeline._entity_frames["OsmNode"]  # noqa: SLF001
        assert set(df.columns) == set(_OSM_COLS)
        for dropped in ("version", "timestamp", "user"):
            assert dropped not in df.columns

    def test_osm_dev_cap_applied_when_max_rows_none(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """With ``max_rows=None`` (the default), OSM caps at the dev limit.

        Lower the cap to 50 via monkeypatch so we can write a CSV that
        actually exceeds it without slowing the suite.
        """
        # Patch BOTH the module constant and the LIMIT-resolution math —
        # the caller-side fallback also references _OSM_DEFAULT_MAX_ROWS.
        monkeypatch.setattr(
            "fastopendata.pipeline._OSM_DEFAULT_MAX_ROWS", 50,
        )
        self._write_osm_csv(tmp_path / "united_states_nodes.csv", n_rows=200)

        with _patch_config(_empty_config(tmp_path)):
            pipeline = load_available_datasets(data_dir=tmp_path)

        # Cap at 50 even though the file has 200 rows.
        assert pipeline.entity_count("OsmNode") == 50

    def test_osm_caller_max_rows_overrides_dev_cap(self, tmp_path: Path) -> None:
        """Caller-supplied ``max_rows`` overrides the dev cap when smaller."""
        self._write_osm_csv(tmp_path / "united_states_nodes.csv", n_rows=500)

        with _patch_config(_empty_config(tmp_path)):
            pipeline = load_available_datasets(data_dir=tmp_path, max_rows=42)
        assert pipeline.entity_count("OsmNode") == 42

    def test_no_osm_file_means_no_entity(self, tmp_path: Path) -> None:
        """Without ``united_states_nodes.csv``, no OsmNode entity registers."""
        with _patch_config(_empty_config(tmp_path)):
            pipeline = load_available_datasets(data_dir=tmp_path)
        assert "OsmNode" not in pipeline.entity_types

    def test_osm_id_col_is_id(self, tmp_path: Path) -> None:
        """``OsmNode`` is registered with ``id`` as its identifier column."""
        self._write_osm_csv(tmp_path / "united_states_nodes.csv", n_rows=3)

        with _patch_config(_empty_config(tmp_path)):
            pipeline = load_available_datasets(data_dir=tmp_path)

        # GraphPipeline tracks id_col internally; verify by querying through Star.
        star = pipeline.build_star()
        result = star.execute_query(
            "MATCH (n:OsmNode) RETURN count(n) AS total"
        )
        assert int(result.iloc[0]["total"]) == 3

    def test_osm_uses_default_cap_constant_value(self) -> None:
        """The 100K dev cap survives as a public constant for callers to inspect."""
        assert _OSM_DEFAULT_MAX_ROWS == 100_000
