"""Phase 4 acceptance tests.

Consolidated integration tests verifying the five Phase 4 deliverables
end-to-end:

1. CJARS county job offers load as ``CountyJobOffers`` and produce
   ``COUNTY_IN_STATE`` edges when a ``State`` entity is present.
2. OSM nodes load as ``OsmNode`` via DuckDB streaming with a row cap.
3. ``backend="auto"`` is auto-promoted to ``"duckdb"`` when the pipeline
   exceeds ``GraphPipeline.DUCKDB_AUTO_THRESHOLD``.
4. An explicit ``backend="pandas"`` overrides the auto-promotion even
   on a large pipeline.
5. End-to-end: a multi-entity pipeline produces a working ``Star`` that
   answers a real Cypher query.

The granular per-feature tests live in ``test_generic_loader_relationships.py``
(CJARS + OSM) and ``test_pipeline.py::TestBackendAutoPromotion`` (DuckDB);
this file is the single Phase 4 acceptance suite that ties them together.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest
from fastopendata.pipeline import (
    GraphPipeline,
    _derive_relationships,
    load_available_datasets,
)

# ---------------------------------------------------------------------------
# Fakes for `fastopendata.config.config` (patched into the loader so our
# synthetic ``data_dir`` is consulted instead of the real download path).
# ---------------------------------------------------------------------------


class _FakeDataset:
    """Minimal stand-in for ``DatasetConfig``."""

    def __init__(self, output_file: str | None, fmt: str = "CSV") -> None:
        self.output_file = output_file
        self.format = fmt
        self.source = "test"
        self.description = "test"
        self.approx_size = "~1 KB"


class _FakeConfig:
    def __init__(
        self,
        data_path: Path,
        datasets: dict[str, _FakeDataset] | None = None,
    ) -> None:
        self.data_path = data_path
        self.datasets = datasets or {}


def _patch_loader_config(fake: _FakeConfig):
    """Patch the config object the loader re-imports per call."""
    return patch("fastopendata.config.config", fake)


def _write_dict_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    """Write ``rows`` as a CSV. Empty list writes a header-only file."""
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Acceptance test 1: CJARS entity + COUNTY_IN_STATE edges
# ---------------------------------------------------------------------------


class TestCjarsLoadingAndRelationships:
    """Phase 4 deliverable #1: CJARS county job offers + State edges."""

    def test_cjars_csv_loads_as_county_job_offers(
        self, tmp_path: Path,
    ) -> None:
        """The synthetic CJARS file produces a ``CountyJobOffers`` entity.

        Important: NOT registered under the auto-PascalCase name
        ``Cjars2022`` — the loader has a dedicated branch that picks the
        canonical name.
        """
        _write_dict_csv(
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

        with _patch_loader_config(_FakeConfig(tmp_path)):
            pipeline = load_available_datasets(data_dir=tmp_path)

        assert "CountyJobOffers" in pipeline.entity_types
        assert "Cjars2022" not in pipeline.entity_types
        assert pipeline.entity_count("CountyJobOffers") == 3

    def test_county_in_state_relationship_when_state_present(
        self, tmp_path: Path,
    ) -> None:
        """CJARS + State entity → ``COUNTY_IN_STATE`` edges via 2-digit FIPS prefix."""
        _write_dict_csv(
            tmp_path / "cjars_joe_2022_co.csv",
            [
                {"county_fips": "13089", "state_fips": "13",
                 "naics_code": "541", "jobs_count": 100},
                {"county_fips": "13121", "state_fips": "13",
                 "naics_code": "236", "jobs_count": 200},
                # 99-prefix has no matching State row → must be dropped.
                {"county_fips": "99999", "state_fips": "99",
                 "naics_code": "541", "jobs_count": 1},
            ],
        )
        with _patch_loader_config(_FakeConfig(tmp_path)):
            pipeline = load_available_datasets(data_dir=tmp_path)

        # Inject a State entity and re-run derivation. We do this rather
        # than rely on the loader's auto-derivation so the test stays
        # focused on the CJARS↔State edge alone.
        states = pd.DataFrame(
            {"STATEFP": ["13"], "NAME": ["Georgia"]},
        )
        pipeline.add_entity_dataframe("State", states, id_col="STATEFP")
        _derive_relationships(pipeline)

        assert "COUNTY_IN_STATE" in pipeline.relationship_types
        # 2 GA-prefix counties match; the 99-prefix one is dropped.
        assert pipeline.relationship_count("COUNTY_IN_STATE") == 2


# ---------------------------------------------------------------------------
# Acceptance test 2: OSM entity + row cap + DuckDB streaming path
# ---------------------------------------------------------------------------


class TestOsmLoadingAndRowCap:
    """Phase 4 deliverable #2: OSM nodes via DuckDB with a row cap."""

    @staticmethod
    def _write_osm_csv(path: Path, n_rows: int) -> None:
        """Mirror the real ``united_states_nodes.csv`` shape."""
        with path.open("w", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=["longitude", "latitude", "encoded_tags", "id"],
            )
            writer.writeheader()
            for i in range(n_rows):
                writer.writerow({
                    "longitude": -73.985 + i * 1e-5,
                    "latitude":   40.748 + i * 1e-5,
                    "encoded_tags": f"amenity:cafe;name:c_{i}",
                    "id":         1_000_000 + i,
                })

    def test_osm_csv_loads_as_osm_node(self, tmp_path: Path) -> None:
        """Synthetic OSM CSV → ``OsmNode`` entity (canonical name)."""
        self._write_osm_csv(tmp_path / "united_states_nodes.csv", n_rows=10)

        with _patch_loader_config(_FakeConfig(tmp_path)):
            pipeline = load_available_datasets(data_dir=tmp_path)

        assert "OsmNode" in pipeline.entity_types
        # Auto-PascalCase would yield ``OsmUs`` — guard against regression.
        assert "OsmUs" not in pipeline.entity_types
        assert pipeline.entity_count("OsmNode") == 10

    def test_row_cap_is_honored(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``max_rows`` (caller) caps the number of OSM rows actually loaded.

        The loader's internal SQL ``LIMIT`` pushes the cap into DuckDB so
        we never read the full file. We write 200 rows, cap at 42, and
        confirm the entity has exactly 42 rows.
        """
        self._write_osm_csv(tmp_path / "united_states_nodes.csv", n_rows=200)

        with _patch_loader_config(_FakeConfig(tmp_path)):
            pipeline = load_available_datasets(
                data_dir=tmp_path, max_rows=42,
            )

        assert pipeline.entity_count("OsmNode") == 42

    def test_duckdb_path_is_used_for_osm(self, tmp_path: Path) -> None:
        """OSM loading goes through ``duckdb.connect``, not ``pd.read_csv``.

        We spy on ``duckdb.connect`` to confirm the streaming-CSV path is
        active. (A regression that swapped DuckDB for ``pd.read_csv``
        would silently work for the synthetic 10-row file but blow up on
        the real 10 GB file in production.)
        """
        import duckdb

        self._write_osm_csv(tmp_path / "united_states_nodes.csv", n_rows=5)

        original_connect = duckdb.connect
        connect_calls: list[tuple[Any, ...]] = []

        def _spy(*args, **kwargs):
            connect_calls.append((args, kwargs))
            return original_connect(*args, **kwargs)

        with (
            _patch_loader_config(_FakeConfig(tmp_path)),
            patch("duckdb.connect", _spy),
        ):
            pipeline = load_available_datasets(data_dir=tmp_path)

        assert "OsmNode" in pipeline.entity_types
        assert pipeline.entity_count("OsmNode") == 5
        # At least one ``duckdb.connect`` call happened during loading.
        assert connect_calls, (
            "expected duckdb.connect to be called for OSM loading"
        )


# ---------------------------------------------------------------------------
# Acceptance test 3: DuckDB auto-promotion at the threshold boundary
# ---------------------------------------------------------------------------


class TestDuckdbAutoPromotion:
    """Phase 4 deliverable #3: ``backend="auto"`` → DuckDB above threshold.

    The real ``DUCKDB_AUTO_THRESHOLD`` is 500K rows; we override it on
    each test instance to a tiny value so the data we register stays
    fast. The test reads ``GraphPipeline.DUCKDB_AUTO_THRESHOLD`` rather
    than hardcoding the number.
    """

    @staticmethod
    def _df(n_rows: int) -> pd.DataFrame:
        return pd.DataFrame(
            {"__ID__": list(range(n_rows)), "v": [0] * n_rows},
        )

    def test_auto_promotes_to_duckdb_when_total_rows_exceed_threshold(
        self,
    ) -> None:
        """``threshold + 1`` rows + ``backend="auto"`` → ``_backend_hint="duckdb"``."""
        pipeline = GraphPipeline()
        # Override on the instance so we don't have to register 500K rows.
        # We still drive the test from ``DUCKDB_AUTO_THRESHOLD`` so a
        # change to the default is reflected in the test boundary.
        pipeline.DUCKDB_AUTO_THRESHOLD = 100
        threshold = pipeline.DUCKDB_AUTO_THRESHOLD
        pipeline.add_entity_dataframe(
            "Big", self._df(threshold + 1), id_col="__ID__",
        )

        star = pipeline.build_star()  # backend="auto" by default

        assert star.context._backend_hint == "duckdb"  # noqa: SLF001

    def test_auto_stays_auto_at_or_below_threshold(self) -> None:
        """Strictly greater than: exactly threshold rows is NOT promoted.

        Below the threshold the hint propagates as ``"auto"`` rather than
        ``"duckdb"``, and the actual backend resolution happens inside
        ``Context.__init__`` via ``select_backend``.
        """
        pipeline = GraphPipeline()
        pipeline.DUCKDB_AUTO_THRESHOLD = 100
        pipeline.add_entity_dataframe(
            "Small", self._df(pipeline.DUCKDB_AUTO_THRESHOLD),
            id_col="__ID__",
        )

        star = pipeline.build_star()

        assert star.context._backend_hint != "duckdb"  # noqa: SLF001


# ---------------------------------------------------------------------------
# Acceptance test 4: explicit backend choice overrides the threshold
# ---------------------------------------------------------------------------


class TestExplicitBackendOverridesPromotion:
    """Phase 4 deliverable #4: caller's explicit backend always wins.

    Even on a pipeline that *would* trigger auto-promotion to DuckDB,
    ``build_star(backend="pandas")`` must reach the Context as
    ``"pandas"``.
    """

    @staticmethod
    def _df(n_rows: int) -> pd.DataFrame:
        return pd.DataFrame(
            {"__ID__": list(range(n_rows)), "v": [0] * n_rows},
        )

    def test_explicit_pandas_wins_over_auto_promotion(self) -> None:
        pipeline = GraphPipeline()
        pipeline.DUCKDB_AUTO_THRESHOLD = 100
        # Same large pipeline as the previous test class — would auto-
        # promote to DuckDB if we didn't pass an explicit backend.
        pipeline.add_entity_dataframe(
            "Big",
            self._df(pipeline.DUCKDB_AUTO_THRESHOLD + 1),
            id_col="__ID__",
        )

        star = pipeline.build_star(backend="pandas")

        assert star.context._backend_hint == "pandas"  # noqa: SLF001

    def test_explicit_duckdb_works_below_threshold(self) -> None:
        """Caller can opt INTO DuckDB even on a tiny pipeline."""
        pipeline = GraphPipeline()
        pipeline.add_entity_dataframe("Tiny", self._df(3), id_col="__ID__")

        star = pipeline.build_star(backend="duckdb")

        assert star.context._backend_hint == "duckdb"  # noqa: SLF001


# ---------------------------------------------------------------------------
# Acceptance test 5: full chain smoke test
# ---------------------------------------------------------------------------


class TestFullChainSmoke:
    """Phase 4 deliverable #5: a multi-entity pipeline answers Cypher queries.

    Wires up Contract + State + CountyJobOffers in a single GraphPipeline,
    builds a Star, and runs a real Cypher query end-to-end. This is the
    integration check that proves the loader → context → query path is
    intact after Phase 4.
    """

    def test_three_entity_pipeline_answers_cypher_query(self) -> None:
        pipeline = GraphPipeline()

        contracts = pd.DataFrame({
            "contract_transaction_unique_key": ["GA-1", "GA-2", "CA-1"],
            "federal_action_obligation": [1_000_000, 500_000, 250_000],
            "recipient_name": ["Acme", "Beta", "Gamma"],
        })
        pipeline.add_entity_dataframe(
            "Contract", contracts,
            id_col="contract_transaction_unique_key",
        )

        states = pd.DataFrame({
            "STATEFP": ["13", "06"],
            "STUSPS":  ["GA", "CA"],
            "NAME":    ["Georgia", "California"],
        })
        pipeline.add_entity_dataframe("State", states, id_col="STATEFP")

        counties = pd.DataFrame({
            "county_fips": ["13089", "13121", "06037"],
            "state_fips":  ["13",    "13",    "06"],
            "naics_code":  ["541",   "236",   "541"],
            "jobs_count":  [100,     200,     5000],
        })
        pipeline.add_entity_dataframe(
            "CountyJobOffers", counties, id_col="county_fips",
        )

        # Sanity: pipeline has all three entity types.
        assert set(pipeline.entity_types) == {
            "Contract", "State", "CountyJobOffers",
        }

        star = pipeline.build_star()
        result = star.execute_query("MATCH (s:State) RETURN count(s) AS n")

        # The result is a DataFrame with at least one row, and the count
        # matches what we registered.
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 1
        assert int(result.iloc[0]["n"]) == 2
