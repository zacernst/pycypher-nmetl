"""Phase 3 acceptance tests.

Consolidated integration tests verifying the five Phase 3 deliverables
end-to-end:

1. ``build_state_pipeline`` works for any FIPS (not just Georgia) and
   produces a ``GraphPipeline`` with both entities and relationships.
2. ``STATE_FIPS`` env var routes ``api._load_datasets_into_star`` to the
   matching state pipeline.
3. ``PipelineConfig.state_fips`` is a settable string field with default
   ``"13"`` (Georgia).
4. ``ConfigManager.set_state_fips`` / ``get_state_fips`` round-trip the
   value and reject invalid input.
5. ``StateSelector`` (the TUI screen) imports cleanly from
   ``pycypher_tui.screens.state_selector``.

The granular per-feature tests live in ``test_etl_pipeline.py``,
``test_api.py``, ``packages/pycypher-tui/tests/test_state_selector.py``,
and ``tests/test_config.py``; this file is the single Phase 3 acceptance
suite that ties them together.
"""

from __future__ import annotations

import csv
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Synthetic-data helpers (small CSV fixtures written to tmp_path)
# ---------------------------------------------------------------------------

_CONTRACT_FIELDNAMES = [
    "contract_transaction_unique_key",
    "federal_action_obligation",
    "prime_award_transaction_recipient_state_fips_code",
    "recipient_state_code",
    "prime_award_transaction_place_of_performance_state_fips_code",
    "primary_place_of_performance_state_code",
    "recipient_name",
    "naics_code",
    "award_type",
    "action_date",
]


def _write_contracts_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_CONTRACT_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def _write_crosswalk_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["STATEFP", "COUNTYFP", "TRACTCE", "PUMA5CE"],
        )
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture
def california_data_dir(tmp_path: Path) -> Path:
    """Synthetic California (FIPS 06) data: 3 contracts + 2 crosswalk rows.

    Two of the three contracts are CA→CA (recipient + POP both CA) and
    one is GA→CA (recipient GA, POP CA), so the resulting pipeline must
    surface both ``PERFORMED_IN_STATE`` and ``AWARDED_IN_STATE`` edges.
    """
    data_dir = tmp_path / "raw_data"
    data_dir.mkdir()

    contracts = [
        {
            "contract_transaction_unique_key": "CA-001",
            "federal_action_obligation": "1500000",
            "prime_award_transaction_recipient_state_fips_code": "06",
            "recipient_state_code": "CA",
            "prime_award_transaction_place_of_performance_state_fips_code": "06",
            "primary_place_of_performance_state_code": "CA",
            "recipient_name": "GoldenStateCo",
            "naics_code": "541512",
            "award_type": "D",
            "action_date": "2025-04-01",
        },
        {
            "contract_transaction_unique_key": "CA-002",
            "federal_action_obligation": "750000",
            "prime_award_transaction_recipient_state_fips_code": "06",
            "recipient_state_code": "CA",
            "prime_award_transaction_place_of_performance_state_fips_code": "06",
            "primary_place_of_performance_state_code": "CA",
            "recipient_name": "BayCorp",
            "naics_code": "236220",
            "award_type": "C",
            "action_date": "2025-05-12",
        },
        {
            "contract_transaction_unique_key": "CA-003",
            "federal_action_obligation": "250000",
            "prime_award_transaction_recipient_state_fips_code": "13",  # GA recipient
            "recipient_state_code": "GA",
            "prime_award_transaction_place_of_performance_state_fips_code": "06",
            "primary_place_of_performance_state_code": "CA",
            "recipient_name": "OutOfStateCo",
            "naics_code": "541330",
            "award_type": "D",
            "action_date": "2025-06-20",
        },
    ]
    _write_contracts_csv(data_dir / "contracts_state_06.csv", contracts)

    crosswalk = [
        {"STATEFP": "06", "COUNTYFP": "037", "TRACTCE": "123400", "PUMA5CE": "06500"},
        {"STATEFP": "06", "COUNTYFP": "075", "TRACTCE": "012345", "PUMA5CE": "07500"},
    ]
    _write_crosswalk_csv(data_dir / "state_county_tract_puma.csv", crosswalk)

    return data_dir


# ---------------------------------------------------------------------------
# Acceptance test 1: build_state_pipeline generalization (non-Georgia FIPS)
# ---------------------------------------------------------------------------


class TestBuildStatePipelineGeneralized:
    """Phase 3 deliverable #1: ``build_state_pipeline`` accepts any FIPS."""

    def test_california_pipeline_returns_graph_pipeline(
        self, california_data_dir: Path,
    ) -> None:
        """A California build returns a fully-formed ``GraphPipeline``."""
        from fastopendata.etl.state_pipeline import build_state_pipeline
        from fastopendata.pipeline import GraphPipeline

        pipeline = build_state_pipeline(
            california_data_dir, state_fips="06",
        )
        assert isinstance(pipeline, GraphPipeline)

    def test_california_pipeline_has_contract_and_state_entities(
        self, california_data_dir: Path,
    ) -> None:
        """Both the per-row Contract entity and the synthetic State entity exist."""
        from fastopendata.etl.state_pipeline import build_state_pipeline

        pipeline = build_state_pipeline(
            california_data_dir, state_fips="06",
        )
        assert "Contract" in pipeline.entity_types
        assert "State" in pipeline.entity_types
        # 3 contracts in the synthetic fixture.
        assert pipeline.entity_count("Contract") == 3
        # Exactly one synthetic State entity per pipeline.
        assert pipeline.entity_count("State") == 1

    def test_california_pipeline_derives_relationships(
        self, california_data_dir: Path,
    ) -> None:
        """At least one relationship type is present (per Phase 3 acceptance)."""
        from fastopendata.etl.state_pipeline import build_state_pipeline

        pipeline = build_state_pipeline(
            california_data_dir, state_fips="06",
        )
        # The generic (non-Georgia) path must derive at least one
        # relationship type — otherwise Phase 3 is a regression vs. the
        # Georgia-only behaviour.
        assert len(pipeline.relationship_types) >= 1
        # Specifically, all three contracts have POP=CA so we should see
        # PERFORMED_IN_STATE; two are CA recipients so AWARDED_IN_STATE
        # also lands; both crosswalk rows produce MAPS_TO_PUMA edges.
        assert "PERFORMED_IN_STATE" in pipeline.relationship_types
        assert "AWARDED_IN_STATE" in pipeline.relationship_types
        assert "MAPS_TO_PUMA" in pipeline.relationship_types

    def test_california_pipeline_uses_california_state_label(
        self, california_data_dir: Path,
    ) -> None:
        """The synthetic State entity carries the CA name + abbreviation."""
        from fastopendata.etl.state_pipeline import build_state_pipeline

        star = build_state_pipeline(
            california_data_dir, state_fips="06",
        ).build_star()
        result = star.execute_query(
            "MATCH (s:State) RETURN s.NAME AS name, s.STUSPS AS abbrev",
        )
        assert result.iloc[0]["name"] == "California"
        assert result.iloc[0]["abbrev"] == "CA"


# ---------------------------------------------------------------------------
# Acceptance test 2: STATE_FIPS env var routes api._load_datasets_into_star
# ---------------------------------------------------------------------------


class TestStateFipsEnvVarRouting:
    """Phase 3 deliverable #2: ``STATE_FIPS`` env var → api state pipeline."""

    @staticmethod
    def _seed_state_files(data_dir: Path, state_fips: str) -> None:
        """Create empty contracts + crosswalk so the env-var branch is taken.

        ``_load_datasets_into_star`` only calls ``build_state_pipeline``
        when both files exist; otherwise it falls back to generic CSV
        discovery, which would bypass the env-var check.
        """
        (data_dir / f"contracts_state_{state_fips}.csv").write_text("")
        (data_dir / "state_county_tract_puma.csv").write_text("")

    def test_state_fips_06_routes_to_california(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``STATE_FIPS=06`` causes the loader to call ``build_state_pipeline``
        with ``state_fips="06"``.
        """
        from fastopendata.api import _load_datasets_into_star
        from fastopendata.pipeline import GraphPipeline

        self._seed_state_files(tmp_path, "06")

        # Patch ``api.config`` (the api-local binding produced by
        # ``from .config import config``) so ``data_dir = config.data_path``
        # sees our temp directory.
        fake_config = MagicMock()
        fake_config.data_path = tmp_path
        monkeypatch.setattr("fastopendata.api.config", fake_config)

        captured: dict[str, str] = {}

        def fake_build(data_dir, *, state_fips, **_kwargs):
            captured["state_fips"] = state_fips
            return GraphPipeline()

        monkeypatch.setattr(
            "fastopendata.etl.state_pipeline.build_state_pipeline", fake_build,
        )
        monkeypatch.setenv("STATE_FIPS", "06")

        _load_datasets_into_star()

        assert captured.get("state_fips") == "06"

    def test_default_when_env_var_unset_is_georgia(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Without ``STATE_FIPS``, the loader defaults to FIPS ``"13"``."""
        from fastopendata.api import _load_datasets_into_star
        from fastopendata.pipeline import GraphPipeline

        self._seed_state_files(tmp_path, "13")

        fake_config = MagicMock()
        fake_config.data_path = tmp_path
        monkeypatch.setattr("fastopendata.api.config", fake_config)

        captured: dict[str, str] = {}

        def fake_build(data_dir, *, state_fips, **_kwargs):
            captured["state_fips"] = state_fips
            return GraphPipeline()

        monkeypatch.setattr(
            "fastopendata.etl.state_pipeline.build_state_pipeline", fake_build,
        )
        monkeypatch.delenv("STATE_FIPS", raising=False)

        _load_datasets_into_star()

        assert captured.get("state_fips") == "13"


# ---------------------------------------------------------------------------
# Acceptance test 3: PipelineConfig.state_fips field
# ---------------------------------------------------------------------------


class TestPipelineConfigStateFipsField:
    """Phase 3 deliverable #3: ``PipelineConfig.state_fips`` field exists."""

    def test_default_state_fips_is_georgia(self) -> None:
        """Default value is ``"13"`` (Georgia) for backward compatibility."""
        from pycypher.ingestion.config import PipelineConfig

        cfg = PipelineConfig()
        assert cfg.state_fips == "13"

    def test_state_fips_can_be_overridden(self) -> None:
        """Field is settable on construction."""
        from pycypher.ingestion.config import PipelineConfig

        cfg = PipelineConfig(state_fips="06")
        assert cfg.state_fips == "06"


# ---------------------------------------------------------------------------
# Acceptance test 4: ConfigManager.set_state_fips / get_state_fips
# ---------------------------------------------------------------------------


class TestConfigManagerStateFipsRoundTrip:
    """Phase 3 deliverable #4: ConfigManager state_fips round-trip + validation."""

    def test_set_then_get_round_trips(self) -> None:
        """``set_state_fips("48")`` makes ``get_state_fips()`` return ``"48"``."""
        from pycypher_tui.config.pipeline import ConfigManager

        cm = ConfigManager()
        cm.set_state_fips("48")
        assert cm.get_state_fips() == "48"
        # And the underlying config model is the source of truth.
        assert cm.get_config().state_fips == "48"

    def test_invalid_state_fips_raises_value_error(self) -> None:
        """Anything that isn't exactly two digits is rejected."""
        from pycypher_tui.config.pipeline import ConfigManager

        cm = ConfigManager()
        for bad in ("1", "131", "AB", "", "1A"):
            with pytest.raises(ValueError, match="2-digit"):
                cm.set_state_fips(bad)
        # Failure must not corrupt the existing value.
        assert cm.get_state_fips() == "13"


# ---------------------------------------------------------------------------
# Acceptance test 5: StateSelector imports cleanly
# ---------------------------------------------------------------------------


class TestStateSelectorImport:
    """Phase 3 deliverable #5: TUI screen module loads without errors."""

    def test_state_selector_imports(self) -> None:
        """``from pycypher_tui.screens.state_selector import StateSelector`` works."""
        from pycypher_tui.screens.state_selector import StateSelector

        # Sanity: the message class advertised in the task description
        # exists on the screen and carries the documented attributes.
        msg = StateSelector.StateSelected(
            state_fips="06", state_name="California",
        )
        assert msg.state_fips == "06"
        assert msg.state_name == "California"
