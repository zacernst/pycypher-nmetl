"""Tests for the state contract extraction script."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest
from fastopendata.processing.extract_state_contracts import (
    POP_STATE_FIPS_COL,
    RECIPIENT_STATE_FIPS_COL,
    extract_state_contracts,
)

HEADER = [
    "contract_transaction_unique_key",
    "federal_action_obligation",
    RECIPIENT_STATE_FIPS_COL,
    "recipient_state_code",
    POP_STATE_FIPS_COL,
    "primary_place_of_performance_state_code",
]


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=HEADER)
        w.writeheader()
        w.writerows(rows)


@pytest.fixture
def sample_csvs(tmp_path: Path) -> Path:
    """Create two small contract CSV files in tmp_path."""
    data_dir = tmp_path / "raw_data"
    data_dir.mkdir()

    rows_1 = [
        {
            "contract_transaction_unique_key": "GA-001",
            "federal_action_obligation": "1000",
            RECIPIENT_STATE_FIPS_COL: "13",
            "recipient_state_code": "GA",
            POP_STATE_FIPS_COL: "13",
            "primary_place_of_performance_state_code": "GA",
        },
        {
            "contract_transaction_unique_key": "TX-001",
            "federal_action_obligation": "2000",
            RECIPIENT_STATE_FIPS_COL: "48",
            "recipient_state_code": "TX",
            POP_STATE_FIPS_COL: "48",
            "primary_place_of_performance_state_code": "TX",
        },
        {
            "contract_transaction_unique_key": "GA-POP-001",
            "federal_action_obligation": "500",
            RECIPIENT_STATE_FIPS_COL: "48",
            "recipient_state_code": "TX",
            POP_STATE_FIPS_COL: "13",
            "primary_place_of_performance_state_code": "GA",
        },
    ]
    _write_csv(data_dir / "FY2025_All_Contracts_Full_20260307_1.csv", rows_1)

    rows_2 = [
        {
            "contract_transaction_unique_key": "CA-001",
            "federal_action_obligation": "3000",
            RECIPIENT_STATE_FIPS_COL: "06",
            "recipient_state_code": "CA",
            POP_STATE_FIPS_COL: "06",
            "primary_place_of_performance_state_code": "CA",
        },
        {
            "contract_transaction_unique_key": "GA-002",
            "federal_action_obligation": "4000",
            RECIPIENT_STATE_FIPS_COL: "13",
            "recipient_state_code": "GA",
            POP_STATE_FIPS_COL: "06",
            "primary_place_of_performance_state_code": "CA",
        },
    ]
    _write_csv(data_dir / "FY2025_All_Contracts_Full_20260307_2.csv", rows_2)

    return tmp_path


class TestExtractStateContracts:
    def test_filters_georgia_rows(self, sample_csvs: Path) -> None:
        data_dir = sample_csvs / "raw_data"
        out = extract_state_contracts("13", data_dir)

        with open(out) as f:
            rows = list(csv.DictReader(f))

        keys = {r["contract_transaction_unique_key"] for r in rows}
        assert keys == {"GA-001", "GA-POP-001", "GA-002"}

    def test_excludes_non_matching_rows(self, sample_csvs: Path) -> None:
        data_dir = sample_csvs / "raw_data"
        out = extract_state_contracts("13", data_dir)

        with open(out) as f:
            rows = list(csv.DictReader(f))

        keys = {r["contract_transaction_unique_key"] for r in rows}
        assert "TX-001" not in keys
        assert "CA-001" not in keys

    def test_includes_pop_only_match(self, sample_csvs: Path) -> None:
        """Row where recipient is TX but place of performance is GA."""
        data_dir = sample_csvs / "raw_data"
        out = extract_state_contracts("13", data_dir)

        with open(out) as f:
            rows = list(csv.DictReader(f))

        pop_match = [
            r for r in rows if r["contract_transaction_unique_key"] == "GA-POP-001"
        ]
        assert len(pop_match) == 1
        assert pop_match[0][RECIPIENT_STATE_FIPS_COL] == "48"
        assert pop_match[0][POP_STATE_FIPS_COL] == "13"

    def test_includes_recipient_only_match(self, sample_csvs: Path) -> None:
        """Row where recipient is GA but place of performance is CA."""
        data_dir = sample_csvs / "raw_data"
        out = extract_state_contracts("13", data_dir)

        with open(out) as f:
            rows = list(csv.DictReader(f))

        recip_match = [
            r for r in rows if r["contract_transaction_unique_key"] == "GA-002"
        ]
        assert len(recip_match) == 1
        assert recip_match[0][RECIPIENT_STATE_FIPS_COL] == "13"
        assert recip_match[0][POP_STATE_FIPS_COL] == "06"

    def test_output_filename_default(self, sample_csvs: Path) -> None:
        data_dir = sample_csvs / "raw_data"
        out = extract_state_contracts("13", data_dir)
        assert out.name == "contracts_state_13.csv"

    def test_output_filename_custom(self, sample_csvs: Path) -> None:
        data_dir = sample_csvs / "raw_data"
        out = extract_state_contracts(
            "13", data_dir, output_filename="georgia_dev.csv"
        )
        assert out.name == "georgia_dev.csv"

    def test_preserves_all_columns(self, sample_csvs: Path) -> None:
        data_dir = sample_csvs / "raw_data"
        out = extract_state_contracts("13", data_dir)

        with open(out) as f:
            reader = csv.DictReader(f)
            assert reader.fieldnames is not None
            assert set(HEADER).issubset(set(reader.fieldnames))

    def test_different_state_fips(self, sample_csvs: Path) -> None:
        data_dir = sample_csvs / "raw_data"
        out = extract_state_contracts("48", data_dir)

        with open(out) as f:
            rows = list(csv.DictReader(f))

        keys = {r["contract_transaction_unique_key"] for r in rows}
        # TX-001 (both fields TX), GA-POP-001 (recipient TX)
        assert keys == {"TX-001", "GA-POP-001"}

    def test_empty_result(self, sample_csvs: Path) -> None:
        data_dir = sample_csvs / "raw_data"
        out = extract_state_contracts("99", data_dir)

        with open(out) as f:
            rows = list(csv.DictReader(f))

        assert len(rows) == 0


class TestFipsValidation:
    """Validate state_fips parameter rejects malicious or malformed inputs."""

    def test_valid_fips_accepted(self, sample_csvs: Path) -> None:
        data_dir = sample_csvs / "raw_data"
        out = extract_state_contracts("13", data_dir)
        assert out.exists()

    @pytest.mark.parametrize(
        "bad_fips",
        [
            "",           # empty
            "1",          # too short
            "123",        # too long
            "GA",         # letters
            "1a",         # mixed
            "../etc",     # path traversal
            "13; rm -rf", # injection attempt
            " 13",        # leading space
            "13\n",       # trailing newline
        ],
    )
    def test_invalid_fips_rejected(self, bad_fips: str, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Invalid state FIPS code"):
            extract_state_contracts(bad_fips, tmp_path)
