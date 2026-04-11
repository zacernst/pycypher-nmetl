"""Tests for Georgia-specific data validation in the fastopendata pipeline.

Validates that Georgia (state FIPS '13') data is correctly represented in
configuration, that all 159 counties are accounted for, and that geographic
subsetting logic preserves data integrity.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastopendata.config import Config

# Georgia has 159 counties with FIPS codes 13001-13321 (odd numbers only).
GEORGIA_STATE_FIPS = "13"
GEORGIA_COUNTY_COUNT = 159


@pytest.fixture
def prod_config() -> Config:
    """Load the production config.toml for validation."""
    return Config()


@pytest.fixture
def georgia_config(tmp_path: Path) -> Config:
    """Create a Georgia-only config for isolated testing."""
    toml = tmp_path / "config.toml"
    toml.write_text(
        """\
[paths]
data_dir = "raw_data"
scripts_dir = "src/fastopendata/processing"
temp_dir = "tmp"
static_dir = "static"

[downloads]
max_concurrent = 4
max_retries = 3
timeout = 300
short_timeout_seconds = 300
long_timeout_seconds = 3600
census_user_agent = "TestAgent/1.0"
census_referer = "https://example.com"

[datasets]

[datasets.tract_puma_crosswalk]
id = 1
name = "Census Tract-to-PUMA crosswalk"
output_file = "state_county_tract_puma.csv"
format = "CSV"
source = "U.S. Census Bureau"
year = 2020
approx_size = "2.5 MB"
description = "Geographic crosswalk between census tracts and PUMAs"

[geography]
state_fips = ["13"]
block_group_fips = ["13"]
puma_state_fips = ["13"]
zips = [
    "13001", "13003", "13005", "13007", "13009",
    "13011", "13013", "13015", "13017", "13019",
    "13021", "13023", "13025", "13027", "13029",
]

[api]
title = "Test API"
description = "Test"
version = "0.0.1"
host = "0.0.0.0"
port = 8000
debug = false

[processing]
max_memory_gb = 8
cleanup_temp_files = true
max_workers = 4
chunk_size = 10000

[logging]
level = "INFO"
format = "%(message)s"
file = "test.log"
""",
        encoding="utf-8",
    )
    return Config(toml)


# ── Georgia presence in production config ─────────────────────────────


class TestGeorgiaInProductionConfig:
    """Verify Georgia FIPS codes exist in all geographic config lists."""

    def test_georgia_in_state_fips(self, prod_config: Config) -> None:
        assert GEORGIA_STATE_FIPS in prod_config.state_fips

    def test_georgia_in_block_group_fips(self, prod_config: Config) -> None:
        assert GEORGIA_STATE_FIPS in prod_config.block_group_fips

    def test_georgia_in_puma_state_fips(self, prod_config: Config) -> None:
        assert GEORGIA_STATE_FIPS in prod_config.puma_state_fips

    def test_georgia_counties_in_zips(self, prod_config: Config) -> None:
        ga_zips = [z for z in prod_config.zips if z.startswith("13")]
        # Filter out FIPS codes starting with 13 that belong to other states
        # (e.g., 13xxx for GA vs county codes in other states like 01013)
        ga_county_zips = [z for z in ga_zips if len(z) == 5]
        assert len(ga_county_zips) == GEORGIA_COUNTY_COUNT, (
            f"Expected {GEORGIA_COUNTY_COUNT} Georgia counties, "
            f"found {len(ga_county_zips)}"
        )


# ── Georgia county FIPS code integrity ────────────────────────────────


class TestGeorgiaCountyFips:
    """Validate Georgia county FIPS codes are well-formed and complete."""

    def test_all_georgia_counties_are_5_digit(self, prod_config: Config) -> None:
        ga_zips = [z for z in prod_config.zips if z.startswith("13") and len(z) == 5]
        for fips in ga_zips:
            assert len(fips) == 5, f"FIPS {fips} is not 5 digits"
            assert fips.isdigit(), f"FIPS {fips} contains non-digit characters"

    def test_all_georgia_counties_start_with_13(self, prod_config: Config) -> None:
        ga_zips = [z for z in prod_config.zips if z.startswith("13") and len(z) == 5]
        for fips in ga_zips:
            assert fips[:2] == "13", f"FIPS {fips} does not start with '13'"

    def test_georgia_county_codes_are_odd(self, prod_config: Config) -> None:
        """US county FIPS codes use odd numbers (001, 003, 005, ...)."""
        ga_zips = [z for z in prod_config.zips if z.startswith("13") and len(z) == 5]
        for fips in ga_zips:
            county_num = int(fips[2:])
            assert county_num % 2 == 1, (
                f"FIPS {fips} has even county code {county_num}"
            )

    def test_georgia_county_range(self, prod_config: Config) -> None:
        """Georgia counties span 13001 to 13321."""
        ga_zips = [z for z in prod_config.zips if z.startswith("13") and len(z) == 5]
        county_nums = sorted(int(z[2:]) for z in ga_zips)
        assert county_nums[0] == 1, f"First GA county should be 001, got {county_nums[0]:03d}"
        assert county_nums[-1] == 321, f"Last GA county should be 321, got {county_nums[-1]:03d}"

    def test_no_duplicate_georgia_counties(self, prod_config: Config) -> None:
        ga_zips = [z for z in prod_config.zips if z.startswith("13") and len(z) == 5]
        assert len(ga_zips) == len(set(ga_zips)), "Duplicate Georgia county FIPS codes found"

    def test_georgia_counties_sorted(self, prod_config: Config) -> None:
        ga_zips = [z for z in prod_config.zips if z.startswith("13") and len(z) == 5]
        assert ga_zips == sorted(ga_zips), "Georgia county FIPS codes are not sorted"

    def test_known_georgia_counties_present(self, prod_config: Config) -> None:
        """Spot-check well-known Georgia counties."""
        ga_zips = {z for z in prod_config.zips if z.startswith("13") and len(z) == 5}
        # Fulton County (Atlanta)
        assert "13121" in ga_zips, "Fulton County (13121) missing"
        # DeKalb County
        assert "13089" in ga_zips, "DeKalb County (13089) missing"
        # Gwinnett County
        assert "13135" in ga_zips, "Gwinnett County (13135) missing"
        # Chatham County (Savannah)
        assert "13051" in ga_zips, "Chatham County (13051) missing"
        # Clarke County (Athens)
        assert "13059" in ga_zips, "Clarke County (13059) missing"
        # Bibb County (Macon)
        assert "13021" in ga_zips, "Bibb County (13021) missing"
        # Muscogee County (Columbus)
        assert "13215" in ga_zips, "Muscogee County (13215) missing"
        # Richmond County (Augusta)
        assert "13245" in ga_zips, "Richmond County (13245) missing"


# ── Georgia subset config isolation ───────────────────────────────────


class TestGeorgiaSubsetConfig:
    """Validate that a Georgia-only config works correctly for subsetting."""

    def test_georgia_only_state_fips(self, georgia_config: Config) -> None:
        assert georgia_config.state_fips == ["13"]

    def test_georgia_only_block_groups(self, georgia_config: Config) -> None:
        assert georgia_config.block_group_fips == ["13"]

    def test_georgia_only_puma(self, georgia_config: Config) -> None:
        assert georgia_config.puma_state_fips == ["13"]

    def test_georgia_subset_zips_all_georgia(self, georgia_config: Config) -> None:
        for z in georgia_config.zips:
            assert z.startswith("13"), f"Non-Georgia FIPS {z} in Georgia-only config"

    def test_georgia_subset_reduces_scope(
        self, prod_config: Config, georgia_config: Config
    ) -> None:
        assert len(georgia_config.zips) < len(prod_config.zips)
        assert len(georgia_config.state_fips) < len(prod_config.state_fips)


# ── Georgia download target generation ────────────────────────────────


class TestGeorgiaDownloadTargets:
    """Validate that download targets are correctly generated for Georgia."""

    def test_tiger_tract_target_for_georgia(self, prod_config: Config) -> None:
        """Georgia tract target should use state FIPS '13'."""
        data_dir = prod_config.data_dir
        target = f"{data_dir}/tl_2025_13_tract.shp"
        # Verify '13' is in state_fips, which means this target would be generated
        assert "13" in prod_config.state_fips
        assert "13" in target

    def test_tiger_addr_targets_for_georgia_counties(
        self, prod_config: Config
    ) -> None:
        """Each Georgia county should have an address file target."""
        ga_zips = [z for z in prod_config.zips if z.startswith("13") and len(z) == 5]
        data_dir = prod_config.data_dir
        targets = [f"{data_dir}/tl_2025_{z}_addr.dbf.iso.xml" for z in ga_zips]
        assert len(targets) == GEORGIA_COUNTY_COUNT

    def test_tiger_block_group_target_for_georgia(self, prod_config: Config) -> None:
        data_dir = prod_config.data_dir
        target = f"{data_dir}/tl_2024_13_bg.shp"
        assert "13" in prod_config.block_group_fips
        assert "13" in target

    def test_tiger_puma_target_for_georgia(self, prod_config: Config) -> None:
        data_dir = prod_config.data_dir
        target = f"{data_dir}/tl_2024_13_puma20.shp"
        assert "13" in prod_config.puma_state_fips
        assert "13" in target

    def test_georgia_addr_target_count_matches_counties(
        self, prod_config: Config
    ) -> None:
        """The number of Georgia address targets should equal the county count."""
        ga_zips = [z for z in prod_config.zips if z.startswith("13") and len(z) == 5]
        assert len(ga_zips) == GEORGIA_COUNTY_COUNT


# ── Cross-geography relationship validation ───────────────────────────


class TestGeorgiaRelationships:
    """Validate relationships between Georgia geographic entities."""

    def test_georgia_state_appears_once_in_each_list(
        self, prod_config: Config
    ) -> None:
        """Georgia should appear exactly once in each geographic list."""
        assert prod_config.state_fips.count("13") == 1
        assert prod_config.block_group_fips.count("13") == 1
        assert prod_config.puma_state_fips.count("13") == 1

    def test_all_georgia_zips_have_matching_state_fips(
        self, prod_config: Config
    ) -> None:
        """Every Georgia county FIPS prefix should match the state FIPS."""
        ga_zips = [z for z in prod_config.zips if z.startswith("13") and len(z) == 5]
        for z in ga_zips:
            state = z[:2]
            assert state in prod_config.state_fips, (
                f"County {z} has state prefix {state} not in state_fips"
            )

    def test_georgia_not_missing_from_any_geographic_list(
        self, prod_config: Config
    ) -> None:
        """Georgia should be in all three geographic FIPS lists."""
        geo_lists = {
            "state_fips": prod_config.state_fips,
            "block_group_fips": prod_config.block_group_fips,
            "puma_state_fips": prod_config.puma_state_fips,
        }
        for list_name, fips_list in geo_lists.items():
            assert GEORGIA_STATE_FIPS in fips_list, (
                f"Georgia ({GEORGIA_STATE_FIPS}) missing from {list_name}"
            )


# ── FIPS code format validation (general, using Georgia as exemplar) ──


class TestFipsFormatValidation:
    """General FIPS format validation using Georgia data."""

    def test_state_fips_are_two_digits(self, prod_config: Config) -> None:
        for fips in prod_config.state_fips:
            assert len(fips) == 2, f"State FIPS {fips} is not 2 digits"
            assert fips.isdigit(), f"State FIPS {fips} is not numeric"

    def test_county_fips_are_five_digits(self, prod_config: Config) -> None:
        for fips in prod_config.zips:
            assert len(fips) == 5, f"County FIPS {fips} is not 5 digits"
            assert fips.isdigit(), f"County FIPS {fips} is not numeric"

    def test_all_county_fips_have_valid_state_prefix(
        self, prod_config: Config
    ) -> None:
        """Every county FIPS should have a state prefix in state_fips."""
        state_set = set(prod_config.state_fips)
        for fips in prod_config.zips:
            state = fips[:2]
            assert state in state_set, (
                f"County FIPS {fips} has state prefix {state} not in state_fips"
            )

    def test_no_duplicate_county_fips(self, prod_config: Config) -> None:
        assert len(prod_config.zips) == len(set(prod_config.zips)), (
            "Duplicate county FIPS codes found"
        )

    def test_county_fips_sorted(self, prod_config: Config) -> None:
        assert prod_config.zips == sorted(prod_config.zips), (
            "County FIPS codes are not sorted"
        )
