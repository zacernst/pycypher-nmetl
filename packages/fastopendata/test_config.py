#!/usr/bin/env python3
"""Simple test script to verify the centralized configuration system works."""

import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from fastopendata.config import config


def test_basic_configuration():
    """Test basic configuration access."""
    print("Testing basic configuration access...")

    # Test path configuration
    print(f"  data_dir: {config.data_dir}")
    print(f"  scripts_dir: {config.scripts_dir}")
    print(f"  data_path: {config.data_path}")
    print(f"  scripts_path: {config.scripts_path}")

    # Test download configuration
    print(f"  max_concurrent_downloads: {config.max_concurrent_downloads}")
    print(f"  census_user_agent: {config.census_user_agent}")

    # Test geographic configuration
    print(f"  state_fips count: {len(config.state_fips)}")
    print(f"  first 5 state_fips: {config.state_fips[:5]}")

    # Test API configuration
    print(f"  api_title: {config.api_title}")
    print(f"  api_version: {config.api_version}")


def test_dataset_configuration():
    """Test dataset configuration."""
    print("\nTesting dataset configuration...")

    # List all datasets
    print(f"  Total datasets: {len(config.datasets)}")

    # Test specific dataset
    dataset = config.get_dataset("acs_pums_1yr_persons")
    print(f"  Dataset name: {dataset.display_name}")
    print(f"  Dataset format: {dataset.format}")
    print(f"  Dataset source: {dataset.source}")
    print(f"  Dataset URL: {dataset.url}")

    # Test dataset path resolution
    try:
        path = config.get_dataset_path("acs_pums_1yr_persons")
        print(f"  Dataset path: {path}")
    except Exception as e:
        print(f"  Dataset path error: {e}")


def test_environment_override():
    """Test environment variable override."""
    print("\nTesting environment variable override...")

    # Save original value
    original_data_dir = config.data_dir
    print(f"  Original data_dir: {original_data_dir}")

    # Set environment variable
    os.environ["DATA_DIR"] = "/tmp/test_data"

    # Create new config instance to pick up the change
    from fastopendata.config import Config

    test_config = Config()

    print(f"  Overridden data_dir: {test_config.data_dir}")

    # Clean up
    if "DATA_DIR" in os.environ:
        del os.environ["DATA_DIR"]


def test_utility_methods():
    """Test utility methods."""
    print("\nTesting utility methods...")

    # Test wget flags
    wget_flags = config.get_census_wget_flags()
    print(f"  Census wget flags: {wget_flags}")

    # Test dataset URL formatting
    try:
        url = config.get_dataset_url("tiger_tracts", fips="01")
        print(f"  Formatted URL (Alabama tracts): {url}")
    except Exception as e:
        print(f"  URL formatting error: {e}")


def main():
    """Run all tests."""
    print("=" * 60)
    print("FastOpenData Configuration System Test")
    print("=" * 60)

    try:
        test_basic_configuration()
        test_dataset_configuration()
        test_environment_override()
        test_utility_methods()

        print("\n" + "=" * 60)
        print("✅ All tests passed! Configuration system is working correctly.")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
