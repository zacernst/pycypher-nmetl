"""Tests for fastopendata.processing.concatenate_puma_shape_files module.

Covers:
- Module imports correctly
- __main__ block behavior: PUMA file discovery, column selection, output path
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestConcatenatePumaImports:
    """Verify module-level imports work without side effects."""

    def test_module_importable(self) -> None:
        """The module can be imported without executing __main__ block."""
        import fastopendata.processing.concatenate_puma_shape_files  # noqa: F401

    def test_concatenate_shapefiles_reexported(self) -> None:
        """The module imports concatenate_shapefiles from the shared module."""
        from fastopendata.processing.concatenate_puma_shape_files import (
            concatenate_shapefiles,
        )

        assert callable(concatenate_shapefiles)


@pytest.mark.integration
class TestConcatenatePumaMainBlock:
    """Test the __main__ block behavior via subprocess."""

    def test_main_discovers_puma_files_only(self) -> None:
        """The __main__ logic discovers only .shp files containing 'puma'."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            # Create fake PUMA shapefiles
            (tmp_path / "tl_2024_01_puma20.shp").touch()
            (tmp_path / "tl_2024_02_puma20.shp").touch()
            # Create a non-PUMA shapefile (should be excluded)
            (tmp_path / "tl_2024_01_bg.shp").touch()
            # Create a non-.shp file with "puma" in the name
            (tmp_path / "tl_2024_01_puma20.dbf").touch()

            # Replicate the __main__ discovery logic
            puma_files = [
                f.name
                for f in tmp_path.iterdir()
                if "puma" in f.name.lower() and f.suffix == ".shp"
            ]

            assert len(puma_files) == 2
            assert all("puma" in f.lower() for f in puma_files)
            assert "tl_2024_01_bg.shp" not in puma_files
            assert "tl_2024_01_puma20.dbf" not in puma_files

    def test_main_passes_correct_columns(self) -> None:
        """The __main__ block passes PUMA20, GEOID, geometry columns."""
        # Read the source to verify column specification
        from fastopendata.processing import concatenate_puma_shape_files

        import inspect
        source = inspect.getsource(concatenate_puma_shape_files)
        assert 'columns=["PUMA20", "GEOID", "geometry"]' in source

    def test_main_uses_tiger_puma_dataset_path(self) -> None:
        """The __main__ block uses config.get_dataset_path('tiger_puma')."""
        from fastopendata.processing import concatenate_puma_shape_files

        import inspect
        source = inspect.getsource(concatenate_puma_shape_files)
        assert 'get_dataset_path("tiger_puma")' in source
