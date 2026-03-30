"""Tests for fastopendata.processing modules.

Covers:
- extract_osm_nodes._is_trivial() tag filtering
- concatenate_shape_files.concatenate_shapefiles() merging logic
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

# ── extract_osm_nodes._is_trivial ────────────────────────────────────


class TestIsTrivial:
    """Test the _is_trivial() tag key classifier from extract_osm_nodes."""

    @pytest.fixture(autouse=True)
    def _import_fn(self) -> None:
        from fastopendata.processing.extract_osm_nodes import _is_trivial

        self.is_trivial = _is_trivial

    def test_exact_trivial_keys(self) -> None:
        for key in ("highway", "name", "barrier", "ref", "source", "tiger", "railway"):
            assert self.is_trivial(key) is True, f"{key} should be trivial"

    def test_prefixed_trivial_keys(self) -> None:
        assert self.is_trivial("tiger:name_base") is True
        assert self.is_trivial("name:en") is True
        assert self.is_trivial("source:url") is True
        assert self.is_trivial("old_name:de") is True

    def test_non_trivial_keys(self) -> None:
        for key in ("amenity", "cuisine", "building", "shop", "tourism", "leisure"):
            assert self.is_trivial(key) is False, f"{key} should NOT be trivial"

    def test_non_trivial_with_colon(self) -> None:
        assert self.is_trivial("amenity:type") is False
        assert self.is_trivial("building:levels") is False

    def test_empty_key(self) -> None:
        # Empty string is NOT in _TRIVIAL_ROOTS
        assert self.is_trivial("") is False

    def test_case_sensitive_fixme(self) -> None:
        # Both "fixme" and "FIXME" are in the trivial roots
        assert self.is_trivial("fixme") is True
        assert self.is_trivial("FIXME") is True

    def test_created_by(self) -> None:
        assert self.is_trivial("created_by") is True

    def test_access_variants(self) -> None:
        assert self.is_trivial("access") is True
        assert self.is_trivial("access_ref") is True


# ── concatenate_shape_files.concatenate_shapefiles ───────────────────


class TestConcatenateShapefiles:
    """Test the concatenate_shapefiles() function."""

    def test_empty_file_list_raises(self) -> None:
        from fastopendata.processing.concatenate_shape_files import (
            concatenate_shapefiles,
        )

        with pytest.raises(FileNotFoundError, match="No shapefiles"):
            concatenate_shapefiles([], "/tmp/data", "/tmp/output.shp")

    @patch("fastopendata.processing.concatenate_shape_files.pyogrio")
    def test_single_file_writes_fresh(self, mock_pyogrio: MagicMock) -> None:
        from fastopendata.processing.concatenate_shape_files import (
            concatenate_shapefiles,
        )

        mock_frame = MagicMock()
        mock_pyogrio.read_dataframe.return_value = mock_frame

        concatenate_shapefiles(["a.shp"], "/data", "/out/merged.shp")

        mock_pyogrio.read_dataframe.assert_called_once_with(
            Path("/data/a.shp"),
        )
        mock_pyogrio.write_dataframe.assert_called_once_with(
            mock_frame,
            Path("/out/merged.shp"),
        )

    @patch("fastopendata.processing.concatenate_shape_files.pyogrio")
    def test_multiple_files_appends_after_first(self, mock_pyogrio: MagicMock) -> None:
        from fastopendata.processing.concatenate_shape_files import (
            concatenate_shapefiles,
        )

        frames = [MagicMock(name=f"frame_{i}") for i in range(3)]
        mock_pyogrio.read_dataframe.side_effect = frames

        concatenate_shapefiles(
            ["a.shp", "b.shp", "c.shp"],
            "/data",
            "/out/merged.shp",
        )

        assert mock_pyogrio.read_dataframe.call_count == 3
        write_calls = mock_pyogrio.write_dataframe.call_args_list
        assert len(write_calls) == 3
        # First write: no append
        assert write_calls[0] == call(frames[0], Path("/out/merged.shp"))
        # Second and third: append=True
        assert write_calls[1] == call(frames[1], Path("/out/merged.shp"), append=True)
        assert write_calls[2] == call(frames[2], Path("/out/merged.shp"), append=True)

    @patch("fastopendata.processing.concatenate_shape_files.pyogrio")
    def test_columns_passed_to_read(self, mock_pyogrio: MagicMock) -> None:
        from fastopendata.processing.concatenate_shape_files import (
            concatenate_shapefiles,
        )

        mock_pyogrio.read_dataframe.return_value = MagicMock()

        concatenate_shapefiles(
            ["a.shp"],
            "/data",
            "/out/merged.shp",
            columns=["GEOID", "geometry"],
        )

        mock_pyogrio.read_dataframe.assert_called_once_with(
            Path("/data/a.shp"),
            columns=["GEOID", "geometry"],
        )

    @patch("fastopendata.processing.concatenate_shape_files.pyogrio")
    def test_corrupt_file_skipped(self, mock_pyogrio: MagicMock) -> None:
        from fastopendata.processing.concatenate_shape_files import (
            concatenate_shapefiles,
        )

        good_frame = MagicMock()
        mock_pyogrio.read_dataframe.side_effect = [
            RuntimeError("corrupt"),
            good_frame,
        ]

        concatenate_shapefiles(
            ["bad.shp", "good.shp"],
            "/data",
            "/out/merged.shp",
        )

        # Only one write (the good file)
        mock_pyogrio.write_dataframe.assert_called_once_with(
            good_frame,
            Path("/out/merged.shp"),
        )

    @patch("fastopendata.processing.concatenate_shape_files.pyogrio")
    def test_all_files_corrupt_raises(self, mock_pyogrio: MagicMock) -> None:
        from fastopendata.processing.concatenate_shape_files import (
            concatenate_shapefiles,
        )

        mock_pyogrio.read_dataframe.side_effect = RuntimeError("corrupt")

        with pytest.raises(RuntimeError, match="All .* shapefiles failed"):
            concatenate_shapefiles(
                ["bad1.shp", "bad2.shp"],
                "/data",
                "/out/merged.shp",
            )

    @patch("fastopendata.processing.concatenate_shape_files.pyogrio")
    def test_path_types_accepted(self, mock_pyogrio: MagicMock) -> None:
        from fastopendata.processing.concatenate_shape_files import (
            concatenate_shapefiles,
        )

        mock_pyogrio.read_dataframe.return_value = MagicMock()

        # Should accept Path objects for directory and output
        concatenate_shapefiles(
            ["a.shp"],
            Path("/data"),
            Path("/out/merged.shp"),
        )

        mock_pyogrio.read_dataframe.assert_called_once()
