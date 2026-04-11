"""Tests for fastopendata.processing.filter_us_nodes module.

Covers:
- _in_us() point-in-polygon test
- _worker() entity processing and coordinate extraction
- _writer() result collection
- _reader() bz2 decompression and queue feeding
- __main__ block validation of required files
"""

from __future__ import annotations

import bz2
import json
import multiprocessing as mp
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.mark.integration
class TestInUs:
    """Test the _in_us() point-in-polygon function."""

    def test_point_with_intersecting_candidates(self) -> None:
        from fastopendata.processing.filter_us_nodes import _in_us

        mock_gdf = MagicMock()
        mock_gdf.sindex.query.return_value = [0, 1]  # Two intersecting polygons

        assert _in_us(mock_gdf, -77.0, 38.9) is True

    def test_point_with_no_candidates(self) -> None:
        from fastopendata.processing.filter_us_nodes import _in_us

        mock_gdf = MagicMock()
        mock_gdf.sindex.query.return_value = []  # No intersections

        assert _in_us(mock_gdf, 0.0, 0.0) is False

    def test_point_query_uses_intersects_predicate(self) -> None:
        from fastopendata.processing.filter_us_nodes import _in_us

        mock_gdf = MagicMock()
        mock_gdf.sindex.query.return_value = []

        _in_us(mock_gdf, 10.0, 20.0)

        # Verify the spatial index query uses the correct predicate
        call_args = mock_gdf.sindex.query.call_args
        assert call_args[1]["predicate"] == "intersects"


class TestWorker:
    """Test the _worker() function that processes entities."""

    def _make_entity_bytes(
        self,
        *,
        entity_id: str = "Q42",
        longitude: float = -77.0,
        latitude: float = 38.9,
    ) -> bytes:
        """Create entity bytes with separator and counter."""
        from fastopendata.processing.filter_us_nodes import _SEP

        entity = {
            "id": entity_id,
            "claims": {
                "P625": [
                    {
                        "mainsnak": {
                            "datavalue": {
                                "value": {
                                    "latitude": latitude,
                                    "longitude": longitude,
                                }
                            }
                        }
                    }
                ]
            },
        }
        return json.dumps(entity).encode() + _SEP + b"42"

    @patch("fastopendata.processing.filter_us_nodes._load_state_gdf")
    def test_worker_sends_matching_entity_to_write_queue(
        self, mock_load_gdf: MagicMock
    ) -> None:
        from fastopendata.processing.filter_us_nodes import _worker

        mock_gdf = MagicMock()
        mock_gdf.sindex.query.return_value = [0]  # Point is in US
        mock_load_gdf.return_value = mock_gdf

        jobs_queue: mp.Queue = mp.Queue()
        write_queue: mp.Queue = mp.Queue()

        entity_bytes = self._make_entity_bytes()
        jobs_queue.put(entity_bytes)
        jobs_queue.put(None)  # Sentinel to stop worker

        _worker(jobs_queue, write_queue)

        result = write_queue.get(timeout=1)
        assert result["id"] == "Q42"
        assert result["_counter"] == 42

        # Worker sends None sentinel when done
        sentinel = write_queue.get(timeout=1)
        assert sentinel is None

    @patch("fastopendata.processing.filter_us_nodes._load_state_gdf")
    def test_worker_skips_non_us_entity(self, mock_load_gdf: MagicMock) -> None:
        from fastopendata.processing.filter_us_nodes import _worker

        mock_gdf = MagicMock()
        mock_gdf.sindex.query.return_value = []  # Not in US
        mock_load_gdf.return_value = mock_gdf

        jobs_queue: mp.Queue = mp.Queue()
        write_queue: mp.Queue = mp.Queue()

        entity_bytes = self._make_entity_bytes()
        jobs_queue.put(entity_bytes)
        jobs_queue.put(None)

        _worker(jobs_queue, write_queue)

        # Only the sentinel None should be in write_queue
        result = write_queue.get(timeout=1)
        assert result is None

    @patch("fastopendata.processing.filter_us_nodes._load_state_gdf")
    def test_worker_skips_entity_without_coordinates(
        self, mock_load_gdf: MagicMock
    ) -> None:
        from fastopendata.processing.filter_us_nodes import _SEP, _worker

        mock_load_gdf.return_value = MagicMock()

        jobs_queue: mp.Queue = mp.Queue()
        write_queue: mp.Queue = mp.Queue()

        # Entity without P625 claims
        entity = {"id": "Q99", "claims": {}}
        item = json.dumps(entity).encode() + _SEP + b"1"
        jobs_queue.put(item)
        jobs_queue.put(None)

        _worker(jobs_queue, write_queue)

        # Only sentinel
        result = write_queue.get(timeout=1)
        assert result is None

    @patch("fastopendata.processing.filter_us_nodes._load_state_gdf")
    def test_worker_skips_malformed_json(self, mock_load_gdf: MagicMock) -> None:
        from fastopendata.processing.filter_us_nodes import _SEP, _worker

        mock_load_gdf.return_value = MagicMock()

        jobs_queue: mp.Queue = mp.Queue()
        write_queue: mp.Queue = mp.Queue()

        # Malformed JSON
        item = b"not valid json" + _SEP + b"1"
        jobs_queue.put(item)
        jobs_queue.put(None)

        _worker(jobs_queue, write_queue)

        # Only sentinel, malformed skipped
        result = write_queue.get(timeout=1)
        assert result is None


class TestWriter:
    """Test the _writer() result collection function."""

    @patch("fastopendata.processing.filter_us_nodes.Progress")
    @patch("fastopendata.processing.filter_us_nodes._NUM_WORKERS", 2)
    def test_writer_writes_entities_to_file(self, mock_progress: MagicMock) -> None:
        from fastopendata.processing.filter_us_nodes import _writer

        mock_ctx = MagicMock()
        mock_progress.return_value.__enter__ = MagicMock(return_value=mock_ctx)
        mock_progress.return_value.__exit__ = MagicMock(return_value=False)
        mock_ctx.add_task.return_value = 0

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_path = Path(f.name)

        write_queue: mp.Queue = mp.Queue()
        entity1 = {"id": "Q1", "_counter": 1}
        entity2 = {"id": "Q2", "_counter": 2}

        write_queue.put(entity1)
        write_queue.put(entity2)
        write_queue.put(None)  # Worker 1 done
        write_queue.put(None)  # Worker 2 done

        with patch(
            "fastopendata.processing.filter_us_nodes.OUTPUT_FILE", output_path
        ):
            _writer(write_queue)

        lines = output_path.read_text().strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0])["id"] == "Q1"
        assert json.loads(lines[1])["id"] == "Q2"

        output_path.unlink()


class TestReader:
    """Test the _reader() bz2 decompression and queue feeding."""

    def test_reader_pushes_lines_to_queue(self) -> None:
        from queue import Empty

        from fastopendata.processing.filter_us_nodes import _SEP, _reader

        # Create a temporary bz2 file with test data
        entity1 = json.dumps({"id": "Q1", "latitude": 1.0})
        entity2 = json.dumps({"id": "Q2", "latitude": 2.0})
        content = (entity1 + "\n" + entity2 + "\n").encode()

        with tempfile.NamedTemporaryFile(suffix=".json.bz2", delete=False) as f:
            f.write(bz2.compress(content))
            bz2_path = f.name

        jobs_queue: mp.Queue = mp.Queue()

        with patch(
            "fastopendata.processing.filter_us_nodes.INPUT_FILE", bz2_path
        ):
            _reader(jobs_queue)

        # Drain queue using get with timeout (more reliable than .empty())
        items = []
        while True:
            try:
                items.append(jobs_queue.get(timeout=2))
            except Empty:
                break

        assert len(items) == 2

        # Each item should contain the separator with counter
        for item in items:
            assert _SEP in item

        Path(bz2_path).unlink()


class TestModuleConstants:
    """Test module-level constants and configuration."""

    def test_sep_is_bytes(self) -> None:
        from fastopendata.processing.filter_us_nodes import _SEP

        assert isinstance(_SEP, bytes)
        assert len(_SEP) > 0

    def test_num_workers_positive(self) -> None:
        from fastopendata.processing.filter_us_nodes import _NUM_WORKERS

        assert _NUM_WORKERS > 0

    def test_data_dir_is_path(self) -> None:
        from fastopendata.processing.filter_us_nodes import DATA_DIR

        assert isinstance(DATA_DIR, Path)
