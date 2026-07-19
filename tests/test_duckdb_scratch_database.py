"""DuckDB full-parity Phase 1 — file-backed database + scratch-file helpers.

Verifies that DuckDBBackend can be backed by a real file on disk (instead of
the previously hardcoded ``:memory:``) and that the scratch-file primitives
(``create_scratch_database_path``, ``sweep_orphaned_scratch_databases``)
behave per docs/duckdb_full_parity_design.md's "Scratch file location" and
"Crash cleanup" decisions.

See docs/duckdb_full_parity_design.md, Phase 1.
"""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING

import pandas as pd
import pytest
from pycypher.backends.duckdb_backend import (
    DuckDBBackend,
    _SCRATCH_FILE_PREFIX,
    create_duckdb_connection,
    create_scratch_database_path,
    sweep_orphaned_scratch_databases,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestDatabasePathBackwardCompat:
    def test_default_is_in_memory(self) -> None:
        backend = DuckDBBackend()
        try:
            assert backend.database_path is None
        finally:
            backend.close()

    def test_create_duckdb_connection_default_is_in_memory(self) -> None:
        con = create_duckdb_connection()
        try:
            # An in-memory database has no on-disk database file.
            assert con.execute("PRAGMA database_list").fetchall()[0][2] in (None, "")
        finally:
            con.close()


class TestFileBackedBackend:
    def test_data_persists_to_file(self, tmp_path: Path) -> None:
        db_path = str(tmp_path / "test.duckdb")
        backend = DuckDBBackend(database_path=db_path)
        try:
            assert backend.database_path == db_path
            backend.connection.execute(
                "CREATE TABLE t AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)",
            )
        finally:
            backend.close()

        assert os.path.exists(db_path)

        # Reopen and confirm the table survived the close.
        backend2 = DuckDBBackend(database_path=db_path)
        try:
            got = backend2.connection.execute("SELECT * FROM t ORDER BY id").fetchdf()
        finally:
            backend2.close()
        pd.testing.assert_frame_equal(
            got.reset_index(drop=True),
            pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}),
            check_dtype=False,
        )

    def test_create_duckdb_connection_file_backed(self, tmp_path: Path) -> None:
        db_path = str(tmp_path / "conn.duckdb")
        con = create_duckdb_connection(database_path=db_path)
        try:
            con.execute("CREATE TABLE t AS SELECT 1 AS x")
        finally:
            con.close()
        assert os.path.exists(db_path)


class TestCreateScratchDatabasePath:
    def test_returns_unique_paths(self) -> None:
        first = create_scratch_database_path()
        second = create_scratch_database_path()
        assert first != second

    def test_respects_scratch_dir_env_var(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        scratch_dir = tmp_path / "scratch"
        monkeypatch.setenv("PYCYPHER_DUCKDB_SCRATCH_DIRECTORY", str(scratch_dir))
        path = create_scratch_database_path()
        assert path.startswith(str(scratch_dir))
        assert scratch_dir.is_dir()

    def test_filename_has_expected_prefix_and_suffix(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PYCYPHER_DUCKDB_SCRATCH_DIRECTORY", str(tmp_path))
        path = create_scratch_database_path()
        filename = os.path.basename(path)
        assert filename.startswith(_SCRATCH_FILE_PREFIX)
        assert filename.endswith(".duckdb")

    def test_usable_as_a_real_database_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PYCYPHER_DUCKDB_SCRATCH_DIRECTORY", str(tmp_path))
        path = create_scratch_database_path()
        backend = DuckDBBackend(database_path=path)
        try:
            backend.connection.execute("CREATE TABLE t AS SELECT 1 AS x")
        finally:
            backend.close()
        assert os.path.exists(path)


class TestSweepOrphanedScratchDatabases:
    def test_removes_only_old_matching_files(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PYCYPHER_DUCKDB_SCRATCH_DIRECTORY", str(tmp_path))

        old_scratch = tmp_path / f"{_SCRATCH_FILE_PREFIX}old.duckdb"
        old_scratch.write_bytes(b"")
        old_time = time.time() - 100_000
        os.utime(old_scratch, (old_time, old_time))

        recent_scratch = tmp_path / f"{_SCRATCH_FILE_PREFIX}recent.duckdb"
        recent_scratch.write_bytes(b"")

        unrelated_old = tmp_path / "not-a-scratch-file.duckdb"
        unrelated_old.write_bytes(b"")
        os.utime(unrelated_old, (old_time, old_time))

        removed = sweep_orphaned_scratch_databases(max_age_seconds=1000)

        assert removed == [str(old_scratch)]
        assert not old_scratch.exists()
        assert recent_scratch.exists()
        assert unrelated_old.exists()

    def test_no_matching_files_returns_empty_list(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PYCYPHER_DUCKDB_SCRATCH_DIRECTORY", str(tmp_path))
        assert sweep_orphaned_scratch_databases(max_age_seconds=1000) == []

    def test_missing_scratch_directory_is_created_and_swept_empty(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        missing_dir = tmp_path / "does-not-exist-yet"
        monkeypatch.setenv("PYCYPHER_DUCKDB_SCRATCH_DIRECTORY", str(missing_dir))
        assert sweep_orphaned_scratch_databases(max_age_seconds=1000) == []
        assert missing_dir.is_dir()
