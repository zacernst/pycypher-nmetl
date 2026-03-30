"""Tests for pycypher.ingestion.output_writer.

Covers:
- write_dataframe_to_uri() for CSV, Parquet, and JSON
- Format inference from URI extension
- Explicit OutputFormat override
- Parent directory creation
- file:// URI scheme stripping
- Error cases (unsupported extension, cloud URI)
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from pycypher.ingestion.config import OutputFormat
from pycypher.ingestion.output_writer import write_dataframe_to_uri

_SAMPLE_DF = pd.DataFrame(
    {
        "name": ["Alice", "Bob"],
        "age": [30, 25],
    },
)


# ===========================================================================
# Format inference from extension
# ===========================================================================


class TestFormatInference:
    """write_dataframe_to_uri infers format from the URI extension."""

    def test_infers_csv(self, tmp_path: Path) -> None:
        uri = str(tmp_path / "output.csv")
        write_dataframe_to_uri(_SAMPLE_DF, uri)
        result = pd.read_csv(uri)
        assert list(result.columns) == ["name", "age"]
        assert len(result) == 2

    def test_infers_parquet(self, tmp_path: Path) -> None:
        uri = str(tmp_path / "output.parquet")
        write_dataframe_to_uri(_SAMPLE_DF, uri)
        result = pd.read_parquet(uri)
        assert len(result) == 2

    def test_infers_json(self, tmp_path: Path) -> None:
        uri = str(tmp_path / "output.json")
        write_dataframe_to_uri(_SAMPLE_DF, uri)
        result = pd.read_json(uri, lines=True)
        assert len(result) == 2

    def test_unknown_extension_raises(self, tmp_path: Path) -> None:
        uri = str(tmp_path / "output.avro")
        with pytest.raises(ValueError, match="infer"):
            write_dataframe_to_uri(_SAMPLE_DF, uri)


# ===========================================================================
# Explicit format override
# ===========================================================================


class TestExplicitFormat:
    """Explicit OutputFormat takes precedence over the URI extension."""

    def test_explicit_csv(self, tmp_path: Path) -> None:
        uri = str(tmp_path / "output.csv")
        write_dataframe_to_uri(_SAMPLE_DF, uri, OutputFormat.CSV)
        result = pd.read_csv(uri)
        assert set(result["name"]) == {"Alice", "Bob"}

    def test_explicit_parquet_overrides_csv_extension(
        self,
        tmp_path: Path,
    ) -> None:
        uri = str(tmp_path / "output.csv")
        write_dataframe_to_uri(_SAMPLE_DF, uri, OutputFormat.PARQUET)
        result = pd.read_parquet(uri)
        assert len(result) == 2

    def test_explicit_json(self, tmp_path: Path) -> None:
        uri = str(tmp_path / "output.json")
        write_dataframe_to_uri(_SAMPLE_DF, uri, OutputFormat.JSON)
        result = pd.read_json(uri, lines=True)
        assert set(result["name"]) == {"Alice", "Bob"}


# ===========================================================================
# Data correctness
# ===========================================================================


class TestDataCorrectness:
    """Written files contain the expected rows and columns."""

    def test_csv_values_preserved(self, tmp_path: Path) -> None:
        uri = str(tmp_path / "output.csv")
        write_dataframe_to_uri(_SAMPLE_DF, uri)
        result = pd.read_csv(uri)
        assert result.loc[0, "name"] == "Alice"
        assert result.loc[1, "name"] == "Bob"
        assert result.loc[0, "age"] == 30

    def test_json_values_preserved(self, tmp_path: Path) -> None:
        uri = str(tmp_path / "output.json")
        write_dataframe_to_uri(_SAMPLE_DF, uri)
        result = pd.read_json(uri, lines=True)
        names = set(result["name"].tolist())
        assert names == {"Alice", "Bob"}

    def test_parquet_values_preserved(self, tmp_path: Path) -> None:
        uri = str(tmp_path / "output.parquet")
        write_dataframe_to_uri(_SAMPLE_DF, uri)
        result = pd.read_parquet(uri)
        assert set(result["name"].tolist()) == {"Alice", "Bob"}


# ===========================================================================
# URI scheme handling
# ===========================================================================


class TestUriSchemeHandling:
    """file:// URIs are correctly resolved to filesystem paths."""

    def test_file_scheme_stripped(self, tmp_path: Path) -> None:
        path = tmp_path / "output.csv"
        uri = f"file://{path}"
        write_dataframe_to_uri(_SAMPLE_DF, uri)
        assert path.exists()
        result = pd.read_csv(path)
        assert len(result) == 2

    def test_bare_path_works(self, tmp_path: Path) -> None:
        path = tmp_path / "output.csv"
        write_dataframe_to_uri(_SAMPLE_DF, str(path))
        assert path.exists()

    def test_cloud_uri_raises_not_implemented(self, tmp_path: Path) -> None:
        with pytest.raises(NotImplementedError, match="[Cc]loud|[Ss]3"):
            write_dataframe_to_uri(_SAMPLE_DF, "s3://bucket/output.csv")


# ===========================================================================
# Parent directory creation
# ===========================================================================


class TestParentDirectoryCreation:
    """Parent directories are created when they do not exist."""

    def test_creates_nested_dirs(self, tmp_path: Path) -> None:
        uri = str(tmp_path / "nested" / "deep" / "output.csv")
        write_dataframe_to_uri(_SAMPLE_DF, uri)
        assert Path(uri).exists()

    def test_existing_dirs_not_an_error(self, tmp_path: Path) -> None:
        uri = str(tmp_path / "output.csv")
        write_dataframe_to_uri(_SAMPLE_DF, uri)
        # calling again should not raise
        write_dataframe_to_uri(_SAMPLE_DF, uri)
        assert Path(uri).exists()


# ===========================================================================
# Output path security
# ===========================================================================


class TestOutputPathSecurity:
    """Output URIs are validated against path traversal attacks."""

    def test_path_traversal_rejected(self) -> None:
        from pycypher.ingestion.security import SecurityError

        with pytest.raises(SecurityError, match="[Tt]raversal"):
            write_dataframe_to_uri(_SAMPLE_DF, "../../etc/cron.d/evil.csv")

    def test_sensitive_system_path_rejected(self) -> None:
        from pycypher.ingestion.security import SecurityError

        with pytest.raises(SecurityError, match="[Ss]ensitive"):
            write_dataframe_to_uri(_SAMPLE_DF, "/etc/output.csv")

    def test_proc_path_rejected(self) -> None:
        from pycypher.ingestion.security import SecurityError

        with pytest.raises(SecurityError, match="[Ss]ensitive"):
            write_dataframe_to_uri(_SAMPLE_DF, "/proc/self/output.csv")

    def test_valid_output_path_allowed(self, tmp_path: Path) -> None:
        uri = str(tmp_path / "safe_output.csv")
        write_dataframe_to_uri(_SAMPLE_DF, uri)
        assert Path(uri).exists()
