"""Tests for pycypher.ingestion package __init__.py — re-exports and __all__.

Validates that:
- All symbols in __all__ are importable from pycypher.ingestion
- Key classes are the correct types
- The package-level convenience imports work as documented
"""

from __future__ import annotations

import pycypher.ingestion as ingestion_pkg
import pytest


class TestIngestionAllExports:
    """Every name in __all__ should be importable and non-None."""

    def test_all_is_defined(self):
        assert hasattr(ingestion_pkg, "__all__")
        assert len(ingestion_pkg.__all__) > 0

    @pytest.mark.parametrize("name", ingestion_pkg.__all__)
    def test_export_importable(self, name: str):
        obj = getattr(ingestion_pkg, name)
        assert obj is not None, f"{name} resolved to None"


class TestIngestionKeyClasses:
    """Spot-check that key re-exported classes are the expected types."""

    def test_context_builder_is_class(self):
        assert isinstance(ingestion_pkg.ContextBuilder, type)

    def test_csv_format_is_class(self):
        assert isinstance(ingestion_pkg.CsvFormat, type)

    def test_parquet_format_is_class(self):
        assert isinstance(ingestion_pkg.ParquetFormat, type)

    def test_json_format_is_class(self):
        assert isinstance(ingestion_pkg.JsonFormat, type)

    def test_file_data_source_is_class(self):
        assert isinstance(ingestion_pkg.FileDataSource, type)

    def test_dataframe_data_source_is_class(self):
        assert isinstance(ingestion_pkg.DataFrameDataSource, type)

    def test_arrow_data_source_is_class(self):
        assert isinstance(ingestion_pkg.ArrowDataSource, type)

    def test_sql_data_source_is_class(self):
        assert isinstance(ingestion_pkg.SqlDataSource, type)

    def test_pipeline_config_is_class(self):
        assert isinstance(ingestion_pkg.PipelineConfig, type)

    def test_validation_result_is_class(self):
        assert isinstance(ingestion_pkg.ValidationResult, type)

    def test_duckdb_reader_is_class(self):
        assert isinstance(ingestion_pkg.DuckDBReader, type)

    def test_data_sampler_is_class(self):
        assert isinstance(ingestion_pkg.DataSampler, type)

    def test_pipeline_builder_is_class(self):
        assert isinstance(ingestion_pkg.PipelineBuilder, type)


class TestIngestionFunctions:
    """Re-exported functions should be callable."""

    def test_data_source_from_uri_is_callable(self):
        assert callable(ingestion_pkg.data_source_from_uri)

    def test_write_dataframe_to_uri_is_callable(self):
        assert callable(ingestion_pkg.write_dataframe_to_uri)

    def test_load_pipeline_config_is_callable(self):
        assert callable(ingestion_pkg.load_pipeline_config)

    def test_validate_config_is_callable(self):
        assert callable(ingestion_pkg.validate_config)

    def test_validate_config_dict_is_callable(self):
        assert callable(ingestion_pkg.validate_config_dict)


class TestIngestionSubmoduleConsistency:
    """Verify that re-exports point to the same objects as direct submodule imports."""

    def test_context_builder_same_object(self):
        from pycypher.ingestion.context_builder import ContextBuilder

        assert ingestion_pkg.ContextBuilder is ContextBuilder

    def test_csv_format_same_object(self):
        from pycypher.ingestion.data_sources import CsvFormat

        assert ingestion_pkg.CsvFormat is CsvFormat

    def test_pipeline_config_same_object(self):
        from pycypher.ingestion.config import PipelineConfig

        assert ingestion_pkg.PipelineConfig is PipelineConfig

    def test_validation_result_same_object(self):
        from pycypher.ingestion.validation import ValidationResult

        assert ingestion_pkg.ValidationResult is ValidationResult

    def test_duckdb_reader_same_object(self):
        from pycypher.ingestion.duckdb_reader import DuckDBReader

        assert ingestion_pkg.DuckDBReader is DuckDBReader
