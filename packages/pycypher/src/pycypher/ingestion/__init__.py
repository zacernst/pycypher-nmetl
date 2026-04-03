"""Ingestion layer for loading external data into pycypher.

Provides Arrow (via PyArrow) as the canonical in-memory tabular format and
DuckDB as the universal ingestion adapter.

Exports:
    DataSource: Abstract base class for all data sources.
    Format: Abstract base class for file-format strategies.
    CsvFormat: CSV format strategy.
    ParquetFormat: Parquet format strategy.
    JsonFormat: JSON format strategy.
    FileDataSource: File-backed data source (format-as-strategy).
    SqlDataSource: Reads from SQL databases via DuckDB.
    DataFrameDataSource: Wraps a pandas DataFrame.
    ArrowDataSource: Wraps an existing Arrow table.
    data_source_from_uri: Factory that builds the correct DataSource from a URI.
    ContextBuilder: Fluent builder for assembling a Context from Arrow-loaded data.
    write_dataframe_to_uri: Write a DataFrame to a URI (CSV, Parquet, or JSON).
    ArrowIngestion: Deprecated alias for DuckDBReader (via ``__getattr__``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pycypher.ingestion.duckdb_reader import DuckDBReader as ArrowIngestion

from pycypher.ingestion.context_builder import ContextBuilder
from pycypher.ingestion.introspector import DataSourceIntrospector
from pycypher.ingestion.pipeline_builder import PipelineBuilder, PipelineOperation, PipelineSnapshot
from pycypher.ingestion.data_sources import (
    ArrowDataSource,
    CsvFormat,
    DataFrameDataSource,
    DataSource,
    FileDataSource,
    Format,
    JsonFormat,
    ParquetFormat,
    SqlDataSource,
    data_source_from_uri,
)
from pycypher.ingestion.data_preview import (
    ColumnStats,
    DataSampler,
    PreviewCache,
    QueryResult,
    QueryTester,
    SamplingStrategy,
    SchemaInfo,
)
from pycypher.ingestion.duckdb_reader import DuckDBReader
from pycypher.ingestion.output_writer import write_dataframe_to_uri
from pycypher.ingestion.config import PipelineConfig, load_pipeline_config
from pycypher.ingestion.validation import ValidationResult, validate_config, validate_config_dict


def __getattr__(name: str) -> type:
    """Emit deprecation warning for ``ArrowIngestion`` alias."""
    if name == "ArrowIngestion":
        from shared.deprecation import emit_deprecation

        emit_deprecation(
            "ArrowIngestion",
            since="0.0.19",
            removed_in="0.1.0",
            alternative="DuckDBReader",
            detail="Import via: from pycypher.ingestion import DuckDBReader",
        )
        return DuckDBReader
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


__all__ = [
    "ArrowDataSource",
    "ArrowIngestion",  # Deprecated — use DuckDBReader; runtime via __getattr__
    "ContextBuilder",
    "DataSourceIntrospector",
    "PipelineBuilder",
    "PipelineOperation",
    "PipelineSnapshot",
    "CsvFormat",
    "DataFrameDataSource",
    "DataSource",
    "DuckDBReader",
    "FileDataSource",
    "Format",
    "JsonFormat",
    "ParquetFormat",
    "SqlDataSource",
    "ColumnStats",
    "DataSampler",
    "PreviewCache",
    "QueryResult",
    "QueryTester",
    "SamplingStrategy",
    "SchemaInfo",
    "data_source_from_uri",
    "write_dataframe_to_uri",
    "PipelineConfig",
    "load_pipeline_config",
    "ValidationResult",
    "validate_config",
    "validate_config_dict",
]
