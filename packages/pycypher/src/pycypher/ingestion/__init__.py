"""Ingestion layer for loading external data into pycypher.

Provides Arrow (via PyArrow) as the canonical in-memory tabular format and
DuckDB as the universal ingestion adapter.

Data Sources
------------

Load data from files, DataFrames, Arrow tables, or SQL databases::

    from pycypher.ingestion import (
        FileDataSource, CsvFormat, ParquetFormat,
        DataFrameDataSource, SqlDataSource, data_source_from_uri,
    )

    # From a CSV file
    source = FileDataSource("people.csv", format=CsvFormat())

    # From a Parquet file
    source = FileDataSource("people.parquet", format=ParquetFormat())

    # Auto-detect format from URI
    source = data_source_from_uri("data/people.csv")

    # From an existing pandas DataFrame
    source = DataFrameDataSource(df)

    # From a SQL database
    source = SqlDataSource("sqlite:///mydb.db", query="SELECT * FROM people")

Context Building
----------------

Use :class:`ContextBuilder` to assemble a query context from data sources::

    from pycypher.ingestion import ContextBuilder
    from pycypher import Star

    context = (
        ContextBuilder()
        .add_entity("Person", FileDataSource("people.csv", format=CsvFormat()))
        .add_relationship("KNOWS", knows_df,
                          source_col="__SOURCE__", target_col="__TARGET__")
        .build()
    )
    star = Star(context=context)

Pipeline Configuration
----------------------

Define and validate YAML-based ETL pipelines::

    from pycypher.ingestion import PipelineConfig, load_pipeline_config, validate_config

    config = load_pipeline_config("pipeline.yaml")
    result = validate_config(config)
    if not result.is_valid:
        for error in result.errors:
            print(error)

Data Preview & Introspection
-----------------------------

Inspect data sources before building full contexts::

    from pycypher.ingestion import DataSourceIntrospector, DataSampler

    introspector = DataSourceIntrospector(source)
    schema = introspector.get_schema()

    sampler = DataSampler(source)
    preview = sampler.sample(n=10)
"""

from __future__ import annotations

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


__all__ = [
    "ArrowDataSource",
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
