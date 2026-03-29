"""Configuration Module (configuration.py)
=======================================
"""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Annotated, Any, Dict, List, Optional, Type

import yaml
from pycypher.fact_collection import FactCollection
from pycypher.fact_collection.foundationdb import (
    FoundationDBFactCollection,
)
from pycypher.fact_collection.simple import SimpleFactCollection
from pydantic import BaseModel, DirectoryPath, Field, FilePath, TypeAdapter
from shared.logger import LOGGER

from nmetl.config import CWD, MONOREPO_BASE_DIR, SRC_BASE_DIR
from nmetl.data_source import DataSource, DataSourceMapping
from nmetl.session_enums import LoggingLevelEnum


class SessionConfig(BaseModel):
    """Configuration model for the ETL session.

    This class represents the entire configuration for an ETL session,
    including fact collection settings, monitoring, and data sources.
    """

    compute_mode: str = "thread"
    data_assets_dir: DirectoryPath | None = None
    data_source_config_file: FilePath | None = None
    fact_collection_class: str = "FoundationDBFactCollection"
    fact_collection_kwargs: dict[str, Any] | None = {}
    foundationdb_cluster_file: str = ""
    get_item_max_attempts: int = 100
    grafana_host: str = ""
    grafana_port: int = 0
    logging_level: LoggingLevelEnum = LoggingLevelEnum.INFO
    max_queue_size: int = 1_000
    max_raw_data_queue_size: int = 16
    max_rows_per_data_source: int = 1_000_000_000
    prometheus_host: str = ""
    prometheus_port: int = 0
    raw_data_base_dir: DirectoryPath | None = None
    run_monitor: bool = False
    sleep_seconds_between_retries: int = 1
    sync_writes: bool = False
    thread_pool_size: int = 10


class DataSchema(BaseModel):
    """Information about casting types for each key/value of a DataSource.

    This class represents the schema for a data source, defining how
    to cast values to specific types. We use pydantic TypeAdapter for this.

    Attributes:
        key (Optional[str]): The key in the data source.
        type (Optional[str]): The type to cast the value to.

    """

    key: str | None = None
    type: str | None = None


class DataSourceMappingConfig(BaseModel):
    """Configuration model for a data source mapping.

    This class represents a mapping between data source fields and
    graph elements (nodes, relationships, attributes).

    Attributes:
        attribute_key (Optional[str]): The key for the attribute in the data source.
        identifier_key (Optional[str]): The key for the identifier in the data source.
        attribute (Optional[str]): The name of the attribute in the graph.

    """

    attribute_key: str | None = None
    identifier_key: str | None = None
    attribute: str | None = None
    label: str | None = None
    source_key: str | None = None
    target_key: str | None = None
    source_label: str | None = None
    target_label: str | None = None
    relationship: str | None = None


class DataSourceConfig(BaseModel):
    """Configuration model for a data source.

    This class represents the configuration for a data source,
    including its URI, mappings, and data types.
    """

    name: str | None = None
    uri: str = ""
    mappings: list[DataSourceMappingConfig] = []
    data_types: dict[str, str] | None = {}
    options: dict[str, str] | None = {}
    session_config: SessionConfig | None = None

    def model_post_init(self, *args, **kwargs):  # pylint: disable=unused-argument
        self.uri = self.uri.format(
            CWD=CWD,
            SRC_BASE_DIR=SRC_BASE_DIR,
            MONOREPO_BASE_DIR=MONOREPO_BASE_DIR,
        )
