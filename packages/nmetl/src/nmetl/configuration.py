"""
Configuration Module (configuration.py)
=======================================
"""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Annotated, Any, Dict, List, Optional, Type

import yaml
from nmetl.config import CWD, MONOREPO_BASE_DIR, SRC_BASE_DIR
from nmetl.data_source import DataSource, DataSourceMapping
from nmetl.session import Session
from nmetl.session_enums import LoggingLevelEnum
from pycypher.fact_collection import FactCollection
from pycypher.fact_collection.foundationdb import \
    FoundationDBFactCollection  # noqa: F401
from pycypher.fact_collection.simple import SimpleFactCollection  # noqa: F401
from pydantic import BaseModel, Field, TypeAdapter
from shared.logger import LOGGER

TYPE_DISPATCH_DICT = {
    "PositiveInteger": TypeAdapter(Annotated[int, Field(gt=0)]),
    "PositiveFloat": TypeAdapter(Annotated[float, Field(gt=0)]),
    "String": TypeAdapter(str),
    "Boolean": TypeAdapter(bool),
    "NegativeInteger": TypeAdapter(Annotated[int, Field(lt=0)]),
    "NegativeFloat": TypeAdapter(Annotated[float, Field(lt=0)]),
    "Integer": TypeAdapter(int),
    "Float": TypeAdapter(float),
    "NonZeroInteger": TypeAdapter(Annotated[int, Field(ne=0)]),
    "NonZeroFloat": TypeAdapter(Annotated[float, Field(ne=0)]),
    "NonEmptyString": TypeAdapter(Annotated[str, Field(min_length=1)]),
    "Date": TypeAdapter(datetime.date),
    "DateTime": TypeAdapter(datetime.datetime),
    "Dict": TypeAdapter(dict),
}

TYPE_DISPATCH_DICT = {
    key: getattr(value, "validate_python")
    for key, value in TYPE_DISPATCH_DICT.items()
}


class SessionConfig(BaseModel):
    """
    Configuration model for the ETL session.

    This class represents the entire configuration for an ETL session,
    including fact collection settings, monitoring, and data sources.
    """

    fact_collection: str = ""
    run_monitor: bool = True
    fact_collection_class: str = ""
    data_sources: List[DataSourceConfig] = []
    logging_level: LoggingLevelEnum = LoggingLevelEnum.INFO
    fact_collection_kwargs: Optional[Dict[str, Any]] = {}


class DataSourceConfig(BaseModel):
    """
    Configuration model for a data source.

    This class represents the configuration for a data source,
    including its URI, mappings, and data types.
    """

    name: Optional[str] = None
    uri: str = ""
    mappings: List[DataSourceMappingConfig] = []
    data_types: Optional[Dict[str, str]] = {}
    options: Optional[Dict[str, str]] = {}

    def model_post_init(self, *args, **kwargs):  # pylint: disable=unused-argument
        self.uri = self.uri.format(
            CWD=CWD,
            SRC_BASE_DIR=SRC_BASE_DIR,
            MONOREPO_BASE_DIR=MONOREPO_BASE_DIR,
        )


class DataSchema(BaseModel):
    """
    Information about casting types for each key/value of a DataSource.

    This class represents the schema for a data source, defining how
    to cast values to specific types. We use pydantic TypeAdapter for this.

    Attributes:
        key (Optional[str]): The key in the data source.
        type (Optional[str]): The type to cast the value to.
    """

    key: Optional[str] = None
    type: Optional[str] = None


class DataSourceMappingConfig(BaseModel):
    """
    Configuration model for a data source mapping.

    This class represents a mapping between data source fields and
    graph elements (nodes, relationships, attributes).

    Attributes:
        attribute_key (Optional[str]): The key for the attribute in the data source.
        identifier_key (Optional[str]): The key for the identifier in the data source.
        attribute (Optional[str]): The name of the attribute in the graph.
    """

    attribute_key: Optional[str] = None
    identifier_key: Optional[str] = None
    attribute: Optional[str] = None
    label: Optional[str] = None
    source_key: Optional[str] = None
    target_key: Optional[str] = None
    source_label: Optional[str] = None
    target_label: Optional[str] = None
    relationship: Optional[str] = None


def load_session_config(
    path: str,
) -> Session:
    """
    Load and parse the Session configuration from a YAML file.

    Args:
        path (str): The file path to the YAML configuration file.

    Returns:
        Session: An instance of the Session class configured according to the YAML file.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        yaml.YAMLError: If there is an error parsing the YAML file.
        TypeError: If the configuration data does not match the expected structure.
    """
    with open(path, "r", encoding="utf8") as f:
        config: dict = yaml.safe_load(f) or {}
    session_config: SessionConfig = SessionConfig(**config)

    fact_collection_class: Type[FactCollection] = globals()[
        session_config.fact_collection_class
    ]

    LOGGER.info(
        f"Creating session with fact collection {fact_collection_class.__name__}"
    )
    session: Session = Session(
        run_monitor=session_config.run_monitor,
        logging_level=session_config.logging_level,
        fact_collection_class=fact_collection_class,
        fact_collection_kwargs=session_config.fact_collection_kwargs,
        session_config=session_config,
    )

    for data_source_config in session_config.data_sources:
        data_source = DataSource.from_uri(
            data_source_config.uri, config=data_source_config
        )
        data_source.name = data_source_config.name

        for mapping_config in data_source_config.mappings:
            mapping: DataSourceMapping = DataSourceMapping(
                attribute_key=mapping_config.attribute_key,
                identifier_key=mapping_config.identifier_key,
                attribute=mapping_config.attribute,
                label=mapping_config.label,
                source_key=mapping_config.source_key,
                target_key=mapping_config.target_key,
                source_label=mapping_config.source_label,
                target_label=mapping_config.target_label,
                relationship=mapping_config.relationship,
            )
            data_source.attach_mapping(mapping)
            data_source.attach_schema(
                data_source_config.data_types, TYPE_DISPATCH_DICT
            )

        session.attach_data_source(data_source)

    return session


SessionConfig.model_rebuild()
