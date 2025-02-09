"""Reads the configuration file into a Pydantic model."""

from __future__ import annotations

import datetime
from typing import Annotated, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, TypeAdapter

from pycypher.etl.data_source import DataSource, DataSourceMapping
from pycypher.etl.fact import FactCollection
from pycypher.etl.goldberg import Goldberg

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
}

TYPE_DISPATCH_DICT = {
    key: getattr(value, "validate_python")
    for key, value in TYPE_DISPATCH_DICT.items()
}


class GoldbergConfig(BaseModel):
    """The entire configuration"""

    fact_collection: Optional[str] = None
    run_monitor: Optional[bool] = True
    fact_collection_class: Optional[str] = None
    data_sources: Optional[List[DataSourceConfig]] = []
    logging_level: Optional[str] = "INFO"


class DataSourceConfig(BaseModel):
    """A list of data source mappings"""

    name: Optional[str] = None
    uri: Optional[str] = None
    mappings: List[DataSourceMappingConfig] = []
    data_types: Optional[Dict[str, str]] = {}


class DataSchema(BaseModel):
    """Information about casting types for each key/value of a DataSource.

    We will use pydantic TypeAdapter for this."""

    key: Optional[str] = None
    type: Optional[str] = None


class DataSourceMappingConfig(BaseModel):
    """A single data source mapping"""

    attribute_key: Optional[str] = None
    identifier_key: Optional[str] = None
    attribute: Optional[str] = None
    label: Optional[str] = None
    source_key: Optional[str] = None
    target_key: Optional[str] = None
    source_label: Optional[str] = None
    target_label: Optional[str] = None
    relationship: Optional[str] = None


def load_goldberg_config(path: str) -> Goldberg:
    with open(path, "r", encoding="utf8") as f:
        config = yaml.safe_load(f)
    goldberg_config = GoldbergConfig(**config)

    goldberg = Goldberg(
        run_monitor=goldberg_config.run_monitor,
        logging_level=goldberg_config.logging_level,
    )

    # fact_collection_config = goldberg_config.fact_collection
    goldberg.fact_collection = FactCollection([])

    for data_source_config in goldberg_config.data_sources:
        data_source = DataSource.from_uri(data_source_config.uri)

        for mapping_config in data_source_config.mappings:
            mapping = DataSourceMapping(
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

        goldberg.attach_data_source(data_source)

    return goldberg
