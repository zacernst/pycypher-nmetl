from __future__ import annotations

from typing import List, Optional

import yaml
from pydantic import BaseModel

from pycypher.etl.data_source import (
    DataSource,
    DataSourceMapping,
    FixtureDataSource,
)
from pycypher.etl.goldberg import Goldberg
from pycypher.etl.trigger import VariableAttribute
from pycypher.etl.fact import FactCollection
from pycypher.util.logger import LOGGER


class GoldbergConfig(BaseModel):
    """The entire configuration"""

    fact_collection: Optional[str] = None
    run_monitor: Optional[bool] = True
    fact_collection_class: Optional[str] = None
    data_sources: Optional[List[DataSources]] = []


class IngestConfig(BaseModel):
    """The entire configuration"""

    data_sources: Optional[List[DataSources]] = []


class DataSources(BaseModel):
    """A list of data source mappings"""

    name: Optional[str] = None
    uri: Optional[str] = None
    mappings: List[DataSourceMappingConfig] = []


class DataSourceMappingConfig(BaseModel):
    """A single data source mapping"""

    attribute_key: Optional[str] = None
    identifier_key: Optional[str] = None
    attribute: Optional[str] = None
    label: Optional[str] = None


def load_goldberg_config(path: str) -> Goldberg:
    with open(path, "r", encoding="utf8") as f:
        config = yaml.safe_load(f)
    goldberg_config = GoldbergConfig(**config)

    goldberg = Goldberg()

    fact_collection_config = goldberg_config.fact_collection
    goldberg.fact_collection = FactCollection([])

    for data_source_config in goldberg_config.data_sources:
        data_source = DataSource.from_uri(data_source_config.uri)

        
        for mapping_config in data_source_config.mappings:
            mapping = DataSourceMapping(
                attribute_key=mapping_config.attribute_key,
                identifier_key=mapping_config.identifier_key,
                attribute=mapping_config.attribute,
                label=mapping_config.label,
            )
            data_source.attach_mapping(mapping)
        
        goldberg.attach_data_source(data_source)

    return goldberg
