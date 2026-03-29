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
from nmetl.data_source_config import (
    DataSchema,
    DataSourceMappingConfig,
    SessionConfig,
)
from nmetl.session_enums import LoggingLevelEnum

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
