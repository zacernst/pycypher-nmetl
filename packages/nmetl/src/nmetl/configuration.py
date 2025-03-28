"""
Configuration Module (configuration.py)
=======================================

The ``configuration.py`` module handles the loading and parsing of configuration files for the `pycypher` system. These configuration files, written in YAML, define how `pycypher` should be set up, including the data sources to ingest and how to map that raw data to facts.

Writing a Configuration File
----------------------------

A `pycypher` configuration file is a YAML file that specifies the settings for your data pipeline. Here's a breakdown of the structure and available options:

Top-Level Structure
-------------------

The root of the configuration file is a dictionary with the following keys:

*   ``fact_collection`` (Optional[str]):
    *   **Purpose:** Specifies the name of a `FactCollection` to use.
    *   **Type:** String.
    *   **Default:** None.
*   ``run_monitor`` (Optional[bool]):
    *   **Purpose:** Indicates whether the `Session` monitor thread should be run.
    *   **Type:** Boolean.
    *   **Default:** True.
*   ``fact_collection_class`` (Optional[str]):
    *   **Purpose:** Specifies a specific class to instantiate for the `FactCollection`.
    *   **Type:** String.
    *   **Default:** None.
*   ``data_sources`` (Optional[List[Dict]]):
    *   **Purpose:** A list of data source configurations.
    *   **Type:** List of dictionaries.
    *   **Default:** An empty list.
*   ``logging_level`` (Optional[str]):
    *   **Purpose:** Specifies the level of logging to use for the `Session` object.
    * **Type**: String.
    * **Default**: `INFO`.

Data Source Configuration
-------------------------

Each data source in the ``data_sources`` list is defined as a dictionary with these keys:

*   ``name`` (Optional[str]):
    *   **Purpose:** A name for the data source (for identification).
    *   **Type:** String.
    *   **Default:** None.
*   ``uri`` (Optional[str]):
    *   **Purpose:** The URI of the data source (e.g., a file path, a database connection string).
    *   **Type:** String.
    *   **Default:** None.
    * **String Formatting**: This field supports string formatting, and provides the `CWD` and `SRC_BASE_DIR` variables.
*   ``mappings`` (List[Dict]):
    *   **Purpose:** A list of data source mapping configurations.
    *   **Type:** List of dictionaries.
    * **Default**: An empty list.
* ``data_types`` (Optional[Dict[str,str]]):
    * **Purpose**: A mapping from column name, to the desired type for that column.
    * **Type**: Dictionary.
    * **Default**: An empty dict.
    * **Valid types**: `PositiveInteger`, `PositiveFloat`, `String`, `Boolean`, `NegativeInteger`, `NegativeFloat`, `Integer`, `Float`, `NonZeroInteger`, `NonZeroFloat`, `NonEmptyString`, `Date`, `DateTime`

Data Source Mapping Configuration
---------------------------------

Each data source mapping in the ``mappings`` list is defined as a dictionary with at least one of these keys:

*   ``attribute_key`` (Optional[str]):
    *   **Purpose:** The key in the raw data that contains the attribute value for a node.
    *   **Type:** String.
    *   **Default:** None.
*   ``identifier_key`` (Optional[str]):
    *   **Purpose:** The key in the raw data that contains the node ID.
    *   **Type:** String.
    *   **Default:** None.
*   ``attribute`` (Optional[str]):
    *   **Purpose:** The name of the attribute to create.
    *   **Type:** String.
    *   **Default:** None.
*   ``label`` (Optional[str]):
    *   **Purpose:** The label to assign to a node.
    *   **Type:** String.
    *   **Default:** None.
*   ``source_key`` (Optional[str]):
    *   **Purpose:** The key in the raw data that contains the source node ID for a relationship.
    *   **Type:** String.
    *   **Default:** None.
*   ``target_key`` (Optional[str]):
    *   **Purpose:** The key in the raw data that contains the target node ID for a relationship.
    *   **Type:** String.
    *   **Default:** None.
* ``source_label`` (Optional[str]):
    * **Purpose**: The label to assign to the source node.
    * **Type**: String.
    * **Default**: None.
* ``target_label`` (Optional[str]):
    * **Purpose**: The label to assign to the target node.
    * **Type**: String.
    * **Default**: None.
*   ``relationship`` (Optional[str]):
    *   **Purpose:** The label to assign to a relationship.
    *   **Type:** String.
    *   **Default:** None.

Example Configuration
---------------------

.. code-block:: yaml

    fact_collection: my_fact_collection
    run_monitor: true
    logging_level: DEBUG
    data_sources:
        - name: people_data
          uri: file://{CWD}/data/people.csv
          data_types:
            name: String
            age: Integer
          mappings:
            - identifier_key: person_id
              label: Person
            - identifier_key: person_id
              attribute_key: name
              attribute: name
            - identifier_key: person_id
              attribute_key: age
              attribute: age
        - name: movie_data
          uri: file://{CWD}/data/movies.parquet
          data_types:
            title: NonEmptyString
            year: PositiveInteger
          mappings:
            - identifier_key: movie_id
              label: Movie
            - identifier_key: movie_id
              attribute_key: title
              attribute: title
            - identifier_key: movie_id
              attribute_key: year
              attribute: year
        - name: know_data
          uri: file://{CWD}/data/knows.csv
          mappings:
              - source_key: person1
                target_key: person2
                relationship: KNOWS
                source_label: Person
                target_label: Person

Loading the Configuration
-------------------------

The ``load_session_config(path: str)`` function loads and parses a YAML configuration file from the specified path and returns a configured ``Session`` object.

.. code-block:: python

    from pycypher.util.configuration import load_session_config

    session = load_session_config("path/to/your/config.yaml")
    # Now you can use the 'session' object to run the pipeline.

Key Classes
-----------

*   ``SessionConfig``: Pydantic model for the top-level configuration structure.
*   ``DataSourceConfig``: Pydantic model for a single data source.
* ``DataSchema``: Pydantic model for the schema of a data source.
*   ``DataSourceMappingConfig``: Pydantic model for a single data source mapping.

"""

from __future__ import annotations

import datetime
from typing import Annotated, Dict, List, Optional

import yaml
from nmetl.config import CWD, MONOREPO_BASE_DIR, SRC_BASE_DIR
from nmetl.data_source import DataSource, DataSourceMapping
from nmetl.session import Session
from pycypher.fact import FactCollection
from pydantic import BaseModel, Field, TypeAdapter

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


class SessionConfig(BaseModel):
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

    def model_post_init(self, *args, **kwargs):  # pylint: disable=unused-argument
        self.uri = self.uri.format(
            CWD=CWD,
            SRC_BASE_DIR=SRC_BASE_DIR,
            MONOREPO_BASE_DIR=MONOREPO_BASE_DIR,
        )


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


def load_session_config(path: str) -> Session:
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
        config = yaml.safe_load(f)
    session_config = SessionConfig(**config)

    session = Session(
        run_monitor=session_config.run_monitor,
        logging_level=session_config.logging_level,
    )

    session.fact_collection = FactCollection([])

    for data_source_config in session_config.data_sources:
        data_source = DataSource.from_uri(data_source_config.uri)
        data_source.name = data_source_config.name

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

        session.attach_data_source(data_source)

    return session
