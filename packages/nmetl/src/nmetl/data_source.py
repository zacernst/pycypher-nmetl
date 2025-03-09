"""
DataSource Module (data_source.py)
==================================

The ``data_source.py`` module in the `pycypher` library defines the core classes for ingesting data from various sources. It provides an abstract base class, ``DataSource``, and several concrete subclasses for reading data from different formats, such as CSV and Parquet files.

Core Concepts
-------------

*   **Data Ingestion:** The primary purpose of this module is to define a standardized way to read raw data from diverse sources and make it available to the `pycypher` ETL pipeline.
*   **Streaming Data:** The module's design allows for the handling of both finite datasets (e.g., files) and potentially infinite data streams.
*   **Abstraction:** The ``DataSource`` abstract base class provides a common interface for working with different data sources, making the rest of the system agnostic to the specific source type.
* **Raw Data:** Data returned from a `DataSource` are shallow dictionaries.
* **Data Mapping:** `DataSource` objects can have mappings attached to them, that specify how to convert the raw data into facts.
* **Schema Attachment**: `DataSource` objects can have a schema attached to them, that will cast the raw data into the correct types.
* **Queue Based**: `DataSource` objects put `RawDatum` and `EndOfData` objects on a queue.

Key Classes
-----------

1.  ``DataSource`` (Abstract Base Class)
    --------------------------------------

    *   **Purpose:** This is the abstract base class for all data sources. It defines the common interface that all concrete data source implementations must adhere to.
    *   **Responsibilities:**
        *   Defines a standard way to initialize a data source.
        *   Provides an abstract ``rows`` method that subclasses must implement to yield data rows.
        *   Manages a queue (``raw_input_queue``) for sending processed data to downstream components.
        *   Handles the starting and stopping of data loading threads.
        * Allows for the attachment of mappings.
        * Allows for the attachment of a schema.
        * Casts the row according to the schema.
        * Maintains statistics about the number of messages received and sent.
    *   **Key Methods:**
        *   ``__init__(self, name: Optional[str] = None)``: Initializes the data source.
            *   **Parameters**:
                *   ``name`` (``Optional[str]``): An optional name for the data source.
        *   ``rows(self) -> Generator[Dict[str, Any], None, None]``: An abstract method that must be implemented by subclasses to yield rows of data as dictionaries.
        *   ``attach_queue(self, queue_obj: QueueGenerator) -> None``: Attaches a queue to the data source for sending data.
            *   **Parameters**:
                *   ``queue_obj`` (``QueueGenerator``): The queue object to attach.
        * ``attach_schema(self, schema: Dict[str, str], dispatch_dict: Dict) -> DataSource``
            * **Purpose**: Attach a schema to the data source.
            * **Parameters**:
                * `schema`: A dict where the key is a column name, and the value is a string referring to the desired type.
                * `dispatch_dict`: a dict that maps the strings in the schema to callable types.
        *   ``queue_rows(self) -> None``: Reads data from ``rows`` and puts it on the queue.
        *   ``from_uri(cls, uri: str | ParseResult) -> "DataSource"``: A class method that acts as a factory for creating data sources from a URI.
            *   **Parameters**:
                *   ``uri`` (``str | ParseResult``): The URI of the data source.
            *   **Returns**:
                 * ``"DataSource"``: A concrete `DataSource` class.
        *   ``cast_row(self, row: Dict[str, Any]) -> Dict[str, Any]``: Casts the row to the correct types specified in the schema.
        * ``attach_mapping(self, data_source_mapping: DataSourceMapping | List[DataSourceMapping]) -> DataSource``:
            * **Purpose**: Attach a mapping to the data source.
            * **Parameters**:
                * `data_source_mapping`: either a single mapping, or a list of mappings.
        * ``__lt__(self, other: DataSource) -> DataSource``: Attach a mapping using the less than operator.
        *  ``start(self) -> None``: Starts the data loading thread.
        *   ``generate_raw_facts_from_row(self, row: Dict[str, Any]) -> Generator[AtomicFact, None, None]``: Generates raw facts from a row based on the attached mappings.
        *  ``__repr__(self) -> str``: return a string representation of the object.
    *   **Attributes:**
        * ``raw_input_queue``: the queue where `RawDatum` and `EndOfData` objects are put.
        * `started`: True if the data source has been started.
        * `finished`: True if the data source has been finished.
        * `loading_thread`: The thread that is loading the data.
        * `message_counter`: A count of the messages that have been processed.
        * `mappings`: the mappings for this data source.
        * `name`: The name of this data source.
        * `received_counter`: A count of the number of rows that have been read.
        * `sent_counter`: A count of the number of rows that have been put on the queue.
        * `started_at`: When this data source was started.
        * `finished_at`: When this data source was finished.
        * `halt`: True if this data source has been told to halt.
        * `schema`: A schema to cast the row into the correct types.

2. ``DataSourceMapping``
    ---------------------
    * **Purpose**: Define how to map raw data rows to facts.
    * **Responsibilities**:
        * Maintain the keys for attributes, labels, and relationships.
        * Convert a single row into one or more facts.
        * Distinguish between label mappings, attribute mappings, and relationship mappings.
        * Add its resulting facts using the `+` operator.
    * **Key Methods**:
        * `process_against_raw_datum(self, row: Dict[str, Any]) -> Generator[AtomicFact, None, None]`
            * **Purpose**: Return a generator of facts based on the row and the mapping.
        * `__add__(self, row: dict[str, Any]) -> Generator[AtomicFact, None, None]`
            * **Purpose**: Allow the use of the `+` operator to process the row against the mapping.
    * **Attributes**:
        * `attribute_key`: The key in the row that contains the attribute value.
        * `identifier_key`: The key in the row that contains the node id.
        * `label`: The label of the node.
        * `attribute`: The attribute name.
        * `source_key`: The key in the row that contains the source node.
        * `target_key`: The key in the row that contains the target node.
        * `source_label`: The label of the source node.
        * `target_label`: The label of the target node.
        * `relationship`: The relationship label.
        * `is_attribute_mapping`: true if it is an attribute mapping.
        * `is_label_mapping`: true if it is a label mapping.
        * `is_relationship_mapping`: true if it is a relationship mapping.

3.  ``FixtureDataSource`` (Concrete Implementation)
    -----------------------------------------------

    *   **Purpose:** A concrete ``DataSource`` implementation useful for testing. It reads data from an in-memory list of dictionaries.
    *   **Responsibilities:**
        *   Provides an easy way to define test data without relying on external files.
        *   Can optionally hang, delay, or loop over its data for testing purposes.
    *   **Key Methods:**
        *   ``__init__(self, data: list[Dict[str, Any]], hang: Optional[bool] = False, delay: Optional[float] = 0, loop: Optional[bool] = False, **kwargs)``: Initializes the data source.
            * **Parameters**:
                * `data`: the in memory data.
                * `hang`: true if this fixture should hang.
                * `delay`: how long to delay between rows.
                * `loop`: true if this fixture should loop through the data.
                * `**kwargs`: other keyword arguments.
        *   ``rows(self) -> Generator[Dict[str, Any], None, None]``: Yields rows from the in-memory data list.

4.  ``CSVDataSource`` (Concrete Implementation)
    ------------------------------------------

    *   **Purpose:** A concrete ``DataSource`` implementation for reading data from CSV files.
    *   **Responsibilities:**
        *   Reads a CSV file and yields rows as dictionaries using the ``csv.DictReader``.
    *   **Key Methods:**
        *   ``__init__(self, uri: str | ParseResult, name: Optional[str] = None)``: Initializes the data source with a URI to a CSV file.
            *   **Parameters**:
                *   ``uri`` (``str | ParseResult``): The URI of the CSV file.
                *   ``name`` (``Optional[str]``): An optional name for the data source.
        *   ``rows(self) -> Generator[Dict[str, Any], None, None]``: Yields rows from the CSV file.

5.  ``ParquetFileDataSource`` (Concrete Implementation)
    --------------------------------------------------

    *   **Purpose:** A concrete ``DataSource`` implementation for reading data from Parquet files.
    *   **Responsibilities:**
        *   Reads a Parquet file and yields rows as dictionaries using the ``pyarrow.parquet`` module.
    *   **Key Methods:**
        *   ``__init__(self, uri: str | ParseResult, name: Optional[str] = None)``: Initializes the data source with a URI to a Parquet file.
            *   **Parameters**:
                *   ``uri`` (``str | ParseResult``): The URI of the Parquet file.
                *   ``name`` (``Optional[str]``): An optional name for the data source.
        *   ``rows(self) -> Generator[Dict[str, Any], None, None]``: Yields rows from the Parquet file.

Workflow
--------

1.  **Initialization:** A ``DataSource`` object is created, usually using the ``from_uri`` factory method or by directly instantiating a concrete subclass.
2.  **Queue Attachment:** A queue is attached to the data source using the ``attach_queue`` method.
3. **Schema Attachment:** Optionally a schema is attached to the data source.
4. **Mapping Attachment:** Optionally mappings are attached to the data source.
5.  **Data Loading:** The ``queue_rows`` method is called, which starts reading data.
6.  **Data Processing:** As data is read, it's placed on the queue as `RawDatum` objects. The `EndOfData` object is put on the queue when the data source is finished.
7.  **Downstream Consumption:** Downstream components consume the data from the queue.

Key Features
------------

*   **URI-Based:** The use of URIs allows for flexibility in specifying data source locations.
*   **Generator Output:** The ``rows`` method uses a generator, making it suitable for handling large or streaming datasets.
*   **Queue-Based Communication:** The use of a queue decouples the data source from downstream consumers, allowing for asynchronous processing.
*   **Factory Pattern:** The ``from_uri`` method provides a convenient way to create data sources based on the URI scheme.
* **Mapping**: The `DataSourceMapping` class makes it easy to define how to convert a row of raw data into one or more `Fact` objects.
* **Schema**: The ability to attach a schema makes it easy to ensure that the data is of the correct type.
* **Raw Data**: All `DataSource` objects return data as a shallow dictionary.

Extensibility
-------------

The module is designed to be extended:

*   **New File Formats:** You can add support for new file formats by creating a new subclass of ``DataSource`` and implementing the ``rows`` method.
*   **New Data Sources:** You can add support for new types of data sources (e.g., databases, APIs, message queues) by creating new subclasses of ``DataSource`` that handle the specifics of reading from those sources.

Use Cases
---------

*   **ETL Pipelines:** The module is fundamental for building ETL pipelines that ingest data from various sources, transform it, and load it into a target system.
*   **Data Migration:** It can be used to read data from legacy systems and migrate it to new systems.
*   **Data Analysis:** It can be used to read data from files or streams for analysis in other tools.
"""

from __future__ import annotations

import csv
import datetime
import hashlib
import threading
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Generator, List, Optional, Protocol, TypeVar
from urllib.parse import ParseResult

import pyarrow.parquet as pq
from nmetl.helpers import QueueGenerator, ensure_uri
from nmetl.message_types import EndOfData, RawDatum
from pycypher.fact import (
    AtomicFact,
    FactNodeHasAttributeWithValue,
    FactNodeHasLabel,
    FactRelationshipHasLabel,
    FactRelationshipHasSourceNode,
    FactRelationshipHasTargetNode,
)
from pycypher.logger import LOGGER


# Not sure this is necessary.
class RawDataThread(threading.Thread):
    """A thread that wraps a data source and loads data into a queue."""

    def __init__(self, data_source: DataSource) -> None:
        super().__init__()
        self.data_source = data_source
        self.thread_has_started = False
        self.raw_input_queue = None
        self.halt = False

    def run(self) -> None:
        """Run the thread."""
        self.thread_has_started = True  # This flag is monotonic
        self.data_source.queue_rows()

    def block(self) -> None:
        """Block until the thread has finished loading data onto queue."""
        LOGGER.debug("Thread %s is blocking", self.data_source.name)
        while (
            not self.thread_has_started or not self.data_source.finished
        ) and not self.halt:
            pass
        if self.halt:
            LOGGER.warning("Thread %s is halting", self.data_source.name)
        else:
            LOGGER.debug("Thread %s is unblocking", self.data_source.name)


ColumnName = TypeVar("ColumnName")


class NewColumn(Protocol[ColumnName]):
    """Protocol for column names."""

    def __getitem__(self, *args, **kwargs) -> None: ...

    def __setitem__(self, *args, **kwargs) -> None: ...


class DataSource(ABC):  # pylint: disable=too-many-instance-attributes
    """
    A ``DataSource`` could be a CSV file, Kafka streaam, etc.

    What makes a ``DataSource`` is that it generates shallow dictionaries.
    There is no difference between a ``DataSource`` that's a finite file with a
    specific number of rows, and one that is infinite stream.
    """

    def __init__(
        self,
        name: Optional[str] = None,
    ) -> None:
        self.raw_input_queue = None
        self.started = False
        self.finished = False
        self.loading_thread = threading.Thread(target=self.queue_rows)
        self.message_counter = 0
        self.mappings = []
        self.name = name or hashlib.md5(str(self).encode()).hexdigest()
        self.received_counter = 0
        self.sent_counter = 0
        self.started_at = None
        self.finished_at = None
        self.halt = False
        self.schema = {}
        self.new_column_configs: Dict[str, NewColumn] = {}

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

    def attach_queue(self, queue_obj: QueueGenerator) -> None:
        """Attach a queue to the data source."""
        if not isinstance(
            queue_obj,
            (
                # queue.Queue,
                QueueGenerator,
            ),
        ):
            raise ValueError(
                f"Expected a QueueGenerator, got {type(queue_obj)}"
            )
        self.raw_input_queue = queue_obj
        self.raw_input_queue.incoming_queue_processors.append(self)

    def attach_schema(
        self, schema: Dict[str, str], dispatch_dict: Dict
    ) -> DataSource:
        """Attach a schema to the data source.

        Each key in the schema is a key in the data source, and each
        value is a callable type.
        """
        self.schema = {
            key: dispatch_dict[value] for key, value in schema.items()
        }

    @abstractmethod
    def rows(self) -> Generator[Dict[str, Any], None, None]:
        """Basic method to get rows from the data source."""

    def attach_mapping(
        self, data_source_mapping: DataSourceMapping | List[DataSourceMapping]
    ) -> DataSource:
        """Attach a mapping to the data source."""
        # Don't think we'll need to link from mapping to data source, but possible.
        if isinstance(data_source_mapping, list):
            for mapping in data_source_mapping:
                self.attach_mapping(mapping)
            return self
        elif isinstance(data_source_mapping, DataSourceMapping):
            LOGGER.debug(
                "Attaching mapping %s to data source %s",
                data_source_mapping,
                self.name,
            )
            self.mappings.append(data_source_mapping)
        else:
            raise ValueError(
                f"Expected a DataSourceMapping, got {type(data_source_mapping)}"
            )

        return self

    def __lt__(self, other: DataSource) -> DataSource:
        """For attaching mappings to data sources."""
        return self.attach_mapping(other)

    @classmethod
    def from_uri(
        cls, uri: str | ParseResult, name: Optional[str] = None
    ) -> "DataSource":
        """Factory for creating a ``DataSource`` from a URI."""
        dispatcher = {
            "csv": CSVDataSource,
            "parquet": ParquetFileDataSource,
        }
        uri = ensure_uri(uri)
        filename_extension = uri.path.split(".")[-1]
        data_source = dispatcher[filename_extension](uri)
        data_source.name = name
        return data_source

    def cast_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Cast the row to the schema."""
        row = {
            key: self.schema[key](value) if key in self.schema else value
            for key, value in row.items()
        }
        return row

    def add_new_columns(self, row: Dict[str, Any]) -> None:
        """Add new columns to the row."""
        for key, new_column_config in self.new_column_configs.items():
            splat = [row[key] for key in new_column_config.parameter_names]
            new_column_value = new_column_config.func(*splat)
            row[key] = new_column_value

    def queue_rows(self) -> None:
        """Places the rows emitted by the ``DataSource`` onto the
        right queue after wrapping them in a ``RawDatum``.
        """
        LOGGER.info("Loading from %s", self.name)
        if self.raw_input_queue is None:
            raise ValueError("Output queue is not set")
        self.started = True
        self.started_at = datetime.datetime.now()
        for row in self.rows():
            row = self.cast_row(row)
            self.add_new_columns(row)
            self.received_counter += 1
            self.raw_input_queue.put(
                RawDatum(data_source=self, row=row),
            )
            self.sent_counter += 1
            if self.halt:
                LOGGER.debug("DataSource %s is halting", self.name)
                break
        self.raw_input_queue.put(
            EndOfData(data_source=self),
        )
        self.finished = True
        self.finished_at = datetime.datetime.now()

    def start(self) -> None:
        """Start the loading thread."""
        self.loading_thread.run()
        self.started = True

    def generate_raw_facts_from_row(
        self, row: Dict[str, Any]
    ) -> Generator[AtomicFact, None, None]:
        """Generate raw facts from a row."""
        for mapping in self.mappings:
            yield from mapping + row


@dataclass
class DataSourceMapping:  # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """
    A mapping from keys to ``Feature`` objects.

    (key, Feature, identifier_key)
    (source_key, Relationship, target_key)

    Will need to expand or subclass for relationships.
    """

    attribute_key: Optional[str] = None
    identifier_key: Optional[str] = None
    label: Optional[str] = None
    attribute: Optional[str] = None
    source_key: Optional[str] = None
    target_key: Optional[str] = None
    source_label: Optional[str] = None
    target_label: Optional[str] = None
    relationship: Optional[str] = None

    @property
    def is_attribute_mapping(self) -> bool:
        """Is this an attribute mapping? (vs a relationship mapping)"""
        return (
            self.attribute_key is not None
            and self.identifier_key is not None
            and self.attribute is not None
            and self.label is not None
        )

    @property
    def is_label_mapping(self) -> bool:
        """Is this a label mapping? (vs an attribute mapping)"""
        return (
            self.attribute_key is None
            and self.identifier_key is not None
            and self.attribute is None
            and self.label is not None
        )

    @property
    def is_relationship_mapping(self) -> bool:
        """Is this a relationship mapping? (vs an attribute mapping)"""
        return (
            self.source_key is not None
            and self.target_key is not None
            and self.source_label is not None
            and self.target_label is not None
            and self.relationship is not None
        )

    def process_against_raw_datum(
        self, row: Dict[str, Any]
    ) -> Generator[AtomicFact, None, None]:
        """Process the mapping against a raw datum."""
        if self.is_attribute_mapping:
            fact = FactNodeHasAttributeWithValue(
                node_id=f"{self.label}::{row[self.identifier_key]}",
                attribute=self.attribute,
                value=row[self.attribute_key],
            )
            yield fact
        elif self.is_label_mapping:
            fact = FactNodeHasLabel(
                node_id=f"{self.label}::{row[self.identifier_key]}",
                label=self.label,
            )
            yield fact
        elif self.is_relationship_mapping:
            relationship_id = uuid.uuid4().hex
            source_fact = FactRelationshipHasSourceNode(
                relationship_id=relationship_id,
                source_node_id=f"{self.source_label}::{row[self.source_key]}",
            )
            target_fact = FactRelationshipHasTargetNode(
                relationship_id=relationship_id,
                target_node_id=f"{self.target_label}::{row[self.target_key]}",
            )
            label_fact = FactRelationshipHasLabel(
                relationship_id=relationship_id,
                relationship_label=self.relationship,
            )
            yield source_fact
            yield target_fact
            yield label_fact
        else:
            raise NotImplementedError(
                "Only attribute and label mappings are supported for now."
            )

    def __add__(
        self, row: dict[str, Any]
    ) -> Generator[AtomicFact, None, None]:
        """Let us use the + operator to process a row against a mapping."""
        yield from self.process_against_raw_datum(row)


class FixtureDataSource(DataSource):
    """A ``DataSource`` that's just a list of dictionaries, useful for testing."""

    def __init__(
        self,
        data: list[Dict[str, Any]],
        hang: Optional[bool] = False,
        delay: Optional[float] = 0,
        loop: Optional[bool] = False,
        **kwargs,
    ):
        self.data = data
        self.hang = hang
        self.delay = delay
        self.loop = loop
        super().__init__(**kwargs)

    def rows(self) -> Generator[Dict[str, Any], None, None]:
        """Generate rows from the data."""
        if self.hang:
            while True:
                time.sleep(1)
        go = True
        while go:
            go = False
            for row in self.data:
                time.sleep(self.delay)
                yield row
            if self.loop:
                go = True


class CSVDataSource(DataSource):
    """Reading from a CSV file."""

    def __init__(
        self,
        uri: str | ParseResult,
        name: Optional[str] = None,
    ):
        self.uri = ensure_uri(uri)
        self.name = name
        self.file = open(self.uri.path, "r", encoding="utf-8")  # pylint: disable=consider-using-with
        self.reader = csv.DictReader(self.file)
        super().__init__()

    def rows(self) -> Generator[Dict[str, Any], None, None]:
        """Generate rows from the CSV file."""
        yield from self.reader


class ParquetFileDataSource(DataSource):
    """Reading from Parquet on local disk."""

    def __init__(
        self,
        uri: str | ParseResult,
        name: Optional[str] = None,
    ):
        self.uri = ensure_uri(uri)
        self.name = name
        super().__init__()

    def rows(self) -> Generator[Dict[str, Any], None, None]:
        """Stream the file in batches from local disk. Eventually include other sources."""
        parquet_file = pq.ParquetFile(self.uri.path)
        for batch in parquet_file.iter_batches():
            df = batch.to_pandas()
            yield from df.to_dict(orient="records")
