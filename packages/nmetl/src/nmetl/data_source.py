"""
DataSource Module (data_source.py)
==================================

The ``data_source.py`` module in the `pycypher` library defines the core classes for
ingesting data from various sources. It provides an abstract base class,
``DataSource``, and several concrete subclasses for reading data from different formats,
such as CSV and Parquet files.
"""

from __future__ import annotations

import cProfile
import csv
import datetime
import hashlib
import io
import pstats
import random
import threading
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    List,
    Literal,
    Optional,
    Protocol,
    TypeVar,
)

import frozendict

if TYPE_CHECKING:
    from nmetl.session import Session
    from prometheus_client import Histogram

from urllib.parse import ParseResult

import pyarrow.parquet as pq
from nmetl.message_types import RawDatum
from nmetl.prometheus_metrics import ROW_PROCESSING_TIME, ROWS_QUEUED
from nmetl.queue_generator import QueueGenerator
from pycypher.fact import (
    AtomicFact,
    FactNodeHasAttributeWithValue,
    FactNodeHasLabel,
    FactRelationshipHasLabel,
    FactRelationshipHasSourceNode,
    FactRelationshipHasTargetNode,
)
from shared.helpers import ensure_uri
from shared.logger import LOGGER

LOGGER.setLevel("ERROR")

MAX_ROWS: Literal[-1] = -1


def profile_thread(func, *args, **kwargs):
    """Profile a function execution in a thread.

    Args:
        func: Function to profile.
        *args: Positional arguments for the function.
        **kwargs: Keyword arguments for the function.
    """
    pr = cProfile.Profile()
    pr.enable()
    func(*args, **kwargs)
    pr.disable()
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats("cumulative")
    ps.print_stats()
    print(s.getvalue())


# Not sure this is necessary.
class RawDataThread(threading.Thread):
    """A thread that wraps a data source and loads data into a queue.

    This class provides a threaded wrapper around a DataSource to enable
    concurrent data loading operations.

    Attributes:
        data_source: The DataSource instance to wrap.
        thread_has_started: Flag indicating if the thread has started.
        halt: Flag to signal thread termination.
    """

    def __init__(self, data_source: DataSource) -> None:
        """
        Initialize a RawDataThread instance.

        Args:
            data_source (DataSource): The data source to wrap in this thread.
        """
        super().__init__()
        self.data_source = data_source
        self.thread_has_started = False
        self._raw_input_queue: QueueGenerator | None = None
        self.halt = False

    @property
    def raw_input_queue(self) -> QueueGenerator:
        if not isinstance(self._raw_input_queue, QueueGenerator):
            raise ValueError("Expected raw_input_queue to be a QueueGenerator")
        return self._raw_input_queue

    def run(self) -> None:
        """Run the thread to start data loading.

        Sets the thread_has_started flag and initiates the data source's
        queue_rows method.
        """
        self.thread_has_started = True  # This flag is monotonic
        self.data_source.queue_rows()

    def block(self) -> None:
        """Block until the thread has finished loading data onto queue.

        This method will wait until either the data source has finished
        loading all data or the halt flag is set.
        """
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
    """Protocol for defining new column operations.

    This protocol defines the interface for objects that can be used
    to create new columns in data processing pipelines.

    Type Parameters:
        ColumnName: The type used for column names.
    """

    def __getitem__(self, *args, **kwargs) -> None:
        """
        Protocol method for indexing operations.

        This is a placeholder method required by the Protocol and is not meant to be called directly.

        Args:
            *args: Variable positional arguments.
            **kwargs: Variable keyword arguments.

        Returns:
            None
        """
        ...

    def __setitem__(self, *args, **kwargs) -> None:
        """
        Protocol method for item assignment operations.

        This is a placeholder method required by the Protocol and is not meant to be called directly.

        Args:
            *args: Variable positional arguments.
            **kwargs: Variable keyword arguments.

        Returns:
            None
        """
        ...


class DataSource(ABC):  # pylint: disable=too-many-instance-attributes
    """Abstract base class for data sources in the ETL pipeline.

    A DataSource represents any source of data that can generate rows as
    dictionaries. This could be a CSV file, Kafka stream, database, etc.
    The key characteristic is that it generates shallow dictionaries representing
    data rows. There is no semantic difference between finite sources (like files)
    and infinite streams.

    Attributes:
        name: Unique identifier for this data source.
        mappings: List of DataSourceMapping objects defining how to process rows.
        started: Flag indicating if data loading has started.
        finished: Flag indicating if data loading has completed.
        loading_thread: Thread for concurrent data loading.
        received_counter: Count of rows received from the source.
        sent_counter: Count of rows sent to the queue.
        started_at: Timestamp when loading started.
        finished_at: Timestamp when loading finished.
        halt: Flag to signal early termination.
        schema: Dictionary mapping column names to type converters.
        new_column_configs: Configuration for dynamically added columns.
    """

    def __init__(
        self,
        name: Optional[str] = None,
    ) -> None:
        """
        Initialize a DataSource instance.

        Args:
            name (Optional[str]): The name of this data source. If None, a hash of the string
                representation of this object will be used. Defaults to None.
        """
        self.started = False
        self.finished = False
        self.loading_thread = threading.Thread(target=self.queue_rows)
        self.message_counter = 0
        self.mappings = []
        self.name = name or hashlib.md5(str(self).encode()).hexdigest()
        self.received_counter = 0
        self.sent_counter = 0
        self.started_at: Optional[datetime.datetime] = None
        self.finished_at: datetime.datetime | None = None
        self.halt = False
        self.schema = {}
        self.new_column_configs: Dict[str, NewColumn] = {}
        self.back_pressure = 0.00001
        self._session: Optional[Any] = None
        self._raw_input_queue: QueueGenerator | None = None
        self.session: Optional[Session] = None
        # self.num_workers = None
        # self.worker_num = None

    def __getstate__(self):
        """Make the data source pickleable for distributed processing.

        Returns:
            Frozen dictionary containing serializable state.
        """
        return frozendict.deepfreeze(
            {
                "mapping": self.mappings,
            }
        )

    def __dask_tokenize__(self):
        """Provide a token for Dask distributed computing.

        Returns:
            String token identifying this data source.
        """
        return self.name

    def __getattr__(self, name: str) -> QueueGenerator | Session:
        """Dynamic attribute access for queue and session.

        Args:
            name: Attribute name to access.

        Returns:
            The requested attribute value.

        Raises:
            AttributeError: If attribute is not found or not allowed.
        """
        if name not in ["raw_input_queue", "session"]:
            raise AttributeError()
        out: QueueGenerator | Session | None = getattr(self, name, None)
        if not out:
            raise AttributeError()
        return out

    def __repr__(self) -> str:
        """
        Return a string representation of the DataSource instance.

        Returns:
            str: A string representation in the format "ClassName(name)".
        """
        return f"{self.__class__.__name__}({self.name})"

    def attach_queue(self, queue_obj: QueueGenerator) -> None:
        """
        Attach a queue to the data source.

        Args:
            queue_obj (QueueGenerator): The queue to attach to this data source.

        Raises:
            ValueError: If queue_obj is not a QueueGenerator.
        """
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
        self._raw_input_queue = queue_obj
        # self._raw_input_queue.incoming_queue_processors.append(self)

    def attach_schema(
        self, schema: Dict[str, str], dispatch_dict: Dict
    ) -> None:
        """
        Attach a schema to the data source.

        Each key in the schema is a key in the data source, and each
        value is a callable type.

        Args:
            schema (Dict[str, str]): A dictionary mapping column names to type names.
            dispatch_dict (Dict): A dictionary mapping type names to callable types.

        Returns:
            DataSource: The data source instance (self) for method chaining.
        """
        self.schema = {
            key: dispatch_dict[value] for key, value in schema.items()
        }

    @abstractmethod
    def rows(self) -> Generator[Dict[str, Any], None, None]:
        """
        Basic method to get rows from the data source.

        Returns:
            Generator[Dict[str, Any], None, None]: A generator yielding dictionaries
                representing rows from the data source.
        """

    def attach_mapping(
        self, data_source_mapping: DataSourceMapping | List[DataSourceMapping]
    ) -> DataSource:
        """
        Attach a mapping to the data source.

        Args:
            data_source_mapping (DataSourceMapping | List[DataSourceMapping]): The mapping(s)
                to attach to this data source.

        Returns:
            DataSource: The data source instance (self) for method chaining.
        """
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

    def __lt__(self, other: DataSourceMapping) -> DataSource:
        """For attaching mappings to data sources."""
        return self.attach_mapping(other)

    @classmethod
    def from_uri(
        cls,
        uri: str | ParseResult,
        config: Any = None,
    ) -> "DataSource":
        """Factory for creating a ``DataSource`` from a URI."""
        dispatcher: dict[
            str, type[CSVDataSource] | type[ParquetFileDataSource]
        ] = {
            "csv": CSVDataSource,
            "parquet": ParquetFileDataSource,
        }
        uri = ensure_uri(uri)
        filename_extension: str = uri.path.split(".")[-1]
        options: dict[str, Any] | Any = config.options if config else {}
        data_source: CSVDataSource | ParquetFileDataSource = dispatcher[
            filename_extension
        ](uri, **options)
        return data_source

    def cast_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Cast row values according to the attached schema.

        Args:
            row: Dictionary representing a data row.

        Returns:
            Row with values cast to appropriate types.
        """
        row = {
            key: self.schema[key](value) if key in self.schema else value
            for key, value in row.items()
        }
        return row

    def add_new_columns(self, row: Dict[str, Any]) -> None:
        """Add dynamically computed columns to the row.

        Args:
            row: Dictionary representing a data row to modify.
        """
        for key, new_column_config in self.new_column_configs.items():
            splat = [row[key] for key in new_column_config.parameter_names]
            new_column_value = new_column_config.func(*splat)
            row[key] = new_column_value

    def queue_rows(self) -> None:
        """Load rows from the data source and place them on the output queue.

        This method processes each row through schema casting and new column
        addition, then wraps it in a RawDatum and places it on the queue.
        """
        LOGGER.info("Loading from %s", self.name)
        if self._raw_input_queue is None:
            raise ValueError("Output queue is not set")
        self.started = True
        self.started_at = datetime.datetime.now()
        for row in self.rows():
            with ROW_PROCESSING_TIME.time():
                row = self.cast_row(row)
                self.add_new_columns(row)
                self.received_counter += 1

                self.raw_input_queue.put(
                    RawDatum(mappings=self.mappings, row=row),
                )
                ROWS_QUEUED.inc()
                LOGGER.debug(row)
                self.sent_counter += 1
                if self.halt:
                    LOGGER.debug("DataSource %s is halting", self.name)
                    break
                if 1 or self.received_counter % 50 == 0:
                    while self.session.tasks_in_memory > 64:
                        LOGGER.debug("DataSource waiting...")
                        time.sleep(random.random())
                    LOGGER.debug(
                        "Number of tasks in memory: %s",
                        self.session.tasks_in_memory,
                    )
        self.finished = True
        self.finished_at = datetime.datetime.now()

    def start(self) -> None:
        """Start the data loading process.

        Initiates the loading thread to begin processing rows.
        """
        self.loading_thread.run()
        self.started = True

    @staticmethod
    def generate_raw_facts_from_row(
        row: Dict[str, Any],
        mappings,
    ) -> Generator[AtomicFact, None, None]:
        """Generate atomic facts from a data row using mappings.

        Args:
            row: Dictionary representing a data row.
            mappings: List of DataSourceMapping objects.

        Yields:
            AtomicFact objects generated from the row.
        """
        for mapping in mappings:
            yield from mapping + row


@dataclass
class DataSourceMapping:  # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """Defines how to map data source columns to graph facts.

    This class specifies how columns in a data source should be interpreted
    as nodes, relationships, labels, or attributes in the graph structure.
    It supports three types of mappings:
    - Label mappings: Create node labels
    - Attribute mappings: Set node attributes
    - Relationship mappings: Create relationships between nodes

    Attributes:
        attribute_key: Column name containing attribute values.
        identifier_key: Column name containing node identifiers.
        label: Label to assign to nodes.
        attribute: Attribute name to set on nodes.
        source_key: Column name for source node identifiers.
        target_key: Column name for target node identifiers.
        source_label: Label for source nodes in relationships.
        target_label: Label for target nodes in relationships.
        relationship: Name of the relationship type.
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
        """Check if this is an attribute mapping.

        Returns:
            True if this mapping sets node attributes, False otherwise.
        """
        return (
            self.attribute_key is not None
            and self.identifier_key is not None
            and self.attribute is not None
            and self.label is not None
        )

    @property
    def is_label_mapping(self) -> bool:
        """Check if this is a label mapping.

        Returns:
            True if this mapping assigns node labels, False otherwise.
        """
        return (
            self.attribute_key is None
            and self.identifier_key is not None
            and self.attribute is None
            and self.label is not None
        )

    @property
    def is_relationship_mapping(self) -> bool:
        """Check if this is a relationship mapping.

        Returns:
            True if this mapping creates relationships, False otherwise.
        """
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
        """Process a data row according to this mapping to generate facts.

        Args:
            row: Dictionary representing a data row.

        Yields:
            AtomicFact objects generated from the row.

        Raises:
            NotImplementedError: If mapping type is not supported.
        """
        if self.is_attribute_mapping:
            fact: FactNodeHasAttributeWithValue = FactNodeHasAttributeWithValue(
                node_id=f"{self.label}::{row[self.identifier_key]}",
                attribute=self.attribute,
                value=row[self.attribute_key],
            )
            yield fact
        elif self.is_label_mapping:
            fact: FactNodeHasLabel = FactNodeHasLabel(
                node_id=f"{self.label}::{row[self.identifier_key]}",
                label=self.label,
            )
            yield fact
        elif self.is_relationship_mapping:
            relationship_id = uuid.uuid4().hex
            source_fact: FactRelationshipHasSourceNode = FactRelationshipHasSourceNode(
                relationship_id=relationship_id,
                source_node_id=f"{self.source_label}::{row[self.source_key]}",
            )
            target_fact: FactRelationshipHasTargetNode = FactRelationshipHasTargetNode(
                relationship_id=relationship_id,
                target_node_id=f"{self.target_label}::{row[self.target_key]}",
            )
            label_fact: FactRelationshipHasLabel = FactRelationshipHasLabel(
                relationship_id=relationship_id,
                relationship_label=self.relationship,
            )
            LOGGER.debug("source_fact: %s", source_fact)
            LOGGER.debug("target_fact: %s", target_fact)
            LOGGER.debug("label_fact: %s", label_fact)

            yield source_fact
            yield target_fact
            yield label_fact
        else:
            raise NotImplementedError(
                "Only attribute and label mappings are supported for now."
            )

    def __add__(self, row: dict[str, Any]) -> Generator[AtomicFact, None, None]:
        """Process a row using the + operator for convenience.

        Args:
            row: Dictionary representing a data row.

        Yields:
            AtomicFact objects generated from the row.
        """
        yield from self.process_against_raw_datum(row)


class FixtureDataSource(DataSource):
    """A DataSource that serves data from an in-memory list.

    This implementation is primarily useful for testing and development,
    providing a simple way to create a DataSource from a list of dictionaries.

    Attributes:
        data: List of dictionaries representing rows.
        hang: If True, the data source will hang indefinitely.
        delay: Delay in seconds between yielding rows.
        loop: If True, continuously loop through the data.
    """

    def __init__(
        self,
        data: list[Dict[str, Any]],
        hang: Optional[bool] = False,
        delay: Optional[float] = 0,
        loop: Optional[bool] = False,
        **kwargs,
    ):
        """
        Initialize a FixtureDataSource instance.

        Args:
            data (list[Dict[str, Any]]): The data to serve as rows.
            hang (Optional[bool]): If True, the data source will hang indefinitely. Defaults to False.
            delay (Optional[float]): The delay in seconds between yielding rows. Defaults to 0.
            loop (Optional[bool]): If True, the data source will loop through the data indefinitely. Defaults to False.
            **kwargs: Additional keyword arguments passed to the parent class.
        """
        self.data = data
        self.hang = hang
        self.delay = delay
        self.loop = loop
        super().__init__(**kwargs)

    def rows(self) -> Generator[Dict[str, Any], None, None]:
        """Generate rows from the in-memory data.

        Yields:
            Dictionary representing each data row.
        """
        if self.hang:
            while True:
                time.sleep(1)
        go = True
        while go:
            go = False
            for row in self.data:
                yield row
            if self.loop:
                go = True


class CSVDataSource(DataSource):
    """DataSource implementation for reading CSV files.

    Provides functionality to read data from CSV files using Python's
    csv.DictReader, with support for various CSV options.

    Attributes:
        uri: URI of the CSV file to read.
        file: Open file handle for the CSV.
        reader: CSV DictReader instance.
    """

    def __init__(
        self,
        uri: str | ParseResult = "",
        name: str = "",
        **kwargs,
    ):
        """
        Initialize a CSVDataSource instance.

        Args:
            uri (str | ParseResult): The URI of the CSV file to read.
            name (Optional[str]): The name of this data source. Defaults to None.
            **options: Additional options passed to the CSV reader.
        """
        self.uri = ensure_uri(uri)
        self.name = name
        self.file = open(self.uri.path, "r", encoding="utf-8")  # pylint: disable=consider-using-with
        self.reader = csv.DictReader(self.file, **kwargs)
        super().__init__()

    @property
    def raw_input_queue(self) -> QueueGenerator:
        if not isinstance(self._raw_input_queue, QueueGenerator):
            raise ValueError("Expected raw_input_queue to be defined")
        return self._raw_input_queue

    def rows(self) -> Generator[Dict[str, Any], None, None]:
        """Generate rows from the CSV file.

        Yields:
            Dictionary representing each CSV row.
        """
        counter = 0
        for row in self.reader:
            counter += 1
            if counter == MAX_ROWS:
                break
            yield row


class ParquetFileDataSource(DataSource):
    """DataSource implementation for reading Parquet files.

    Provides functionality to read data from Parquet files using PyArrow,
    with efficient batch processing for large files.

    Attributes:
        uri: URI of the Parquet file to read.
    """

    def __init__(
        self,
        uri: str | ParseResult,
        name: Optional[str] = None,
    ):
        """
        Initialize a ParquetFileDataSource instance.

        Args:
            uri (str | ParseResult): The URI of the Parquet file to read.
            name (Optional[str]): The name of this data source. Defaults to None.
        """
        self.uri = ensure_uri(uri)
        self.name = name
        super().__init__()

    def rows(self) -> Generator[Dict[str, Any], None, None]:
        """Stream rows from the Parquet file in batches.

        Reads the Parquet file in batches for memory efficiency,
        converting each batch to pandas DataFrame and yielding individual rows.

        Yields:
            Dictionary representing each Parquet row.
        """
        parquet_file = pq.ParquetFile(self.uri.path)
        for batch in parquet_file.iter_batches():
            df = batch.to_pandas()
            yield from df.to_dict(orient="records")
