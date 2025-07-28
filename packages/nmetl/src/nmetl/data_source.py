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


def profile_thread(func, *args, **kwargs):
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
    """A thread that wraps a data source and loads data into a queue."""

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
        # self.num_workers = None
        # self.worker_num = None

    # Make the data source pickleable
    def __getstate__(self):
        return frozendict.deepfreeze(
            {
                "mapping": self.mappings,
            }
        )

    def __dask_tokenize__(self):
        return self.name

    def __getattr__(self, name: str) -> QueueGenerator | Session:
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

    def __lt__(self, other: DataSource) -> DataSource:
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
                if self.received_counter % 50 == 0:
                    while self.session.tasks_in_memory > 1:
                        LOGGER.debug("DataSource waiting...")
                        time.sleep(random.random())
                    LOGGER.debug(
                        "Number of tasks in memory: %s",
                        self.session.tasks_in_memory,
                    )
        self.finished = True
        self.finished_at = datetime.datetime.now()

    def start(self) -> None:
        """Start the loading thread."""
        self.loading_thread.run()
        self.started = True

    @staticmethod
    def generate_raw_facts_from_row(
        row: Dict[str, Any],
        mappings,
    ) -> Generator[AtomicFact, None, None]:
        """Generate raw facts from a row."""
        for mapping in mappings:
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
            yield source_fact
            yield target_fact
            yield label_fact
        else:
            raise NotImplementedError(
                "Only attribute and label mappings are supported for now."
            )

    def __add__(self, row: dict[str, Any]) -> Generator[AtomicFact, None, None]:
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
        """Generate rows from the data."""
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
    """Reading from a CSV file."""

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
        """Generate rows from the CSV file."""
        counter = 0
        for row in self.reader:
            counter += 1
            if counter == -1:
                break
            yield row


class ParquetFileDataSource(DataSource):
    """Reading from Parquet on local disk."""

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
        """Stream the file in batches from local disk. Eventually include other sources."""
        parquet_file = pq.ParquetFile(self.uri.path)
        for batch in parquet_file.iter_batches():
            df = batch.to_pandas()
            yield from df.to_dict(orient="records")
