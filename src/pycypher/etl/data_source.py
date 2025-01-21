"""Abstract base class for data sources."""

from __future__ import annotations

import csv
import hashlib
import queue
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Any, Generator, Optional
from urllib.parse import ParseResult

import pyarrow.parquet as pq

from pycypher.etl.message_types import EndOfData, RawDatum
from pycypher.util.helpers import ensure_uri, QueueGenerator
from pycypher.util.logger import LOGGER


# Not sure this is necessary.
class RawDataThread(threading.Thread):
    """A thread that wraps a data source and loads data into a queue."""

    def __init__(self, data_source: DataSource) -> None:
        super().__init__()
        self.data_source = data_source
        self.thread_has_started = False
        self.raw_input_queue = None

    def run(self) -> None:
        """Run the thread."""
        self.thread_has_started = True  # This flag is monotonic
        self.data_source.queue_rows()

    def block(self) -> None:
        """Block until the thread has finished loading data onto queue."""
        LOGGER.debug("Thread %s is blocking", self.data_source.name)
        while not self.thread_has_started:
            pass
        while not self.data_source.sent_end_of_data:
            pass
        LOGGER.debug("Thread %s is unblocking", self.data_source.name)


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
        self.loading_thread = threading.Thread(target=self.queue_rows)
        self.message_counter = 0
        self.mappings = []
        self.name = name or hashlib.md5(str(self).encode()).hexdigest()
        self.sent_end_of_data = False

    def attach_queue(self, queue_obj: queue.Queue) -> None:
        """Attach a queue to the data source."""
        if not isinstance(queue_obj, (queue.Queue, QueueGenerator,)):
            raise ValueError(f"Expected a Queue, got {type(queue_obj)}")
        self.raw_input_queue = queue_obj

    @abstractmethod
    def rows(self) -> Generator[dict[str, Any], None, None]:
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
        cls, uri: str | ParseResult, session: "Session"
    ) -> "DataSource":
        """Factory for creating a ``DataSource`` from a URI."""
        dispatcher = {
            "csv": CSVDataSource,
            "parquet": ParquetFileDataSource,
        }
        uri = ensure_uri(uri)
        filename_extension = uri.path.split(".")[-1]
        return dispatcher[filename_extension](uri, session)

    def queue_rows(self) -> None:
        """Places the rows emitted by the ``DataSource`` onto the
        right queue after wrapping them in a ``RawDatum``.
        """
        LOGGER.info("Loading from %s", self.name)
        if self.raw_input_queue is None:
            raise ValueError("Output queue is not set")
        for row in self.rows():
            self.raw_input_queue.put(
                RawDatum(data_source=self, row=row),
            )
            self.message_counter += 1
        self.raw_input_queue.put(
            EndOfData(data_source=self),
        )
        self.sent_end_of_data = True
        LOGGER.info("Finished loading %s with %s rows", self.name, self.message_counter)

    def start(self) -> None:
        """Start the loading thread."""
        self.loading_thread.run()
        self.started = True

    def unfinished(self) -> bool:
        """Is there unread data in the queue? or is the loading thread still running?
        or has the loading thread not started yet?"""
        return not self.started or not self.sent_end_of_data


@dataclass
class DataSourceMapping:  # pylint: disable=too-few-public-methods
    """
    A mapping from keys to ``Feature`` objects.

    (key, Feature, identifier_key)
    (source_identifier_key, Relationship, target_identifier_key)

    Will need to expand or subclass for relationships.
    """

    attribute_key: str
    identifier_key: str
    attribute: str


class FixtureDataSource(DataSource):
    """A ``DataSource`` that's just a list of dictionaries, useful for testing."""

    def __init__(
        self,
        data: list[dict[str, Any]],
        hang: bool = False,
        delay: float = 0,
        **kwargs,
    ):
        self.data = data
        self.hang = hang
        self.delay = delay
        super().__init__(**kwargs)

    def rows(self) -> Generator[dict[str, Any], None, None]:
        """Generate rows from the data."""
        if self.hang:
            while True:
                time.sleep(1)
        for row in self.data:
            time.sleep(self.delay)
            yield row


class CSVDataSource(DataSource):
    """Reading from a CSV file."""

    def __init__(
        self,
        uri: str | ParseResult,
        name: Optional[str] = None,
    ):
        self.uri = ensure_uri(uri)
        self.name = name
        self.file = open(self.uri.path, "r", encoding="utf-8")
        self.reader = csv.DictReader(self.file)
        super().__init__()

    def rows(self) -> Generator[dict[str, Any], None, None]:
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

    def rows(self) -> Generator[dict[str, Any], None, None]:
        """Stream the file in batches from local disk. Eventually include other sources."""
        parquet_file = pq.ParquetFile(self.uri.path)
        for batch in parquet_file.iter_batches():
            df = batch.to_pandas()
            yield from df.to_dict(orient="records")
