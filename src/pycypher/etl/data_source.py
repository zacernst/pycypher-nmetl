"""Abstract base class for data sources."""

import csv
import hashlib
import queue
import threading
from abc import ABC, abstractmethod
from typing import Any, Generator, Optional
from urllib.parse import ParseResult

import pyarrow.parquet as pq

from pycypher.etl.message_types import EndOfData, RawDatum
from pycypher.util.helpers import ensure_uri
from pycypher.util.logger import LOGGER


class DataSource(ABC):
    """
    A ``DataSource`` could be a CSV file, Kafka streaam, etc.

    What makes a ``DataSource`` is that it generates shallow dictionaries.
    There is no difference between a ``DataSource`` that's a finite file with a
    specific number of rows, and one that is infinite stream.
    """

    def __init__(self, name: Optional[str] = None) -> None:
        self.raw_data_queue = None
        self.started = False
        self.loading_thread = threading.Thread(target=self.queue_rows)
        self.message_counter = 0
        self.mapping_dict = {}
        self.mappings = []
        self.name = name or hashlib.md5(str(self).encode()).hexdigest()

    def attach_queue(self, queue_obj: queue.Queue) -> None:
        """Attach a queue to the data source."""
        if not isinstance(queue_obj, queue.Queue):
            raise ValueError(f"Expected a Queue, got {type(queue_obj)}")
        self.raw_data_queue = queue_obj

    @abstractmethod
    def rows(self) -> Generator[dict[str, Any], None, None]:
        """Basic method to get rows from the data source."""

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
        if self.raw_data_queue is None:
            raise ValueError("Output queue is not set")
        for row in self.rows():
            self.raw_data_queue.put(
                RawDatum(data_source=self, row=row),
            )
            self.message_counter += 1
        self.raw_data_queue.put(
            EndOfData(data_source=self),
        )
        LOGGER.info("Finished loading %s", self.name)


class DataSourceMapping:
    """
    A mapping from keys to ``Feature`` objects.

    (key, Feature, identifier_key)
    (source_identifier_key, Relationship, target_identifier_key)
    """

    def __init__(
        self,
        data_source: DataSource,
        attribute_key: str,
        identifier_key: str,
        attribute: str,
    ):
        self.data_source = data_source
        self.attribute_key = attribute_key
        self.identifier_key = identifier_key
        self.attribute = attribute

        data_source.mappings.append(self)

    # Change this to using Facts directly
    # def attach(self, mapping: Tuple[str, Type[Feature], str]):
    #     if issubclass(mapping[1], Feature):
    #         self.mapping[mapping[0]] = mapping[1]
    #         self.identifier_key = mapping[2]
    #         self.data_source.mapping_dict[mapping[0]] = (
    #             mapping[1],
    #             self.identifier_key,
    #         )
    #     elif issubclass(mapping[1], Relationship):
    #         self.data_source.mapping_dict[(mapping[0], mapping[2])] = mapping[
    #             1
    #         ]
    #     else:
    #         raise ValueError("Mapping must be a Feature or Relationship")


class FixtureDataSource(DataSource):
    """A ``DataSource`` that's just a list of dictionaries, useful for testing."""

    def __init__(
        self,
        data: list[dict[str, Any]],
        **kwargs,
    ):
        self.data = data
        super().__init__(**kwargs)

    def rows(self) -> Generator[dict[str, Any], None, None]:
        """Generate rows from the data."""
        yield from self.data


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
