"""TableWriter"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pyarrow as pa

from pycypher.util.helpers import ensure_uri


class TableWriter(ABC):
    """The function of a TableWriter is to write data to a table
    from a generator of shallow dictionaries that are rows.
    """

    def __init__(self, target_uri: str, **kwargs):
        self.target_uri = ensure_uri(target_uri)
        self.kwargs = kwargs

    @abstractmethod
    def write(self, generator):
        raise NotImplementedError

    @classmethod
    def get_writer(cls, uri: str) -> TableWriter:
        uri = ensure_uri(uri)
        if uri.scheme == "parquet":
            return ParquetTableWriter(uri)
        elif uri.scheme == "csv":
            return CSVTableWriter(uri)
        else:
            raise ValueError(f"Unsupported URI scheme: {uri.scheme}")


class ParquetTableWriter(TableWriter):
    """Parquet"""

    def write(self, generator):
        """Write in batches"""
        schema = pa.schema(generator)
        with pa.RecordBatchFileWriter(self.target_uri, schema) as writer:
            for batch in generator:
                writer.write_table(pa.Table.from_pydict(batch))


class CSVTableWriter(TableWriter):
    """CSV"""

    def write(self, generator):
        """Ugh, CSV files are so awful."""
        raise NotImplementedError
