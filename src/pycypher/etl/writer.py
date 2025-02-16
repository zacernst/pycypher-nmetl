"""TableWriter"""

from __future__ import annotations

import pathlib
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
        """ABC for writing data to a table from a generator of rows."""
        raise NotImplementedError

    @property
    def path(self) -> str:
        """Return the path portion of the URI."""
        out = pathlib.Path(str(self.target_uri.encode().path, encoding="utf8"))
        return out

    @property
    def extension(self):
        """Return the file extension of the URI."""
        return self.path.suffix

    @classmethod
    def get_writer(cls, uri: str) -> TableWriter:
        """Return the correct writer for the URI's scheme."""
        uri = ensure_uri(uri)
        suffix = pathlib.Path(uri.path).suffix
        match suffix:
            case ".parquet":
                return ParquetTableWriter(uri)
            case ".csv":
                return CSVTableWriter(uri)
            case _:
                raise ValueError(f"Unsupported file type: {uri}")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.target_uri.geturl()})"


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
