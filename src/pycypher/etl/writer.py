"""TableWriter"""

from __future__ import annotations

import csv
import pathlib
from abc import ABC, abstractmethod
from typing import Optional

import pyarrow as pa

from pycypher.etl.goldberg import Goldberg
from pycypher.util.helpers import ensure_uri


class TableWriter(ABC):
    """The function of a TableWriter is to write data to a table
    from a generator of shallow dictionaries that are rows.
    """

    def __init__(self, target_uri: str, **kwargs):
        self.target_uri = ensure_uri(target_uri)
        self.kwargs = kwargs

    @abstractmethod
    def write(self, generator, entity: Optional[str] = None, goldberg: Optional[Goldberg] = None, **kwargs):
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

    def write_entity_table(self, goldberg: Goldberg, entity: str):
        """Write an entity table to the target URI."""
        gen = goldberg.rows_by_node_label(entity)
        self.write(gen, entity=entity, goldberg=goldberg)

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

    def write(self, generator, entity: str = None, goldberg: Goldberg = None, **kwargs):
        """Write in batches"""
        schema = pa.schema(generator)
        with pa.RecordBatchFileWriter(self.target_uri, schema) as writer:
            for batch in generator:
                writer.write_table(pa.Table.from_pydict(batch))


class CSVTableWriter(TableWriter):
    """CSV"""

    def write(self, generator, entity: Optional[str] = None, goldberg: Optional[Goldberg] = None, **kwargs):
        """Ugh, CSV files are so awful."""
        fieldnames = sorted(
            list(goldberg.node_label_attribute_inventory()[entity])
        )
        with open(self.target_uri.path, "w", encoding="utf8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for row in generator:
                writer.writerow(row)
