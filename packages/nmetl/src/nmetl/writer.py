"""
Writer Module (writer.py)
==========================

The ``writer.py`` module in the `pycypher` library provides a set of tools for writing tabular data to various file formats. It defines a flexible and extensible system for handling the output of data from the `pycypher` ETL pipeline.

Core Concepts
-------------

*   **Tabular Data:** The module is designed to handle data that can be represented in a table-like structure (rows and columns), where each row is a collection of data and the columns are attributes.
*   **Data Destinations:** The module supports writing data to different destinations, such as local files with varying formats.
*   **Extensibility:** The design is extensible, allowing you to add support for new file types or output destinations by creating new classes that adhere to the established interfaces.

Key Components
--------------

1.  **``TableWriter`` (Abstract Base Class):**

    *   **Purpose:** This is the abstract base class that defines the common interface for all table writers. It outlines the basic methods and properties that all concrete table writer classes must implement.
    *   **Responsibilities:**
        *   Defines a standard way to initialize a writer with a target URI.
        *   Specifies an abstract ``write`` method that subclasses must implement to handle the actual writing of data.
        *   Provides methods for easily retrieving the ``path`` and ``extension`` from the target URI.
        *   Provides a ``write_entity_table`` method to easily output the table of facts for a single node label.
        *   Contains a ``get_writer`` class method to create the correct subclass based on the file type specified.
    *   **Key Methods:**
        *   ``__init__(self, target_uri: str, **kwargs)``: Initializes the writer with the target URI and any additional keyword arguments.
        *   ``write(self, generator, entity: Optional[str] = None, session: Optional[Session] = None, **kwargs)``: An abstract method that must be implemented by subclasses to write data from a generator to the specified target.
        *   ``path(self) -> str``: A property that returns the file path from the target URI.
        *   ``extension(self)``: A property that returns the file extension from the target URI.
        * `write_entity_table(self, session: Session, entity: str)`: write out all the rows for a particular entity type.
        * `get_writer(cls, uri: str) -> TableWriter`: return the correct sub class based on the uri.
        * ``__repr__(self) -> str``: Returns a string representation of the `TableWriter` object.
        *

2.  **``ParquetTableWriter`` (Concrete Implementation):**

    *   **Purpose:** A concrete implementation of ``TableWriter`` that writes data to Parquet files.
    *   **Responsibilities:**
        *   Implements the ``write`` method to handle the specifics of writing tabular data to Parquet format using the ``pyarrow`` library.
        *   Takes a generator that yields rows of data as dictionaries.
    *   **Key Methods:**
        *   ``write(self, generator, entity: str = None, session: Session = None, **kwargs)``: Writes data to a Parquet file.

3.  **``CSVTableWriter`` (Concrete Implementation):**

    *   **Purpose:** A concrete implementation of ``TableWriter`` that writes data to CSV files.
    *   **Responsibilities:**
        *   Implements the ``write`` method to handle the specifics of writing tabular data to CSV format using the built-in ``csv`` module.
        * Takes a generator that yields rows of data as dictionaries.
    *   **Key Methods:**
        *   ``write(self, generator, entity: Optional[str] = None, session: Optional[Session] = None, **kwargs)``: Writes data to a CSV file.

Workflow
--------

1.  **Initialization:** A ``TableWriter`` subclass is created with a specified target URI.
2.  **Data Preparation:** A generator is created that yields tabular data (e.g., lists, dictionaries).
3.  **Writing:** The ``write`` method of the ``TableWriter`` is called, passing the data generator.
4.  **Format Handling:** The concrete ``TableWriter`` subclass handles the specific logic for writing to the target format (e.g., Parquet or CSV).

Key Features
------------

*   **URI Handling:** The module uses URIs to represent data destinations, allowing for flexibility in specifying various targets.
*   **Generator Input:** Writers expect a generator as input, allowing for efficient processing of large datasets without loading everything into memory.
*   **Format-Specific Logic:** Each concrete writer class handles the details of writing to its specific format.
* **File Extension Detection**: The `get_writer` class method correctly creates the appropriate class based on the file extension in the target uri.

Extensibility
-------------

The module is designed to be extended:

*   **New File Types:** You can add support for new file formats by creating a new class that inherits from ``TableWriter`` and implements the ``write`` method.
*   **New Destinations:** You can support new destinations (e.g., cloud storage, databases) by creating new ``TableWriter`` subclasses that handle the specifics of writing to those destinations.

Use Cases
---------

*   **Saving ETL Results:** The module is ideal for writing the results of an ETL process to a file or set of files.
*   **Data Export:** It can be used to export data from a graph-like structure to a tabular format for analysis in other tools.
*   **Data Interchange:** It facilitates the exchange of data in standard tabular formats between different systems.
"""

from __future__ import annotations

import csv
import pathlib
from abc import ABC, abstractmethod
from typing import Optional

import pyarrow as pa
from nmetl.session import Session
from shared.helpers import ensure_uri


class TableWriter(ABC):
    """
    Abstract base class for writing tabular data to various storage formats.

    This class defines the common interface for table writers, providing methods
    for writing data from a generator to a specified target URI.
    """

    def __init__(self, target_uri: str, **kwargs):
        """
        Initializes a TableWriter instance.

        Args:
            target_uri (str): The URI of the target storage location.
            **kwargs: Additional keyword arguments to be passed to the writer.
        """
        self.target_uri = ensure_uri(target_uri)
        self.kwargs = kwargs

    @abstractmethod
    def write(
        self,
        generator,
        entity: Optional[str] = None,
        session: Optional[Session] = None,
        **kwargs,
    ):
        """
        Abstract method for writing data to a table from a generator of rows.

        Args:
            generator: An iterator or generator yielding rows of data.
            entity (Optional[str]): The name of the entity being written. Defaults to None.
            session (Optional[Session]): The Session instance. Defaults to None.
            **kwargs: Additional keyword arguments for the specific writer.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.
        """
        raise NotImplementedError

    @property
    def path(self) -> str:
        """
        Returns the path portion of the target URI.

        Returns:
            str: The path portion of the URI.
        """
        out = pathlib.Path(str(self.target_uri.encode().path, encoding="utf8"))
        return out

    @property
    def extension(self):
        """
        Returns the file extension of the target URI.

        Returns:
            str: The file extension of the URI.
        """
        return self.path.suffix

    def write_entity_table(self, session: Session, entity: str):
        """
        Writes an entity table to the target URI.

        This method retrieves rows associated with a specific entity label from
        the Session instance and writes them using the concrete writer implementation.

        Args:
            session (Session): The Session instance.
            entity (str): The label of the entity to write.
        """
        gen = session.rows_by_node_label(entity)
        self.write(gen, entity=entity, session=session)

    @classmethod
    def get_writer(cls, uri: str) -> TableWriter:
        """
        Returns the correct writer instance based on the URI's scheme.

        Args:
            uri (str): The target URI.

        Returns:
            TableWriter: A concrete TableWriter instance.

        Raises:
            ValueError: If the file type is unsupported.
        """
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
        """
        Returns a string representation of the TableWriter.

        Returns:
            str: A string representation of the TableWriter.
        """
        return f"{self.__class__.__name__}({self.target_uri.geturl()})"


class ParquetTableWriter(TableWriter):
    """
    TableWriter implementation for writing data to Parquet files.
    """

    def write(
        self,
        generator,
        entity: str = None,
        session: Session = None,
        **kwargs,
    ):
        """
        Writes data from a generator to a Parquet file.

        Args:
            generator: An iterator or generator yielding rows of data (dicts).
            entity (str, optional): The name of the entity being written. Defaults to None.
            session (Session, optional): A reference to the Session object. Defaults to None.
            **kwargs: additional keyword arguments.
        """
        schema = pa.schema(generator)
        with pa.RecordBatchFileWriter(self.target_uri, schema) as writer:
            for batch in generator:
                writer.write_table(pa.Table.from_pydict(batch))


class CSVTableWriter(TableWriter):
    """
    TableWriter implementation for writing data to CSV files.
    """

    def write(
        self,
        generator,
        entity: Optional[str] = None,
        session: Optional[Session] = None,
        **kwargs,
    ):
        """
        Writes data from a generator to a CSV file.

        Args:
            generator: An iterator or generator yielding rows of data (dicts).
            entity (Optional[str]): The label of the entity being written. Defaults to None.
            session (Optional[Session]): A reference to the Session object. Defaults to None.
            **kwargs: additional keyword arguments.
        """
        fieldnames = sorted(
            list(session.node_label_attribute_inventory()[entity])
        )
        with open(self.target_uri.path, "w", encoding="utf8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for row in generator:
                writer.writerow(row)
