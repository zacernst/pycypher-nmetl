import threading
import queue
import time
from typing import Any, Dict, List, Optional, Callable, Generator, Union
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, Future
import pandas as pd
import csv
from pathlib import Path
from urllib.parse import urlparse

from nmetl.queue_generator import QueueGenerator, Shutdown
from nmetl.worker_context import WorkerContext
from shared.logger import LOGGER


class NewColumn:
    """Type hint for new column definitions."""
    def __init__(self, column_name: str):
        self.column_name = column_name
    
    def __class_getitem__(cls, column_name: str):
        return cls(column_name)


class DataSourceMapping:
    """Mapping configuration for data source attributes and relationships."""
    
    def __init__(
        self,
        attribute_key: Optional[str] = None,
        identifier_key: Optional[str] = None,
        attribute: Optional[str] = None,
        label: Optional[str] = None,
        source_key: Optional[str] = None,
        target_key: Optional[str] = None,
        source_label: Optional[str] = None,
        target_label: Optional[str] = None,
        relationship: Optional[str] = None,
    ):
        self.attribute_key = attribute_key
        self.identifier_key = identifier_key
        self.attribute = attribute
        self.label = label
        self.source_key = source_key
        self.target_key = target_key
        self.source_label = source_label
        self.target_label = target_label
        self.relationship = relationship


class DataSource(ABC):
    """Abstract base class for all data sources."""
    
    def __init__(self, uri: str, config: Optional[Any] = None):
        self.uri = uri
        self.config = config
        self.name: Optional[str] = None
        self.mappings: List[DataSourceMapping] = []
        self.data_types: Dict[str, Any] = {}
        self.schema: Dict[str, Any] = {}
        self._raw_input_queue: Optional[QueueGenerator] = None
        self._thread_pool: Optional[ThreadPoolExecutor] = None
        self._processing_thread: Optional[threading.Thread] = None
        self._shutdown_event = threading.Event()
        self._rows_queued = 0
        self._rows_processed = 0
        self._lock = threading.RLock()
        self.new_column_configs = {}
        self.finished = False

        self._processing_thread = threading.Thread(target=self.queue_rows(), daemon=True)
    
    @classmethod
    def from_uri(cls, uri: str, config: Optional[Any] = None) -> 'DataSource':
        """Factory method to create appropriate DataSource from URI."""
        parsed = urlparse(uri)
        
        if parsed.scheme == 'file':
            path = Path(parsed.path)
            if path.suffix.lower() == '.csv':
                return CSVDataSource(uri, config)
            elif path.suffix.lower() in ['.parquet', '.pq']:
                return ParquetFileDataSource(uri, config)
            else:
                raise ValueError(f"Unsupported file type: {path.suffix}")
        elif parsed.scheme in ['http', 'https']:
            return HTTPDataSource(uri, config)
        elif parsed.scheme == 'fixture':
            return FixtureDataSource(uri, config)
        else:
            raise ValueError(f"Unsupported URI scheme: {parsed.scheme}")
    
    def attach_mapping(self, mapping: DataSourceMapping):
        """Attach a mapping configuration to this data source."""
        with self._lock:
            self.mappings.append(mapping)
    
    def attach_schema(self, data_types: Dict[str, Any], type_dispatch: Dict[str, Any]):
        """Attach schema information to this data source."""
        with self._lock:
            self.data_types.update(data_types)
            self.schema.update(type_dispatch)
    
    def attach_output_queue(self, raw_input_queue: QueueGenerator):
        """Set the output queue for this data source."""
        self._raw_input_queue = raw_input_queue
    
    @abstractmethod
    def _load_data(self) -> Generator[Dict[str, Any], None, None]:
        """Load data from the source. Must be implemented by subclasses."""
        pass
    
    def _process_row(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process a single row according to mappings."""
        processed_items = []
        
        try:
            # Apply data type conversions
            typed_row = {}
            for key, value in row.items():
                if key in self.data_types and value is not None:
                    type_converter = self.schema.get(self.data_types[key])
                    if type_converter:
                        typed_row[key] = type_converter(value)
                    else:
                        typed_row[key] = value
                else:
                    typed_row[key] = value
            
            # Process mappings
            for mapping in self.mappings:
                if mapping.attribute_key and mapping.identifier_key:
                    # Attribute mapping
                    if (mapping.attribute_key in typed_row and 
                        mapping.identifier_key in typed_row):
                        
                        processed_items.append({
                            'type': 'attribute',
                            'entity_id': typed_row[mapping.identifier_key],
                            'entity_label': mapping.label,
                            'attribute': mapping.attribute,
                            'value': typed_row[mapping.attribute_key],
                            'source': self.name
                        })
                
                elif mapping.source_key and mapping.target_key:
                    # Relationship mapping
                    if (mapping.source_key in typed_row and 
                        mapping.target_key in typed_row):
                        
                        processed_items.append({
                            'type': 'relationship',
                            'source_id': typed_row[mapping.source_key],
                            'target_id': typed_row[mapping.target_key],
                            'source_label': mapping.source_label,
                            'target_label': mapping.target_label,
                            'relationship': mapping.relationship,
                            'source': self.name
                        })
        
        except Exception as e:
            LOGGER.error(f"Error processing row in {self.name}: {e}")
            LOGGER.debug(f"Row data: {row}")
        
        return processed_items
    
    def queue_rows(self):
        """Queue all rows from this data source."""
        if not self._raw_input_queue:
            LOGGER.error(f"No output queue set for data source {self.name}")
            return
        
        LOGGER.info(f"Starting to queue rows from {self.name}")
        try:
            for row in self._load_data():
                if self._shutdown_event.is_set():
                    break
                if 0 and self._rows_queued >= max_rows:
                    LOGGER.warning(f"Max rows reached for {self.name}")
                    break
                processed_items = self._process_row(row)
                
                for item in processed_items:
                    success = self._raw_input_queue.put(item)
                    if success:
                        with self._lock:
                            self._rows_queued += 1
                    else:
                        LOGGER.warning(f"Failed to queue item from {self.name}")
                        break
            
            LOGGER.info(f"Finished queuing {self._rows_queued} items from {self.name}")
            self.finished = True
            
        except Exception as e:
            LOGGER.error(f"Error queuing rows from {self.name}: {e}")
        finally:
            # Signal completion
            if self._raw_input_queue:
                self._raw_input_queue.put(Shutdown(), timeout=1.0)
    
    def start_processing(self):
        """Start processing this data source in a separate thread."""
        if self._processing_thread and self._processing_thread.is_alive():
            LOGGER.warning(f"Data source {self.name} is already processing")
            return
        
        self._processing_thread = threading.Thread(
            target=self.queue_rows,
            name=f"DataSource-{self.name}",
            daemon=True
        )
        self._processing_thread.start()
        LOGGER.info(f"Started processing thread for {self.name}")
    
    def stop_processing(self):
        """Stop processing this data source."""
        self._shutdown_event.set()
        if self._processing_thread:
            self._processing_thread.join(timeout=10.0)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics."""
        with self._lock:
            return {
                'name': self.name,
                'rows_queued': self._rows_queued,
                'rows_processed': self._rows_processed,
                'is_processing': self._processing_thread and self._processing_thread.is_alive(),
                'shutdown_requested': self._shutdown_event.is_set()
            }


class CSVDataSource(DataSource):
    """Data source for CSV files."""
    
    def _load_data(self) -> Generator[Dict[str, Any], None, None]:
        """Load data from CSV file."""
        parsed = urlparse(self.uri)
        file_path = Path(parsed.path)
        
        LOGGER.info(f"Loading CSV data from {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row_num, row in enumerate(reader, 1):
                    if self._shutdown_event.is_set():
                        break
                    
                    # Clean up row data
                    cleaned_row = {k.strip(): v.strip() if isinstance(v, str) else v 
                                 for k, v in row.items() if k is not None}
                    
                    yield cleaned_row
                    
                    if row_num % 1000 == 0:
                        LOGGER.debug(f"Processed {row_num} rows from {file_path}")
                        
        except Exception as e:
            LOGGER.error(f"Error reading CSV file {file_path}: {e}")
            raise


class ParquetFileDataSource(DataSource):
    """Data source for Parquet files."""
    
    def _load_data(self) -> Generator[Dict[str, Any], None, None]:
        """Load data from Parquet file."""
        parsed = urlparse(self.uri)
        file_path = Path(parsed.path)
        
        LOGGER.info(f"Loading Parquet data from {file_path}")
        
        try:
            df = pd.read_parquet(file_path)
            
            for index, row in df.iterrows():
                if self._shutdown_event.is_set():
                    break
                
                # Convert pandas Series to dict, handling NaN values
                row_dict = {}
                for key, value in row.items():
                    if pd.isna(value):
                        row_dict[key] = None
                    else:
                        row_dict[key] = value
                
                yield row_dict
                
                if (index + 1) % 1000 == 0:
                    LOGGER.debug(f"Processed {index + 1} rows from {file_path}")
                    
        except Exception as e:
            LOGGER.error(f"Error reading Parquet file {file_path}: {e}")
            raise


class HTTPDataSource(DataSource):
    """Data source for HTTP endpoints."""
    
    def _load_data(self) -> Generator[Dict[str, Any], None, None]:
        """Load data from HTTP endpoint."""
        # Implementation would depend on the specific HTTP API
        # This is a placeholder for future implementation
        LOGGER.warning(f"HTTPDataSource not fully implemented for {self.uri}")
        return
        yield  # Make this a generator


class FixtureDataSource(DataSource):
    """Data source for test fixtures."""
    
    def __init__(self, uri: str, config: Optional[Any] = None):
        super().__init__(uri, config)
        self.fixture_data: List[Dict[str, Any]] = []
    
    def set_fixture_data(self, data: List[Dict[str, Any]]):
        """Set the fixture data directly."""
        self.fixture_data = data
    
    def _load_data(self) -> Generator[Dict[str, Any], None, None]:
        """Load data from fixture."""
        LOGGER.info(f"Loading fixture data: {len(self.fixture_data)} rows")
        
        for row in self.fixture_data:
            if self._shutdown_event.is_set():
                break
            yield row


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
        super().__init__(daemon=True, name=f"RawDataThread-{data_source.name}")
        self.data_source = data_source
        self.thread_has_started = False
        self.halt = False
        self._raw_input_queue: Optional[QueueGenerator] = None

    @property
    def raw_input_queue(self) -> Optional[QueueGenerator]:
        """Get the raw input queue."""
        return self._raw_input_queue

    @raw_input_queue.setter
    def raw_input_queue(self, queue: QueueGenerator):
        """Set the raw input queue."""
        self._raw_input_queue = queue
        self.data_source.set_queue(queue)

    def run(self) -> None:
        """Run the thread to process data source."""
        self.thread_has_started = True
        LOGGER.info(f"Starting RawDataThread for {self.data_source.name}")
        
        try:
            self.data_source.queue_rows()
        except Exception as e:
            LOGGER.error(f"Error in RawDataThread for {self.data_source.name}: {e}")
        finally:
            LOGGER.info(f"RawDataThread finished for {self.data_source.name}")

    def block(self) -> None:
        """Block until the thread completes."""
        if self.is_alive():
            LOGGER.debug(f"Blocking on thread {self.data_source.name}")
            self.join()
            LOGGER.debug(f"Thread {self.data_source.name} is unblocking")
