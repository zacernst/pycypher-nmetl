from __future__ import annotations

import datetime
import functools
import inspect
import queue
import random
import threading
import time
import uuid
from dataclasses import dataclass
from types import MappingProxyType
from typing import (TYPE_CHECKING, Any, Callable, Dict, Generator, List,
                    Optional, Self, Set, Type)

import pyarrow as pa
import pyarrow.parquet as pq
from docstring_parser import parse as parse_docstring
from nmetl.config import MONITOR_LOOP_DELAY  # type: ignore
from nmetl.config import \
    TRIGGERED_LOOKUP_PROCESSOR_QUEUE_SIZE  # pyrefly: ignore; pyrefly: ignore; type: ignore
from nmetl.data_asset import DataAsset
from nmetl.data_source import DataSource
from nmetl.exceptions import (BadTriggerReturnAnnotationError, EmptyQueueError,
                              UnknownDataSourceError)
from nmetl.queue_generator import QueueGenerator
from nmetl.queue_processor import (CheckFactAgainstTriggersQueueProcessor,
                                   FactGeneratedQueueProcessor,
                                   RawDataProcessor, TriggeredLookupProcessor)
from nmetl.session_enums import LoggingLevelEnum
from nmetl.thread_manager import ThreadManager
from nmetl.trigger import (NodeRelationship, NodeRelationshipTrigger,
                           VariableAttribute, VariableAttributeTrigger)
from pycypher.cypher_parser import CypherParser
from pycypher.fact_collection import FactCollection
from pycypher.fact_collection.simple import SimpleFactCollection
from rich.console import Console
from rich.table import Table
from shared.logger import LOGGER

LOGGER.setLevel("WARNING")


@dataclass
class NewColumnConfig:
    """Dataclass to hold the configuration for a new column."""

    func: Any
    parameter_names: List[str]
    data_source_name: str
    new_column_name: str


# def _make_check_fact_against_triggers_queue_processor(
#     incoming_queue, outgoing_queue, status_queue, session_config, trigger_dict
# ) -> None:
#     CheckFactAgainstTriggersQueueProcessor(
#         incoming_queue=incoming_queue,
#         outgoing_queue=outgoing_queue,
#         status_queue=status_queue,
#         session_config=session_config,
#         trigger_dict=trigger_dict,
#         data_assets=self.data_assets,
#     )


class Session:  # pylint: disable=too-many-instance-attributes,too-many-public-methods
    """
    Manages the ETL pipeline execution, including triggers and fact collection.

    The Session class is the central coordinator for the ETL process. It manages:
    - Data sources and their mappings
    - Fact collection for storing processed data
    - Triggers for data transformations
    - Processing queues and threads
    - Monitoring and logging
    """

    def __init__(
        self,
        # compute_class_name: ComputeClassNameEnum = ComputeClassNameEnum.THREADING,
        # compute_options: Optional[Dict[str, Any]] = None,
        data_assets: Optional[dict[str, DataAsset]] = None,
        data_sources: Optional[List[DataSource]] = None,
        fact_collection_class: Type[FactCollection] = SimpleFactCollection,
        fact_collection_kwargs: Optional[Dict[str, Any]] = None,
        logging_level: LoggingLevelEnum = LoggingLevelEnum.WARNING,
        queue_list: Optional[List[QueueGenerator]] = None,
        queue_options: Optional[Dict[str, Any]] = None,
        run_monitor: Optional[bool] = False,
        session_config: Optional[Any] = None,
        create_queue_generators: bool = True,
        thread_manager: Optional[ThreadManager] = None,
        max_workers: int = 10,
        trigger_dict: Optional[Dict[str, Any]] = {},
        max_buffer_size: int = 1000,
        max_buffer_size_raw_data: int = 1_000,
        # worker_num: Optional[int] = 0,
        # num_workers: Optional[int] = 1,
    ):  # pylint: disable=too-many-arguments
        """
        Initialize a new Session instance.

        Args:
            fact_collection (Optional[FactCollection], optional): Collection to store facts.
                Defaults to None (creates a new FactCollection).
            data_assets (Optional[List[DataAsset]], optional): List of data assets.
                Defaults to None.
            logging_level (Optional[str], optional): Logging level for the session.
                Defaults to "WARNING".
            queue_options (Optional[Dict[str, Any]], optional): Options for queue initialization.
                Defaults to None.
            data_sources (Optional[List[DataSource]], optional): List of data sources.
                Defaults to None.
            queue_list (Optional[List[QueueGenerator]], optional): List of queues.
                Defaults to None.
            run_monitor (Optional[bool], optional): Whether to run the monitor thread.
                Defaults to True.
        """
        self.data_sources: List[DataSource] = data_sources or []
        self.data_assets: dict[str, DataAsset] = data_assets or {}
        self.run_monitor = run_monitor
        self.queue_options = queue_options or {}
        self.queue_list: List[QueueGenerator] = queue_list or []
        self.logging_level = logging_level
        self.new_column_dict = {}
        self.fact_collection_kwargs = fact_collection_kwargs or {}
        self.session_config = session_config
        self.status_queue = queue.Queue()

        self.fact_collection: FactCollection = fact_collection_class(
            **self.fact_collection_kwargs
        )

        self.trigger_dict = trigger_dict
        if create_queue_generators:
            self.raw_input_queue = QueueGenerator(
                name="RawInput",
                maxsize=max_buffer_size,
            )

            self.queue_list.append(self.raw_input_queue)

            self.fact_generated_queue = QueueGenerator(
                name="FactGenerated",
                maxsize=max_buffer_size,
            )
            self.queue_list.append(self.fact_generated_queue)

            # Must run in different processes
            self.check_fact_against_triggers_queue = QueueGenerator(
                name="CheckFactTrigger",
                maxsize=max_buffer_size,
            )
            self.queue_list.append(self.check_fact_against_triggers_queue)

            # Same -- run in different processes
            self.triggered_lookup_processor_queue = QueueGenerator(
                name="TriggeredLookupProcessor",
                maxsize=max_buffer_size,
            )
            self.queue_list.append(self.triggered_lookup_processor_queue)

            self.raw_data_processor = RawDataProcessor(
                incoming_queue=self.raw_input_queue,
                outgoing_queue=self.fact_generated_queue,
                status_queue=self.status_queue,
                session_config=self.session_config,
                priority=-10,
                fact_collection=self.fact_collection,
                trigger_dict=self.trigger_dict,
                data_assets=self.data_assets,
                max_buffer_size=max_buffer_size,
            )

            self.fact_generated_queue_processor = FactGeneratedQueueProcessor(
                incoming_queue=self.fact_generated_queue,
                outgoing_queue=self.check_fact_against_triggers_queue,
                status_queue=self.status_queue,
                session_config=self.session_config,
                priority=0,
                fact_collection=self.fact_collection,
                trigger_dict=self.trigger_dict,
                data_assets=self.data_assets,
                max_buffer_size=max_buffer_size,
            )

            self.check_fact_against_triggers_queue_processor = (
                CheckFactAgainstTriggersQueueProcessor(
                    incoming_queue=self.check_fact_against_triggers_queue,
                    outgoing_queue=self.triggered_lookup_processor_queue,
                    status_queue=self.status_queue,
                    session_config=self.session_config,
                    priority=4,
                    fact_collection=self.fact_collection,
                    trigger_dict=self.trigger_dict,
                    data_assets=self.data_assets,
                    max_buffer_size=max_buffer_size,
                )
            )

            self.triggered_lookup_processor = TriggeredLookupProcessor(
                incoming_queue=self.triggered_lookup_processor_queue,
                outgoing_queue=self.fact_generated_queue,  # Changed
                status_queue=self.status_queue,
                session_config=self.session_config,
                priority=8,
                fact_collection=self.fact_collection,
                trigger_dict=self.trigger_dict,
                data_assets=self.data_assets,
                max_buffer_size=max_buffer_size,
            )

        # Instantiate threads
        if self.run_monitor:
            self.monitor_thread = threading.Thread(
                target=self.monitor, daemon=True, name="MonitorThread"
            )

        # fact_collection: FactCollection = globals()[self.session_config.fact_collection_class](**(self.session_config.fact_collection_kwargs or {}))

    # @property
    # def tasks_in_memory(self) -> int:
    #     """Gets the number of tasks in memory by inspecting the scheduler's state."""
    #     num_tasks: int = self.dask_client.run_on_scheduler(
    #         lambda dask_scheduler: len(dask_scheduler.tasks)
    #     )
    #     return num_tasks

    def __call__(self, block: bool = True) -> None:
        """
        Start the session when the instance is called as a function.

        Args:
            block (bool, optional): Whether to block until all threads are finished.
                Defaults to True.
        """
        LOGGER.warning('Clearing FDB') 
        self.fact_collection.db.clear_range(b'', b'\xFF')
        self.start_threads()
        if block:
            self.block_until_finished()

    def attribute_table(self) -> None:
        """
        Print a table showing the attributes and their descriptions.

        This method uses rich.console.Console to print a formatted table
        of attribute names and descriptions.
        """
        console: Console = Console()

        table = Table(title="Derived attributes")

        table.add_column("Name", justify="right", style="cyan", no_wrap=True)
        table.add_column("Description", style="green")
        table.add_column("Called", style="green")

        # for attribute_metadata in self.attribute_metadata_dict.values():
        #     table.add_row(
        #         attribute_metadata.attribute_name,
        #         attribute_metadata.description,
        #     )

        for trigger in self.trigger_dict.values():
            feature_name: str = trigger.attribute_set
            short_description: str = parse_docstring(
                trigger.docstring
            ).short_description
            table.add_row(
                feature_name, short_description, str(trigger.call_counter)
            )

        console.print(table)

        table = Table(title="Source attributes")

        table.add_column("Name", justify="right", style="cyan", no_wrap=True)
        table.add_column("Entity Type", style="green")

        row_list = []
        for data_source in self.data_sources:
            for mapping in data_source.mappings:
                if not mapping.attribute_key:
                    continue  # Relationships not supported yet for docs table
                row = [mapping.attribute_key, mapping.label]
                row_list.append(row)
        for row in row_list:
            table.add_row(*row)

        console.print(table)

    def start_threads(self) -> None:
        """
        Start all the processing threads for this session.

        This includes:
            - Monitor thread (if run_monitor is True)
            - Data source loading threads
            - Raw data processor thread
            - Fact generated queue processor thread
            - Check fact against triggers queue processor thread
        """
        # Start the monitor thread
        if self.run_monitor:
            self.monitor_thread.start()
        else:
            LOGGER.warning("Not starting monitor thread")
        
        LOGGER.warning('Clearing FDB') 
        self.fact_collection.db.clear_range(b'', b'\xFF')

        # Start the data source threads
        for data_source in self.data_sources:
            if data_source.name != 'state_county_tract_puma':
                continue
            LOGGER.error("Starting: %s", data_source.name)
            data_source.start_processing()

        # Process rows into Facts
        self.raw_data_processor.processing_thread.start()

        # Insert facts into the FactCollection
        self.fact_generated_queue_processor.processing_thread.start()

        # Check facts against triggers
        # for i in self.check_fact_against_triggers_queue_processor:
        #     i.processing_thread.start()
        self.check_fact_against_triggers_queue_processor.processing_thread.start()

        # Triggered lookup processor
        self.triggered_lookup_processor.processing_thread.start()

    def halt(self, level: int = 0) -> None:
        """Stop the threads."""
        LOGGER.critical("Halt: Signal received with level %s", level)
        for queue_processor in [
            self.fact_generated_queue_processor,
            self.check_fact_against_triggers_queue_processor,
            self.raw_data_processor,
            self.triggered_lookup_processor,
        ]:
            queue_processor.halt_signal = True
            LOGGER.info("Halt signal sent...")

    def get_all_known_labels(self) -> List[str]:
        """Search through the data sources and extract all the labels"""
        mappings = []
        for data_source in self.data_sources:
            mappings.extend(data_source.mappings)
        labels = sorted(
            list(
                set(
                    mapping.label
                    for mapping in mappings
                    if mapping.label is not None
                )
            )
        )
        return labels

    def get_population_with_label(self, label: str) -> Set[str]:
        """Search through the data sources and extract all the labels"""
        population = set()
        for fact in self.fact_collection.node_has_label_facts():
            if fact.label == label:
                population.add(fact.node_id)
        return population

    def get_all_attributes_for_label(self, label: str) -> List[str]:
        """Search through the data sources and extract all the attributes for a label"""
        attributes = []
        population = self.get_population_with_label(label)
        for fact in self.fact_collection.node_has_attribute_with_value_facts():
            if fact.node_id in population:
                attributes.append(fact.attribute)
        return sorted(list(set(attributes)))

    def pyarrow_from_node_label(self, label: str) -> pa.Table:
        """Search through the data sources and extract all the attributes for a label"""
        LOGGER.info("Creating pyarrow table from node label %s", label)
        table = pa.Table.from_pylist(list(self.rows_by_node_label(label)))
        return table

    def parquet_from_node_label(
        self, label: str, path: Optional[str] = None
    ) -> None:
        """Write a parquet file from a node label"""
        table = self.pyarrow_from_node_label(label)
        pq.write_table(table, path or f"{label}.parquet")

    def get_all_known_attributes(self) -> List[str]:
        """Search through the data sources and extract all the attributes"""
        attributes = []
        for data_source in self.data_sources:
            attributes.extend(data_source.mappings)
        attributes = [
            i.attribute for i in attributes if i.attribute is not None
        ]
        for trigger in self.trigger_dict.values():
            attributes.append(trigger.attribute_set)
        attributes = sorted(list(set(attributes)))
        return attributes

    def block_until_finished(self) -> None:
        """Block until all data sources have finished. Re-raise
        exceptions from threads.
        """

        def _check_status_queue() -> None:
            try:
                obj = self.status_queue.get(timeout=0.1)
            except EmptyQueueError:
                return
            if isinstance(obj, Exception):
                LOGGER.error("Error in thread: %s", obj)
                raise obj
            LOGGER.error("Unknown object on status queue: %s", obj)
            raise ValueError(f"Unknown object on status queue {obj}")

        while any(
            not data_source.finished for data_source in self.data_sources
        ):
            time.sleep(0.1)
        LOGGER.warning("Data sources are all finished...")

        while 1:
            pass

        while not all(
            process.idle
            for process in [
                self.fact_generated_queue_processor,
                self.check_fact_against_triggers_queue_processor,
                self.raw_data_processor,
            ]
        ):
            time.sleep(0.1)

        LOGGER.info("All threads are idle...")
        LOGGER.info("Halting...")
        self.halt()
        LOGGER.info("Halted.")

    def monitor(self) -> None:
        """Loop the _monitor function"""
        time.sleep(MONITOR_LOOP_DELAY)
        while True:
            self._monitor()
            time.sleep(MONITOR_LOOP_DELAY)

    def _monitor(self):
        """Generate stats on the ETL process."""
        # Run in daemon thread
        for data_source in self.data_sources:
            LOGGER.debug(
                "Data source %s has sent %s messages",
                data_source.name,
                data_source.sent_counter,
            )

        console = Console()
        table = Table(title="Thread queues")

        table.add_column("Thread", justify="right", style="cyan", no_wrap=True)
        table.add_column("Started", style="magenta")
        table.add_column("Finished", justify="right", style="green")
        table.add_column("Received", justify="right", style="green")
        table.add_column("Sent", justify="right", style="green")

        monitored_threads: list[
            DataSource
            | CheckFactAgainstTriggersQueueProcessor
            | FactGeneratedQueueProcessor
            | RawDataProcessor
        ] = [
            self.fact_generated_queue_processor,
            self.check_fact_against_triggers_queue_processor,
            self.raw_data_processor,
        ]
        monitored_threads.extend(self.data_sources)
        for monitored_thread in monitored_threads:
            end_time: datetime.datetime | None = (
                datetime.datetime.now()
                if not monitored_thread.finished
                else monitored_thread.finished_at
            )
            received_rate: float = round(
                monitored_thread.received_counter
                / float(
                    (end_time - monitored_thread.started_at).total_seconds()
                ),
                1,
            )
            sent_rate = round(
                monitored_thread.sent_counter
                / float(
                    (end_time - monitored_thread.started_at).total_seconds()
                ),
                1,
            )
            started_at = (
                monitored_thread.started_at.strftime("%H:%M:%S")
                if monitored_thread.started_at
                else "---"
            )
            finished_at = (
                monitored_thread.finished_at.strftime("%H:%M:%S")
                if monitored_thread.finished_at
                else "---"
            )
            table.add_row(
                getattr(
                    monitored_thread,
                    "name",
                    monitored_thread.__class__.__name__,
                ),
                started_at,
                finished_at,
                f"{monitored_thread.received_counter} ({received_rate}/s)",
                f"{monitored_thread.sent_counter} ({sent_rate}/s)",
            )

        console.print(table)

        table = Table(title="Queues")

        console.print(table)

    def attach_fact_collection(self, fact_collection: FactCollection) -> Self:
        """Attach a ``FactCollection`` to the machine."""
        if not isinstance(fact_collection, FactCollection):
            raise ValueError(
                f"Expected a FactCollection, got {type(fact_collection)}"
            )
        self.fact_collection = fact_collection
        return self

    def has_unfinished_data_source(self) -> bool:
        """Checks whether any of the data sources are still loading data."""
        LOGGER.debug("Checking if any data sources are still loading data")
        return any(
            not data_source.finished for data_source in self.data_sources
        )

    def data_source_by_name(self, name: str) -> Optional[DataSource]:
        """Return the data source with the given name."""
        for data_source in self.data_sources:
            if data_source.name == name:
                return data_source
        LOGGER.error("Data source %s not found", name)
        raise ValueError(f"Data source {name} not found")

    def __iadd__(self, other: FactCollection | DataSource) -> Session:
        """Add a ``FactCollection`` or ``DataSource``."""
        if isinstance(other, FactCollection):
            self.attach_fact_collection(other)
        elif isinstance(other, DataSource):
            self.attach_data_source(other)
        else:
            raise ValueError(
                f"Expected a DataSource or FactCollection, got {type(other)}"
            )
        return self

    def dump_entities(self, dir_path: str) -> None:
        """Loop over all entity labels and dump them to a parquet file."""
        for label in self.get_all_known_labels():
            self.parquet_from_node_label(label, f"{dir_path}/{label}.parquet")

    def attach_data_source(self, data_source: DataSource) -> None:
        """Attach a DataSource to the machine."""
        LOGGER.debug("Attaching input queue to %s", data_source)
        data_source.attach_output_queue(self.raw_input_queue)
        LOGGER.debug("Attaching data source %s", data_source)
        # data_source.worker_num = self.worker_num
        # data_source.num_workers = self.num_workers
        data_source.session = self
        self.data_sources.append(data_source)

    def node_label_attribute_inventory(self):
        """wraps the fact_collection method"""
        return self.fact_collection.node_label_attribute_inventory()

    def process_variable_attribute_annotation(
        self, return_annotation, parameter_names, trigger_argument, func
    ):
        variable_attribute_args = return_annotation.__args__
        if len(variable_attribute_args) != 2:
            raise ValueError(
                "Function must have a return annotation with two arguments."
            )

        variable_name: str = variable_attribute_args[0].__forward_arg__
        attribute_name: str = variable_attribute_args[1].__forward_arg__

        data_asset_parameters: dict[str, DataAsset] = {
            parameter: self.data_assets[parameter]
            for parameter in parameter_names
            if parameter in self.data_assets
        }
        non_data_asset_parameters: list[str] = [
            parameter
            for parameter in parameter_names
            if parameter not in self.data_assets
        ]

        trigger: VariableAttributeTrigger | NodeRelationshipTrigger = (
            VariableAttributeTrigger(
                function=functools.partial(func, **data_asset_parameters),
                cypher_string=trigger_argument,
                variable_set=variable_name,
                attribute_set=attribute_name,
                parameter_names=non_data_asset_parameters,
                # session=self,
            )
        )

        return trigger

    def process_relationship_annotation(
        self, return_annotation, parameter_names, trigger_argument, func
    ) -> VariableAttributeTrigger | NodeRelationshipTrigger:
        node_relationship_args = return_annotation.__args__
        # This might be outdated:

        source_variable_name = node_relationship_args[0].__forward_arg__
        relationship_name = node_relationship_args[1].__forward_arg__
        target_variable_name = node_relationship_args[2].__forward_arg__

        all_cypher_variables = CypherParser(
            trigger_argument
        ).parse_tree.cypher.match_clause.with_clause.all_variables
        if source_variable_name not in all_cypher_variables:
            raise BadTriggerReturnAnnotationError(
                f"Variable {source_variable_name} not in {all_cypher_variables}."
            )
        if target_variable_name not in all_cypher_variables:
            raise BadTriggerReturnAnnotationError(
                f"Variable {target_variable_name} not in {all_cypher_variables}"
            )
        trigger: VariableAttributeTrigger | NodeRelationshipTrigger = (
            NodeRelationshipTrigger(
                function=func,
                cypher_string=trigger_argument,
                source_variable=source_variable_name,
                relationship_name=relationship_name,
                target_variable=target_variable_name,
                parameter_names=parameter_names,
                # session=self,
            )
        )

        return trigger

    def process_return_annotation(self, func: Callable, trigger_argument: str):
        parameters: MappingProxyType[str, inspect.Parameter] = (
            inspect.signature(func).parameters
        )
        parameter_names: list[str] = list(parameters.keys())
        if not parameter_names:
            raise ValueError(
                "CypherTrigger functions require at least one parameter."
            )

        return_annotation = inspect.signature(func).return_annotation

        if return_annotation is inspect.Signature.empty:
            raise ValueError("Function must have a return annotation.")

        if return_annotation.__origin__ is VariableAttribute:
            trigger = self.process_variable_attribute_annotation(
                return_annotation, parameter_names, trigger_argument, func
            )
        elif return_annotation.__origin__ is NodeRelationship:
            trigger: NodeRelationshipTrigger | VariableAttributeTrigger = (
                self.process_relationship_annotation(
                    return_annotation, parameter_names, trigger_argument, func
                )
            )
        else:
            raise ValueError()

        trigger.docstring = func.__doc__

        return trigger

    def trigger(self, trigger_argument: str) -> Callable:
        """Decorator that registers a trigger with a Cypher string and a function."""

        def decorator(func) -> Callable:
            @functools.wraps
            def wrapper(*args, **kwargs) -> Any:
                result: Any = func(*args, **kwargs)
                return result

            trigger: NodeRelationshipTrigger | VariableAttributeTrigger = (
                self.process_return_annotation(func, trigger_argument)
            )

            self.trigger_dict[uuid.uuid4().hex] = trigger

            return wrapper

        return decorator

    def register_data_asset(self, data_asset: DataAsset) -> None:
        """Register a DataAsset with the session."""
        if not isinstance(data_asset, DataAsset):
            raise ValueError(f"Expected a DataAsset, got {type(data_asset)}")
        self.data_assets[data_asset.name] = data_asset.obj
        LOGGER.info("Registered data asset %s", data_asset.name)

    def new_column(
        self,
        data_source_name: str,
        attach_to_data_source: Optional[bool] = True,
    ) -> Callable[..., functools._Wrapper[Callable[..., Any], Any]]:
        """Decorator that registers a function as creating a "new column" from a DataSource."""

        def decorator(func) -> Callable[..., Any]:
            """Decorator that registers a function as creating a "new column" from a DataSource."""

            @functools.wraps
            def wrapper(*args) -> Any:
                result = func(*args)
                return result

            new_column_annotation = inspect.signature(func).return_annotation
            if not new_column_annotation:
                raise ValueError(
                    "new_column_annotation function must have a return annotation."
                )

            # if len(new_column_annotation.__args__) != 1:
            #     raise ValueError(
            #         "Function must have a return annotation with exactly one argument."
            #     )
            new_column_name = new_column_annotation.column_name

            parameters: MappingProxyType[str, inspect.Parameter] = (
                inspect.signature(func).parameters
            )
            parameter_names: list[str] = list(parameters.keys())
            if not parameter_names:
                raise ValueError(
                    "Derived column functions require at least one parameter."
                )

            self.new_column_dict[new_column_name] = NewColumnConfig(
                func=func,
                parameter_names=parameter_names,
                data_source_name=data_source_name,
                new_column_name=new_column_name,
            )

            if attach_to_data_source:
                if data_source_name not in [
                    ds.name for ds in self.data_sources
                ]:
                    raise UnknownDataSourceError(
                        f"Data source {data_source_name} not found in data sources"
                    )
                self.data_source_by_name(data_source_name).new_column_configs[
                    new_column_name
                ] = self.new_column_dict[new_column_name]

            return wrapper

        return decorator

    def rows_by_node_label(
        self, entity: str
    ) -> Generator[Dict[str, Any], None, None]:
        """Yield rows by node label"""
        yield from self.fact_collection.rows_by_node_label(entity)
