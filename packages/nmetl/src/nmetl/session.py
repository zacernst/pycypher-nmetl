from __future__ import annotations

import datetime
import functools
import inspect
import queue
import random
import threading
import time
import tomllib
import uuid
from dataclasses import dataclass
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Self,
    Set,
    Type,
)

import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from docstring_parser import parse as parse_docstring
from nmetl.config import MONITOR_LOOP_DELAY  # type: ignore
from nmetl.configuration import TYPE_DISPATCH_DICT
from nmetl.data_asset import DataAsset
from nmetl.data_source import DataSource, DataSourceMapping
from nmetl.data_source_config import DataSourceConfig, SessionConfig
from nmetl.exceptions import (
    BadTriggerReturnAnnotationError,
    EmptyQueueError,
    UnknownDataSourceError,
)
from nmetl.queue_generator import QueueGenerator
from nmetl.queue_processor import (
    CheckFactAgainstTriggersQueueProcessor,
    FactGeneratedQueueProcessor,
    RawDataProcessor,
    TriggeredLookupProcessor,
)
from nmetl.session_enums import LoggingLevelEnum
from nmetl.thread_manager import ThreadManager
from nmetl.trigger import (
    NodeRelationship,
    NodeRelationshipTrigger,
    VariableAttribute,
    VariableAttributeTrigger,
)
from pycypher.cypher_parser import CypherParser
from pycypher.fact_collection import FactCollection
from pycypher.fact_collection.foundationdb import FoundationDBFactCollection
from pycypher.fact_collection.simple import SimpleFactCollection
from pydantic import FilePath
from rich.console import Console
from rich.table import Table
from shared.logger import LOGGER


@dataclass
class NewColumnConfig:
    """Dataclass to hold the configuration for a new column."""

    func: Any
    parameter_names: List[str]
    data_source_name: str
    new_column_name: str


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
        session_config_file: FilePath,
    ):  # pylint: disable=too-many-arguments
        """
        Initialize a new Session instance.
        """
        self.data_sources: List[DataSource] = []
        self.data_assets: dict[str, DataAsset] = {}
        self.queue_list: List[QueueGenerator] = []
        self.new_column_dict = {}
        self.status_queue = queue.Queue()
        self.trigger_dict = {}
        self.thread_manager: ThreadManager = ThreadManager(max_workers=10)

        # Read configuration
        with open(session_config_file, "r") as config_file:
            config_string: str = config_file.read()
            self.configuration = SessionConfig.model_validate(
                tomllib.loads(config_string)
            )

        # Create FactCollection
        fact_collection_class: Type = globals()[
            self.configuration.fact_collection_class
        ]
        self.fact_collection: FactCollection = fact_collection_class(
            foundationdb_cluster_file=self.configuration.foundationdb_cluster_file,
            sync_writes=self.configuration.sync_writes,
        )

        # Load the data source configs
        data_source_config_list: list[DataSourceConfig] = []
        with open(
            self.configuration.data_source_config_file, "r"
        ) as data_source_config_file:
            for data_source_config_dict in yaml.safe_load(
                data_source_config_file
            )["data_sources"]:
                data_source_config_dict["session_config"] = self.configuration
                data_source_config = DataSourceConfig.model_validate(
                    data_source_config_dict
                )
                data_source_config_list.append(data_source_config)

        # Create queues
        self.raw_input_queue = QueueGenerator(
            name="RawInput",
            session=self,
        )

        self.queue_list.append(self.raw_input_queue)

        self.fact_generated_queue = QueueGenerator(
            name="FactGenerated",
            session=self,
        )
        self.queue_list.append(self.fact_generated_queue)

        self.check_fact_against_triggers_queue = QueueGenerator(
            name="CheckFactTrigger",
            session=self,
        )
        self.queue_list.append(self.check_fact_against_triggers_queue)

        # Attach data sources
        for data_source_config in data_source_config_list:
            data_source = DataSource.from_uri(
                data_source_config.uri, config=data_source_config
            )
            data_source.name = data_source_config.name

            for mapping_config in data_source_config.mappings:
                mapping: DataSourceMapping = DataSourceMapping(
                    attribute_key=mapping_config.attribute_key,
                    identifier_key=mapping_config.identifier_key,
                    attribute=mapping_config.attribute,
                    label=mapping_config.label,
                    source_key=mapping_config.source_key,
                    target_key=mapping_config.target_key,
                    source_label=mapping_config.source_label,
                    target_label=mapping_config.target_label,
                    relationship=mapping_config.relationship,
                )
                data_source.attach_mapping(mapping)
                data_source.attach_schema(
                    data_source_config.data_types, TYPE_DISPATCH_DICT
                )

            LOGGER.info("Adding data source: %s", data_source.name)
            self.attach_data_source(data_source)

        self.triggered_lookup_processor_queue = QueueGenerator(
            name="TriggeredLookupProcessor",
            session=self,
        )
        self.queue_list.append(self.triggered_lookup_processor_queue)

        # Create QueueProcessor objects
        self.raw_data_processor = RawDataProcessor(
            session=self,
            incoming_queue=self.raw_input_queue,
            outgoing_queue=self.fact_generated_queue,
        )

        self.fact_generated_queue_processor = FactGeneratedQueueProcessor(
            session=self,
            incoming_queue=self.fact_generated_queue,
            outgoing_queue=self.check_fact_against_triggers_queue,
        )

        self.check_fact_against_triggers_queue_processor = (
            CheckFactAgainstTriggersQueueProcessor(
                session=self,
                incoming_queue=self.check_fact_against_triggers_queue,
                outgoing_queue=self.triggered_lookup_processor_queue,
            )
        )

        self.triggered_lookup_processor = TriggeredLookupProcessor(
            session=self,
            incoming_queue=self.triggered_lookup_processor_queue,
            outgoing_queue=self.fact_generated_queue,
        )

    def __getattr__(self, attr: str) -> Any:
        if hasattr(self.configuration, attr):
            return getattr(self.configuration, attr)
        raise ValueError(f"No such attribute: {attr}")

    def __call__(self, block: bool = True) -> None:
        """
        Start the session when the instance is called as a function.

        Args:
            block (bool, optional): Whether to block until all threads are finished.
                Defaults to True.
        """
        self.start_threads()

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
            - Data source loading threads
            - Raw data processor thread
            - Fact generated queue processor thread
            - Check fact against triggers queue processor thread
        """

        LOGGER.warning("Clearing FDB")
        self.fact_collection.db.clear_range(b"", b"\xff")
        LOGGER.warning("Starting threads...")

        # Start the data source threads
        for data_source in self.data_sources:
            if 0 and data_source.name != "state_county_tract_puma":
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
        if isinstance(other, DataSource):
            self.attach_data_source(other)
        else:
            raise ValueError(f"Expected a DataSource, got {type(other)}")
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
