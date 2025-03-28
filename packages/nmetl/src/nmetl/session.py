"""
Session Class Documentation
===========================

The ``Session`` class is the central orchestrator within the ``pycypher``
library. It manages the entire data processing pipeline, from ingesting raw data
to executing triggers based on facts and constraints.
"""

from __future__ import annotations

import datetime
import functools
import inspect
import queue
import threading
import time
from dataclasses import dataclass
from hashlib import md5
from typing import Any, Dict, Generator, Iterable, List, Optional, Type

from nmetl.config import MONITOR_LOOP_DELAY  # pylint: disable=no-name-in-module
from nmetl.data_asset import DataAsset
from nmetl.data_source import DataSource
from nmetl.exceptions import (
    BadTriggerReturnAnnotationError,
    UnknownDataSourceError,
)
from nmetl.helpers import QueueGenerator
from nmetl.message_types import EndOfData
from nmetl.queue_processor import (
    CheckFactAgainstTriggersQueueProcessor,
    FactGeneratedQueueProcessor,
    RawDataProcessor,
    TriggeredLookupProcessor,
)
from nmetl.trigger import (
    NodeRelationship,
    NodeRelationshipTrigger,
    VariableAttribute,
    VariableAttributeTrigger,
)
from pycypher.cypher_parser import CypherParser
from pycypher.fact import AtomicFact, FactCollection
from pycypher.logger import LOGGER
from pycypher.solver import Constraint
from rich.console import Console
from rich.table import Table


@dataclass
class NewColumnConfig:
    """Dataclass to hold the configuration for a new column."""

    func: Any
    parameter_names: List[str]
    data_source_name: str
    new_column_name: str


class Session:  # pylint: disable=too-many-instance-attributes
    """Holds the triggers and fact collection and makes everything go."""

    def __init__(
        self,
        fact_collection: Optional[FactCollection] = None,
        data_assets: Optional[List[DataAsset]] = None,
        logging_level: Optional[str] = "WARNING",
        queue_class: Optional[Type] = QueueGenerator,
        queue_options: Optional[Dict[str, Any]] = None,
        data_sources: Optional[List[DataSource]] = None,
        queue_list: Optional[List[QueueGenerator]] = None,
        status_queue: Optional[queue.Queue] = None,
        run_monitor: Optional[bool] = True,
    ):  # pylint: disable=too-many-arguments
        # Instantiate the various queues using the queue_class
        self.data_sources = data_sources or []
        self.run_monitor = run_monitor
        self.queue_class = queue_class
        self.queue_options = queue_options or {}
        self.queue_list = queue_list or []
        self.logging_level = logging_level  # Not so sure
        # No need for a fancy queue class here
        self.status_queue = status_queue or queue.Queue()
        self.new_column_dict = {}

        self.raw_input_queue = self.queue_class(
            session=self, name="RawInput", **self.queue_options
        )
        self.data_assets = data_assets or []
        self.data_asset_names = [
            data_asset.name for data_asset in self.data_assets
        ]

        self.fact_generated_queue = self.queue_class(
            session=self, name="FactGenerated", **self.queue_options
        )
        self.check_fact_against_triggers_queue = self.queue_class(
            session=self, name="CheckFactTrigger", **self.queue_options
        )
        self.triggered_lookup_processor_queue = self.queue_class(
            session=self,
            name="TriggeredLookupProcessor",
            **self.queue_options,
        )

        self.trigger_dict = {}
        self.fact_collection = fact_collection or FactCollection(
            facts=[], session=self
        )
        self.raw_data_processor = RawDataProcessor(
            self,
            incoming_queue=self.raw_input_queue,
            outgoing_queue=self.fact_generated_queue,
            status_queue=self.status_queue,
        )
        self.fact_generated_queue_processor = FactGeneratedQueueProcessor(
            self,
            incoming_queue=self.fact_generated_queue,
            outgoing_queue=self.check_fact_against_triggers_queue,
            status_queue=self.status_queue,
        )
        self.check_fact_against_triggers_queue_processor = (
            CheckFactAgainstTriggersQueueProcessor(
                self,
                incoming_queue=self.check_fact_against_triggers_queue,
                outgoing_queue=self.triggered_lookup_processor_queue,
                status_queue=self.status_queue,
            )
        )
        self.triggered_lookup_processor = TriggeredLookupProcessor(
            self,
            incoming_queue=self.triggered_lookup_processor_queue,
            outgoing_queue=None,
            status_queue=self.status_queue,
        )

        # Instantiate threads
        if self.run_monitor:
            self.monitor_thread = threading.Thread(
                target=self.monitor, daemon=True, name="MonitorThread"
            )

    def __call__(self, block: bool = True):
        self.start_threads()
        if block:
            self.block_until_finished()

    # def attach_new_columns_to_data_sources(self):
    #     """Attach new columns to data sources."""
    #     for new_column in self.new_column_dict.values():
    #         data_source = self.data_source_by_name(new_column.data_source_name)
    #         data_source.new_columns[new_column.new_column_name] = new_column

    def start_threads(self):
        """Start the threads."""
        # Start the monitor thread
        if self.run_monitor:
            self.monitor_thread.start()
        else:
            LOGGER.warning("Not starting monitor thread")

        # Start the data source threads
        for data_source in self.data_sources:
            data_source.loading_thread.start()

        # Process rows into Facts
        self.raw_data_processor.processing_thread.start()

        # Insert facts into the FactCollection
        self.fact_generated_queue_processor.processing_thread.start()

        # Check facts against triggers
        time.sleep(0.5)
        self.check_fact_against_triggers_queue_processor.processing_thread.start()

        # Triggered lookup processor
        self.triggered_lookup_processor.processing_thread.start()

    def halt(self, level: int = 0):
        """Stop the threads."""
        LOGGER.critical("Halt: Signal received")
        if level == 0:
            for data_source in self.data_sources:
                LOGGER.critical("Halt: Stopping data source %s", data_source)
                data_source.halt = True
                data_source.raw_input_queue.put(EndOfData())
            LOGGER.critical("Halt: Waiting for data sources to finish")
            for data_source in self.data_sources:
                data_source.loading_thread.join()
            LOGGER.critical("Halt: Data sources finished")
            LOGGER.critical("Halt: Putting EndOfData on raw_input_queue")
            self.raw_input_queue.put(EndOfData())
        else:
            LOGGER.critical("Halting at non-zero level not yet implemented.")

    def block_until_finished(self):
        """Block until all data sources have finished. Re-raise
        exceptions from threads.
        """

        def _check_status_queue():
            try:
                obj = self.status_queue.get(timeout=0.1)
            except queue.Empty:
                return
            if isinstance(obj, Exception):
                LOGGER.error("Error in thread: %s", obj)
                raise obj
            LOGGER.error("Unknown object on status queue: %s", obj)
            raise ValueError(f"Unknown object on status queue {obj}")

        while not self.raw_data_processor.finished:
            _check_status_queue()

        while not self.check_fact_against_triggers_queue_processor.finished:
            _check_status_queue()

    def monitor(self):
        """Loop the _monitor function"""
        time.sleep(MONITOR_LOOP_DELAY)
        while True:
            self._monitor()
            time.sleep(MONITOR_LOOP_DELAY)

    def _monitor(self):
        """Generate stats on the ETL process."""
        # Run in daemon thread
        for data_source in self.data_sources:
            LOGGER.info(
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

        monitored_threads = [
            self.fact_generated_queue_processor,
            self.check_fact_against_triggers_queue_processor,
            self.raw_data_processor,
        ]
        monitored_threads.extend(self.data_sources)
        for monitored_thread in monitored_threads:
            end_time = (
                datetime.datetime.now()
                if not monitored_thread.finished
                else monitored_thread.finished_at
            )
            received_rate = round(
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
                monitored_thread.__class__.__name__,
                started_at,
                finished_at,
                f"{monitored_thread.received_counter} ({received_rate}/s)",
                f"{monitored_thread.sent_counter} ({sent_rate}/s)",
            )

        console.print(table)

        table = Table(title="Queues")

        table.add_column("Queue", justify="right", style="cyan", no_wrap=True)
        table.add_column("Size", style="magenta")
        table.add_column("Total", justify="right", style="green")
        table.add_column("Completed", justify="right", style="green")

        for session_queue in self.queue_list:
            table.add_row(
                session_queue.name,
                f"{session_queue.queue.qsize()}",
                f"{session_queue.counter}",
                f"{session_queue.completed}",
            )

        console.print(table)

    def attach_fact_collection(self, fact_collection: FactCollection) -> None:
        """Attach a ``FactCollection`` to the machine."""
        if not isinstance(fact_collection, FactCollection):
            raise ValueError(
                f"Expected a FactCollection, got {type(fact_collection)}"
            )
        self.fact_collection = fact_collection

    def has_unfinished_data_source(self) -> bool:
        """Checks whether any of the data sources are still loading data."""
        LOGGER.debug("Checking if any data sources are still loading data")
        return any(
            not data_source.finished for data_source in self.data_sources
        )

    def walk_constraints(self) -> Generator[Constraint, None, None]:
        """Yield all the triggers' constraints."""
        for trigger in self.trigger_dict.values():
            yield from trigger.constraints

    def data_source_by_name(self, name: str) -> Optional[DataSource]:
        """Return the data source with the given name."""
        for data_source in self.data_sources:
            if data_source.name == name:
                return data_source
        LOGGER.error("Data source %s not found", name)
        raise ValueError(f"Data source {name} not found")

    @property
    def constraints(self) -> List[Constraint]:
        """Return all the constraints from the triggers."""
        constraints = list(self.walk_constraints())
        return constraints

    def facts_matching_constraints(
        self, fact_generator: Iterable
    ) -> Generator[AtomicFact, None, None]:
        """Yield all the facts that match the constraints."""
        for fact in fact_generator:
            for constraint in self.constraints:
                if sub := fact + constraint:
                    yield fact, constraint, sub

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

    def attach_data_source(self, data_source: DataSource) -> None:
        """Attach a DataSource to the machine."""
        LOGGER.debug("Attaching input queue to %s", data_source)
        data_source.attach_queue(self.raw_input_queue)
        if not isinstance(data_source, DataSource):
            raise ValueError(f"Expected a DataSource, got {type(data_source)}")
        LOGGER.debug("Attaching data source %s", data_source)
        self.data_sources.append(data_source)

    def node_label_attribute_inventory(self):
        """wraps the fact_collection method"""
        return self.fact_collection.node_label_attribute_inventory()

    def trigger(self, arg1: str):
        """Decorator that registers a trigger with a Cypher string and a function."""

        def decorator(func):
            @functools.wraps
            def wrapper(*args, **kwargs):
                result = func(*args, **kwargs)
                return result

            parameters = inspect.signature(func).parameters
            parameter_names = list(parameters.keys())
            if not parameter_names:
                raise ValueError(
                    "CypherTrigger functions require at least one parameter."
                )

            return_annotation = inspect.signature(func).return_annotation

            if return_annotation is inspect.Signature.empty:
                raise ValueError("Function must have a return annotation.")

            if return_annotation.__origin__ is VariableAttribute:
                variable_attribute_args = return_annotation.__args__
                if len(variable_attribute_args) != 2:
                    raise ValueError(
                        "Function must have a return annotation with two arguments."
                    )

                variable_name = variable_attribute_args[0].__forward_arg__
                attribute_name = variable_attribute_args[1].__forward_arg__

                trigger = VariableAttributeTrigger(
                    function=func,
                    cypher_string=arg1,
                    variable_set=variable_name,
                    attribute_set=attribute_name,
                    parameter_names=parameter_names,
                    session=self,
                )

                if any(
                    param
                    not in trigger.cypher.parse_tree.cypher.return_clause.variables
                    and param not in self.data_asset_names
                    for param in parameter_names
                ):
                    raise BadTriggerReturnAnnotationError()

            elif return_annotation.__origin__ is NodeRelationship:
                node_relationship_args = return_annotation.__args__
                if (
                    len(node_relationship_args) != 3
                ):  # This might be impossible path
                    raise BadTriggerReturnAnnotationError(
                        "NodeRelationship annotation must have a return annotation "
                        "with three arguments."
                    )

                source_variable_name = node_relationship_args[
                    0
                ].__forward_arg__
                relationship_name = node_relationship_args[1].__forward_arg__
                target_variable_name = node_relationship_args[
                    2
                ].__forward_arg__

                all_cypher_variables = CypherParser(
                    arg1
                ).parse_tree.cypher.match_clause.with_clause.all_variables
                if source_variable_name not in all_cypher_variables:
                    raise BadTriggerReturnAnnotationError(
                        f"Variable {source_variable_name} not in {all_cypher_variables}."
                    )
                if target_variable_name not in all_cypher_variables:
                    raise BadTriggerReturnAnnotationError(
                        f"Variable {target_variable_name} not in {all_cypher_variables}"
                    )

                trigger = NodeRelationshipTrigger(
                    function=func,
                    cypher_string=arg1,
                    source_variable=source_variable_name,
                    relationship_name=relationship_name,
                    target_variable=target_variable_name,
                    parameter_names=parameter_names,
                    session=self,
                )

            else:
                raise ValueError(
                    "Trigger function must have a return annotation of type "
                    "VariableAttribute or NodeRelationship"
                )

            self.trigger_dict[
                md5(trigger.cypher_string.encode()).hexdigest()
            ] = trigger

            return wrapper

        return decorator

    def register_data_asset(self, data_asset: DataAsset) -> None:
        """Register a DataAsset with the session."""
        if not isinstance(data_asset, DataAsset):
            raise ValueError(f"Expected a DataAsset, got {type(data_asset)}")
        self.data_assets.append(data_asset)
        self.data_asset_names.append(data_asset.name)
        LOGGER.debug("Registered data asset %s", data_asset.name)

    def get_data_asset_by_name(self, name: str) -> DataAsset:
        """Get a DataAsset by name."""
        for data_asset in self.data_assets:
            if data_asset.name == name:
                return data_asset
        LOGGER.error("Data asset %s not found", name)
        raise ValueError(f"Data asset {name} not found")

    def new_column(
        self,
        data_source_name: str,
        attach_to_data_source: Optional[bool] = True,
    ):
        """Decorator that registers a function as creating a "new column" from a DataSource."""

        def decorator(func):
            @functools.wraps
            def wrapper(*args):
                result = func(*args)
                return result

            new_column_annotation = inspect.signature(func).return_annotation
            if new_column_annotation is inspect.Signature.empty:
                raise ValueError(
                    "new_column_annotation function must have a return annotation."
                )

            if len(new_column_annotation.__args__) != 1:
                raise ValueError(
                    "Function must have a return annotation with exactly one argument."
                )
            new_column_name = new_column_annotation.__args__[0].__forward_arg__

            parameters = inspect.signature(func).parameters
            parameter_names = list(parameters.keys())
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
