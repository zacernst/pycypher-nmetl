"""Main class for managing ETL flow."""

from __future__ import annotations

import datetime
import functools
import inspect
import queue
import sys
import threading
import time
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass
from hashlib import md5
from typing import Any, Dict, Generator, Iterable, List, Optional, Type

from rich.console import Console
from rich.table import Table

from pycypher.core.node_classes import AliasedName
from pycypher.etl.data_source import DataSource
from pycypher.etl.fact import (
    AtomicFact,
    FactCollection,
    FactNodeHasAttributeWithValue,
)
from pycypher.etl.message_types import EndOfData
from pycypher.etl.query import NullResult, QueryValueOfNodeAttribute
from pycypher.etl.solver import Constraint
from pycypher.etl.trigger import CypherTrigger
from pycypher.util.config import MONITOR_LOOP_DELAY  # pylint: disable=no-name-in-module
from pycypher.util.helpers import QueueGenerator
from pycypher.util.logger import LOGGER


@dataclass
class SubTriggerPair:
    """A pair of a sub and a trigger."""

    sub: Dict[str, str]
    trigger: CypherTrigger

    def __hash__(self):
        return hash(
            (
                tuple(self.sub),
                self.trigger,
            )
        )


class QueueProcessor(ABC):  # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """ABC that processes items from a queue and places the results onto another queue."""

    def __init__(
        self,
        goldberg: Optional[Goldberg] = None,
        incoming_queue: Optional[QueueGenerator] = None,
        outgoing_queue: Optional[QueueGenerator] = None,
        status_queue: Optional[queue.Queue] = None,
    ) -> None:
        self.goldberg = goldberg
        self.processing_thread = threading.Thread(
            target=self.process_queue, name=self.__class__.__name__
        )
        self.started = False
        self.started_at = None
        self.finished = False
        self.finished_at = None
        self.received_counter = 0
        self.sent_counter = 0
        self.incoming_queue = incoming_queue
        self.outgoing_queue = outgoing_queue
        self.status_queue = status_queue

        if self.outgoing_queue:
            self.outgoing_queue.incoming_queue_processors.append(self)

    def process_queue(self) -> None:
        """Process every item in the queue using the yield_items method."""
        self.started = True
        self.started_at = datetime.datetime.now()
        for item in self.incoming_queue.yield_items():
            self.received_counter += 1
            try:
                out = self._process_item_from_queue(item)
            except Exception as e:  # pylint: disable=broad-except
                exc_type, exc_value, exc_traceback = sys.exc_info()
                formatted_traceback = traceback.format_exception(
                    exc_type, exc_value, exc_traceback
                )
                formatted_traceback = "\n".join(
                    [line.strip() for line in formatted_traceback]
                )
                error_msg = f"in thread: {threading.current_thread().name}\n"
                error_msg += f"Error processing item {item}: {e}]\n"
                error_msg += f"Traceback: {formatted_traceback}]\n"
                LOGGER.error(error_msg)
                self.status_queue.put(e)
                continue
            if not out:
                continue
            if not isinstance(out, list):
                out = [out]
            for out_item in out:
                self.outgoing_queue.put(out_item)
                self.sent_counter += 1
        self.finished = True
        self.finished_at = datetime.datetime.now()

    @abstractmethod
    def process_item_from_queue(self, item: Any) -> Any:
        """Process an item from the queue."""

    def _process_item_from_queue(self, item: Any) -> Any:
        """Wrap the process call in case we want some logging."""
        return self.process_item_from_queue(item)


class RawDataProcessor(QueueProcessor):
    """Runs in a thread to process raw data from all the DataSource objects."""

    def process_item_from_queue(self, item) -> List[AtomicFact]:
        """Process raw data from the ``raw_input_queue``, generate facts."""
        data_source = item.data_source
        row = item.row
        out = []
        for fact in data_source.generate_raw_facts_from_row(row):
            out.append(fact)
        return out


class FactGeneratedQueueProcessor(QueueProcessor):  # pylint: disable=too-few-public-methods
    """
    Reads from the fact_generated_queue and processes the facts
    by inserting them into the ``FactCollection``.
    """

    def process_item_from_queue(self, item: Any) -> None:
        """Process new facts from the fact_generated_queue. Attaches the Golderg object to the fact."""
        if item in self.goldberg.fact_collection:
            LOGGER.debug("Fact %s already in collection", item)
            return
        item.goldberg = self.goldberg
        self.goldberg.fact_collection.append(item)
        # Put the fact in the queue to be checked for triggers
        self.outgoing_queue.put(item)


class CheckFactAgainstTriggersQueueProcessor(QueueProcessor):  # pylint: disable=too-few-public-methods
    """
    Reads from the check_fact_against_triggers_queue and processes the facts
    by checking them against the triggers.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def process_item_from_queue(self, item: Any) -> None:
        """Process new facts from the check_fact_against_triggers_queue."""

        out = []
        LOGGER.debug("Checking fact %s against triggers", item)
        for _, trigger in self.goldberg.trigger_dict.items():
            LOGGER.debug("Checking trigger %s", trigger)
            for constraint in trigger.constraints:
                LOGGER.debug(
                    "Checking item: %s, constraint %s, trigger %s result: %s",
                    item,
                    constraint,
                    trigger,
                    item + constraint,
                )

                if sub := (item + constraint):
                    LOGGER.debug("Fact %s matched a trigger", item)
                    sub_trigger_pair = SubTriggerPair(sub=sub, trigger=trigger)
                    out.append(sub_trigger_pair)
        return out


class TriggeredLookupProcessor(QueueProcessor):  # pylint: disable=too-few-public-methods
    """
    Reads from the check_fact_against_triggers_queue and processes the facts
    by checking them against the triggers.
    """

    def process_item_from_queue(self, item: SubTriggerPair) -> List[Any]:
        """Process new facts from the check_fact_against_triggers_queue."""
        sub_trigger_obj = item
        self.started = True
        self.started_at = datetime.datetime.now()
        self.received_counter += 1
        variable_to_set = sub_trigger_obj.trigger.variable_set
        # match_clause = (
        #     sub_trigger_obj.trigger.cypher.parse_tree.cypher.match_clause
        # )
        # # match_clause.constraints.append(specific_object_constraint)
        fact_collection = self.goldberg.fact_collection

        return_clause = (
            sub_trigger_obj.trigger.cypher.parse_tree.cypher.return_clause
        )
        solutions = return_clause._evaluate(fact_collection)  # pylint: disable=protected-access

        for solution in solutions:
            splat = [
                solution.get(alias.name)
                for alias in return_clause.projection.lookups
            ]
            if any(isinstance(arg, NullResult) for arg in splat):
                LOGGER.debug("NullResult found in splat %s", splat)
                continue
            # Prevent call from happening if NullResult is present
            computed_value = sub_trigger_obj.trigger.function(*splat)
            sub_trigger_obj.trigger.call_counter += 1
            target_attribute = sub_trigger_obj.trigger.attribute_set
            # variable no longer present in solution because of alias renaming
            # import pdb; pdb.set_trace()
            # node_id = alias[name]
            node_id = solution["__with_clause_projection__"][
                "__match_solution__"
            ][variable_to_set]
            computed_fact = FactNodeHasAttributeWithValue(
                node_id=node_id,
                attribute=target_attribute,
                value=computed_value,
            )
            LOGGER.debug(">>>>>>> Computed fact: %s", computed_fact)
            self.goldberg.fact_generated_queue.put(computed_fact)
        self.finished = True
        self.finished_at = datetime.datetime.now()

    def process_item_from_queue_bak(self, item: SubTriggerPair) -> List[Any]:
        """Process new facts from the check_fact_against_triggers_queue."""
        sub_trigger_obj = item
        if isinstance(item, str):
            import pdb

            pdb.set_trace()
        self.started = True
        self.started_at = datetime.datetime.now()
        self.received_counter += 1
        variable = tuple(sub_trigger_obj.sub)[0]
        node_id = sub_trigger_obj.sub[variable]  # pylint: disable=unused-variable
        match_clause = (
            sub_trigger_obj.trigger.cypher.parse_tree.cypher.match_clause
        )
        # match_clause.constraints.append(specific_object_constraint)
        fact_collection = self.goldberg.fact_collection

        solutions = match_clause.solutions(fact_collection)  # pylint: disable=unused-variable
        return_clause = (
            sub_trigger_obj.trigger.cypher.parse_tree.cypher.return_clause
        )
        aliases = return_clause.projection.lookups
        for solution in solutions:
            splat = []
            for alias in aliases:
                try:
                    if hasattr(alias, "reference") and hasattr(
                        alias.reference, "aggregation"
                    ):  # pylint: disable=no-else-raise
                        # TODO: Implement aggregation in RETURN statement
                        raise NotImplementedError(
                            "Aggregation in RETURN not yet implemented"
                        )
                    elif isinstance(alias, AliasedName):
                        variable = alias.name
                        node_id = solution[variable]
                        attribute_value_query = QueryValueOfNodeAttribute(
                            node_id=node_id,
                            attribute=alias.name,
                        )
                    else:
                        variable = alias.reference.object  # HERE
                        node_id = solution[variable]
                        attribute = alias.reference.attribute
                        attribute_value_query = QueryValueOfNodeAttribute(
                            node_id=node_id,
                            attribute=attribute,
                        )
                except:
                    import pdb

                    pdb.set_trace()
                attribute_value = fact_collection.query(attribute_value_query)
                splat.append(attribute_value)
            if any(isinstance(arg, NullResult) for arg in splat):
                continue
            computed_value = sub_trigger_obj.trigger.function(*splat)
            sub_trigger_obj.trigger.call_counter += 1
            target_attribute = sub_trigger_obj.trigger.attribute_set
            computed_fact = FactNodeHasAttributeWithValue(
                node_id=node_id,
                attribute=target_attribute,
                value=computed_value,
            )
            self.goldberg.fact_generated_queue.put(computed_fact)
        self.finished = True
        self.finished_at = datetime.datetime.now()


class Goldberg:  # pylint: disable=too-many-instance-attributes
    """Holds the triggers and fact collection and makes everything go."""

    def __init__(
        self,
        fact_collection: Optional[FactCollection] = None,
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

        self.raw_input_queue = self.queue_class(
            goldberg=self, name="RawInput", **self.queue_options
        )
        self.fact_generated_queue = self.queue_class(
            goldberg=self, name="FactGenerated", **self.queue_options
        )
        self.check_fact_against_triggers_queue = self.queue_class(
            goldberg=self, name="CheckFactTrigger", **self.queue_options
        )
        self.triggered_lookup_processor_queue = self.queue_class(
            goldberg=self,
            name="TriggeredLookupProcessor",
            **self.queue_options,
        )

        self.trigger_dict = {}
        self.fact_collection = fact_collection or FactCollection(
            facts=[], goldberg=self
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

        for goldberg_queue in self.queue_list:
            table.add_row(
                goldberg_queue.name,
                f"{goldberg_queue.queue.qsize()}",
                f"{goldberg_queue.counter}",
                f"{goldberg_queue.completed}",
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

    def __iadd__(self, other: FactCollection | DataSource) -> Goldberg:
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

    def cypher_trigger(self, arg1):
        """Decorator that registers a trigger with a Cypher string and a function."""

        def decorator(func):
            @functools.wraps
            def wrapper(*args, **kwargs):
                # if any(isinstance(arg, NullResult) for arg in args):
                #     LOGGER.debug("NullResult found in args")
                #     return NullResult(None)  # Add something here

                result = func(*args, **kwargs)
                return result

            variable_attribute_annotation = inspect.signature(
                func
            ).return_annotation
            if variable_attribute_annotation is inspect.Signature.empty:
                raise ValueError("Function must have a return annotation.")

            variable_attribute_args = variable_attribute_annotation.__args__
            if len(variable_attribute_args) != 2:
                raise ValueError(
                    "Function must have a return annotation with two arguments."
                )
            variable_name = variable_attribute_args[0].__forward_arg__
            attribute_name = variable_attribute_args[1].__forward_arg__

            parameters = inspect.signature(func).parameters
            parameter_names = list(parameters.keys())
            if not parameter_names:
                raise ValueError(
                    "CypherTrigger functions require at least one parameter."
                )

            trigger = CypherTrigger(
                function=func,
                cypher_string=arg1,
                variable_set=variable_name,
                attribute_set=attribute_name,
                parameter_names=parameter_names,
                goldberg=self,
            )

            # Check that parameters are in the Return statement of the Cypher string
            for param in parameter_names:
                if (
                    param
                    not in trigger.cypher.parse_tree.cypher.return_clause.variables
                ):
                    raise ValueError(
                        f"Parameter {param} not found in Cypher string"
                    )

            self.trigger_dict[
                md5(trigger.cypher_string.encode()).hexdigest()
            ] = trigger

            return wrapper

        return decorator

    def rows_by_node_label(
        self, entity: str
    ) -> Generator[Dict[str, Any], None, None]:
        """Yield rows by node label"""
        yield from self.fact_collection.rows_by_node_label(entity)
