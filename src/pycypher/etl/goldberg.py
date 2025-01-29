"""Main class for managing ETL flow."""

from __future__ import annotations

import datetime
import functools
import inspect
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from hashlib import md5
from typing import Any, Dict, Generator, Iterable, List, Optional, Type

from rich.console import Console
from rich.table import Table

from pycypher.etl.data_source import DataSource
from pycypher.etl.fact import AtomicFact, FactCollection
from pycypher.etl.message_types import EndOfData, RawDatum
from pycypher.etl.solver import (
    Constraint,
    ConstraintVariableRefersToSpecificObject,
)
from pycypher.etl.trigger import CypherTrigger
from pycypher.util.config import MONITOR_LOOP_DELAY  # pylint: disable=no-name-in-module
from pycypher.util.helpers import QueueGenerator
from pycypher.util.logger import LOGGER


@dataclass
class SubTriggerPair:
    """A pair of a sub and a trigger."""

    sub: Dict[str, str]
    trigger: CypherTrigger


class QueueProcessor(ABC):
    """ABC that processes items from a queue and places the results onto another queue."""

    def __init__(
        self,
        goldberg: Optional[Goldberg] = None,
        incoming_queue: Optional[QueueGenerator] = None,
        outgoing_queue: Optional[QueueGenerator] = None,
    ) -> None:
        self.goldberg = goldberg
        self.processing_thread = threading.Thread(target=self.process_queue)
        self.started = False
        self.started_at = None
        self.finished = False
        self.finished_at = None
        self.received_counter = 0
        self.sent_counter = 0
        self.incoming_queue = incoming_queue
        self.outgoing_queue = outgoing_queue

    def process_queue(self) -> None:
        self.started = True
        self.started_at = datetime.datetime.now()
        for item in self.incoming_queue.yield_items():
            self.received_counter += 1
            out = self.process_item_from_queue(item)
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
        pass


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
        """Process new facts from the fact_generated_queue."""
        self.goldberg.fact_collection.append(item)
        # Put the fact in the queue to be checked for triggers
        self.outgoing_queue.put(item)


class CheckFactAgainstTriggersQueueProcessor(QueueProcessor):  # pylint: disable=too-few-public-methods
    """
    Reads from the check_fact_against_triggers_queue and processes the facts
    by checking them against the triggers.
    """

    def process_item_from_queue(self, item: Any) -> None:
        """Process new facts from the check_fact_against_triggers_queue."""
        out = []
        for _, trigger in self.goldberg.trigger_dict.items():
            for constraint in trigger.constraints:
                if sub := item + constraint:
                    LOGGER.debug("Fact %s matched a trigger", item)
                    sub_trigger_pair = SubTriggerPair(sub=sub, trigger=trigger)
                    out.append(sub_trigger_pair)
        return out


class TriggeredLookupProcessor(QueueProcessor):  # pylint: disable=too-few-public-methods
    """
    Reads from the check_fact_against_triggers_queue and processes the facts
    by checking them against the triggers.
    """

    def process_item_from_queue(self, item: Any) -> List[Any]:
        """Process new facts from the check_fact_against_triggers_queue."""
        self.started = True
        self.started_at = datetime.datetime.now()
        sub_trigger_obj = item
        self.received_counter += 1
        # sub_trigger_obj looks like:
        # subTriggerPair(sub={'n': 'Person::001'}, trigger=CypherTrigger())
        # Would add:
        #     ConstraintNodeHasAttributeWithValue('n', 'Identifier', 'Person::001')
        variable = tuple(sub_trigger_obj.sub)[0]
        node_id = sub_trigger_obj.sub[variable]

        specific_object_constraint = ConstraintVariableRefersToSpecificObject(
            variable=variable, node_id=node_id
        )
        #
        match_clause = item.trigger.cypher.parse_tree.cypher.match_clause
        # match_clause.constraints.append(specific_object_constraint)
        fact_collection = self.goldberg.fact_collection
        solutions = match_clause.solutions(fact_collection)

        # Need a constraint class saying that a node is a specific object.
        # TODO:
        # Add "NodeHasAttributeWithValue" constraint to the cypher
        # object's Match clause. Evaluate against the FactCollection.
        # Find all the solutions, project the return clause and
        # splat the results into the function.
        # Put (trigger, sub,) into another queue
        self.finished = True
        self.finished_at = datetime.datetime.now()
        return None


class Goldberg:  # pylint: disable=too-many-instance-attributes
    """Holds the triggers and fact collection and makes everything go."""

    def __init__(
        self,
        fact_collection: Optional[FactCollection] = None,
        queue_class: Optional[Type] = QueueGenerator,
        queue_options: Optional[Dict[str, Any]] = None,
        data_sources: Optional[List[DataSource]] = None,
        queue_list: Optional[List[QueueGenerator]] = None,
        run_monitor: Optional[bool] = True,
    ):  # pylint: disable=too-many-arguments
        # Instantiate the various queues using the queue_class
        self.data_sources = data_sources or []
        self.run_monitor = run_monitor
        self.queue_class = queue_class
        self.queue_options = queue_options or {}
        self.queue_list = queue_list or []

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
        self.fact_collection = fact_collection or FactCollection(facts=[])
        self.raw_data_processor = RawDataProcessor(
            self,
            incoming_queue=self.raw_input_queue,
            outgoing_queue=self.fact_generated_queue,
        )
        self.fact_generated_queue_processor = FactGeneratedQueueProcessor(
            self,
            incoming_queue=self.fact_generated_queue,
            outgoing_queue=self.check_fact_against_triggers_queue,
        )
        self.check_fact_against_triggers_queue_processor = (
            CheckFactAgainstTriggersQueueProcessor(
                self,
                incoming_queue=self.check_fact_against_triggers_queue,
                outgoing_queue=self.triggered_lookup_processor_queue,
            )
        )
        self.triggered_lookup_processor = TriggeredLookupProcessor(
            self,
            incoming_queue=self.triggered_lookup_processor_queue,
            outgoing_queue=None,
        )

        # Instantiate threads
        if self.run_monitor:
            self.monitor_thread = threading.Thread(
                target=self.monitor, daemon=True
            )

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
        """Block until all data sources have finished."""
        # TODO: Change this when we've got the next stage in the pipeline
        while not self.raw_data_processor.finished:
            pass
        # while not self.fact_generated_queue.completed:
        #     pass
        while not self.check_fact_against_triggers_queue_processor.finished:
            pass

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

        for queue in self.queue_list:
            table.add_row(
                queue.name,
                f"{queue.queue.qsize()}",
                f"{queue.counter}",
                f"{queue.completed}",
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

    def attach_raw_data_processor(
        self, raw_data_processor: Optional[RawDataProcessor] = None
    ) -> None:
        """Attach the raw data processor to the machine."""
        pass

    def attach_data_source(self, data_source: DataSource) -> None:
        """Attach a DataSource to the machine."""
        LOGGER.debug("Attaching input queue to %s", data_source)
        data_source.attach_queue(self.raw_input_queue)
        if not isinstance(data_source, DataSource):
            raise ValueError(f"Expected a DataSource, got {type(data_source)}")
        LOGGER.debug("Attaching data source %s", data_source)
        self.data_sources.append(data_source)

    def cypher_trigger(self, arg1):
        """Decorator that registers a trigger with a Cypher string and a function."""

        def decorator(func):
            @functools.wraps
            def wrapper(*args, **kwargs):
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
