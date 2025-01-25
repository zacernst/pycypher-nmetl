"""Main class for managing ETL flow."""

from __future__ import annotations

from dataclasses import dataclass
import datetime
import functools
import inspect
import threading
import time
from hashlib import md5
from typing import Dict, Any, Generator, Iterable, List, Optional, Type

from rich.console import Console
from rich.table import Table

from pycypher.etl.data_source import DataSource
from pycypher.etl.fact import AtomicFact, FactCollection
from pycypher.etl.message_types import EndOfData, RawDatum
from pycypher.etl.solver import Constraint
from pycypher.etl.trigger import CypherTrigger
from pycypher.util.config import MONITOR_LOOP_DELAY  # pylint: disable=no-name-in-module
from pycypher.util.helpers import QueueGenerator
from pycypher.util.logger import LOGGER


class RawDataProcessor:
    """Runs in a thread to process raw data from all the DataSource objects."""

    def __init__(self, goldberg: Optional[Goldberg] = None) -> None:
        self.goldberg = goldberg
        self.processing_thread = threading.Thread(target=self.process_raw_data)
        self.started = False
        self.started_at = None
        self.finished = False
        self.finished_at = None
        self.received_counter = 0
        self.sent_counter = 0

    def process_raw_data(self) -> None:
        """Process raw data from the ``raw_input_queue``, generate facts,
        and put them into the ``fact_generated_queue``."""
        self.started = True
        self.started_at = datetime.datetime.now()
        for datum in self.goldberg.raw_input_queue.yield_items():
            self.received_counter += 1
            data_source = datum.data_source
            row = datum.row
            for fact in data_source.generate_raw_facts_from_row(row):
                self.goldberg.fact_generated_queue.put(fact)
                self.sent_counter += 1
                LOGGER.debug("Added fact %s to the fact_generated_queue", fact)
        self.finished = True
        self.finished_at = datetime.datetime.now()

    def raw_datum_to_facts(
        self, raw_datum: RawDatum
    ) -> Generator[AtomicFact, None, None]:
        """Convert a RawDatum to a generator of AtomicFact objects."""
        data_source = raw_datum.data_source
        row = raw_datum.row
        for fact in data_source.generate_raw_facts_from_row(row):
            yield fact


class FactGeneratedQueueProcessor:  # pylint: disable=too-few-public-methods
    """Reads from the fact_generated_queue and processes the facts
    by inserting them into the ``FactCollection``.
    """

    def __init__(self, goldberg: Optional[Goldberg] = None) -> None:
        self.goldberg = goldberg
        self.processing_thread = threading.Thread(
            target=self.process_generated_facts
        )
        self.started = False
        self.finished = False
        self.started_at = None
        self.finished_at = None
        self.received_counter = 0
        self.sent_counter = 0

    def process_generated_facts(self) -> None:
        """Process new facts from the fact_generated_queue."""
        self.started = True
        self.started_at = datetime.datetime.now()
        for fact in self.goldberg.fact_generated_queue.yield_items():
            self.received_counter += 1
            if not isinstance(
                fact,
                (
                    AtomicFact,
                    EndOfData,
                ),
            ):
                LOGGER.debug("Expected an AtomicFact, got %s", type(fact))
                continue
            LOGGER.debug(
                "Adding fact %s to the fact collection: %s",
                fact,
                self.sent_counter,
            )
            self.goldberg.fact_collection.append(fact)

            # Put the fact in the queue to be checked for triggers
            self.goldberg.check_fact_against_triggers_queue.put(fact)
            self.sent_counter += 1
        self.finished = True
        self.finished_at = datetime.datetime.now()


@dataclass
class SubTriggerPair:
    """A pair of a sub and a trigger."""
    sub: Dict[str, str]
    trigger: CypherTrigger


class CheckFactAgainstTriggersQueueProcessor:  # pylint: disable=too-few-public-methods
    """Reads from the check_fact_against_triggers_queue and processes the facts
    by checking them against the triggers.
    """

    def __init__(self, goldberg: Optional[Goldberg] = None) -> None:
        self.goldberg = goldberg
        self.processing_thread = threading.Thread(
            target=self.process_facts_against_triggers
        )
        self.started = False
        self.finished = False
        self.started_at = None
        self.finished_at = None
        self.received_counter = 0
        self.sent_counter = 0

    def process_facts_against_triggers(self) -> None:
        """Process new facts from the check_fact_against_triggers_queue."""
        self.started = True
        self.started_at = datetime.datetime.now()
        for (
            fact
        ) in self.goldberg.check_fact_against_triggers_queue.yield_items():
            self.received_counter += 1
            if not isinstance(
                fact,
                (
                    AtomicFact,
                    EndOfData,
                ),
            ):
                LOGGER.debug("Expected an AtomicFact, got %s", type(fact))
                continue
            LOGGER.debug(
                "Checking fact %s against triggers: %s",
                fact,
                self.received_counter,
            )
            for _, trigger in self.goldberg.trigger_dict.items():
                for constraint in trigger.constraints:
                    if sub := fact + constraint:
                        LOGGER.debug("Fact %s matched a trigger", fact)
                        # import pdb

                        # pdb.set_trace()
                        # import pdb; pdb.set_trace()
                        sub_trigger_pair = SubTriggerPair(sub=sub, trigger=trigger)
                        self.goldberg.triggered_lookup_processor_queue.put(
                            sub_trigger_pair
                        )
                        self.sent_counter += (
                            1  # Not yet sending anything to outbound queue
                        )
                        # TODO:
                        # Add "NodeHasAttributeWithValue" constraint to the cypher
                        # object's Match clause. Evaluate against the FactCollection.
                        # Find all the solutions, project the return clause and
                        # splat the results into the function.
                        # Put (trigger, sub,) into another queue
        self.goldberg.triggered_lookup_processor_queue.put(EndOfData())
        self.finished = True
        self.finished_at = datetime.datetime.now()


class TriggeredLookupProcessor:  # pylint: disable=too-few-public-methods
    """Reads from the check_fact_against_triggers_queue and processes the facts
    by checking them against the triggers.
    """

    def __init__(self, goldberg: Optional[Goldberg] = None) -> None:
        self.goldberg = goldberg
        self.processing_thread = threading.Thread(
            target=self.process_triggered_lookups
        )
        self.started = False
        self.finished = False
        self.started_at = None
        self.finished_at = None
        self.received_counter = 0
        self.sent_counter = 0

    def process_triggered_lookups(self) -> None:
        """Process new facts from the check_fact_against_triggers_queue."""
        self.started = True
        self.started_at = datetime.datetime.now()
        for sub_trigger_obj in self.goldberg.triggered_lookup_processor_queue.yield_items():
            self.received_counter += 1
            # Add "NodeHasAttributeWithValue" constraint to the cypher
            # object's Match clause. Evaluate against the FactCollection.
            # Find all the solutions, project the return clause and
            # splat the results into the function.
            # Put (trigger, sub,) into another queue
        self.finished = True
        self.finished_at = datetime.datetime.now()


class Goldberg:  # pylint: disable=too-many-instance-attributes
    """Holds the triggers and fact collection and makes everything go."""

    def __init__(
        self,
        fact_collection: Optional[FactCollection] = None,
        queue_class: Optional[Type] = QueueGenerator,
        queue_options: Optional[Dict[str, Any]] = None,
        data_sources: Optional[List[DataSource]] = None,
        queue_list: Optional[List[QueueGenerator]] = None,
    ):  # pylint: disable=too-many-arguments
        self.trigger_dict = {}
        self.fact_collection = fact_collection or FactCollection(facts=[])
        self.raw_data_processor = RawDataProcessor(self)
        self.fact_generated_queue_processor = FactGeneratedQueueProcessor(self)
        self.check_fact_against_triggers_queue_processor = (
            CheckFactAgainstTriggersQueueProcessor(self)
        )
        self.triggered_lookup_processor = TriggeredLookupProcessor(self)

        self.queue_class = queue_class
        self.queue_options = queue_options or {}
        self.queue_list = queue_list or []

        # Instantiate the various queues using the queue_class
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
            goldberg=self, name="TriggeredLookupProcessor", **self.queue_options
        )

        # Instantiate threads
        self.monitor_thread = threading.Thread(
            target=self.monitor, daemon=True
        )
        self.data_sources = data_sources or []

    def start_threads(self):
        """Start the threads."""
        # Start the monitor thread
        self.monitor_thread.start()

        for data_source in self.data_sources:
            data_source.loading_thread.start()
        
        # Process rows into Facts
        self.raw_data_processor.processing_thread.start()

        # Insert facts into the FactCollection
        self.fact_generated_queue_processor.processing_thread.start()

        # Check facts against triggers
        self.check_fact_against_triggers_queue_processor.processing_thread.start()

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
        '''Loop the _monitor function'''
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

        table.add_column(
            "Thread", justify="right", style="cyan", no_wrap=True
        )
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
                    (
                        end_time - monitored_thread.started_at
                    ).total_seconds()
                ),
                1,
            )
            sent_rate = round(
                monitored_thread.sent_counter
                / float(
                    (
                        end_time - monitored_thread.started_at
                    ).total_seconds()
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

        table.add_column(
            "Queue", justify="right", style="cyan", no_wrap=True
        )
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
        constraints = list(i for i in self.walk_constraints())
        return constraints

    def facts_matching_constraints(
        self, fact_generator: Iterable
    ) -> Generator[AtomicFact, None, None]:
        """Yield all the facts that match the constraints."""
        for fact in fact_generator:
            for constraint in self.constraints:
                if sub := fact + constraint:
                    yield fact, constraint, sub

    def __iadd__(
        self, other: CypherTrigger | FactCollection | AtomicFact
    ) -> Goldberg:
        """Add a CypherTrigger, FactCollection, or Fact to the machine."""
        if isinstance(other, CypherTrigger):
            self.register_trigger(other)
        elif isinstance(other, FactCollection):
            self.attach_fact_collection(other)
        elif isinstance(other, AtomicFact):
            self.fact_collection.append(other)
        elif isinstance(other, DataSource):
            self.attach_data_source(other)
        elif isinstance(other, RawDataProcessor):
            self.attach_raw_data_processor(other)
        else:
            raise ValueError(
                f"Expected a CypherTrigger, FactCollection, or Fact, got {type(other)}"
            )
        return self

    def attach_raw_data_processor(
        self, raw_data_processor: Optional[RawDataProcessor] = None
    ) -> None:
        """Attach the raw data processor to the machine."""
        if raw_data_processor is None:
            raw_data_processor = RawDataProcessor()
        self.raw_data_processor = raw_data_processor
        raw_data_processor.goldberg = self

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

            self.register_trigger(trigger)

            return wrapper

        return decorator

    def register_trigger(self, cypher_trigger: CypherTrigger) -> None:
        """
        Register a CypherTrigger with the machine.
        """
        self.trigger_dict[
            md5(cypher_trigger.cypher_string.encode()).hexdigest()
        ] = cypher_trigger
