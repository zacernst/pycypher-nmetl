"""Main class for managing ETL flow."""

from __future__ import annotations

import functools
import inspect
import threading
import time
from hashlib import md5
from typing import Dict, Generator, Iterable, List, Optional, Type

from pycypher.etl.data_source import DataSource
from pycypher.etl.fact import AtomicFact, FactCollection, FactNodeHasAttributeWithValue
from pycypher.etl.message_types import RawDatum, EndOfData
from pycypher.etl.solver import Constraint
from pycypher.etl.trigger import CypherTrigger
from pycypher.util.config import MONITOR_LOOP_DELAY  # pylint: disable=no-name-in-module
from pycypher.util.logger import LOGGER
from pycypher.util.helpers import QueueGenerator


class RawDataProcessor:
    """Runs in a thread to process raw data from all the DataSource objects."""

    def __init__(self, goldberg: Optional[Goldberg] = None) -> None:
        self.goldberg = goldberg
        self.processing_thread = threading.Thread(target=self.process_raw_data)
        self.started = False

    def process_raw_data(self) -> None:
        """Process raw data from all the DataSource objects."""
        self.started = True
        counter = 0
        raw_input_queue_counter = 0
        for datum in self.goldberg.raw_input_queue.yield_items():
            raw_input_queue_counter += 1
            for fact in self.raw_datum_to_facts(datum):
                self.goldberg.fact_generated_queue.put(fact)
                counter += 1
        self.goldberg.fact_generated_queue.put(EndOfData())
        LOGGER.info("Processed %s facts", counter)
        LOGGER.info("Got %s raw data items", raw_input_queue_counter)
    
    def raw_datum_to_facts(
        self, raw_datum: RawDatum
    ) -> Generator[AtomicFact, None, None]:
        """Convert a RawDatum to a generator of AtomicFact objects."""
        data_source = raw_datum.data_source
        row = raw_datum.row
        for mapping in data_source.mappings:
            # Create a new fact from the raw_datum and the mapping
            LOGGER.debug("Mapping %s to %s", raw_datum, mapping)
            identifier = row[mapping.identifier_key]
            attribute_value = row[mapping.attribute_key]
            attribute = mapping.attribute
            
            LOGGER.debug("Creating fact %s", (identifier, attribute, attribute_value))
            fact = FactNodeHasAttributeWithValue(node_id=identifier, attribute=attribute, value=attribute_value)
            LOGGER.debug("Fact created: %s", fact)

            yield fact


class FactGeneratedQueueProcessor:  # pylint: disable=too-few-public-methods
    '''Reads from the fact_generated_queue and processes the facts
    by inserting them into the ``FactCollection``.
    '''
    def __init__(self, goldberg: Optional[Goldberg] = None) -> None:
        self.goldberg = goldberg
        self.processing_thread = threading.Thread(target=self.process_generated_facts)
        self.started = False

    def process_generated_facts(self) -> None:
        """Process new facts from the fact_generated_queue."""
        self.started = True
        counter = 0
        for fact in self.goldberg.fact_generated_queue.yield_items():
            if not isinstance(fact, (AtomicFact, EndOfData,)):
                LOGGER.warning('Expected an AtomicFact, got %s', type(fact))
                continue
            counter += 1
            LOGGER.warning('Adding fact %s to the fact collection: %s', fact, counter)
            self.goldberg.fact_collection.append(fact)
        LOGGER.warning('Processed %s facts', counter)


class Goldberg:  # pylint: disable=too-many-instance-attributes
    """Holds the triggers and fact collection and makes everything go."""

    def __init__(
        self,
        trigger_dict: Optional[Dict[str, CypherTrigger]] = None,
        fact_collection: Optional[FactCollection] = None,
        queue_class: Optional[Type] = QueueGenerator,
        queue_options: Optional[Dict] = None,
        data_sources: Optional[List[DataSource]] = None,
        raw_data_processor: Optional[RawDataProcessor] = None,
    ):  # pylint: disable=too-many-arguments
        self.trigger_dict = trigger_dict or {}
        self.fact_collection = fact_collection or FactCollection(facts=[])
        self.raw_data_processor = raw_data_processor or RawDataProcessor(self)
        self.fact_generated_queue_processor = FactGeneratedQueueProcessor(self)

        self.queue_class = queue_class
        self.queue_options = queue_options or {}

        # Instantiate the various queues using the queue_class
        self.raw_input_queue = self.queue_class(**self.queue_options)
        self.fact_generated_queue = self.queue_class(**self.queue_options)

        # Instantiate threads
        self.monitor_thread = threading.Thread(
            target=self.monitor, daemon=True
        )
        self.data_sources = data_sources or []

    def start_threads(self):
        """Start the threads."""
        # self.monitor_thread.start()
        for data_source in self.data_sources:
            data_source.loading_thread.start()
        # Process rows into Facts
        self.raw_data_processor.processing_thread.start()

        # Insert facts into the FactCollection
        self.fact_generated_queue_processor.processing_thread.start()
    
    def block_until_finished(self):
        """Block until all data sources have finished."""
        while not self.fact_generated_queue.completed:
            pass

    def monitor(self):
        """Generate stats on the ETL process."""
        # Run in daemon thread
        while True:
            time.sleep(MONITOR_LOOP_DELAY)

    def attach_fact_collection(self, fact_collection: FactCollection) -> None:
        """Attach a ``FactCollection`` to the machine."""
        if not isinstance(fact_collection, FactCollection):
            raise ValueError(
                f"Expected a FactCollection, got {type(fact_collection)}"
            )
        self.fact_collection = fact_collection

    def has_unfinished_data_source(self) -> bool:
        """Checks whether any of the data sources are still loading data."""
        LOGGER.warning('Checking if any data sources are still loading data')
        return any(
            not data_source.sent_end_of_data
            for data_source in self.data_sources
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
