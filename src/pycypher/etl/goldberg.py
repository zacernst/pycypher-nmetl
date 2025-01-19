"""Main class for managing ETL flow."""

from __future__ import annotations

import functools
import inspect
import threading
import time
from hashlib import md5
from queue import Queue
from typing import Dict, Generator, Iterable, List, Optional, Type

from pycypher.etl.data_source import DataSource
from pycypher.etl.fact import AtomicFact, FactCollection
from pycypher.etl.solver import Constraint
from pycypher.etl.trigger import CypherTrigger
from pycypher.util.config import MONITOR_LOOP_DELAY  # pylint: disable=unknown-import
from pycypher.util.logger import LOGGER


class Goldberg:  # pylint: disable=too-many-instance-attributes
    """Holds the triggers and fact collection and makes everything go."""

    def __init__(
        self,
        trigger_dict: Optional[Dict[str, CypherTrigger]] = None,
        fact_collection: Optional[FactCollection] = None,
        queue_class: Optional[Type] = Queue,
        queue_options: Optional[Dict] = None,
        data_sources: Optional[List[DataSource]] = None,
    ):
        self.trigger_dict = trigger_dict or {}
        self.fact_collection = fact_collection or FactCollection(facts=[])

        self.queue_class = queue_class
        self.queue_options = queue_options or {}

        # Instantiate the various queues using the queue_class
        self.raw_input_queue = self.queue_class(**self.queue_options)
        self.fact_generated_queue = self.queue_class(**self.queue_options)

        # Instantiate threads
        self.monitor_thread = threading.Thread(target=self.monitor)
        self.data_sources = data_sources or []

    def start_threads(self):
        """Start the threads."""
        self.monitor_thread.start()

    def monitor(self):
        """Generate stats on the ETL process."""
        while True:
            time.sleep(MONITOR_LOOP_DELAY)

    def attach_fact_collection(self, fact_collection: FactCollection) -> None:
        """Attach a ``FactCollection`` to the machine."""
        if not isinstance(fact_collection, FactCollection):
            raise ValueError(
                f"Expected a FactCollection, got {type(fact_collection)}"
            )
        self.fact_collection = fact_collection

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
        else:
            raise ValueError(
                f"Expected a CypherTrigger, FactCollection, or Fact, got {type(other)}"
            )
        return self

    def attach_data_source(self, data_source: DataSource) -> None:
        """Attach a DataSource to the machine."""
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
