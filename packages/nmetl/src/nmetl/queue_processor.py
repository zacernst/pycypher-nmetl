"""
This module defines the core components for processing items in a queue-based
system within the pycypher library.

It provides an abstract base class, ``QueueProcessor``, and concrete
implementations for specific tasks in the data processing pipeline.

"""

from __future__ import annotations

# import cProfile
import datetime
import hashlib
import inspect
import sys
import threading

# import threading
import time
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass

# import queue
from queue import Queue
from typing import Any, Dict, List, Optional

from nmetl.helpers import Idle, QueueGenerator
from nmetl.trigger import CypherTrigger
from pycypher.fact import (
    AtomicFact,
    FactNodeHasAttributeWithValue,
    FactRelationshipHasLabel,
    FactRelationshipHasSourceNode,
    FactRelationshipHasTargetNode,
    NullResult,
)
from pycypher.logger import LOGGER
from pycypher.node_classes import Collection


@dataclass
class SubTriggerPair:
    """A pair of a sub and a trigger."""

    sub: Dict[str, str]
    trigger: CypherTrigger

    def __hash__(self):
        """
        Generate a hash value for this SubTriggerPair instance.

        The hash is based on the sub dictionary (converted to a tuple) and the trigger.

        Returns:
            int: A hash value for this instance.
        """
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
        session: Optional["Session"] = None,  # type: ignore
        incoming_queue: Optional[QueueGenerator] = None,
        outgoing_queue: Optional[QueueGenerator] = None,
        status_queue: Optional[Queue] = None,
        compute: Optional[Any] = None,
    ) -> None:
        """
        Initialize a QueueProcessor instance.

        Args:
            session (Optional[Session]): The session this processor belongs to. Defaults to None.
            incoming_queue (Optional[QueueGenerator]): The queue from which to read items. Defaults to None.
            outgoing_queue (Optional[QueueGenerator]): The queue to which processed items are sent.
            status_queue (Optional[Queue]): The queue for status messages. Defaults to None.
        """
        self.session = session
        self.started = False
        self.started_at = None
        self.finished = False
        self.finished_at = None
        self.halt_signal = False
        ###########################################
        # The next lines are the ones that need to
        # be generalized by the compute engine.
        ###########################################
        self.processing_thread = threading.Thread(
            target=self.process_queue, daemon=True
        )
        self.compute = compute
        self.received_counter = 0
        self.sent_counter = 0
        self.incoming_queue = incoming_queue
        self.outgoing_queue = outgoing_queue
        self.status_queue = status_queue
        self.profiler = None
        self.idle = False  # True if we're not doing anything -- will use for determining whether to stop
        # Stop when all idle and data sources are empty.

        if self.outgoing_queue:
            self.outgoing_queue.incoming_queue_processors.append(self)
        self.secondary_cache = []
        self.secondary_cache_max_size = 1_000

    # def profile_thread_function(self):
    #     if self.session.profiler:
    #         self.profiler = cProfile.Profile()
    #         self.profiler.enable()
    #         self.process_queue()
    #         self.profiler.disable()
    #         self.profiler.dump_stats("profile_output_thread")
    #     else:
    #         self.process_queue()

    def process_queue(self) -> None:
        """Process every item in the queue using the yield_items method."""
        self.started = True
        self.started_at = datetime.datetime.now()
        while not self.halt_signal:
            for item in self.incoming_queue.yield_items():
                if isinstance(item, Idle):
                    self.idle = True
                    time.sleep(0.1)
                    if self.halt_signal:
                        break
                    continue
                self.idle = False
                self.received_counter += 1
                # dump profiler data
                if (
                    self.profiler
                    and self.received_counter
                    % self.session.dump_profile_interval
                    == 0
                ):
                    self.profiler.dump_stats(self.__class__.__name__ + ".prof")
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
                    error_msg = (
                        f"in thread: {threading.current_thread().name}\n"
                    )
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
        LOGGER.info(
            "Halt signal received, thread %s is halting",
            threading.current_thread().name,
        )
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
        """Process new facts from the fact_generated_queue."""
        if item in self.session.fact_collection:
            LOGGER.debug("Fact %s already in collection", item)
            return
        item.session = self.session
        self.session.fact_collection.append(item)
        # Put the fact in the queue to be checked for triggers
        self.outgoing_queue.put(item)


class CheckFactAgainstTriggersQueueProcessor(QueueProcessor):  # pylint: disable=too-few-public-methods
    """
    Reads from the check_fact_against_triggers_queue and processes the facts
    by checking them against the triggers.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize a CheckFactAgainstTriggersQueueProcessor instance.

        Args:
            *args: Variable positional arguments passed to the parent class.
            **kwargs: Variable keyword arguments passed to the parent class.
        """
        super().__init__(*args, **kwargs)

    def process_item_from_queue(self, item: Any) -> None:
        """Process new facts from the check_fact_against_triggers_queue."""

        out = []
        LOGGER.debug("Checking fact %s against triggers", item)
        while item not in self.session.fact_collection:
            LOGGER.debug(
                "Fact %s not in collection, requeueing...", item.__dict__
            )

            self.incoming_queue.put(item)
        LOGGER.debug("IN COLLECTION: %s", item.__dict__)
        # Let's filter out the facts that are irrelevant to this trigger
        for _, trigger in self.session.trigger_dict.items():
            # Let's bomb out if the attribute in the fact is not in the trigger
            if (
                isinstance(item, FactNodeHasAttributeWithValue)
                and not item.attribute
                in trigger.cypher.parse_tree.attribute_names
            ):
                LOGGER.debug(
                    "Fact %s does not match trigger %s, skipping",
                    item,
                    trigger,
                )
                continue
            LOGGER.debug("Checking trigger %s", trigger)
            # item is a Fact subclass
            for constraint in trigger.constraints:
                LOGGER.warning(
                    "Checking item: %s, constraint %s, trigger %s result: %s",
                    item,
                    constraint,
                    trigger,
                    item + constraint,
                )

                if sub := item + constraint:
                    LOGGER.warning("Fact %s matched a trigger", item)
                    sub_trigger_pair = SubTriggerPair(sub=sub, trigger=trigger)
                    out.append(sub_trigger_pair)
        return out


class TriggeredLookupProcessor(QueueProcessor):  # pylint: disable=too-few-public-methods
    """
    Reads from the check_fact_against_triggers_queue and processes the facts
    by checking them against the triggers.
    """

    WITH_CLAUSE_PROJECTION_KEY = "__with_clause_projection__"
    MATCH_SOLUTION_KEY = "__match_solution__"

    def process_item_from_queue(self, item: SubTriggerPair) -> List[Any] | None:
        """Process new facts from the check_fact_against_triggers_queue."""
        self.started = True
        self.started_at = datetime.datetime.now()
        self.received_counter += 1

        try:
            return self._process_sub_trigger_pair(item)
        except Exception as e:  # pylint: disable=broad-exception-caught
            LOGGER.info("Error processing trigger: %s", e)
            self.status_queue.put(e)
        finally:
            self.finished = True
            self.finished_at = datetime.datetime.now()
            return None  # pylint: disable=return-in-finally,lost-exception

    def _process_sub_trigger_pair(
        self, sub_trigger_pair: SubTriggerPair
    ) -> List[Any]:
        """Helper function to process a sub_trigger_pair"""
        fact_collection = self.session.fact_collection
        return_clause = (
            sub_trigger_pair.trigger.cypher.parse_tree.cypher.return_clause
        )
        solutions = return_clause._evaluate(fact_collection)  # pylint: disable=protected-access
        if sub_trigger_pair.trigger.is_relationship_trigger:
            source_variable = sub_trigger_pair.trigger.source_variable
            target_variable = sub_trigger_pair.trigger.target_variable
            relationship_name = sub_trigger_pair.trigger.relationship_name

            process_solution_args = [  # prepend `solutions` onto this when called later
                sub_trigger_pair,
                source_variable,
                target_variable,
                relationship_name,
                return_clause,
            ]
            process_solution_function = self._process_solution_node_relationship
        elif sub_trigger_pair.trigger.is_attribute_trigger:
            variable_to_set = sub_trigger_pair.trigger.variable_set
            process_solution_args = [  # prepend `solutions` onto this when called later
                sub_trigger_pair,
                variable_to_set,
                return_clause,
            ]
            process_solution_function = self._process_solution_node_attribute
        else:
            raise ValueError(
                "Unknown trigger type: Expected VariableAttributeTrigger "
                "or NodeRelationshipTrigger "
                f"got {sub_trigger_pair.trigger.__class__.__name__}"
            )

        computed_facts = []
        for solution in solutions:
            try:
                computed_facts.append(
                    process_solution_function(solution, *process_solution_args)
                )
            except Exception as e:  # pylint: disable=broad-exception-caught
                LOGGER.error("Error processing solution: %s", e)
                self.status_queue.put(e)

        return computed_facts

    def _process_solution_node_relationship(
        self,
        solution: Dict,
        sub_trigger_pair: SubTriggerPair,
        source_variable: str,
        target_variable: str,
        relationship_name: str,
        return_clause,
    ) -> List[
        FactNodeHasAttributeWithValue
        | FactRelationshipHasSourceNode
        | FactRelationshipHasTargetNode
    ]:
        """Process a solution and generate a list of facts."""
        splat = self._extract_splat_from_solution(solution, return_clause)
        computed_value = sub_trigger_pair.trigger.function(*splat)
        if computed_value:
            sub_trigger_pair.trigger.call_counter += 1
            source_node_id = self._extract_node_id_from_solution(
                solution, source_variable
            )
            target_node_id = self._extract_node_id_from_solution(
                solution, target_variable
            )
            relationship_id = hashlib.md5(
                f"{source_node_id}{target_node_id}{relationship_name}".encode()
            ).hexdigest()
            fact_1 = FactRelationshipHasSourceNode(
                source_node_id=source_node_id,
                relationship_id=relationship_id,
            )
            fact_2 = FactRelationshipHasTargetNode(
                target_node_id=target_node_id,
                relationship_id=relationship_id,
            )
            fact_3 = FactRelationshipHasLabel(
                relationship_id=relationship_id,
                relationship_label=relationship_name,
            )
            self.session.fact_generated_queue.put(fact_1)
            self.session.fact_generated_queue.put(fact_2)
            self.session.fact_generated_queue.put(fact_3)

    def _process_solution_node_attribute(
        self,
        solution: Dict,
        sub_trigger_pair: SubTriggerPair,
        variable_to_set: str,
        return_clause,
    ) -> FactNodeHasAttributeWithValue:
        """Process a solution and generate a fact."""
        splat = self._extract_splat_from_solution(solution, return_clause)

        if any(isinstance(arg, NullResult) for arg in splat):
            LOGGER.debug("NullResult found in splat %s", splat)
            return NullResult

        data_asset_parameters = [
            sub_trigger_pair.trigger.session.get_data_asset_by_name(
                parameter
            ).obj
            for parameter in inspect.signature(
                sub_trigger_pair.trigger.function
            ).parameters.keys()
            if parameter in sub_trigger_pair.trigger.session.data_asset_names
        ]
        computed_value = sub_trigger_pair.trigger.function(
            *(splat + data_asset_parameters)
        )
        sub_trigger_pair.trigger.call_counter += 1
        target_attribute = sub_trigger_pair.trigger.attribute_set
        node_id = self._extract_node_id_from_solution(solution, variable_to_set)
        computed_fact = FactNodeHasAttributeWithValue(
            node_id=node_id,
            attribute=target_attribute,
            value=computed_value,
        )
        LOGGER.debug(">>>>>>> Computed fact: %s", computed_fact)
        self.session.fact_generated_queue.put(computed_fact)
        return computed_fact

    def _extract_splat_from_solution(
        self, solution: Dict, return_clause
    ) -> List[Any]:
        """Extract the splat (arguments for the trigger function) from a solution."""

        def to_python(x):
            """
            Convert a value to a Python native type.

            If the value is a Collection, recursively convert its values.

            Args:
                x: The value to convert.

            Returns:
                The converted value.
            """
            if isinstance(x, Collection):
                return [to_python(y) for y in x.values]
            return x

        try:
            out = [
                to_python(solution.get(alias.name))
                for alias in return_clause.projection.lookups
            ]

        except Exception as e:
            raise ValueError(f"Error extracting splat: {e}") from e

        return out

    def _extract_node_id_from_solution(
        self, solution: Dict, variable_to_set: str
    ) -> str:
        """Extract the node ID from the solution."""
        try:
            node_id = solution[self.WITH_CLAUSE_PROJECTION_KEY][
                self.MATCH_SOLUTION_KEY
            ][variable_to_set]
            return node_id
        except KeyError as e:
            raise ValueError(f"Error extracting node ID: {e}") from e
