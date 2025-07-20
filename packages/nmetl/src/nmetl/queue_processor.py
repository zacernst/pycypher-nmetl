"""
This module defines the core components for processing items in a queue-based
system within the pycypher library.

It provides an abstract base class, ``QueueProcessor``, and concrete
implementations for specific tasks in the data processing pipeline.
"""

from __future__ import annotations

import datetime
import hashlib
import multiprocessing as mp
import queue
from dask.distributed import get_worker

# import queue
import threading

# import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from queue import Queue
from typing import TYPE_CHECKING, Callable, Type, Any, Dict, List, Optional

if TYPE_CHECKING:
    from dask.distributed import Client
    from nmetl.configuration import SessionConfig
    from pycypher.fact_collection import FactCollection

from nmetl.queue_generator import QueueGenerator
from nmetl.trigger import CypherTrigger
from pycypher.lineage import Appended, FromMapping
from pycypher.fact import (
    FactNodeHasAttributeWithValue,
    FactRelationshipHasLabel,
    FactRelationshipHasSourceNode,
    FactRelationshipHasTargetNode,
)
from pycypher.node_classes import Collection
from pycypher.query import NullResult
from shared.logger import LOGGER


from nmetl.prometheus_metrics import REQUEST_TIME


LOGGER.setLevel("DEBUG")

WITH_CLAUSE_PROJECTION_KEY = "__with_clause_projection__"
MATCH_SOLUTION_KEY = "__match_solution__"


@dataclass
class SubTriggerPair:
    """A pair of a sub and a trigger."""

    sub: Dict[str, str]
    trigger: CypherTrigger

    def __hash__(self) -> int:
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
        incoming_queue: Optional[QueueGenerator] = None,
        outgoing_queue: Optional[QueueGenerator] = None,
        status_queue: queue.Queue | mp.Queue = Queue(),
        priority: Optional[int] = 0,
        session_config: Optional[SessionConfig] = None,
        dask_client: Optional[Client] = None,
        max_buffer_size: int = 1_000,
        buffer_timeout: float = 2.0,
    ) -> None:
        """
        Initialize a QueueProcessor instance.

        Args:
            session (Optional[Session]): The session this processor belongs to. Defaults to None.
            incoming_queue (Optional[QueueGenerator]): The queue from which to read items. Defaults to None.
            outgoing_queue (Optional[QueueGenerator]): The queue to which processed items are sent.
            status_queue (Optional[Queue]): The queue for status messages. Defaults to None.
        """
        LOGGER.info("Initializing QueuePreocessor: %s", self)
        self._session_config = session_config
        self.started = False
        self.priority = priority
        self.started_at: Optional[datetime.datetime] = None
        self.finished = False
        self.finished_at: Optional[datetime.datetime] = None
        self.halt_signal = False

        LOGGER.info(session_config)

        self.processing_thread = threading.Thread(
            target=self.process_queue, daemon=True
        )
        self.received_counter = 0
        self.sent_counter = 0
        self.incoming_queue = incoming_queue
        self.outgoing_queue = outgoing_queue
        self.status_queue = status_queue
        self.profiler = None
        self.idle = False  # True if we're not doing anything -- will use for determining whether to stop
        # Stop when all idle and data sources are empty.

        # if self.outgoing_queue:
        #     self.outgoing_queue.incoming_queue_processors.append(self)
        self.secondary_cache = []
        self.secondary_cache_max_size = 1_000
        self.dask_client = dask_client

        self.name = self.__class__.__name__

        self.buffer: list[Any] = []
        self.max_buffer_size: int = max_buffer_size
        self.buffer_timeout: float = buffer_timeout

    def __dask_tokenize__(self):
        return self.name

    @classmethod
    def get_fact_collection(cls) -> FactCollection:
        _fact_collection: FactCollection = (
            get_worker().fact_collection
        )  # pyrefly: ignore
        return _fact_collection

    @classmethod
    def get_trigger_dict(cls) -> dict[str, CypherTrigger]:
        trigger_dict: dict[str, CypherTrigger] = (
            get_worker().trigger_dict
        )  # pyrefly: ignore
        return trigger_dict

    @property
    def fact_collection(self) -> FactCollection:
        _fact_collection: FactCollection = (
            get_worker().fact_collection
        )  # pyrefly: ignore
        return _fact_collection

    @property
    def session_config(self) -> SessionConfig:
        if self._session_config.__class__ is not SessionConfig:
            raise ValueError(
                f"Expected a session config: {self._session_config.__class__.__name__}"
            )
        return self._session_config  # pyrefly: ignore (it's correct, actually)

    def process_queue(self) -> None:
        """Process every item in the queue using the yield_items method."""
        LOGGER.debug("process_queue %s", self.name)
        self.started = True
        self.started_at = datetime.datetime.now()
        last_item_received_at: float = time.time()
        while not self.halt_signal:
            for item in self.incoming_queue.yield_items():
                LOGGER.debug(
                    "Processing an item from %s", self.__class__.__name__
                )
                item.session = None
                item.data_source = None
                self.buffer.append(item)
                if (
                    len(self.buffer) <= self.max_buffer_size
                    and (time.time() - last_item_received_at)
                    <= self.buffer_timeout
                ):
                    continue
                last_item_received_at = time.time()
                self.received_counter += 1
                LOGGER.debug("GET: self.name: %s, ITEM: %s", self.name, item)
                LOGGER.debug(item.__dict__)
                self._process_item_from_queue()
                # try:
                #     LOGGER.debug('GET: self.name: %s, ITEM: %s', self.name, item)
                #     LOGGER.debug(item.__dict__)
                #     thing = self._process_item_from_queue()
                #     # future.add_done_callback(self.handle_result_future)
                #     # out = future.result()
                # except Exception as e:
                #     exc_type, exc_value, exc_traceback = sys.exc_info()
                #     formatted_traceback = traceback.format_exception(
                #         exc_type, exc_value, exc_traceback
                #     )
                #     formatted_traceback = "\n".join(
                #         [line.strip() for line in formatted_traceback]
                #     )
                #     error_msg: str = (
                #         f"in thread: {threading.current_thread().name}\n"
                #     )
                #     raise(e)
                #     error_msg += f"Error processing item {item}: {e}]\n"
                #     error_msg += f"Traceback: {formatted_traceback}]\n"
                #     LOGGER.error(error_msg)
                #     self.status_queue.put(e)
                #     continue
        self.finished = True
        self.finished_at = datetime.datetime.now()

    def handle_result_future(self, completed_future):
        batch_result_list = completed_future.result()
        LOGGER.debug(
            "In handle_result_futures: %s: %s",
            self.__class__.__name__,
            batch_result_list,
        )
        for result_list in batch_result_list or []:
            if result_list is None:
                LOGGER.error("result_list is None")
                raise ValueError("result_list is None")
            if not isinstance(result_list, list):
                result_list = [result_list]
            for result in result_list:
                LOGGER.debug(
                    "PUT:  queue: %s, ITEM: %s",
                    self.outgoing_queue.name,
                    result,
                )
                self.outgoing_queue.put(result)

    @staticmethod
    @abstractmethod
    def process_item_from_queue(buffer: List[Any]) -> Any:
        """Process an item from the queue."""

    @REQUEST_TIME.time()
    def _process_item_from_queue(self) -> Any:
        """Wrap the process call in case we want some logging."""
        LOGGER.debug(
            "Sending buffer to %s (%s)",
            self.__class__.__name__,
            len(self.buffer),
        )
        future = self.dask_client.submit(
            self.__class__.process_item_from_queue,
            self.buffer,
            priority=self.priority,
        )
        self.buffer = []
        future.add_done_callback(self.handle_result_future)
        # result = future.result()
        # LOGGER.info('Got result: %s', result)
        # return result


class RawDataProcessor(QueueProcessor):
    """Runs in a thread to process raw data from all the DataSource objects."""

    @staticmethod
    def process_item_from_queue(buffer: List[Any]) -> List[List[Any]]:
        """Process raw data from the ``raw_input_queue``, generate facts."""
        all_results: list[list[Any]] = []
        for item in buffer:
            row: dict[str, Any] = item.row
            out: list[Any] = []
            for mapping in item.mappings:
                for out_item in mapping + row:
                    out_item.lineage = FromMapping()
                    out.append(out_item)
            all_results.append(out)
        return all_results


class FactGeneratedQueueProcessor(QueueProcessor):  # pylint: disable=too-few-public-methods
    """
    Reads from the fact_generated_queue and processes the facts
    by inserting them into the ``FactCollection``.
    """

    @staticmethod
    def process_item_from_queue(buffer: List[Any]) -> List[List[Any]]:
        """Process new facts from the fact_generated_queue."""
        LOGGER.debug("FGQP: %s", buffer)

        # if item in self.fact_collection:
        #     LOGGER.debug("Fact %s already in collection", item)
        #     self.in_counter += 1
        #     return
        # else:
        fact_collection: FactCollection = QueueProcessor.get_fact_collection()
        for item in buffer:
            LOGGER.debug("Writing: %s", item)
            item.lineage = Appended(lineage=getattr(item, "lineage", None))
            fact_collection.append(item)
            # SAMPLE_HISTOGRAM.observe(random.random() * 10)
        # size = len(list(self.fact_collection._prefix_read_items(b'')))
        # LOGGER.debug('Fact collection elements: %s in FactGeneratedQueueProcessor', size)
        # Put the fact in the queue to be checked for triggers
        #### self.outgoing_queue.put(item)
        return buffer


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

    @staticmethod
    def process_item_from_queue(buffer: Any) -> None:
        """Process new facts from the check_fact_against_triggers_queue."""
        out: list[SubTriggerPair] = []
        for item in buffer:
            LOGGER.debug("Checking fact %s against triggers", item)
            item_in_collection: bool = (
                item in QueueProcessor.get_fact_collection()
            )
            # LOGGER.debug("Item %s in fact_collection: %s", item, item in QueueProcessor.get_fact_collection())
            while not item_in_collection:
                LOGGER.info(
                    "Fact %s not in collection, requeueing...", item.__dict__
                )
                time.sleep(0.1)

            LOGGER.debug("IN COLLECTION: %s", item.__dict__)
            # Let's filter out the facts that are irrelevant to this trigger
            for _, trigger in QueueProcessor.get_trigger_dict().items():
                # Let's bomb out if the attribute in the fact is not in the trigger
                # TODO: Move this somewhere else, where it can be extended for efficiency
                # if (
                #     isinstance(item, FactNodeHasAttributeWithValue)
                #     and item.attribute
                #     not in trigger.cypher.parse_tree.attribute_names
                # ):
                #     LOGGER.debug(
                #         "Fact %s does not match trigger %s, skipping",
                #         item,
                #         trigger,
                #     )
                #     continue
                LOGGER.debug("Checking trigger %s", trigger)
                # Get all the variables in the trigger.
                # Get the name of the entity in the fact.
                # Check if there is a solution to the Cypher query which includes the named entity
                #    when it's substituted for each variable.
                # If there is, then add the fact to the output list.

                for constraint in trigger.constraints:
                    LOGGER.debug(
                        "Checking item: %s, constraint %s, trigger %s result: %s",
                        item,
                        constraint,
                        trigger,
                        item + constraint,
                    )

                    if sub := item + constraint:
                        LOGGER.debug("Fact %s matched a trigger", item)
                        sub_trigger_pair: SubTriggerPair = SubTriggerPair(
                            sub=sub, trigger=trigger
                        )
                        out.append(sub_trigger_pair)
                        LOGGER.debug("SubTriggerPair: %s", sub_trigger_pair)
        return out


class TriggeredLookupProcessor(QueueProcessor):  # pylint: disable=too-few-public-methods
    """
    Reads from the check_fact_against_triggers_queue and processes the facts
    by checking them against the triggers.
    """

    @staticmethod
    def process_item_from_queue(buffer: list) -> List[Any]:
        """Process new facts from the check_fact_against_triggers_queue."""
        out: list[list[Any]] = []
        for item in buffer:
            LOGGER.debug("Got item in TriggeredLookupProcessor")
            result: list[Any] = (
                TriggeredLookupProcessor._process_sub_trigger_pair(item)
            )
            out.append(result)
        return out

    @staticmethod
    def _process_sub_trigger_pair(
        sub_trigger_pair: SubTriggerPair,
    ) -> List[Any]:
        """Helper function to process a sub_trigger_pair"""
        fact_collection: FactCollection = QueueProcessor.get_fact_collection()
        LOGGER.debug(str(sub_trigger_pair))
        return_clause = (
            sub_trigger_pair.trigger.cypher.parse_tree.cypher.return_clause
        )
        start_time: datetime.datetime = datetime.datetime.now()
        LOGGER.debug("Calling _evaluate on return clause")
        solutions = return_clause._evaluate(
            fact_collection, sub_trigger_pair=sub_trigger_pair
        )  # pylint: disable=protected-access
        LOGGER.debug(
            "Done with _evaluate: %s",
        )
        end_time: datetime.datetime = datetime.datetime.now()
        LOGGER.debug("solutions in _process: %s", solutions)
        if sub_trigger_pair.trigger.is_relationship_trigger:
            LOGGER.debug(
                "Processing relationship trigger: %s", sub_trigger_pair
            )
            source_variable = sub_trigger_pair.trigger.source_variable
            target_variable = sub_trigger_pair.trigger.target_variable
            relationship_name = sub_trigger_pair.trigger.relationship_name

            process_solution_args: list[
                SubTriggerPair | Any
            ] = [  # prepend `solutions` onto this when called later
                sub_trigger_pair,
                source_variable,
                target_variable,
                relationship_name,
                return_clause,
            ]
            process_solution_function: Callable = (
                TriggeredLookupProcessor._process_solution_node_relationship
            )
        elif sub_trigger_pair.trigger.is_attribute_trigger:
            variable_to_set = sub_trigger_pair.trigger.variable_set
            process_solution_args = [  # prepend `solutions` onto this when called later
                sub_trigger_pair,
                variable_to_set,
                return_clause,
            ]
            process_solution_function = (
                TriggeredLookupProcessor._process_solution_node_attribute
            )
        else:
            raise ValueError(
                "Unknown trigger type: Expected VariableAttributeTrigger "
                "or NodeRelationshipTrigger "
                f"got {sub_trigger_pair.trigger.__class__.__name__}"
            )

        computed_facts = []
        for solution in solutions:
            LOGGER.debug("Processing args: %s", process_solution_args)
            LOGGER.debug("Processing solution: %s", solution)
            LOGGER.debug(
                "Processing solution function: %s", process_solution_function
            )
            try:
                computed_facts.append(
                    process_solution_function(solution, *process_solution_args)
                )
            except Exception as e:  # pylint: disable=broad-exception-caught
                LOGGER.error("Error processing solution: %s", e)

        LOGGER.debug("Computed facts: %s", computed_facts)
        return computed_facts

    def _process_solution_node_relationship(
        self,
        solution: Dict,
        sub_trigger_pair: SubTriggerPair,
        source_variable: str,
        target_variable: str,
        relationship_name: str,
        return_clause,
    ) -> None:
        """Process a solution and generate a list of facts."""
        assert False
        splat: list[Any] = self._extract_splat_from_solution(
            solution, return_clause
        )
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
            fact_1: FactRelationshipHasSourceNode = (
                FactRelationshipHasSourceNode(
                    source_node_id=source_node_id,
                    relationship_id=relationship_id,
                )
            )
            fact_2: FactRelationshipHasTargetNode = (
                FactRelationshipHasTargetNode(
                    target_node_id=target_node_id,
                    relationship_id=relationship_id,
                )
            )
            fact_3: FactRelationshipHasLabel = FactRelationshipHasLabel(
                relationship_id=relationship_id,
                relationship_label=relationship_name,
            )
            self.outgoing_queue.put(fact_1)
            self.outgoing_queue.put(fact_2)
            self.outgoing_queue.put(fact_3)

    def _process_solution_node_attribute(
        solution: Dict,
        sub_trigger_pair: SubTriggerPair,
        variable_to_set: str,
        return_clause,
    ) -> FactNodeHasAttributeWithValue | Type[NullResult]:
        """Process a solution and generate a fact."""
        splat = TriggeredLookupProcessor._extract_splat_from_solution(
            solution, return_clause
        )

        if any(isinstance(arg, NullResult) for arg in splat):
            LOGGER.debug("NullResult found in splat %s", splat)
            return NullResult

        computed_value = sub_trigger_pair.trigger.function(*splat)
        sub_trigger_pair.trigger.call_counter += 1
        target_attribute = sub_trigger_pair.trigger.attribute_set
        node_id = TriggeredLookupProcessor._extract_node_id_from_solution(
            solution, variable_to_set
        )
        computed_fact: FactNodeHasAttributeWithValue = (
            FactNodeHasAttributeWithValue(
                node_id=node_id,
                attribute=target_attribute,
                value=computed_value,
            )
        )
        LOGGER.debug(">>>>>>> Computed fact: %s", computed_fact)
        # self.outgoing_queue.put(computed_fact)
        return computed_fact

    @staticmethod
    def _extract_splat_from_solution(
        solution: Dict, return_clause
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

    @staticmethod
    def _extract_node_id_from_solution(
        solution: Dict, variable_to_set: str
    ) -> str:
        """Extract the node ID from the solution."""
        try:
            node_id = solution[WITH_CLAUSE_PROJECTION_KEY][MATCH_SOLUTION_KEY][
                variable_to_set
            ]
            return node_id
        except KeyError as e:
            raise ValueError(f"Error extracting node ID: {e}") from e
