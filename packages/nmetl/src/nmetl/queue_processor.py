"""
Queue Processing Components
==========================

This module defines the core components for processing items in a distributed
queue-based data processing pipeline within the nmetl library.

The module provides:
- ``QueueProcessor``: Abstract base class for all queue processors
- ``RawDataProcessor``: Processes raw data from data sources into facts
- ``FactGeneratedQueueProcessor``: Inserts generated facts into the fact collection
- ``CheckFactAgainstTriggersQueueProcessor``: Checks facts against registered triggers
- ``TriggeredLookupProcessor``: Executes triggered computations and generates new facts
- ``SubTriggerPair``: Data structure pairing substitutions with triggers

"""

from __future__ import annotations

import datetime
import hashlib
import multiprocessing as mp
import queue
import random
import threading
import time
from prometheus_client import Counter
from abc import ABC, abstractmethod
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from queue import Queue
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type

from nmetl.prometheus_metrics import TRIGGER_CHECK_COUNT, RAW_DATA_COUNTER, REQUEST_TIME
from nmetl.queue_generator import QueueGenerator
from nmetl.thread_manager import ThreadManager
from nmetl.trigger import CypherTrigger
from pycypher.fact import (FactNodeHasAttributeWithValue, FactNodeHasLabel,
                           FactRelationshipHasLabel,
                           FactRelationshipHasSourceNode,
                           FactRelationshipHasTargetNode)
from pycypher.fact_collection import FactCollection
from pycypher.node_classes import Collection
from pycypher.query import NullResult
from pycypher.solutions import Projection, ProjectionList
from shared.helpers import ensure_bytes
from shared.logger import LOGGER

LOGGER.setLevel("DEBUG")

MATCH_SOLUTION_KEY = "__MATCH_SOLUTION_KEY__"
WHERE_SOLUTION_KEY = "__where_solution__"


@dataclass
class SubTriggerPair:
    """
    Represents a pairing of variable substitutions with a Cypher trigger.

    This class is used to track which variable bindings should be applied
    when evaluating a specific trigger in the fact processing pipeline.

    Attributes:
        sub: Dictionary mapping variable names to their bound values
        trigger: The CypherTrigger to be evaluated with the substitutions
    """

    sub: Dict[str, str]
    trigger: CypherTrigger
    projection_list: Optional[ProjectionList]

    def __hash__(self) -> int:
        """
        Generate a hash value for this SubTriggerPair instance.

        The hash is computed from both the variable substitutions (as a sorted tuple)
        and the trigger object to ensure unique identification of trigger evaluations.

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
    """
    Abstract base class for distributed queue-based data processors.

    This class provides the foundation for processing items from input queues,
    applying transformations using Dask distributed computing, and forwarding
    results to output queues. All concrete implementations must define the
    ``process_item_from_queue`` method.

    The processor uses buffering to batch items for efficient distributed processing
    and provides monitoring capabilities through status queues and counters.
    """

    def __init__(
        self,
        incoming_queue: Optional[QueueGenerator] = None,
        outgoing_queue: Optional[QueueGenerator] = None,
        status_queue: queue.Queue = None,
        priority: Optional[int] = 0,
        session_config: Optional[SessionConfig] = None,
        thread_manager: Optional[ThreadManager] = None,
        max_buffer_size: int = 1_000_000,
        buffer_timeout: float = 1.25,
        fact_collection: Optional[FactCollection] = None,
        trigger_dict: Optional[Dict[str, Any]] = None,
        data_assets: dict = {},
    ) -> None:
        """
        Initialize a QueueProcessor instance.

        Args:
            incoming_queue: The queue from which to read items for processing.
            outgoing_queue: The queue to which processed items are sent.
            status_queue: Queue for status messages and error reporting.
            priority: Processing priority for Dask task scheduling (higher = more priority).
            session_config: Configuration object containing session settings.
            thread_manager: ThreadManager for task execution.
            max_buffer_size: Maximum number of items to buffer before processing.
            buffer_timeout: Maximum time (seconds) to wait before processing partial buffer.
        """
        LOGGER.debug("Initializing QueuePreocessor: %s", self)
        self._session_config = session_config
        self.started = False
        self.priority = priority
        self.started_at: Optional[datetime.datetime] = None
        self.finished = False
        self.finished_at: Optional[datetime.datetime] = None
        self.halt_signal = False
        self.fact_collection = fact_collection
        self.trigger_dict = trigger_dict
        self.data_assets = data_assets

        LOGGER.debug(session_config)

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
        # self.secondary_cache = []
        # self.secondary_cache_max_size = 1_000

        self.name = self.__class__.__name__

        self.buffer: list[Any] = []
        self.max_buffer_size: int = max_buffer_size
        self.buffer_timeout: float = buffer_timeout

        self.thread_manager: ThreadManager = ThreadManager()
        self.item_counter: Counter = Counter(self.__class__.__name__, self.__class__.__name__)

    # @classmethod
    # def get_fact_collection(cls) -> FactCollection:
    #     _fact_collection: FactCollection = (
    #         get_worker().fact_collection
    #     )  # pyrefly: ignore
    #     return _fact_collection

    # @property
    # def fact_collection(self) -> FactCollection:
    #     _fact_collection: FactCollection = (
    #         get_worker().fact_collection
    #     )  # pyrefly: ignore
    #     return _fact_collection

    @property
    def session_config(self) -> SessionConfig:
        if self._session_config.__class__ is not SessionConfig:
            raise ValueError(
                f"Expected a session config: {self._session_config.__class__.__name__}"
            )
        return self._session_config  # pyrefly: ignore (it's correct, actually)

    def process_queue(self) -> None:
        """
        Main processing loop that consumes items from the incoming queue.

        This method runs in a separate thread and continuously processes items
        from the incoming queue, buffering them for efficient batch processing.
        The loop continues until a halt signal is received.

        The method handles buffering logic, timing constraints, and delegates
        actual processing to the abstract ``process_item_from_queue`` method.
        """
        self.started = True
        self.started_at = datetime.datetime.now()
        last_item_received_at: float = time.time()
        while not self.halt_signal:
            for item in self.incoming_queue.yield_items():
                LOGGER.debug(
                    "Processing an item from %s", self.__class__.__name__
                )
                # item.data_source = None
                if not item:
                    continue
                self._process_item_from_queue(item)  # HERE
        self.finished = True
        self.finished_at = datetime.datetime.now()

    def handle_result_future(self, completed_future):
        result = completed_future.result()
        LOGGER.debug(
            "In handle_result_futures: %s: %s",
            self.__class__.__name__,
            result,
        )
        if not result:
            LOGGER.debug("result is None")
            return
            # raise ValueError("result_list is None")
        self.outgoing_queue.put(result)

    @abstractmethod
    def process_item_from_queue(self, buffer: List[Any]) -> Any:
        """
        Process a buffer of items from the queue.

        This abstract method must be implemented by all concrete queue processors.
        It receives a buffer of items and should return processed results that will
        be forwarded to the outgoing queue.

        Args:
            buffer: List of items to process in this batch.

        Returns:
            Processed results to be sent to the outgoing queue.
        """

    @REQUEST_TIME.time()
    def _process_item_from_queue(self, item) -> Any:
        """
        Submit buffered items for distributed processing via Dask.

        This method wraps the abstract ``process_item_from_queue`` method,
        submitting the current buffer to the Dask cluster for processing.
        It handles the distributed execution and result callback setup.

        The method is decorated with timing metrics for monitoring performance.
        """
        # LOGGER.debug(
        #     "Sending buffer to %s (%s)",
        #     self.__class__.__name__,
        #     len(self.buffer),
        # )
        if not isinstance(item, list):
            item = [item]
        # I'm ashamed of this
        for sub_item in item:
            self.item_counter.inc(1)
            future = self.thread_manager.submit_task(
                self.__class__.process_item_from_queue,
                self,
                sub_item,
            )
            future.add_done_callback(self.handle_result_future)

    def submit_work(self, func: Callable, *args, **kwargs) -> Future:
        """Submit work to thread pool instead of Dask."""
        return self.thread_manager.submit_task(func, *args, **kwargs)


class RawDataProcessor(QueueProcessor):
    """
    Processes raw data from data sources and converts it into facts.

    This processor takes raw data items from data sources, applies the configured
    mappings to transform the data into fact objects, and prepares them for
    insertion into the fact collection.
    """

    # def process_buffer(self, buffer: List[Any]) -> List[Any]:
    #     """Process buffer using threads instead of Dask futures."""
    #     for item in buffer:
    #         results = self._process_single_item(item)
    #     return results

    # @staticmethod
    # def _process_single_item(item: Any) -> List[Any]:
    #     row: dict[str, Any] = item.row
    #     out: list[Any] = []
    #     for mapping in item.mappings:
    #         for out_item in mapping + row:
    #             out_item.lineage = None  # FromMapping()
    #             out.append(out_item)
    #     return out

    def process_item_from_queue(self, item):
        """
        Transform raw data items into facts using configured mappings.

        For each raw data item in the buffer, this method applies all associated
        data source mappings to generate the corresponding fact objects.

        Args:
            buffer: List of raw data items from data sources.

        Returns:
            List of lists, where each inner list contains the facts generated
            from one raw data item.
        """
        all_results = []
        LOGGER.debug(item)
        match item["type"]:
            case "attribute":
                fact = FactNodeHasAttributeWithValue(
                    node_id=item["entity_id"],
                    attribute=item["attribute"],
                    value=item["value"],
                )
                all_results.append(fact)

                fact = FactNodeHasLabel(
                    node_id=item["entity_id"],
                    label=item["entity_label"],
                )
                all_results.append(fact)
            case "relationship":
                relationship_id: str = hashlib.sha256(
                    bytes(str(random.random()), encoding="utf8")
                ).hexdigest()

                fact = FactRelationshipHasSourceNode(
                    relationship_id=relationship_id,
                    source_node_id=item["source_id"],
                )
                all_results.append(fact)

                fact = FactRelationshipHasTargetNode(
                    relationship_id=relationship_id,
                    target_node_id=item["target_id"],
                )
                all_results.append(fact)

                fact = FactRelationshipHasLabel(
                    relationship_id=relationship_id,
                    relationship_label=item["relationship"],
                )
                all_results.append(fact)
            case _:
                raise Exception('Unknown item dict type')



        LOGGER.debug(all_results)
        if all_results:
            RAW_DATA_COUNTER.inc(len(all_results))

        return all_results


class FactGeneratedQueueProcessor(QueueProcessor):  # pylint: disable=too-few-public-methods
    """
    Inserts generated facts into the distributed fact collection.

    This processor takes newly generated facts and persists them to the
    configured fact collection backend (FoundationDB, RocksDB, etc.).
    It ensures facts are properly stored and available for trigger evaluation.
    """

    # def process_buffer(self, buffer: List[Any]) -> List[Any]:
    #     """Process facts using thread pool."""
    #     # Group facts for batch processing
    #     fact_batches = self._create_batches(buffer, batch_size=100)
    #
    #     futures = []
    #     for batch in fact_batches:
    #         future = self.submit_work(self._process_fact_batch, batch)
    #         futures.append(future)
    #
    #     # Wait for completion
    #     for future in futures:
    #         try:
    #             future.result(timeout=60)
    #         except Exception as e:
    #             LOGGER.error(f"Error processing fact batch: {e}")
    #
    #     return buffer

    # def _create_batches(self, buffer: List[Any], batch_size: int) -> List[List[Any]]:
    #     """Create batches of facts for processing."""
    #     return [buffer[i:i + batch_size] for i in range(0, len(buffer), batch_size)]

    # def _process_fact_batch(self, batch: List[Any]) -> None:
    #     """Process a batch of facts."""
    #     fact_collection: FactCollection = self.fact_collection
    #     for item in batch:
    #         LOGGER.debug("Writing: %s", item)
    #         item.lineage = (
    #             None  # Appended(lineage=getattr(item, "lineage", None))
    #         )
    #         fact_collection.append(item)

    def process_item_from_queue(self, item) -> List[List[Any]]:
        """
        Insert facts from the buffer into the fact collection.

        Each fact in the buffer is appended to the distributed fact collection,
        making it available for querying and trigger evaluation.

        Args:
            buffer: List of fact objects to be inserted.

        Returns:
            The original buffer of facts that were inserted.
        """
        LOGGER.debug("Writing: %s", item)
        if not isinstance(item, list):
            self.fact_collection.append(item)
        else:
            for fact in item:
                self.process_item_from_queue(fact)
        return item


class CheckFactAgainstTriggersQueueProcessor(QueueProcessor):  # pylint: disable=too-few-public-methods
    """
    Evaluates facts against registered Cypher triggers to identify matches.

    This processor examines each fact to determine which triggers it might
    activate. It performs initial filtering based on attribute names and
    node types, then creates SubTriggerPair objects for triggers that
    should be evaluated.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize a CheckFactAgainstTriggersQueueProcessor instance.

        Args:
            *args: Variable positional arguments passed to the parent class.
            **kwargs: Variable keyword arguments passed to the parent class.
        """
        super().__init__(*args, **kwargs)

    def evaluate_fact_against_trigger(
        self, trigger, item, variable
    ) -> ProjectionList:
        if variable == 't':
            LOGGER.warning("evaluate_fact_against_trigger: %s:::%s", item, variable)
        result: ProjectionList = trigger.cypher._evaluate(
            self.fact_collection,
            projection_list=ProjectionList(
                projection_list=[
                    Projection(projection={variable: item.node_id})
                ]
            ),
        )
        TRIGGER_CHECK_COUNT.inc(1)
        # Can we filter out bad results here.
        LOGGER.info('Evaluated trigger: %s', trigger.cypher)
        LOGGER.info(
            "Result of evaluate_fact_against_trigger %s is %s, %s, %s",
            item,
            result,
            variable,
            item.node_id,
        )
        return result

    def process_item_from_queue(self, item) -> SubTriggerPair:
        """
        Check facts against all registered triggers and create trigger pairs.

        For each fact in the buffer, this method:
        1. Filters triggers based on attribute relevance
        2. Evaluates trigger conditions with the fact as a starting point
        3. Creates SubTriggerPair objects for successful matches

        Args:
            buffer: List of facts to check against triggers.

        Returns:
            List of SubTriggerPair objects representing triggered evaluations.
        """
        out: List[SubTriggerPair] = []
        try:
            assert isinstance(
                ensure_bytes(
                    self.fact_collection.make_index_for_fact(item)
                ),
                bytes,
            )
        except Exception as e:
            LOGGER.debug("Bad item in CheckFact... %s", item)
            return
        LOGGER.debug("Checking fact %s against triggers", item)

        PARANOID = True
        if PARANOID:
            counter: int = 0
            while item not in self.fact_collection and counter < 100:
                LOGGER.debug(
                    "Fact %s not in collection, requeueing... (%s)",
                    item.__dict__,
                    str(counter),
                )
                counter += 1
                time.sleep(1.0)
            if counter >= 100:
                LOGGER.error(
                    "Fact %s not in collection after 100 attempts, skipping",
                    item.__dict__,
                )
                return

        LOGGER.debug("IN COLLECTION: %s", item.__dict__)
        # Let's filter out the facts that are irrelevant to this trigger
        for _, trigger in self.trigger_dict.items():
            # Get the attributes that are in the Match clause
            # TODO: Find out why parse_tree is ever None
            LOGGER.info(
                "Checking trigger: %s", trigger.cypher.cypher_query
            )
            if trigger.cypher.parse_tree is None:
                continue
            LOGGER.debug(
                "Trigger parse tree: %s", trigger.cypher.parse_tree
            )
            attribute_names_in_trigger: List[str] = (
                trigger.cypher.parse_tree.attribute_names
            )
            LOGGER.info('attribute_names_in_trigger: %s', attribute_names_in_trigger)
            if (
                isinstance(item, FactNodeHasAttributeWithValue)
                and item.attribute not in attribute_names_in_trigger
            ):
                LOGGER.debug(
                    "Attribute %s not in trigger %s, skipping",
                    item.attribute,
                    trigger,
                )
                continue
            elif isinstance(item, FactNodeHasLabel):
                LOGGER.debug("FactNodeHasLabel %s, skipping", item)
                continue
            else:
                pass
            for (
                variable,
                node,
            ) in trigger.cypher.parse_tree.get_node_variables():
                # FactNodeHasLabel might not be enough to trigger
                LOGGER.debug(
                    "Checking trigger %s against fact %s",
                    trigger,
                    item,
                )
                LOGGER.debug(
                    "variable node loop: %s %s",
                    variable,
                    node,
                )
                # Seem to be checking FactNodeHasAttributeWithValue against relationship node,
                # which is suspicious.
                #### KLUDGE
                if (
                    isinstance(item, (FactNodeHasAttributeWithValue,))
                    and (variable != "r")
                ):
                    successful: bool = False
                    attempts: int = 0
                    while not successful:
                        try:
                            result: ProjectionList = (
                                self.evaluate_fact_against_trigger(
                                    trigger, item, variable
                                )
                            )
                            successful = True
                        except Exception as e:
                            raise e
                            LOGGER.warning(
                                f"Key error or something like that... {attempts}:{variable}:{type(variable)}"
                            )
                            LOGGER.warning(f"Error: {e}")
                            time.sleep(1)
                            attempts += 1

                    # LOGGER.debug(
                    #     ">>>>>>>>> Result of evaluate_fact_against_trigger %s is %s",
                    #     item,
                    #     result,
                    # )
                    if result:
                        # Here look into the projection, pull out the variable in the return signature
                        # We expect that there was only one projection passed to the cypher object
                        LOGGER.debug(
                            "++++++++++++++ Result of evaluate_fact_against_trigger %s is %s",
                            item,
                            result,
                        )
                        if len(result.root.projection_list) != 1:
                            raise ValueError(
                                "Expected only one projection passed to Cypher query, got: %s",
                                result.root.projection_list,
                            )
                        # Maybe shouldn't be for all variables -- just what's in the return statement
                        sub_trigger_pair: SubTriggerPair = SubTriggerPair(
                            sub={
                                variable: item.node_id
                            },  # item.node_id is wrong
                            trigger=trigger,
                            projection_list=result,
                        )
                        LOGGER.debug(
                            "Created SubTriggerPair: %s", sub_trigger_pair
                        )
                        out.append(sub_trigger_pair)
                        successful = True
                    else:
                        # Add back Relationship facts here...
                        continue

                LOGGER.debug("Checking trigger %s", trigger)
            LOGGER.debug("Setting processed = True")
        return out


class TriggeredLookupProcessor(QueueProcessor):  # pylint: disable=too-few-public-methods
    """
    Executes triggered Cypher queries and generates computed facts.

    This processor takes SubTriggerPair objects, evaluates the associated
    Cypher triggers with the provided variable bindings, and executes
    the trigger functions to compute new attribute values or relationships.
    """

    def process_item_from_queue(self, item):
        """
        Process SubTriggerPair objects to execute triggered computations.

        For each SubTriggerPair in the buffer, this method evaluates the
        trigger's Cypher query and executes the associated Python function
        to generate new facts.

        Args:
            buffer: List of SubTriggerPair objects to process.

        Returns:
            List of lists containing the computed facts from each trigger execution.
        """
        LOGGER.debug("Got item in TriggeredLookupProcessor: %s", item)
        result: list[Any] = self._process_sub_trigger_pair(item)
        return result

    def _process_sub_trigger_pair(
        self,
        sub_trigger_pair: SubTriggerPair,
    ) -> FactNodeHasAttributeWithValue | None:
        """
        Evaluate a single SubTriggerPair and generate computed facts.

        This method executes the Cypher query with the provided variable
        bindings, then calls the trigger function with the results to
        compute new fact values.

        Args:
            sub_trigger_pair: The trigger and variable bindings to evaluate.

        Returns:
            List of computed facts generated by the trigger function.
        """
        # Find the value of `variable_set` that would be updated with the variable and
        # trigger in `item`.
        success: bool = False
        if isinstance(sub_trigger_pair, list) and len(sub_trigger_pair) > 1:
            LOGGER.error(f"sub_trigger_pair: {len(sub_trigger_pair)}")
        elif isinstance(sub_trigger_pair, list) and len(sub_trigger_pair) == 1:
            sub_trigger_pair = sub_trigger_pair[0]
        while not success:
            out: ProjectionList = sub_trigger_pair.trigger.cypher._evaluate(
                self.fact_collection,
                ProjectionList(
                    projection_list=[
                        Projection(projection=sub_trigger_pair.sub)
                    ]
                ),
            )

            # out.projection_list[0].parent.parent (contains s)
            variable_to_set_substitution_list: list[str] = list(
                i
                for i in out.find_variable(
                    sub_trigger_pair.trigger.variable_set
                )
                if i is not None
            )
            LOGGER.debug(
                "variables: %s", sub_trigger_pair.trigger.variable_set
            )
            if variable_to_set_substitution_list:
                out = sub_trigger_pair.trigger.cypher._evaluate(
                    self.fact_collection,
                    ProjectionList(
                        projection_list=[
                            Projection(
                                projection={
                                    sub_trigger_pair.trigger.variable_set: variable_to_set_substitution_list[
                                        0
                                    ]
                                }
                            )
                        ]
                    ),
                )
            else:
                out = None
            if not out:
                return None
            func_arg_dict = {
                key: value.pythonify()
                if hasattr(value, "pythonify")
                else value
                for key, value in out[0].projection.items()
            }
            for parameter_name in sub_trigger_pair.trigger.parameter_names:
                if parameter_name in self.data_assets.keys():
                    import pdb

                    pdb.set_trace()
                    func_arg_dict[parameter_name] = self.data_assets[
                        parameter_name
                    ]
            # Check no NullResult in arguments
            if any(
                isinstance(value, NullResult)
                for value in func_arg_dict.values()
            ):
                LOGGER.debug("Found NullResult")
                LOGGER.debug(f"func_arg_dict: {func_arg_dict}")
                LOGGER.debug(f"SubTrigger object: {sub_trigger_pair}")
                return None

            try:
                function_result: Any = sub_trigger_pair.trigger.function(
                    **func_arg_dict
                )
                success = True
                # Following might not work because of how we're serializing
                # triggers for Dask...
                sub_trigger_pair.trigger.call_counter += 1
            except TypeError as one_error:
                LOGGER.error(
                    f"TypeError thingy... waiting and restarting... {func_arg_dict}: {one_error}"
                )
                LOGGER.error(
                    f"SubTrigger object: {sub_trigger_pair}::{self.data_assets.keys()}"
                )
                time.sleep(0.1)
        # Convert to a Fact
        computed_fact: FactNodeHasAttributeWithValue = (
            FactNodeHasAttributeWithValue(
                node_id=variable_to_set_substitution_list[0],
                attribute=sub_trigger_pair.trigger.attribute_set,
                value=function_result,
            )
        )
        LOGGER.debug(
            "Generated fact: %s",
            computed_fact,
        )

        return computed_fact
