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

All processors use Dask for distributed computation and ZMQ for inter-process communication.
"""

from __future__ import annotations

import datetime
import hashlib
import multiprocessing as mp
import queue

# import queue
import threading

# import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from queue import Queue
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type

from dask.distributed import get_worker

if TYPE_CHECKING:
    from dask.distributed import Client
    from nmetl.configuration import SessionConfig
    from pycypher.fact_collection import FactCollection

from nmetl.prometheus_metrics import REQUEST_TIME
from nmetl.queue_generator import QueueGenerator
from nmetl.trigger import CypherTrigger
from pycypher.fact import (
    FactNodeHasAttributeWithValue,
    FactNodeHasLabel,
    FactRelationshipHasLabel,
    FactRelationshipHasSourceNode,
    FactRelationshipHasTargetNode,
)
from pycypher.lineage import Appended, FromMapping
from pycypher.node_classes import Collection
from pycypher.query import NullResult
from shared.logger import LOGGER

LOGGER.setLevel("DEBUG")

WITH_SOLUTION_KEY = "__with_solution__"
MATCH_SOLUTION_KEY = "__MATCH_SOLUTION_KEY__"
WHERE_SOLUTION_KEY = '__where_solution__'


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
        status_queue: queue.Queue | mp.Queue = Queue(),
        priority: Optional[int] = 0,
        session_config: Optional[SessionConfig] = None,
        dask_client: Optional[Client] = None,
        max_buffer_size: int = 1_024,
        buffer_timeout: float = 2.0,
    ) -> None:
        """
        Initialize a QueueProcessor instance.

        Args:
            incoming_queue: The queue from which to read items for processing.
            outgoing_queue: The queue to which processed items are sent.
            status_queue: Queue for status messages and error reporting.
            priority: Processing priority for Dask task scheduling (higher = more priority).
            session_config: Configuration object containing session settings.
            dask_client: Dask distributed client for task execution.
            max_buffer_size: Maximum number of items to buffer before processing.
            buffer_timeout: Maximum time (seconds) to wait before processing partial buffer.
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
        """
        Main processing loop that consumes items from the incoming queue.
        
        This method runs in a separate thread and continuously processes items
        from the incoming queue, buffering them for efficient batch processing.
        The loop continues until a halt signal is received.
        
        The method handles buffering logic, timing constraints, and delegates
        actual processing to the abstract ``process_item_from_queue`` method.
        """
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
    def _process_item_from_queue(self) -> Any:
        """
        Submit buffered items for distributed processing via Dask.
        
        This method wraps the abstract ``process_item_from_queue`` method,
        submitting the current buffer to the Dask cluster for processing.
        It handles the distributed execution and result callback setup.
        
        The method is decorated with timing metrics for monitoring performance.
        """
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
    """
    Processes raw data from data sources and converts it into facts.
    
    This processor takes raw data items from data sources, applies the configured
    mappings to transform the data into fact objects, and prepares them for
    insertion into the fact collection.
    """

    @staticmethod
    def process_item_from_queue(buffer: List[Any]) -> List[List[Any]]:
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
        all_results: list[list[Any]] = []
        for item in buffer:
            row: dict[str, Any] = item.row
            out: list[Any] = []
            for mapping in item.mappings:
                for out_item in mapping + row:
                    out_item.lineage = None  # FromMapping()
                    out.append(out_item)
            all_results.append(out)
        return all_results


class FactGeneratedQueueProcessor(QueueProcessor):  # pylint: disable=too-few-public-methods
    """
    Inserts generated facts into the distributed fact collection.
    
    This processor takes newly generated facts and persists them to the
    configured fact collection backend (FoundationDB, RocksDB, etc.).
    It ensures facts are properly stored and available for trigger evaluation.
    """

    @staticmethod
    def process_item_from_queue(buffer: List[Any]) -> List[List[Any]]:
        """
        Insert facts from the buffer into the fact collection.
        
        Each fact in the buffer is appended to the distributed fact collection,
        making it available for querying and trigger evaluation.
        
        Args:
            buffer: List of fact objects to be inserted.
            
        Returns:
            The original buffer of facts that were inserted.
        """
        LOGGER.debug("FGQP: %s", buffer)

        # if item in self.fact_collection:
        #     LOGGER.debug("Fact %s already in collection", item)
        #     self.in_counter += 1
        #     return
        # else:
        fact_collection: FactCollection = QueueProcessor.get_fact_collection()
        for item in buffer:
            LOGGER.info("Writing: %s", item)
            item.lineage = None  # Appended(lineage=getattr(item, "lineage", None))
            fact_collection.append(item)
            # SAMPLE_HISTOGRAM.observe(random.random() * 10)
        # size = len(list(self.fact_collection._prefix_read_items(b'')))
        # LOGGER.debug('Fact collection elements: %s in FactGeneratedQueueProcessor', size)
        # Put the fact in the queue to be checked for triggers
        #### self.outgoing_queue.put(item)
        return buffer


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

    # TODO: Divide the following method into smaller functions
    @staticmethod
    def process_item_from_queue(buffer: Any) -> List[SubTriggerPair]:
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
        for item in buffer:
            LOGGER.debug("Checking fact %s against triggers", item)
            # item_in_collection: bool = (
            #     item in QueueProcessor.get_fact_collection()
            # )
            # # LOGGER.debug("Item %s in fact_collection: %s", item, item in QueueProcessor.get_fact_collection())
            PARANOID = True
            if PARANOID:
                while item not in QueueProcessor.get_fact_collection():
                    LOGGER.info(
                        "Fact %s not in collection, requeueing...", item.__dict__
                    )
                    time.sleep(0.1)

            LOGGER.debug("IN COLLECTION: %s", item.__dict__)
            # Let's filter out the facts that are irrelevant to this trigger
            for _, trigger in QueueProcessor.get_trigger_dict().items():
                # Get the attributes that are in the Match clause
                attribute_names_in_trigger: List[str] = trigger.cypher.parse_tree.attribute_names 
                if isinstance(item, FactNodeHasAttributeWithValue) and item.attribute not in attribute_names_in_trigger:
                    continue
                elif isinstance(item, FactNodeHasLabel):
                    continue
                for variable, node in trigger.cypher.parse_tree.get_node_variables():
                    # FactNodeHasLabel might not be enough to trigger
                    if isinstance(item, (FactNodeHasAttributeWithValue,)):
                        result: list[dict[str, Any]] = trigger.cypher._evaluate(QueueProcessor.get_fact_collection(), start_entity_var_id_mapping={variable: item.node_id})
                        LOGGER.debug('Result of %s is %s, %s, %s', item, result, variable, item.node_id)
                        if result:
                            sub_trigger_pair: SubTriggerPair = SubTriggerPair(sub={variable: item.node_id}, trigger=trigger)
                            LOGGER.debug('Created SubTriggerPair: %s', sub_trigger_pair)
                            out.append(sub_trigger_pair)
                    else:
                        # Add back Relationship facts here...
                        continue
                    
                    LOGGER.debug("Checking trigger %s", trigger)
                LOGGER.debug('Setting processed = True')
        return out


class TriggeredLookupProcessor(QueueProcessor):  # pylint: disable=too-few-public-methods
    """
    Executes triggered Cypher queries and generates computed facts.
    
    This processor takes SubTriggerPair objects, evaluates the associated
    Cypher triggers with the provided variable bindings, and executes
    the trigger functions to compute new attribute values or relationships.
    """

    @staticmethod
    def process_item_from_queue(buffer: list) -> List[Any]:
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
        out: list[list[Any]] = []
        for item in buffer:
            LOGGER.debug("Got item in TriggeredLookupProcessor: %s", item)
            result: list[Any] = (
                TriggeredLookupProcessor._process_sub_trigger_pair(item)
            )
            out.append(result)
        return out

    @staticmethod
    def _process_sub_trigger_pair(
        sub_trigger_pair: SubTriggerPair,
    ) -> List[Any]:
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
        fact_collection: FactCollection = QueueProcessor.get_fact_collection()
        LOGGER.debug(str(sub_trigger_pair))
        # return_clause = (
        #     sub_trigger_pair.trigger.cypher.parse_tree.cypher.return_clause
        # )
        try:
            solutions = sub_trigger_pair.trigger.cypher._evaluate(
                fact_collection, start_entity_var_id_mapping=sub_trigger_pair.sub
            )  # pylint: disable=protected-access
        except KeyError:
            solutions = []
        LOGGER.debug(
            "Done with _evaluate in TriggeredLookupProcessor: %s",
        )
        LOGGER.debug("solutions in _process: %s", solutions)
        LOGGER.debug('Trigger: %s %s', sub_trigger_pair.trigger, sub_trigger_pair.trigger.is_attribute_trigger)
        if 0 and sub_trigger_pair.trigger.is_relationship_trigger:
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
            ]
            process_solution_function: Callable = (
                TriggeredLookupProcessor._process_solution_node_relationship
            )
        elif sub_trigger_pair.trigger.is_attribute_trigger:
            variable_to_set = sub_trigger_pair.trigger.variable_set
            process_solution_args = [  # prepend `solutions` onto this when called later
                sub_trigger_pair,
                variable_to_set,
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
                ####### Ensure that projection from Match clause is includedin solution dict
                computed_facts.append(
                    process_solution_function(solution, *process_solution_args)
                )
            except Exception as e:  # pylint: disable=broad-exception-caught
                LOGGER.error("In TriggeredLookupProcessor: Error processing solution: %s, %s, %s", e, solution, process_solution_args)

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
    ) -> FactNodeHasAttributeWithValue | Type[NullResult]:
        """
        Process a Cypher solution to generate a node attribute fact.
        
        This method extracts the necessary arguments from the solution,
        calls the trigger function to compute the new attribute value,
        and creates a FactNodeHasAttributeWithValue object.
        
        Args:
            solution: Dictionary containing variable bindings from Cypher evaluation.
            sub_trigger_pair: The trigger and substitutions being processed.
            variable_to_set: The variable name identifying the target node.
            
        Returns:
            A new FactNodeHasAttributeWithValue object, or NullResult if
            the computation cannot be completed.
        """
        # splat = TriggeredLookupProcessor._extract_splat_from_solution(
        #     solution, return_clause
        # )
        LOGGER.debug('Called _process_solution_node_attribute with arguments: %s', sub_trigger_pair)

        if any(isinstance(arg, NullResult) for arg in solution.values()):
            LOGGER.debug("NullResult found in solution splat %s", solution)
            return NullResult

        arguments: Dict[str, Any] = {key: value for key, value in solution.items() if not key.startswith('__')}
        LOGGER.debug('Will compute value with arguments: %s', arguments)

        python_args: Dict[str, Any] = {
            key: value.value for key, value in arguments.items()
            if not key.startswith('__')
        }
        if any(isinstance(i, NullResult) for i in python_args.values()):
            return NullResult
        LOGGER.debug('Will compute value with python arguments: %s', python_args)
        computed_value = sub_trigger_pair.trigger.function(**python_args)
        LOGGER.debug('Computed value: %s', computed_value)
        target_attribute = sub_trigger_pair.trigger.attribute_set
        LOGGER.debug('Target attribute: %s', target_attribute)
        LOGGER.debug('Solution: %s', solution)
        node_id = TriggeredLookupProcessor._extract_node_id_from_solution(
            solution, variable_to_set
        )
        LOGGER.debug('node_id: %s', node_id)
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
        """
        Extract the node ID for a specific variable from the Cypher solution.
        
        This method looks up the node ID associated with a variable name
        in the solution's match results, which are stored under the
        MATCH_SOLUTION_KEY.
        
        Args:
            solution: Dictionary containing variable bindings from Cypher evaluation.
            variable_to_set: The variable name whose node ID should be extracted.
            
        Returns:
            The node ID string for the specified variable.
            
        Raises:
            ValueError: If the variable is not found in the solution.
        """

        # MATCH_SOLUTION_KEY: str = '__MATCH_SOLUTION_KEY__'
        LOGGER.debug("_extract_node_id_from_solution: %s", solution)
        try:
            node_id = solution[MATCH_SOLUTION_KEY][
                variable_to_set
            ]
            return node_id
        except KeyError as e:
            raise ValueError(f"Error extracting node ID: {variable_to_set}, {solution}") from e
        # Match solution key is empty for `solution` if the MATCH clause has a relationship.
        # Perhaps I didn't update the code to propagate the solution through a RelationshipChain or
        # RelationshipChainList?`
