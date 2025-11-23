"""
FoundationDB Fact Collection
============================

Use FoundationDB as the key-value store of the ``FactCollection``.
"""

from __future__ import annotations

import datetime
import gzip
import inspect
import pickle
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor as ThreadPool
from queue import Queue
from typing import Any, Dict, Generator, Literal, Optional

from nmetl.logger import LOGGER
from nmetl.prometheus_metrics import (
    FACTS_APPENDED,
    FDB_WRITE_TIME,
    NUMBER_OF_KEYS_SCANNED,
    TIME_IN_FDB_ITERATOR,
)
from pycypher.fact import (
    AtomicFact,
    FactNodeHasAttributeWithValue,
    FactNodeHasLabel,
    FactRelationshipHasSourceNode,
)
from pycypher.fact_collection import FactCollection
from pycypher.fact_collection.key_value import KeyValue
from pycypher.query import (
    NullResult,
    QueryNodeLabel,
    QuerySourceNodeOfRelationship,
    QueryTargetNodeOfRelationship,
    QueryValueOfNodeAttribute,
)
from shared.helpers import decode, encode, ensure_bytes

try:
    import fdb

    fdb.api_version(730)
except ModuleNotFoundError:
    LOGGER.warning("fdb not installed, fdb support disabled")

LOGGER.setLevel("INFO")

EXPERIMENTAL_WRITE_QUEUE: Queue = Queue(maxsize=4_192)


def write_queue_worker(db):
    counter: int = 0
    while 1:
        index, obj = EXPERIMENTAL_WRITE_QUEUE.get()
        db[index] = obj  # encode(fact, to_bytes=True)
        if counter % 1000 == 0:
            LOGGER.debug(
                "Written %s items, queue is %s",
                counter,
                EXPERIMENTAL_WRITE_QUEUE.qsize(),
            )
        counter += 1
        FACTS_APPENDED.inc(1.0)


def write_fact(db, index, fact) -> Literal[True]:
    """Write a ``Fact`` to FoundationDB"""
    if index is None:
        return True
    LOGGER.debug("Writing to FoundationDB: %s", index)
    # db[index] = encode(fact, to_bytes=True)
    obj = encode(fact, to_bytes=True)
    EXPERIMENTAL_WRITE_QUEUE.put(
        (
            index,
            obj,
        )
    )

    # FACTS_APPENDED.inc(1)
    return True


def write_fact_secondary(db, index, fact) -> Literal[True]:
    """Write a ``Fact`` to FoundationDB"""
    if index is None:
        return True
    LOGGER.debug("Writing to FoundationDB: %s", index)
    # db[index] = encode(fact, to_bytes=True)
    obj = encode(fact, to_bytes=True)
    EXPERIMENTAL_WRITE_QUEUE.put(
        (
            index,
            obj,
        )
    )

    return True


class FoundationDBFactCollection(FactCollection, KeyValue):
    """
    ``FactCollection`` that uses FoundationDB as a backend.

    Attributes:
        session (Session): The session object associated with the fact collection.
    """

    def __init__(self, *args, foundationdb_cluster_file: str, sync_writes: bool = False, **kwargs):
        """
        Initialize a FoundationDB-backed FactCollection.

        Args:
            *args: Variable positional arguments passed to parent class.
            sync_writes: If True, use synchronous writes to FoundationDB.
                        If False, use asynchronous writes for better performance.
                        Defaults to False.
            **kwargs: Variable keyword arguments passed to parent class.
        """

        self.db = fdb.open(foundationdb_cluster_file)
        self.thread_pool = ThreadPool(16)
        self.pending_facts = []
        self.sync_writes = sync_writes
        self.write_queue_worker = threading.Thread(
            target=write_queue_worker, args=(self.db,), daemon=True
        )
        self.write_queue_worker.start()
        super().__init__(*args, **kwargs)

    def _prefix_read_items(
        self,
        prefix: bytes,
        continue_to_end: Optional[bool] = False,
        only_one_result: Optional[bool] = False,
    ) -> Generator[Any, Any]:
        """
        Read a range of keys from the database.

        Yields:
            Any: The values associated with the keys in the range.
        """
        LOGGER.debug("_prefix_read_items called")
        start_time: datetime.datetime = datetime.datetime.now()
        if prefix is None:
            return
        counter: int = 0
        prefix: bytes = ensure_bytes(prefix, encoding="utf8")

        end_key: bytes = b"\xff" if continue_to_end else prefix + b"\xff"
        for key_value_obj in self.db.get_range(
            ensure_bytes(ensure_bytes(prefix), encoding="utf8"),
            end_key,
            1 if only_one_result else 0,
        ):
            value: AtomicFact = decode(key_value_obj.value)
            key: str = key_value_obj.key.decode("utf8")
            if continue_to_end or key.startswith(str(prefix, encoding="utf8")):
                counter += 1
                yield key, value
            else:
                break
            if only_one_result:
                break
        LOGGER.debug("Done with _prefix_read_items: %s: %s", prefix, counter)
        end_time: datetime.datetime = datetime.datetime.now()
        seconds_in_iterator: float = (end_time - start_time).total_seconds()
        TIME_IN_FDB_ITERATOR.observe(seconds_in_iterator)
        NUMBER_OF_KEYS_SCANNED.observe(counter)

    def make_index_for_fact(self, fact: AtomicFact) -> bytes:
        """Used for the memcache index"""
        # Call the superclass's version of the method and convert to bytes
        try:
            index: bytes | str = KeyValue.make_index_for_fact(self, fact)
        except:
            LOGGER.warning("Could not make index for %s", fact)
            return None
        return ensure_bytes(index, encoding="utf8")

    def make_secondary_index_for_fact(self, fact: AtomicFact) -> bytes:
        """Used for the memcache index"""
        # Call the superclass's version of the method and convert to bytes
        try:
            index: bytes | str = KeyValue.make_secondary_index_for_fact(
                self, fact
            )
        except:
            LOGGER.warning("Could not make secondary index for %s", fact)
            return None
        return ensure_bytes(index, encoding="utf8")

    def _prefix_read_keys(
        self,
        prefix: str,
        only_one_item: bool = False,
        continue_to_end: Optional[bool] = False,
    ) -> Generator[Any, Any]:
        """
        Read a range of keys from the database.

        Args:
            start_key (str): The starting key of the range.
            end_key (str): The ending key of the range.

        Yields:
            Any: The values associated with the keys in the range.
        """
        LOGGER.debug("_prefix_read_keys called")
        for key, _ in self._prefix_read_items(
            ensure_bytes(prefix, encoding="utf8"),
            continue_to_end=continue_to_end,
            only_one_item=only_one_item,
        ):
            yield key

    def _prefix_read_values(
        self,
        prefix: bytes,
        continue_to_end: Optional[bool] = False,
        only_one_result: Optional[bool] = False,
    ) -> Generator[Any, Any]:
        """
        Read a range of keys from the database.

        Args:
            prefix (str): The prefix of the keys to read.

        Yields:
            Any: The values associated with the keys in the range.
        """
        LOGGER.debug("_prefix_read_values called")
        counter = 0
        for _, value in self._prefix_read_items(
            ensure_bytes(prefix, encoding="utf8"),
            continue_to_end=continue_to_end,
            only_one_result=only_one_result,
        ):
            counter += 1
            yield value
        LOGGER.debug("Done with _prefix_read_values: %s", counter)

    def keys(self) -> Generator[str]:
        """
        Yields:
            str: Each key
        """
        LOGGER.debug("keys called")
        for key_value_obj in self.db.get_range(b"\x00", b"\xff"):
            key = key_value_obj.key.decode("utf8")
            yield key

    def values(self) -> Generator[AtomicFact]:
        """
        Iterate over all values stored in the memcached server.
        """
        LOGGER.debug("values called")
        yield from self

    def relationships_with_specific_source_node_facts(
        self, source_node_id: str
    ) -> Generator[str, None, None]:
        """
        Return a generator of facts that have a specific source node.
        """
        LOGGER.debug(
            "relationships_with_specific_source_node_facts called: %s",
            source_node_id,
        )
        for fact in self._prefix_read_values(
            f"relationship_source_node_secondary:{source_node_id}:",
            continue_to_end=False,
        ):
            yield fact

    # TODO
    def query_relationship_source(
        self, query: QuerySourceNodeOfRelationship
    ) -> str | NullResult:
        """
        Query the source node of a relationship.

        Args:
            query (QueryRelationshipSource): Query object containing the relationship_id.

        Returns:
            str: The source node ID of the relationship.
        """
        # b'relationship_source_node:d22f4848b11c4bb0a541d58a73b8d926:Tract::01015002300', RelationshipHasSourceNode: d22f4848b11c4bb0a541d58a73b8d926 Tract::01015002300)
        prefix: bytes = ensure_bytes(
            f"relationship_source_node:{query.relationship_id}:",
            encoding="utf8",
        )
        LOGGER.debug(
            "query_relationship_source called: %s", query.relationship_id
        )
        key = self.db.get_key(fdb.KeySelector.first_greater_than(prefix))
        LOGGER.debug("key: %s", key)
        value = self.db[key]
        LOGGER.debug("value: %s", value)
        return decode(value).source_node_id

        # fact_list: list[AtomicFact] = list(self._prefix_read_values(prefix, continue_to_end=False, only_one_result=True))
        # if len(fact_list) == 0:
        #     raise ValueError(f"Found no source nodes for {query}")
        # elif len(fact_list) > 1:
        #     raise ValueError(f"Found multiple source nodes for {query}: {fact_list}")
        # else:
        #     return fact_list[0].source_node_id

    def query_relationship_target(
        self, query: QueryTargetNodeOfRelationship
    ) -> str | NullResult:
        """
        Query the target node of a relationship.

        Args:
            query (QueryRelationshipTarget): Query object containing the relationship_id.

        Returns:
            str: The target node ID of the relationship.
        """
        prefix: bytes = ensure_bytes(
            f"relationship_target_node:{query.relationship_id}:",
            encoding="utf8",
        )
        LOGGER.debug(
            "query_relationship_target called: %s", query.relationship_id
        )
        fact_list: list[AtomicFact] = list(
            self._prefix_read_values(
                prefix, continue_to_end=False, only_one_result=True
            )
        )
        if len(fact_list) == 0:
            raise ValueError(f"Found no source nodes for {query}")
        elif len(fact_list) > 1:
            raise ValueError(
                f"Found multiple source nodes for {query}: {fact_list}"
            )
        else:
            return fact_list[0].target_node_id

    def relationships_with_specific_target_node_facts(
        self, target_node_id: str
    ) -> Generator[str, None, None]:
        """
        Return a generator of facts that have a specific target node.
        """
        LOGGER.debug(
            "relationships_with_specific_target_node_facts called: %s",
            target_node_id,
        )
        for fact in self._prefix_read_values(
            f"relationship_target_node_secondary:{target_node_id}:",
            continue_to_end=False,
        ):
            yield fact

    def node_has_attribute_with_specific_value_facts(
        self, attribute: str, value: Any
    ):
        """
        Return a generator of facts that have a specific attribute and value.

        TODO: This can be refactored for efficiency. Will have to add an index key for certain facts.
        """
        LOGGER.debug("node_has_attribute_with_specific_value_facts called")
        for fact in self._prefix_read_values(
            b"node_attribute:", continue_to_end=True
        ):
            if (
                isinstance(fact, FactNodeHasAttributeWithValue)
                and fact.attribute == attribute
                and fact.value == value
            ):
                yield fact
        LOGGER.debug("done")

    def __delitem__(self, key: str):
        """
        Delete a fact.

        Raises:
            IndexError: If the index is out of range.
        """
        del self.db[ensure_bytes(key, encoding="utf8")]

    def __len__(self):
        """
        Get the number of facts in the collection.

        Returns:
            int: The number of facts in the collection.
        """
        counter = 0
        for k in self.keys():
            LOGGER.debug("counting: %s", k)
            counter += 1
        return counter

    def close(self) -> None:
        """Erase all the keys in the db"""
        LOGGER.warning("Deleting FoundationDB data")
        self.db.clear_range(b"\x00", b"\xff")
        time.sleep(1)

    def __contains__(self, fact: AtomicFact) -> bool:
        """
        Check if a fact is in the collection.

        Args:
            fact (AtomicFact): The fact to check for.

        Returns:
            bool: True if the fact is in the collection, False otherwise.
        """
        index: bytes = self.make_index_for_fact(fact)
        index = ensure_bytes(index, encoding="utf8")
        #### return self.key_cache.exists(index)
        # return self.db.get(index) is not None

        # output = list(
        #     i
        #     for i in self._prefix_read_values(index, only_one_result=True)
        #     if i is not None
        # )
        # # size: int = len(list(self._prefix_read_values(b'')))
        # # LOGGER.debug("Checking membership with index: %s: %s: %s", index, output, size)
        # return len(output) > 0
        value: AtomicFact = self.db.get(index)
        return (
            value is not None
        )  # decode(value) == fact if value is not None else False

    def attributes_for_specific_node(
        self, node_id: str, *attributes: str
    ) -> Dict[str, Any]:
        """
        Return a dictionary of all the attributes for a specific node.

        Args:
            node_id (str): The ID of the node.

        Returns:
            dict: A dictionary of all the attributes for the specified node.
        """
        row = {attribute: None for attribute in attributes}
        for attribute in attributes:
            prefix = ensure_bytes(
                f"node_attribute:{node_id}:{attribute}:", encoding="utf8"
            )
            for fact in self._prefix_read_values(prefix):
                row[fact.attribute] = fact.value
                break  # This shouldn't be necessary
        return row

    def node_has_specific_label_facts(self, label: str):
        """
        Generator function that yields facts of type `FactNodeHasLabel`.

        Iterates over the `facts` attribute and yields each fact that is an instance
        of `FactNodeHasLabel`.

        Yields:
            FactNodeHasLabel: Facts that are instances of `FactNodeHasLabel`.

        TODO: Also optimizew this by adding an index key on inserts.
        """
        LOGGER.debug("Node has specific label facts...")
        prefix = bytes(f"node_label:{label}::", encoding="utf8")
        for fact in self._prefix_read_values(prefix):
            if isinstance(fact, FactNodeHasLabel) and fact.label == label:
                yield fact

    def node_has_label_facts(self):
        """
        Generator function that yields facts of type `FactNodeHasLabel`.

        Iterates over the `facts` attribute and yields each fact that is an instance
        of `FactNodeHasLabel`.

        Yields:
            FactNodeHasLabel: Facts that are instances of `FactNodeHasLabel`.
        """
        for fact in self._prefix_read_values(b"node_label:"):
            if isinstance(fact, FactNodeHasLabel):
                yield fact

    def query_node_label(self, query: QueryNodeLabel):
        """Given a query for a node label, return the label if it exists.

        If no label exists, return a NullResult. If multiple labels
        exist, raise a ValueError.

        Args:
            query: The query to execute.

        Returns:
            The label of the node, or a NullResult if no label exists.

        Raises:
            ValueError: If multiple labels exist for the node.
        """
        node_id_parts: list[str] = query.node_id.split("::")
        if len(node_id_parts) == 2:
            return node_id_parts[0]
        LOGGER.debug("Query node label...")
        for fact in self.node_has_label_facts():
            if (
                isinstance(fact, FactNodeHasLabel)
                and fact.node_id == query.node_id
            ):
                LOGGER.debug("Found label: %s", fact.label)
                return fact.label
        return NullResult(query)

    def append(self, fact: AtomicFact) -> None:
        """
        Insert an AtomicFact.

        Args:
            value (AtomicFact): The AtomicFact object to be inserted.

        Returns:
            None
        """
        LOGGER.debug("Append called: %s: %s", fact, self.put_counter)
        index: bytes = self.make_index_for_fact(fact)
        index1: bytes = self.make_secondary_index_for_fact(fact)
        # self.db[index] = encode(fact, to_bytes=True)
        # self.db[index1] = encode(fact, to_bytes=True)
        # if index is None:
        #     LOGGER.warning("Append failed.")
        #     return
        # # apply_function = self.thread_pool.apply
        self.thread_pool.submit(
            write_fact,
            db=self.db,
            index=index,
            fact=fact,
        )
        self.thread_pool.submit(
            write_fact_secondary, db=self.db, index=index1, fact=fact
        )
        LOGGER.debug("Submitted to thread pool")
        self.put_counter += 1

    def __repr__(self):
        return "FoundationDB"

    def query_value_of_node_attribute(self, query: QueryValueOfNodeAttribute):
        """
        Query the value of a node's attribute.

        Args:
            query (QueryValueOfNodeAttribute): Query object containing the node_id
                and attribute to look up.

        Returns:
            Any: The value of the requested attribute if found.
            NullResult: If no matching attribute is found.

        Raises:
            ValueError: If multiple values are found for the same attribute.

        """
        prefix = ensure_bytes(
            f"node_attribute:{query.node_id}:{query.attribute}:",
            encoding="utf8",
        )
        LOGGER.debug("Querying value of node attribute prefix: %s", prefix)
        result = list(self._prefix_read_values(prefix, only_one_result=True))
        if result is None:
            raise ValueError("Could not find expected fact...")
        if len(result) == 1:
            fact = result[0]
            return fact.value
        if len(result) > 1:
            raise ValueError(f"Found multiple values for {query}: {result}")
        return NullResult(query)

    def nodes_with_label(self, label: str) -> Generator[str]:
        """
        Return a list of all the nodes with a specific label.

        Args:
            label (str): The label of the nodes to return.

        Returns:
            list: A list of all the nodes with the specified label.
        """
        prefix = ensure_bytes(
            f"node_label:{label}:",
            encoding="utf8",
        )
        result: list[AtomicFact] = list(
            self._prefix_read_values(prefix, only_one_result=False)
        )
        for fact in result:
            yield fact.node_id

    def relationships_with_label(self, label: str) -> Generator[str]:
        """
        Return a list of all the nodes with a specific label.

        Args:
            label (str): The label of the nodes to return.

        Returns:
            list: A list of all the nodes with the specified label.
        """
        prefix = ensure_bytes(
            f"relationship_label_secondary:{label}:",
            encoding="utf8",
        )
        result: list[AtomicFact] = list(
            self._prefix_read_values(prefix, only_one_result=False)
        )
        for fact in result:
            yield fact.relationship_id

    def nodes_with_label_facts(self, label: str) -> Generator[str]:
        """
        Return a list of all the nodes with a specific label.

        Args:
            label (str): The label of the nodes to return.

        Returns:
            list: A list of all the nodes with the specified label.
        """
        prefix: bytes = bytes(f"node_label:{label}::", encoding="utf8")
        yield from self._prefix_read_values(prefix)

    def relationship_has_source_node_facts(self):
        """
        Generator method that yields facts of type FactRelationshipHasSourceNode.

        This method iterates over the `facts` attribute of the instance and yields
        each fact that is an instance of the FactRelationshipHasSourceNode class.

        Yields:
            FactRelationshipHasSourceNode: Facts that are instances of
                FactRelationshipHasSourceNode.
        """
        LOGGER.debug("Relationship has source node facts called...")
        for fact in self:
            if isinstance(fact, FactRelationshipHasSourceNode):
                yield fact

    def __iter__(self):
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        LOGGER.debug("__iter__ called: %s", calframe[1][3])

        for key_value_obj in self.db.get_range(b"\x00", b"\xff"):
            value = decode(key_value_obj.value)
            self.yielded_counter += 1
            yield value

    def node_has_attribute_with_value_facts(self):
        """
        Generator method that yields facts of type FactNodeHasAttributeWithValue.

        Iterates over the list of facts and yields each fact that is an instance
        of FactNodeHasAttributeWithValue.

        Yields:
            FactNodeHasAttributeWithValue: Facts that are instances of
                FactNodeHasAttributeWithValue.
        """
        for fact in self._prefix_read_items(b"node_attribute:OSM_Node::"):
            yield fact

    def enumerate_in_batches(
        self,
        begin: Optional[bytes] = b"",
        end: Optional[bytes] = b"\xff",
        batch_size: Optional[int] = 128,
    ):
        """For streaming through long-running transactions"""
        keys_found: bool = True
        counter = 0
        while keys_found:
            keys_found = False
            for k, v in self.db.get_range(begin, end, limit=batch_size):
                last_key = k
                yield k, decode(v)
                keys_found = True
                counter += 1
            if keys_found:
                begin = fdb.KeySelector.first_greater_than(
                    last_key
                )  # pyrefly: ignore
        LOGGER.debug("Enumerated %s keys", counter)

    def first_key_after(self, key: bytes, offset: Optional[int] = 1):
        x = self.db.get_range(
            key,
            fdb.KeySelector.first_greater_than(key) + offset,
        )  # pylint: disable=E1101
        return x[-1].key if x else None

    def skip_keys(
        self, offset: Optional[int] = 1000, max_keys: Optional[int] = None
    ):
        current_key: bytes = b""
        next_key: None | bytes = self.first_key_after(
            current_key, offset=offset
        )
        counter = 0
        yield next_key
        while current_key != next_key:
            counter += 1
            current_key: None | bytes = next_key
            next_key = self.first_key_after(current_key, offset=offset)
            yield current_key
            if max_keys and counter > max_keys:
                break

    def approx_len(
        self, max_keys: Optional[int] = None, increment: int = 10_000
    ):
        counter: int = 0
        for _ in self.skip_keys(increment, max_keys=max_keys):
            counter += 1
        return counter * increment

    def parallel_read(
        self,
        max_keys: Optional[int] = None,
        num_threads: Optional[int] = 4,
        increment: Optional[int] = None,
    ):
        executor: ThreadPool = ThreadPool(num_threads)
        streaming_queue = queue.Queue()

        def _get_range(current_key, next_key):
            for k, v in self.db.get_range(current_key, next_key):
                streaming_queue.put((k, v))
            streaming_queue.put(None)

        futures = []

        def _start_threads():
            current_key = b""
            for next_key in self.skip_keys(
                offset=increment, max_keys=max_keys
            ):
                future = executor.submit(
                    _get_range,
                    current_key=current_key,
                    next_key=next_key,
                )
                futures.append(future)
                current_key = next_key

        queueing_thread: threading.Thread = threading.Thread(
            target=_start_threads
        )
        queueing_thread.start()

        endings = 0
        time.sleep(1)
        while (
            not all(future.done() for future in futures)
            or queueing_thread.is_alive()
            or endings != len(futures)
        ):
            got_one: bool = True
            while got_one:
                got_one = False
                try:
                    thing = streaming_queue.get(timeout=4000)
                except Exception:
                    break
                if thing is None:
                    endings += 1
                    continue
                key, value = thing
                yield endings, len(futures), key, decode(value)
                # print(all(future.ready() for future in futures))
            if not queueing_thread.is_alive() and endings == len(futures):
                break

    def dump_facts_to_file(self, filename: str):
        """Dump all facts to a file"""
        LOGGER.debug("Dumping facts to file: %s", filename)
        with gzip.open(filename, "wb") as f:
            for _, _, key, value in self:
                f.write(pickle.dumps((key, value)))
                f.write(b"\n")

    def serialize(self) -> None:
        counter = 0
        for _, _, key, value in self.parallel_read(increment=1024):
            print(key, pickle.dumps(value))
            counter += 1
