"""
Etcd3
=====
"""

from __future__ import annotations

import time

import etcd3
from nmetl.config import (  # pylint: disable=no-name-in-module
    BLOOM_FILTER_ERROR_RATE,
    BLOOM_FILTER_SIZE,
    ETCD3_RETRY_DELAY,
)
from nmetl.logger import LOGGER
from pycypher.fact_collection import FactCollection
from pycypher.fact_collection.key_value import KeyValue
from pycypher.query import QueryValueOfNodeAttribute
from rbloom import Bloom  # pylint: disable=no-name-in-module, import-error
from shared.helpers import decode, encode


class Etcd3FactCollection(FactCollection, KeyValue):
    """
    ``FactCollection`` that uses etcd version 3 as a backend.

    Attributes:
        facts (List[AtomicFact]): A list of AtomicFact objects.
        session (Session): The session object associated with the fact collection.
    """

    LAST_KEY = "\xff"

    def __init__(self, *args, **kwargs):
        """
        Initialize a etcd3 instance.

        Args:
            *args: Variable positional arguments (ignored).
            **kwargs: Variable keyword arguments (ignored).
        """
        self.client = etcd3.Client("127.0.0.1", 2379)
        self.bloom = Bloom(BLOOM_FILTER_SIZE, BLOOM_FILTER_ERROR_RATE)
        self.bloom_filter_diversions = 0
        self.cache_hits = 0
        self.secondary_cache = []
        self.secondary_cache_max_size = 1
        self.transaction = self.client.Txn()
        self.put_counter = 0
        super().__init__(*args, **kwargs)

    def query_value_of_node_attribute_bak(
        self, query: QueryValueOfNodeAttribute
    ):
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
        # node_attribute:Tract::01013952900:tract_fips:01013952900
        prefix: str = f"node_attribute:{query.node_id}:{query.attribute}:"
        matches = self.client.range(prefix, prefix + self.LAST_KEY).kvs or []
        if len(matches) == 1:
            return (
                matches[0].value.value
                if hasattr(matches[0].value, "value")
                else matches[0].value
            )
        if not matches:
            return NullResult(query)
        if len(matches) > 1:
            raise ValueError(f"Found multiple values for {query}: {matches}")
        raise ValueError("Unknown error")

    def keys(self) -> Generator[str]:
        """
        Iterate over all keys stored in the memcached server.

        Yields:
            str: Each key stored in the memcached server.
        """
        for key_value in self.client.range(all=True).kvs:
            key: str = key_value.key.decode("utf-8")
            yield key

    def __delitem__(self, key: str):
        """
        Delete a fact at a specific index.

        Args:
            index (int): The index of the fact to delete.

        Raises:
            IndexError: If the index is out of range.
        """
        self.client.delete_range(key=key)

    def __len__(self):
        """
        Get the number of facts in the collection.

        Returns:
            int: The number of facts in the collection.
        """
        num_keys = self.client.range(all=True).count
        return num_keys

    def values(self) -> Generator[AtomicFact]:
        """
        Iterate over all values stored in the memcached server.

        Yields:
            AtomicFact: Each value stored in the memcached server.

        Raises:
            pymemcache.exceptions.MemcacheError: If there's an error communicating with the memcached server.
            pickle.PickleError: If there's an error unpickling the data.
        """
        for key_value in self.client.range(all=True).kvs:
            value = key_value.value
            yield decode(value)

    def close(self):
        """Erase all the keys in the etcd3"""
        LOGGER.info("Clearing etcd3")
        self.client.delete_range(all=True)
        # time.sleep(1)

    def insert(self, _, value):
        """Vacuously satisfy the interface"""
        self.append(value)

    def delete_fact(self, fact):
        """Delete a fact from the etcd3"""
        index = self.make_index_for_fact(fact)
        self.client.delete_range(key=index)

    def __contains__(self, fact: AtomicFact) -> bool:
        """
        Check if a fact is in the collection.

        Args:
            fact (AtomicFact): The fact to check for.

        Returns:
            bool: True if the fact is in the collection, False otherwise.
        """
        index = self.make_index_for_fact(fact)

        # Try to divert from etcd3 with a Bloom filter
        if index not in self.bloom:
            self.bloom_filter_diversions += 1
            LOGGER.debug(
                "Bloom filter diversion: %s, cache hits: %s",
                self.bloom_filter_diversions,
                self.cache_hits,
            )
            return False

        if key_value_list := self.client.range(index, index + "\0").kvs:
            key_value = key_value_list[0]
            LOGGER.debug("Cache hit: %s", self.cache_hits)
            self.cache_hits += 1
            return decode(key_value.value) == fact

        return False

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

        facts = [
            fact
            for fact in self.node_has_label_facts()
            if fact.node_id == query.node_id
        ]
        if len(facts) == 1:
            return facts[0].label
        if not facts:
            return NullResult(query)
        if len(facts) > 1:
            raise ValueError(f"Found multiple labels for {query}")
        raise ValueError("Unknown error")

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
        prefix = f"node_attribute:{query.node_id}:{query.attribute}:"

        key_value_list = (
            self.client.range(prefix, prefix + "\0").kvs or []
        )  # Could be None
        if len(key_value_list) == 1:
            key_value = key_value_list[0]
            fact = decode(key_value.value)
            return fact
        else:
            return NullResult(query)

    def append(self, fact: AtomicFact) -> None:
        """
        Insert an AtomicFact into the facts list at the specified index.

        Args:
            index (int): The position at which to insert the value.
            value (AtomicFact): The AtomicFact object to be inserted.

        Returns:
            None
        """
        self.put_counter += 1
        if self.put_counter % 1000 == 0:
            LOGGER.debug("Put counter: %s", self.put_counter)
        index = self.make_index_for_fact(fact)
        self.secondary_cache.append(
            (
                index,
                fact,
            )
        )
        retry_counter = 0
        while 1:
            try:
                self.client.put(index, encode(fact))
                break
            except Exception as e:  # pylint: disable=broad-exception-caught
                LOGGER.debug("Error writing to etcd3: %s", e)
                retry_counter += 1
                if retry_counter > 10:
                    raise e
                time.sleep(ETCD3_RETRY_DELAY)
                continue
        if len(self.secondary_cache) <= self.secondary_cache_max_size:
            return
        LOGGER.debug("Flushing cache")
        transaction = self.client.Txn()
        cached_indexes = []
        for index, cached_fact in self.secondary_cache:
            if index in cached_indexes:
                LOGGER.warning("Duplicate index %s", index)
                continue
            transaction.success(transaction.put(index, encode(cached_fact)))
            cached_indexes.append(index)
            self.bloom.add(index)
        transaction.commit()
        transaction.clear()
        self.secondary_cache = []

    def __iter__(self) -> Generator[AtomicFact]:
        yield from self.values()
