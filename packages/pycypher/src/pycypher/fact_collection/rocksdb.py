"""
RocksDB
=======
"""

from __future__ import annotations

import inspect
from typing import Any, Dict, Generator

from nmetl.logger import LOGGER
from pycypher.fact import (
    AtomicFact,
    FactNodeHasAttributeWithValue,
    FactNodeHasLabel,
)
from pycypher.fact_collection import FactCollection
from pycypher.fact_collection.key_value import KeyValue
from pycypher.query import (
    NullResult,
    QueryNodeLabel,
    QueryValueOfNodeAttribute,
)
from rocksdict import (  # pylint: disable=no-name-in-module, import-error
    BlockBasedIndexType,
    BlockBasedOptions,
    Options,
    Rdict,
    ReadOptions,
    WriteOptions,
)
from shared.helpers import decode, encode


class RocksDBFactCollection(FactCollection, KeyValue):
    """
    ``FactCollection`` that uses RocksDB as a backend.

    Attributes:
        session (Session): The session object associated with the fact collection.
    """

    def __init__(self, *args, db_path: str = "rocksdb", **kwargs):
        """
        Initialize a RocksDB-backed FactCollection.

        Args:
            *args: Variable positional arguments (ignored).
            **kwargs: Variable keyword arguments (ignored).
        """
        self.db_path = db_path
        self.write_options = WriteOptions()
        self.write_options.sync = False
        self.options = Options()
        self.options.set_unordered_write(False)
        self.options.create_if_missing(True)
        self.options.set_max_background_jobs(10)
        self.options.set_max_write_buffer_number(10)
        self.options.set_write_buffer_size(16 * 1024 * 1024 * 1024)

        self.write_options = WriteOptions()
        self.write_options.disable_wal = True

        self.db = Rdict(self.db_path, self.options)
        self.db.set_write_options(self.write_options)
        block_opts = BlockBasedOptions()
        block_opts.set_index_type(BlockBasedIndexType.hash_search())
        block_opts.set_bloom_filter(20, False)
        self.options.set_block_based_table_factory(block_opts)
        self.iter = self.db.iter(ReadOptions())
        self.LAST_KEY = "\xff"  # pylint: disable=invalid-name
        self.diverted_counter = 0
        self.diversion_miss_counter = 0
        super().__init__(*args, **kwargs)

    def _prefix_read_items(self, prefix: str) -> Generator[Any, Any]:
        """
        Read a range of keys from the database.

        Args:
            start_key (str): The starting key of the range.
            end_key (str): The ending key of the range.

        Yields:
            Any: The values associated with the keys in the range.
        """
        for key in self._prefix_read_keys(prefix):
            value = decode(self.db.get(key))
            if value is None:
                break
            yield key, value

    def _prefix_read_keys(self, prefix: str) -> Generator[Any, Any]:
        """
        Read a range of keys from the database.

        Args:
            start_key (str): The starting key of the range.
            end_key (str): The ending key of the range.

        Yields:
            Any: The values associated with the keys in the range.
        """
        if not isinstance(prefix, str):
            prefix = str(prefix, encoding="utf8")
        for key in self.db.keys(from_key=prefix):
            if not key.startswith(prefix):
                return
            yield key

    def _prefix_read_values(self, prefix: str) -> Generator[Any, Any]:
        """
        Read a range of keys from the database.

        Args:
            prefix (str): The prefix of the keys to read.

        Yields:
            Any: The values associated with the keys in the range.
        """
        for key in self._prefix_read_keys(prefix):
            value = decode(self.db.get(key))
            if value is None:
                break
            yield value

    def range_read(self, start_key, end_key) -> Generator[Any, Any]:
        """
        Read a range of keys from the database.

        Args:
            start_key (str): The starting key of the range.
            end_key (str): The ending key of the range.

        Yields:
            Any: The values associated with the keys in the range.
        """
        for key in self.db.keys(from_key=start_key):
            if key > end_key:
                break
            value = self.db.get(key)
            if value is None:
                break
            self.yielded_counter += 1
            yield decode(value)

    def keys(self) -> Generator[str]:
        """
        Yields:
            str: Each key
        """
        yield from self._prefix_read_keys("")

    def values(self) -> Generator[AtomicFact]:
        """
        Iterate over all values stored in the memcached server.
        """
        yield from self._prefix_read_values("")

    def node_has_attribute_with_specific_value_facts(
        self, attribute: str, value: Any
    ):
        """
        Return a generator of facts that have a specific attribute and value.
        """
        for fact in self.range_read("node_attribute:", "node_attribute:\xff"):
            if (
                isinstance(fact, FactNodeHasAttributeWithValue)
                and fact.attribute == attribute
                and fact.value == value
            ):
                yield fact

    def __delitem__(self, key: str):
        """
        Delete a fact.

        Raises:
            IndexError: If the index is out of range.
        """
        del self.db[key]

    def __len__(self):
        """
        Get the number of facts in the collection.

        Returns:
            int: The number of facts in the collection.
        """
        counter = 0
        for k in self.db.keys():
            LOGGER.debug("counting: %s", k)
            counter += 1
        return counter

    def close(self):
        """Erase all the keys in the db"""
        LOGGER.warning("Deleting RocksDB")
        self.db.delete_range("\x00", "\xff")

    def __contains__(self, fact: AtomicFact) -> bool:
        """
        Check if a fact is in the collection.

        Args:
            fact (AtomicFact): The fact to check for.

        Returns:
            bool: True if the fact is in the collection, False otherwise.
        """
        # curframe = inspect.currentframe()
        # calframe = inspect.getouterframes(curframe, 2)
        # LOGGER.debug("__contains__ called: %s", calframe[1][3])
        index = self.make_index_for_fact(fact)
        if self.db.key_may_exist(index):
            value = self.db.get(index)
            if value is None:
                self.diversion_miss_counter += 1
            return decode(value) == fact if value is not None else False
        self.diverted_counter += 1
        return False

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
            prefix = f"node_attribute:{node_id}:{attribute}:"
            for fact in self._prefix_read_values(prefix):
                row[fact.attribute] = fact.value
                break
        return row

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

        case FactNodeHasLabel():
            return f"node_label:{fact.node_id}:{fact.label}"
        case FactNodeHasAttributeWithValue():
            return f"node_attribute:{fact.node_id}:{fact.attribute}:{fact.value}"
        case FactRelationshipHasLabel():
            return f"relationship_label:{fact.relationship_id}:{fact.relationship_label}"
        case FactRelationshipHasAttributeWithValue():
            return f"relationship_attribute:{fact.relationship_id}:{fact.attribute}:{fact.value}"
        case FactRelationshipHasSourceNode():
            return f"relationship_source_node:{fact.relationship_id}:{fact.source_node_id}"
        case FactRelationshipHasTargetNode():
            return f"relationship_target_node:{fact.relationship_id}:{fact.target_node_id}"
        case FactNodeRelatedToNode():
            return f"node_relationship:{fact.node1_id}:{fact.node2_id}:{fact.relationship_label}"
        """
        if self.session is None:
            LOGGER.warning("Session is not set. Reverting to brute-force.")
            return FactCollection.query_node_label(self, query)
        prefix = "node_label:"
        labels = self.session.get_all_known_labels()
        for label in labels:
            prefix = f"node_label:{label}::{query.node_id}"
            result = NullResult(query)
            for fact in self._prefix_read_values(prefix):
                if (
                    isinstance(fact, FactNodeHasLabel)
                    and fact.node_id == query.node_id
                ):
                    result = fact.label
                    break
        return result

    def append(self, fact: AtomicFact) -> None:
        """
        Insert an AtomicFact.

        Args:
            value (AtomicFact): The AtomicFact object to be inserted.

        Returns:
            None
        """
        self.put_counter += 1
        index = self.make_index_for_fact(fact)
        self.db.put(index, encode(fact), write_opt=self.write_options)
        self.db.flush()

    def __iter__(self) -> Generator[AtomicFact]:
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        LOGGER.debug("__iter__ called: %s", calframe[1][3])

        yield from self.values()
        LOGGER.debug("Done iterating values by brute force.")

    def __repr__(self):
        return "Rocks"

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
        result = list(self._prefix_read_values(prefix))
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
        prefix = f"node_label:{label}::"
        for fact in self._prefix_read_values(prefix):
            if isinstance(fact, FactNodeHasLabel) and fact.label == label:
                yield fact.node_id

    def nodes_with_label_facts(self, label: str) -> Generator[str]:
        """
        Return a list of all the nodes with a specific label.

        Args:
            label (str): The label of the nodes to return.

        Returns:
            list: A list of all the nodes with the specified label.
        """
        prefix = f"node_label:{label}::"
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
        prefix = "relationship_source_node:"
        yield from self._prefix_read_values(prefix)

    def relationship_has_target_node_facts(self):
        """
        Generator method that yields facts of type FactRelationshipHasSourceNode.

        This method iterates over the `facts` attribute of the instance and yields
        each fact that is an instance of the FactRelationshipHasSourceNode class.

        Yields:
            FactRelationshipHasTargetNode: Facts that are instances of
                FactRelationshipHasTargetNode.
        """
        prefix = "relationship_target_node:"
        yield from self.range_read(prefix, prefix + self.LAST_KEY)
