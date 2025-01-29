"""Shim for NetworkX graphs"""

from __future__ import annotations

import copy
import uuid

import networkx as nx

from pycypher.core.node_classes import Literal
from pycypher.etl.fact import (
    FactCollection,
    FactNodeHasAttributeWithValue,
    FactNodeHasLabel,
    FactRelationshipHasLabel,
    FactRelationshipHasSourceNode,
    FactRelationshipHasTargetNode,
)
from pycypher.shims import Shim


class NetworkX(Shim):
    """
    A shim class for converting a NetworkX directed graph into a FactCollection.

    Attributes:
        graph (nx.DiGraph): The NetworkX directed graph to be converted.

    Methods:
        __repr__(): Returns a string representation of the NetworkX object.
        __str__(): Returns a string representation of the NetworkX object.
        make_fact_collection() -> FactCollection: Converts the NetworkX graph into a FactCollection.
    """

    def __init__(self, graph: nx.DiGraph):
        self.graph = graph

    def __repr__(self):
        return f"NetworkX({self.graph})"

    def __str__(self):
        return f"NetworkX({self.graph})"

    def make_fact_collection(self) -> FactCollection:
        """
        Creates a FactCollection from the current graph.

        This method deep copies the current graph and assigns unique IDs and empty label lists
        to each node and edge. It then populates the labels and attributes for nodes and edges,
        and constructs a list of facts representing these properties.

        Returns:
            FactCollection: A collection of facts representing the nodes and edges of the graph.
        """
        graph_cypher = copy.deepcopy(self.graph)
        for node in graph_cypher.nodes:
            graph_cypher.nodes[node]["_labels"] = []
            graph_cypher.nodes[node]["_id"] = uuid.uuid4().hex

        for edge in graph_cypher.edges:
            graph_cypher.edges[edge]["_labels"] = []
            graph_cypher.edges[edge]["_id"] = uuid.uuid4().hex

        for node in graph_cypher.nodes:
            graph_cypher.nodes[node]["_labels"].append(
                graph_cypher.nodes[node]["category"]
            )

        for edge in graph_cypher.edges:
            graph_cypher.edges[edge]["_labels"].append(
                "Knows"
            )  # TODO: Fix this!

        fact_list = []

        for node in graph_cypher.nodes:
            for label in graph_cypher.nodes[node]["_labels"]:
                fact_list.append(
                    FactNodeHasLabel(
                        node_id=graph_cypher.nodes[node]["_id"],
                        label=label,
                    )
                )
            for attribute in graph_cypher.nodes[node]:
                if attribute not in ["_labels", "_id"]:
                    fact_list.append(
                        FactNodeHasAttributeWithValue(
                            node_id=graph_cypher.nodes[node]["_id"],
                            attribute=attribute,
                            value=Literal(graph_cypher.nodes[node][attribute]),
                        )
                    )

        for edge in graph_cypher.edges.data():
            source_node_name, target_node_name, edge_data = edge
            source_node_id = graph_cypher.nodes[source_node_name]["_id"]
            target_node_id = graph_cypher.nodes[target_node_name]["_id"]
            for label in edge_data["_labels"]:
                fact_list.append(
                    FactRelationshipHasLabel(
                        relationship_id=edge_data["_id"],
                        relationship_label=label,
                    )
                )
            fact_list.append(
                FactRelationshipHasSourceNode(
                    relationship_id=edge_data["_id"],
                    source_node_id=source_node_id,
                )
            )
            fact_list.append(
                FactRelationshipHasTargetNode(
                    relationship_id=edge_data["_id"],
                    target_node_id=target_node_id,
                )
            )

        fact_collection = FactCollection(fact_list)

        return fact_collection
