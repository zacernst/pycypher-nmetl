import copy
import uuid

import networkx as nx

from pycypher.cypher_parser import CypherParser
from pycypher.fact import (
    FactCollection,
    FactNodeHasAttributeWithValue,
    FactNodeHasLabel,
    FactRelationshipHasLabel,
    FactRelationshipHasSourceNode,
    FactRelationshipHasTargetNode,
)
from pycypher.node_classes import Literal


def make_fact_collection(graph: nx.DiGraph):
    graph_cypher = copy.deepcopy(graph)
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
        graph_cypher.edges[edge]["_labels"].append("Knows")

    fact_list = []

    for node in graph_cypher.nodes:
        for label in graph_cypher.nodes[node]["_labels"]:
            fact_list.append(
                FactNodeHasLabel(
                    node_id=graph_cypher.nodes[node]["_id"], node_label=label
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
                    relationship_id=edge_data["_id"], relationship_label=label
                )
            )
        fact_list.append(
            FactRelationshipHasSourceNode(
                relationship_id=edge_data["_id"], source_node_id=source_node_id
            )
        )
        fact_list.append(
            FactRelationshipHasTargetNode(
                relationship_id=edge_data["_id"], target_node_id=target_node_id
            )
        )

    fact_collection = FactCollection(fact_list)

    return fact_collection


if __name__ == "__main__":
    edge_dictionary = {
        "a": ["b", "c", "d", "e"],
        "b": ["a", "e"],
        "c": ["a", "d"],
        "d": ["a", "c"],
        "e": ["a", "b"],
        "f": ["g"],
        "g": ["f"],
    }

    graph = nx.DiGraph(edge_dictionary)

    graph.nodes["a"]["name"] = "Alice"
    graph.nodes["b"]["name"] = "Bob"
    graph.nodes["c"]["name"] = "Charlie"
    graph.nodes["d"]["name"] = "David"
    graph.nodes["e"]["name"] = "Eve"
    graph.nodes["f"]["name"] = "Frank"
    graph.nodes["g"]["name"] = "Grace"

    graph.nodes["a"]["age"] = 25
    graph.nodes["b"]["age"] = 30
    graph.nodes["c"]["age"] = 35
    graph.nodes["d"]["age"] = 40
    graph.nodes["e"]["age"] = 45
    graph.nodes["f"]["age"] = 50
    graph.nodes["g"]["age"] = 55

    graph.nodes["a"]["category"] = "Person"
    graph.nodes["b"]["category"] = "Person"
    graph.nodes["c"]["category"] = "Person"
    graph.nodes["d"]["category"] = "Person"
    graph.nodes["e"]["category"] = "Person"
    graph.nodes["f"]["category"] = "Person"
    graph.nodes["g"]["category"] = "Person"

    cypher = "MATCH (n:Person {age: 50})-[r:Knows]->(m:Person) RETURN n.name, m.name"
    result = CypherParser(cypher)
    solutions = result.solutions(make_fact_collection(graph))
    print(solutions)
