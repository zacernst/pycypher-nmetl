"""
Tests for the NetworkX shim.
"""

import pytest
import networkx as nx

from pycypher.shims.networkx_cypher import NetworkX
from pycypher.fact import (
    FactNodeHasLabel,
    FactRelationshipHasLabel,
    FactRelationshipHasSourceNode,
    FactRelationshipHasTargetNode,
)
from fixtures import networkx_graph


def test_networkx_shim_fact_collection_correctness(networkx_graph):
    """
    Test that the NetworkX shim correctly converts a NetworkX graph to a fact collection.
    
    This test verifies:
    1. All nodes are converted to facts with correct labels
    2. All node properties are converted to facts
    3. All edges are converted to facts with correct relationships
    4. The total number of facts matches the expected count
    """
    # Create the shim
    shim = NetworkX(networkx_graph)
    
    # Get the fact collection
    fact_collection = shim.to_fact_collection()
    
    # Get all facts as a list for easier testing
    facts = fact_collection.facts
    
    # Verify node label facts
    node_label_facts = [f for f in facts if isinstance(f, FactNodeHasLabel)]
    assert len(node_label_facts) == 7  # 7 nodes in the graph
    
    # Verify all nodes have the "Person" category as a label
    person_label_facts = [f for f in node_label_facts if f.label == "Person"]
    assert len(person_label_facts) == 7
    
    # Each node has 'name' and 'age' properties (7 nodes * 2 properties = 14)
    assert len(node_property_facts) == 14
    
    # Verify specific node properties
    alice_name_fact = next(
        (f for f in node_property_facts 
         if f.node_id == "a" and f.property_name == "name"),
        None
    )
    assert alice_name_fact is not None
    assert alice_name_fact.property_value == "Alice"
    
    bob_age_fact = next(
        (f for f in node_property_facts 
         if f.node_id == "b" and f.property_name == "age"),
        None
    )
    assert bob_age_fact is not None
    assert bob_age_fact.property_value == 30
    
    # Verify relationship facts
    relationship_label_facts = [f for f in facts if isinstance(f, FactRelationshipHasLabel)]
    
    # Count the number of edges in the original graph
    edge_count = len(networkx_graph.edges())
    assert len(relationship_label_facts) == edge_count
    
    # Verify relationship source and target facts
    relationship_source_facts = [f for f in facts if isinstance(f, FactRelationshipHasSourceNode)]
    relationship_target_facts = [f for f in facts if isinstance(f, FactRelationshipHasTargetNode)]
    
    assert len(relationship_source_facts) == edge_count
    assert len(relationship_target_facts) == edge_count
    
    # Verify specific relationships
    # Find a relationship from 'a' to 'b'
    a_to_b_relationships = [
        rel_id for rel_id in [f.relationship_id for f in relationship_source_facts if f.node_id == "a"]
        if any(f.relationship_id == rel_id and f.node_id == "b" for f in relationship_target_facts)
    ]
    assert len(a_to_b_relationships) == 1
    
    # Find a relationship from 'f' to 'g'
    f_to_g_relationships = [
        rel_id for rel_id in [f.relationship_id for f in relationship_source_facts if f.node_id == "f"]
        if any(f.relationship_id == rel_id and f.node_id == "g" for f in relationship_target_facts)
    ]
    assert len(f_to_g_relationships) == 1
    
    # Verify total number of facts
    # Expected facts:
    # - 7 node label facts (one for each node)
    # - 14 node property facts (name and age for each node)
    # - edge_count relationship label facts
    # - edge_count relationship source facts
    # - edge_count relationship target facts
    expected_fact_count = 7 + 14 + (edge_count * 3)
    assert len(facts) == expected_fact_count


def test_networkx_shim_custom_node_labels():
    """
    Test that the NetworkX shim correctly handles custom node labels.
    """
    # Create a simple graph with custom node labels
    graph = nx.DiGraph()
    graph.add_node("1", labels=["Person", "Employee"])
    graph.add_node("2", labels=["Person", "Manager"])
    graph.add_edge("1", "2", label="REPORTS_TO")
    
    # Create the shim
    shim = NetworkX(graph)
    
    # Get the fact collection
    fact_collection = shim.to_fact_collection()
    
    # Get all facts as a list for easier testing
    facts = fact_collection.facts
    
    # Verify node label facts
    node_label_facts = [f for f in facts if isinstance(f, FactNodeHasLabel)]
    
    # Should have 4 label facts: 2 nodes with 2 labels each
    assert len(node_label_facts) == 4
    
    # Verify node 1 has both Person and Employee labels
    node1_labels = [f.label for f in node_label_facts if f.node_id == "1"]
    assert sorted(node1_labels) == ["Employee", "Person"]
    
    # Verify node 2 has both Person and Manager labels
    node2_labels = [f.label for f in node_label_facts if f.node_id == "2"]
    assert sorted(node2_labels) == ["Manager", "Person"]
    
    # Verify relationship label
    relationship_label_facts = [f for f in facts if isinstance(f, FactRelationshipHasLabel)]
    assert len(relationship_label_facts) == 1
    assert relationship_label_facts[0].label == "REPORTS_TO"
