#!/usr/bin/env python3
"""Script 6: Advanced Analytics — Sophisticated Graph Analytics.

Demonstrates analytical patterns that are difficult or impossible with
traditional SQL: multi-hop traversals, network analysis, temporal cohorts,
anomaly detection, and fraud-pattern recognition — all expressed naturally
in Cypher.

Run with:
    uv run python demos/data_scientist_showcase/06_advanced_analytics.py
"""

from __future__ import annotations

import os
import sys

import pandas as pd

# --- path setup so the script is runnable from the repo root ---
sys.path.insert(0, os.path.dirname(__file__))

from _common import done, section, setup_demo, show_count, show_result, timed
from data.generators import analytics_network
from pycypher import ContextBuilder, Star

# =========================================================================
# Data setup — a rich network with clusters, influence, and typed edges
# =========================================================================

def build_analytics_graph() -> Star:
    """Build an analytics-ready graph with nodes, edges, and metadata.

    The network models an organization where nodes collaborate, report to
    each other, and advise across clusters.
    """
    nodes, edges = analytics_network(n_nodes=30, n_edges=80)

    context = ContextBuilder.from_dict(
        {
            "Node": nodes,
            "CONNECTS": edges,
        }
    )
    return Star(context=context)


# =========================================================================
# 1. Multi-Hop Traversals
# =========================================================================

def demo_multi_hop(star: Star) -> None:
    """Explore paths beyond direct connections — impossible in flat SQL."""
    section("1. MULTI-HOP TRAVERSALS — Beyond direct connections")

    # Direct connections (1-hop)
    print("  1-hop: Who does each node connect to directly?")
    query_1hop = """
        MATCH (a:Node)-[:CONNECTS]->(b:Node)
        RETURN a.name AS source, b.name AS target, a.cluster AS src_cluster
    """
    with timed("1-hop query"):
        result = star.execute_query(query_1hop)
    show_count(result, label="  Direct connections")
    print()

    # 2-hop paths — find indirect connections
    print("  2-hop: Who is reachable through an intermediary?")
    query_2hop = """
        MATCH (a:Node)-[:CONNECTS]->(b:Node)-[:CONNECTS]->(c:Node)
        WHERE a.name <> c.name
        RETURN a.name AS source, b.name AS via, c.name AS reachable
    """
    with timed("2-hop query"):
        result = star.execute_query(query_2hop)
    show_count(result, label="  2-hop paths")
    show_result(result, label="  Sample 2-hop paths", max_rows=10)

    # 3-hop paths — deep network reach
    print("  3-hop: Deep network exploration")
    query_3hop = """
        MATCH (a:Node)-[:CONNECTS]->(b:Node)-[:CONNECTS]->(c:Node)-[:CONNECTS]->(d:Node)
        WHERE a.name <> d.name AND a.name <> c.name AND b.name <> d.name
        RETURN a.name AS origin, d.name AS distant, count(*) AS path_count
    """
    with timed("3-hop query"):
        result = star.execute_query(query_3hop)
    show_count(result, label="  Distinct 3-hop pairs")
    show_result(result, label="  Nodes with most 3-hop paths", max_rows=8)

    print("  Key insight: SQL would need self-joins per hop. Cypher scales")
    print("  to arbitrary depth with the same readable pattern syntax.")
    print()


# =========================================================================
# 2. Network Hub Detection
# =========================================================================

def demo_hub_detection(star: Star) -> None:
    """Find the most connected and influential nodes in the network."""
    section("2. NETWORK HUB DETECTION — Finding influential nodes")

    # Outgoing connection count (out-degree)
    print("  Out-degree: Who connects to the most others?")
    query_outdeg = """
        MATCH (n:Node)-[:CONNECTS]->(target:Node)
        RETURN n.name AS node, n.cluster AS cluster, count(target) AS out_degree
        ORDER BY out_degree DESC
    """
    with timed("Out-degree"):
        result = star.execute_query(query_outdeg)
    show_result(result, label="  Top connectors (by out-degree)", max_rows=10)

    # Incoming connection count (in-degree) — who is sought out?
    print("  In-degree: Who do others seek out?")
    query_indeg = """
        MATCH (source:Node)-[:CONNECTS]->(n:Node)
        RETURN n.name AS node, n.cluster AS cluster, count(source) AS in_degree
        ORDER BY in_degree DESC
    """
    with timed("In-degree"):
        result = star.execute_query(query_indeg)
    show_result(result, label="  Most sought-after nodes (by in-degree)", max_rows=10)

    # Combine with influence score for a richer picture
    print("  Influence-weighted: High-degree + high-influence nodes")
    query_weighted = """
        MATCH (n:Node)-[:CONNECTS]->(target:Node)
        RETURN
            n.name AS node,
            n.cluster AS cluster,
            n.influence AS influence,
            count(target) AS connections
        ORDER BY connections DESC
    """
    with timed("Weighted hubs"):
        result = star.execute_query(query_weighted)
    show_result(result, label="  Network hubs", max_rows=10)
    print()


# =========================================================================
# 3. Cross-Cluster Analysis
# =========================================================================

def demo_cross_cluster(star: Star) -> None:
    """Identify connections that bridge organizational clusters."""
    section("3. CROSS-CLUSTER ANALYSIS — Bridge connections")

    # Cross-cluster connections
    query = """
        MATCH (a:Node)-[:CONNECTS]->(b:Node)
        WHERE a.cluster <> b.cluster
        RETURN
            a.cluster AS from_cluster,
            b.cluster AS to_cluster,
            count(*) AS cross_links
        ORDER BY cross_links DESC
    """
    with timed("Cross-cluster"):
        result = star.execute_query(query)
    show_result(result, label="  Cross-cluster connection matrix")

    # Bridge nodes — connected to multiple clusters
    query_bridges = """
        MATCH (n:Node)-[:CONNECTS]->(target:Node)
        WHERE n.cluster <> target.cluster
        RETURN
            n.name AS bridge_node,
            n.cluster AS home_cluster,
            count(target) AS external_connections
        ORDER BY external_connections DESC
    """
    with timed("Bridge nodes"):
        result = star.execute_query(query_bridges)
    show_result(result, label="  Bridge nodes (most cross-cluster links)", max_rows=10)

    print("  Bridge nodes are critical for information flow between")
    print("  organizational silos — losing them fragments the network.")
    print()


# =========================================================================
# 4. Cluster Cohesion Analysis
# =========================================================================

def demo_cluster_cohesion(star: Star) -> None:
    """Measure how tightly connected each cluster is internally."""
    section("4. CLUSTER COHESION — Internal vs. external connectivity")

    # Internal cluster links
    query_internal = """
        MATCH (a:Node)-[:CONNECTS]->(b:Node)
        WHERE a.cluster = b.cluster
        RETURN a.cluster AS cluster, count(*) AS internal_links
    """
    with timed("Internal links"):
        internal = star.execute_query(query_internal)
    show_result(internal, label="  Internal connections per cluster")

    # Cluster sizes
    query_sizes = """
        MATCH (n:Node)
        RETURN n.cluster AS cluster, count(n) AS size
    """
    sizes = star.execute_query(query_sizes)
    show_result(sizes, label="  Cluster sizes")

    # Cluster influence distribution
    query_influence = """
        MATCH (n:Node)
        RETURN
            n.cluster AS cluster,
            count(n) AS members,
            avg(n.influence) AS avg_influence
    """
    with timed("Influence analysis"):
        result = star.execute_query(query_influence)
    show_result(result, label="  Influence distribution by cluster")
    print()


# =========================================================================
# 5. Anomaly Detection Patterns
# =========================================================================

def demo_anomaly_detection(star: Star) -> None:
    """Identify unusual patterns that may indicate problems or fraud."""
    section("5. ANOMALY DETECTION — Finding unusual patterns")

    # Nodes with high influence but low connectivity (potential bottlenecks)
    print("  Pattern 1: High-influence nodes with few connections")
    print("  (Potential single points of failure)")
    query_bottleneck = """
        MATCH (n:Node)-[:CONNECTS]->(target:Node)
        WHERE n.influence > 7.0
        RETURN
            n.name AS node,
            n.cluster AS cluster,
            n.influence AS influence,
            count(target) AS connections
        ORDER BY connections DESC
    """
    with timed("Bottleneck detection"):
        result = star.execute_query(query_bottleneck)
    show_result(result, label="  High-influence nodes and their connectivity")

    # Nodes that receive many connections but send few (information sinks)
    print("  Pattern 2: Asymmetric connectivity (information sinks)")
    # Compare in-degree vs out-degree per node
    query_indeg = """
        MATCH (source:Node)-[:CONNECTS]->(n:Node)
        RETURN n.name AS node, n.cluster AS cluster, count(source) AS in_degree
        ORDER BY in_degree DESC
    """
    query_outdeg = """
        MATCH (n:Node)-[:CONNECTS]->(target:Node)
        RETURN n.name AS node, count(target) AS out_degree
        ORDER BY out_degree DESC
    """
    with timed("Asymmetry detection"):
        in_df = star.execute_query(query_indeg)
        out_df = star.execute_query(query_outdeg)
        # Merge for comparison
        merged = in_df.merge(out_df, on="node", how="left").fillna(0)
        merged["out_degree"] = merged["out_degree"].astype(int)
        merged["asymmetry"] = merged["in_degree"] - merged["out_degree"]
        merged = merged.sort_values("asymmetry", ascending=False)
    show_result(merged, label="  Connection asymmetry (in - out)", max_rows=10)

    # Inactive nodes that are still heavily connected (stale references)
    print("  Pattern 3: Inactive nodes with active connections")
    query_stale = """
        MATCH (source:Node)-[:CONNECTS]->(n:Node)
        WHERE n.active = false
        RETURN
            n.name AS inactive_node,
            n.cluster AS cluster,
            count(source) AS incoming_links
        ORDER BY incoming_links DESC
    """
    with timed("Stale reference detection"):
        result = star.execute_query(query_stale)
    show_result(result, label="  Inactive nodes still receiving connections")
    print()


# =========================================================================
# 6. Aggregation Pipelines
# =========================================================================

def demo_aggregation_pipelines(star: Star) -> None:
    """Chain WITH clauses for multi-stage analytical pipelines."""
    section("6. AGGREGATION PIPELINES — Multi-stage WITH chains")

    # Two-stage pipeline: aggregate, then filter aggregates
    print("  Stage 1: Count connections per node")
    print("  Stage 2: Filter to only high-connectivity nodes")
    query = """
        MATCH (n:Node)-[:CONNECTS]->(target:Node)
        WITH n.name AS node, n.cluster AS cluster, count(target) AS degree
        WHERE degree >= 3
        RETURN node, cluster, degree
        ORDER BY degree DESC
    """
    with timed("Two-stage pipeline"):
        result = star.execute_query(query)
    show_result(result, label="  High-connectivity nodes (degree >= 3)")

    # Three-stage pipeline: traverse → aggregate → rank
    print("  Three-stage pipeline: traverse → aggregate → rank")
    query_3stage = """
        MATCH (a:Node)-[:CONNECTS]->(b:Node)-[:CONNECTS]->(c:Node)
        WHERE a.name <> c.name
        WITH a.name AS origin, count(DISTINCT c.name) AS reach
        WHERE reach >= 5
        RETURN origin, reach
        ORDER BY reach DESC
    """
    with timed("Three-stage pipeline"):
        result = star.execute_query(query_3stage)
    show_result(result, label="  Nodes with broadest 2-hop reach")

    print("  WITH clauses let you build SQL-like CTEs directly in Cypher,")
    print("  but the graph traversal step is something SQL can't express.")
    print()


# =========================================================================
# 7. Connection Pattern Analysis
# =========================================================================

def demo_connection_patterns(star: Star) -> None:
    """Analyze the types and strengths of connections in the network."""
    section("7. CONNECTION PATTERNS — Edge type and strength analysis")

    # Connection type distribution
    query_types = """
        MATCH (a:Node)-[c:CONNECTS]->(b:Node)
        RETURN c.type AS connection_type, count(*) AS frequency
        ORDER BY frequency DESC
    """
    with timed("Type distribution"):
        result = star.execute_query(query_types)
    show_result(result, label="  Connection type distribution")

    # Strong connections across clusters
    query_strong = """
        MATCH (a:Node)-[c:CONNECTS]->(b:Node)
        WHERE c.strength > 0.7 AND a.cluster <> b.cluster
        RETURN
            a.name AS from_node,
            b.name AS to_node,
            a.cluster AS from_cluster,
            b.cluster AS to_cluster,
            c.strength AS strength
        ORDER BY c.strength DESC
    """
    with timed("Strong cross-cluster"):
        result = star.execute_query(query_strong)
    show_result(result, label="  Strong cross-cluster connections (strength > 0.7)", max_rows=10)

    # Average connection strength by cluster pair
    query_avg = """
        MATCH (a:Node)-[c:CONNECTS]->(b:Node)
        RETURN
            a.cluster AS from_cluster,
            b.cluster AS to_cluster,
            avg(c.strength) AS avg_strength,
            count(*) AS num_connections
        ORDER BY avg_strength DESC
    """
    with timed("Avg strength by cluster"):
        result = star.execute_query(query_avg)
    show_result(result, label="  Average connection strength between clusters")
    print()


# =========================================================================
# 8. Fraud Detection Patterns
# =========================================================================

def demo_fraud_patterns(star: Star) -> None:
    """Demonstrate graph patterns commonly used in fraud detection."""
    section("8. FRAUD DETECTION PATTERNS — Suspicious network structures")

    # Circular reference detection (A→B→C→A)
    print("  Pattern 1: Circular references (potential collusion)")
    query_circles = """
        MATCH (a:Node)-[:CONNECTS]->(b:Node)-[:CONNECTS]->(c:Node)-[:CONNECTS]->(a)
        WHERE a.name < b.name AND b.name < c.name
        RETURN a.name AS node_a, b.name AS node_b, c.name AS node_c
    """
    with timed("Triangle detection"):
        result = star.execute_query(query_circles)
    show_count(result, label="  Triangles found")
    show_result(result, label="  Circular reference triangles", max_rows=10)

    # Nodes connecting to many in the same cluster they don't belong to
    # (potential infiltration pattern)
    print("  Pattern 2: Concentrated external targeting")
    query_targeting = """
        MATCH (n:Node)-[:CONNECTS]->(target:Node)
        WHERE n.cluster <> target.cluster
        WITH n, target.cluster AS target_cluster, count(target) AS links
        WHERE links >= 2
        RETURN
            n.name AS node,
            n.cluster AS home_cluster,
            target_cluster,
            links AS concentrated_links
        ORDER BY concentrated_links DESC
    """
    with timed("Targeting detection"):
        result = star.execute_query(query_targeting)
    show_result(result, label="  Concentrated external targeting", max_rows=10)

    print()
    print("  In real fraud systems, these Cypher patterns replace hundreds")
    print("  of lines of procedural code and self-join SQL queries.")
    print()


# =========================================================================
# 9. Comparative Network Summary
# =========================================================================

def demo_network_summary(star: Star) -> None:
    """Produce a high-level network health summary."""
    section("9. NETWORK SUMMARY — High-level health metrics")

    # Total nodes and edges
    node_count = star.execute_query("MATCH (n:Node) RETURN count(n) AS cnt")
    edge_count = star.execute_query("MATCH ()-[c:CONNECTS]->() RETURN count(c) AS cnt")

    n_nodes = node_count.iloc[0]["cnt"] if len(node_count) > 0 else 0
    n_edges = edge_count.iloc[0]["cnt"] if len(edge_count) > 0 else 0

    print(f"  Nodes:       {n_nodes}")
    print(f"  Edges:       {n_edges}")
    if n_nodes > 0:
        print(f"  Density:     {n_edges / n_nodes:.1f} edges/node")
    print()

    # Active vs inactive
    query_active = """
        MATCH (n:Node)
        RETURN n.active AS active, count(n) AS cnt
    """
    result = star.execute_query(query_active)
    show_result(result, label="  Active vs. inactive nodes")

    # Cluster summary
    query_summary = """
        MATCH (n:Node)
        RETURN
            n.cluster AS cluster,
            count(n) AS members,
            avg(n.influence) AS avg_influence
        ORDER BY members DESC
    """
    result = star.execute_query(query_summary)
    show_result(result, label="  Cluster summary")
    print()


# =========================================================================
# Main
# =========================================================================

def main() -> None:
    setup_demo("Script 6: Advanced Analytics — Sophisticated Graph Analytics")

    print("These analytical patterns are difficult or impossible with")
    print("traditional SQL. Cypher expresses complex graph relationships")
    print("that would require recursive CTEs, self-joins, or procedural code.")
    print()

    star = build_analytics_graph()

    node_count = star.execute_query("MATCH (n:Node) RETURN count(n) AS cnt")
    edge_count = star.execute_query("MATCH ()-[c:CONNECTS]->() RETURN count(c) AS cnt")
    print(f"Graph loaded: {node_count.iloc[0]['cnt']} nodes, "
          f"{edge_count.iloc[0]['cnt']} edges\n")

    demo_multi_hop(star)
    demo_hub_detection(star)
    demo_cross_cluster(star)
    demo_cluster_cohesion(star)
    demo_anomaly_detection(star)
    demo_aggregation_pipelines(star)
    demo_connection_patterns(star)
    demo_fraud_patterns(star)
    demo_network_summary(star)

    # Closing message
    section("WHAT YOU'VE SEEN")
    print()
    capabilities = [
        ("Multi-hop traversals", "Paths of arbitrary depth, naturally expressed"),
        ("Hub detection", "Degree centrality with influence weighting"),
        ("Cross-cluster analysis", "Bridge nodes and information flow"),
        ("Cluster cohesion", "Internal vs. external connectivity metrics"),
        ("Anomaly detection", "Bottlenecks, sinks, and stale references"),
        ("Aggregation pipelines", "Multi-stage WITH chains for complex analytics"),
        ("Connection patterns", "Edge type/strength analysis across clusters"),
        ("Fraud patterns", "Triangles, targeting, and collusion detection"),
        ("Network summaries", "High-level health metrics in a single query"),
    ]
    for capability, description in capabilities:
        print(f"  • {capability:30s}  {description}")

    print()
    print("  All of these patterns read naturally in Cypher but would require")
    print("  complex SQL with recursive CTEs, self-joins, or procedural code.")
    print()
    done()


if __name__ == "__main__":
    main()
