"""Demonstrate multi-query composition with dependency analysis.

Shows how :class:`~pycypher.multi_query_rewriter.MultiQueryRewriter`
analyses inter-query dependencies and combines multiple Cypher queries
into a single executable query.

Usage::

    uv run python examples/multi_query_composition.py

"""

from __future__ import annotations

from pycypher.multi_query_rewriter import MultiQueryRewriter


def main() -> None:
    """Run the multi-query composition example."""
    queries = [
        (
            "extract_customers",
            (
                "MATCH (p:Person) "
                "WHERE p.age >= 18 "
                "CREATE (c:Customer {id: p.id, name: p.name, age: p.age})"
            ),
        ),
        (
            "customer_metrics",
            (
                "MATCH (c:Customer), (o:Order) "
                "WHERE c.id = o.customer_id "
                "WITH c.id AS customer_id, sum(o.amount) AS total_spent "
                "CREATE (m:Metrics {customer_id: customer_id, "
                "total_spent: total_spent}) "
                "RETURN customer_id, total_spent"
            ),
        ),
    ]

    rewriter = MultiQueryRewriter()
    dependency_graph = rewriter.analyze_dependencies(queries)

    print("=== Dependency Analysis ===")  # noqa: T201
    for node in dependency_graph.nodes:
        print(f"Query: {node.query_id}")  # noqa: T201
        print(f"  Produces: {node.produces}")  # noqa: T201
        print(f"  Consumes: {node.consumes}")  # noqa: T201
        print(f"  Dependencies: {node.dependencies}")  # noqa: T201
        print()  # noqa: T201

    execution_order = dependency_graph.topological_sort()
    print(  # noqa: T201
        "Execution order:",
        [node.query_id for node in execution_order],
    )

    # NOTE: Actual execution requires a Star instance:
    # result = rewriter.execute_combined(queries, star)


if __name__ == "__main__":
    main()
