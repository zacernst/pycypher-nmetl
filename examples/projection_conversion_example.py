"""
Example: Converting SAT solver solutions to ProjectionList objects.

This example demonstrates how to use CypherQuerySolver to convert
Cypher queries into SAT problems, solve them, and convert the results
into ProjectionList objects for integration with the pycypher system.
"""

import sys
from pathlib import Path

# Add the pycypher package to path
sys.path.insert(
    0, str(Path(__file__).parent.parent / "packages" / "pycypher" / "src")
)

from pycypher.fact_collection.foundationdb import FoundationDBFactCollection
from pycypher.fact_collection.solver import CypherQuerySolver


def example_basic_projection_conversion():
    """
    Example 1: Basic conversion from solutions to ProjectionList.

    Shows the simplest way to convert all SAT solutions to a ProjectionList.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 1: Basic ProjectionList Conversion")
    print("=" * 70)

    # Initialize
    fact_collection = FoundationDBFactCollection(
        foundationdb_cluster_file="/pycypher-nmetl/fdb.cluster",
    )
    solver = CypherQuerySolver(fact_collection)

    # Query
    query = "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n, r, m"
    print(f"\nQuery: {query}")

    # Convert to ProjectionList
    projection_list = solver.solutions_to_projection_list(query)

    print(f"\nFound {len(projection_list)} solutions")

    # Display solutions
    for i, projection in enumerate(projection_list[:5]):  # Show first 5
        print(f"\nSolution {i + 1}:")
        print(f"  {projection.pythonify()}")


def example_filtered_projection():
    """
    Example 2: Filtered ProjectionList matching RETURN clause.

    Shows how to get only the variables specified in the RETURN clause,
    filtering out internal variables used during matching.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 2: Filtered ProjectionList")
    print("=" * 70)

    fact_collection = FoundationDBFactCollection(
        foundationdb_cluster_file="/pycypher-nmetl/fdb.cluster",
    )
    solver = CypherQuerySolver(fact_collection)

    # Query with RETURN limiting variables
    query = "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n, m"
    print(f"\nQuery: {query}")
    print("Note: RETURN clause only mentions 'n' and 'm', not 'r'")

    # Get filtered projection list
    projection_list = solver.solutions_to_projection_list_filtered(query)

    print(f"\nFound {len(projection_list)} solutions")
    print("Each solution only contains variables from RETURN clause:\n")

    for i, projection in enumerate(projection_list[:5]):
        result = projection.pythonify()
        print(f"  Solution {i + 1}: {result}")
        print(f"    Variables: {list(result.keys())}")


def example_manual_filtering():
    """
    Example 3: Manually specify which variables to include.

    Shows how to explicitly control which variables appear in the result,
    regardless of the RETURN clause.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 3: Manual Variable Filtering")
    print("=" * 70)

    fact_collection = FoundationDBFactCollection(
        foundationdb_cluster_file="/pycypher-nmetl/fdb.cluster",
    )
    solver = CypherQuerySolver(fact_collection)

    query = "MATCH (n:Person)-[r:KNOWS]->(m:Person)-[s:LIVES_IN]->(c:City) RETURN n, m, c"
    print(f"\nQuery: {query}")

    # Get only specific variables
    print("\nGetting only 'n' and 'c' (skipping 'm'):")
    projection_list = solver.solutions_to_projection_list_filtered(
        query,
        return_variables=["n", "c"],
    )

    for i, projection in enumerate(projection_list[:3]):
        print(f"  Solution {i + 1}: {projection.pythonify()}")


def example_working_with_projections():
    """
    Example 4: Working with Projection objects.

    Shows various operations you can perform with Projection and ProjectionList.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 4: Working with Projection Objects")
    print("=" * 70)

    fact_collection = FoundationDBFactCollection(
        foundationdb_cluster_file="/pycypher-nmetl/fdb.cluster",
    )
    solver = CypherQuerySolver(fact_collection)

    query = "MATCH (n:Person) RETURN n"
    projection_list = solver.solutions_to_projection_list(query)

    if len(projection_list) > 0:
        # Access individual projections
        first = projection_list[0]
        print(f"\nFirst projection: {first.pythonify()}")

        # Check if variable exists
        if "n" in first:
            print(f"Variable 'n' = {first['n']}")

        # Iterate over key-value pairs
        print("\nAll variables in first projection:")
        for key, value in first.items():
            print(f"  {key}: {value}")

        # Get length
        print(f"\nNumber of variables: {len(first)}")

        # Convert to plain dict
        plain_dict = first.pythonify()
        print(f"As dictionary: {plain_dict}")
        print(f"Dictionary type: {type(plain_dict)}")


def example_iterating_solutions():
    """
    Example 5: Iterating solutions without storing all in memory.

    Shows how to process solutions one at a time without creating
    a full ProjectionList, useful for large result sets.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 5: Iterating Solutions (Memory Efficient)")
    print("=" * 70)

    fact_collection = FoundationDBFactCollection(
        foundationdb_cluster_file="/pycypher-nmetl/fdb.cluster",
    )
    solver = CypherQuerySolver(fact_collection)

    query = "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n, m"
    print(f"\nQuery: {query}")
    print("\nProcessing solutions one at a time:\n")

    count = 0
    for solution_constraints in solver.solutions(query):
        # Convert each solution to Projection individually
        projection = solver.solution_to_projection(solution_constraints)

        count += 1
        if count <= 5:  # Only print first 5
            print(f"  Solution {count}: {projection.pythonify()}")

        # You could process, filter, or save each solution here
        # without keeping all solutions in memory

    print(f"\nTotal solutions processed: {count}")


def example_comparison():
    """
    Example 6: Comparing different conversion approaches.

    Shows the difference between various conversion methods and when to use each.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 6: Comparing Conversion Approaches")
    print("=" * 70)

    fact_collection = FoundationDBFactCollection(
        foundationdb_cluster_file="/pycypher-nmetl/fdb.cluster",
    )
    solver = CypherQuerySolver(fact_collection)

    query = "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n, m"

    # Approach 1: Full projection (all variables)
    print("\n1. Full projections (all variables):")
    full_list = solver.solutions_to_projection_list(query)
    if len(full_list) > 0:
        print(f"   Example: {full_list[0].pythonify()}")
        print(f"   Variables: {list(full_list[0].pythonify().keys())}")

    # Approach 2: Filtered projection (RETURN variables only)
    print("\n2. Filtered projections (RETURN clause only):")
    filtered_list = solver.solutions_to_projection_list_filtered(query)
    if len(filtered_list) > 0:
        print(f"   Example: {filtered_list[0].pythonify()}")
        print(f"   Variables: {list(filtered_list[0].pythonify().keys())}")

    # Approach 3: Raw constraints
    print("\n3. Raw SAT constraints:")
    for i, solution_constraints in enumerate(solver.solutions(query)):
        if i == 0:  # Just show first
            print(f"   Number of constraints: {len(solution_constraints)}")
            print(f"   First constraint: {solution_constraints[0]}")
        break

    print("\nRecommendation:")
    print("  - Use approach 2 (filtered) for final query results")
    print("  - Use approach 1 (full) when you need all intermediate variables")
    print("  - Use approach 3 (raw) when integrating with custom SAT logic")


if __name__ == "__main__":
    """Run all examples."""

    examples = [
        ("Basic Projection Conversion", example_basic_projection_conversion),
        ("Filtered Projection", example_filtered_projection),
        ("Manual Variable Filtering", example_manual_filtering),
        ("Working with Projections", example_working_with_projections),
        ("Iterating Solutions", example_iterating_solutions),
        ("Comparison of Approaches", example_comparison),
    ]

    print("\n" + "=" * 70)
    print("ProjectionList Conversion Examples")
    print("=" * 70)

    for name, example_func in examples:
        try:
            example_func()
        except Exception as e:
            print(f"\n✗ Example '{name}' failed: {e}")
            import traceback

            traceback.print_exc()

    print("\n" + "=" * 70)
    print("Examples completed")
    print("=" * 70)
