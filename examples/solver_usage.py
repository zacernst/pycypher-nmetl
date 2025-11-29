"""
Example usage of CypherQuerySolver with different SAT solver integrations.

This module demonstrates various ways to use the CypherQuerySolver to convert
Cypher graph queries into SAT problems and solve them with different backends.
"""

import sys
from pathlib import Path

# Add the pycypher package to path
sys.path.insert(0, str(Path(__file__).parent.parent / "packages" / "pycypher" / "src"))

from pycypher.fact_collection.solver import CypherQuerySolver, cypher_query_to_cnf
from pycypher.fact_collection.foundationdb import FoundationDBFactCollection


def example_1_basic_cnf():
    """
    Example 1: Basic CNF generation and inspection.
    
    This shows how to convert a Cypher query to CNF without solving it.
    Useful for understanding the constraint structure or implementing
    custom solving logic.
    """
    print("\n" + "="*70)
    print("EXAMPLE 1: Basic CNF Generation")
    print("="*70)
    
    # Initialize fact collection
    fact_collection = FoundationDBFactCollection(
        foundationdb_cluster_file='/pycypher-nmetl/fdb.cluster'
    )
    
    # Define query
    query = "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n, m"
    
    # Solve to get CNF
    cnf = cypher_query_to_cnf(query, fact_collection)
    
    print(f"\nQuery: {query}")
    print(f"Number of constraints: {len(cnf.constraints)}")
    print(f"CNF structure: {cnf}")
    
    # Walk through all constraints
    constraint_count = sum(1 for _ in cnf.walk())
    print(f"Total constraints (including nested): {constraint_count}")


def example_2_dimacs_file():
    """
    Example 2: Generate DIMACS file for external SAT solvers.
    
    This shows how to export the problem to DIMACS CNF format
    which can be used with external solvers like MiniSat, CryptoMiniSat,
    or Glucose.
    """
    print("\n" + "="*70)
    print("EXAMPLE 2: DIMACS File Generation")
    print("="*70)
    
    fact_collection = FoundationDBFactCollection(
        foundationdb_cluster_file='/pycypher-nmetl/fdb.cluster'
    )
    
    solver = CypherQuerySolver(fact_collection)
    query = "MATCH (n:City)-[r:IN]->(s:State) RETURN n, s"
    
    cnf = solver.solve_query(query)
    dimacs = solver.to_dimacs(cnf)
    
    # Save to file
    output_file = "/tmp/cypher_query.cnf"
    with open(output_file, 'w') as f:
        f.write(dimacs)
    
    print(f"\nDIMACS file saved to: {output_file}")
    print("\nTo solve with external solver:")
    print(f"  minisat {output_file} /tmp/solution.txt")
    print("  # Exit code 10 = SAT, 20 = UNSAT")
    
    print("\nDIMACS content preview:")
    print(dimacs[:500] + "..." if len(dimacs) > 500 else dimacs)


def example_3_python_sat():
    """
    Example 3: Solve with python-sat library (Recommended).
    
    This is the recommended approach for Python applications.
    python-sat provides multiple solver backends and is easy to use.
    """
    print("\n" + "="*70)
    print("EXAMPLE 3: python-sat Library (Recommended)")
    print("="*70)
    
    try:
        from pysat.solvers import Glucose3, Minisat22
    except ImportError:
        print("ERROR: python-sat not installed")
        print("Install with: pip install python-sat")
        return
    
    fact_collection = FoundationDBFactCollection(
        foundationdb_cluster_file='/pycypher-nmetl/fdb.cluster'
    )
    
    solver = CypherQuerySolver(fact_collection)
    query = "MATCH (n:Person) RETURN n"
    
    # Solve query and get clauses
    cnf = solver.solve_query(query)
    clauses, reverse_map, forward_map = solver.get_clauses(cnf)
    
    print(f"\nQuery: {query}")
    print(f"Variables: {len(forward_map)}")
    print(f"Clauses: {len(clauses)}")
    
    # Solve with Glucose3
    print("\nSolving with Glucose3...")
    with Glucose3() as sat:
        for clause in clauses:
            sat.add_clause(clause)
        
        if sat.solve():
            print("✓ SAT - Solution found!")
            model = sat.get_model()
            
            # Interpret the solution
            print("\nVariable assignments:")
            for var_id in model:
                if var_id > 0:
                    constraint = reverse_map[var_id]
                    print(f"  {constraint}")
        else:
            print("✗ UNSAT - No solution exists")
    
    # Try alternative solver
    print("\nSolving with Minisat22...")
    with Minisat22() as sat:
        for clause in clauses:
            sat.add_clause(clause)
        
        if sat.solve():
            print("✓ SAT - Solution confirmed with alternative solver")
        else:
            print("✗ UNSAT")


def example_4_pycosat():
    """
    Example 4: Solve with pycosat (Fast C-based solver).
    
    pycosat is a Python binding to PicoSAT, a very fast C-based SAT solver.
    Good for performance-critical applications.
    """
    print("\n" + "="*70)
    print("EXAMPLE 4: pycosat (Fast C-based)")
    print("="*70)
    
    try:
        import pycosat
    except ImportError:
        print("ERROR: pycosat not installed")
        print("Install with: pip install pycosat")
        return
    
    fact_collection = FoundationDBFactCollection(
        foundationdb_cluster_file='/pycypher-nmetl/fdb.cluster'
    )
    
    solver = CypherQuerySolver(fact_collection)
    query = "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n, m"
    
    # Solve query and get clauses
    cnf = solver.solve_query(query)
    clauses, reverse_map, _ = solver.get_clauses(cnf)
    
    print(f"\nQuery: {query}")
    print(f"Clauses: {len(clauses)}")
    
    # Solve with pycosat
    print("\nSolving with pycosat...")
    solution = pycosat.solve(clauses)
    
    if solution != "UNSAT":
        print("✓ SAT - Solution found!")
        
        print("\nTrue assignments:")
        for var_id in solution:
            if var_id > 0:
                constraint = reverse_map[var_id]
                print(f"  {constraint}")
        
        # Verify solution
        print("\nVerifying solution...")
        valid = all(
            any(lit in solution for lit in clause)
            for clause in clauses
        )
        print(f"Solution valid: {valid}")
    else:
        print("✗ UNSAT - No solution exists")


def example_5_iterative_solving():
    """
    Example 5: Iterative solving with additional constraints.
    
    This shows how to add constraints incrementally and resolve.
    Useful for interactive query refinement or constraint-based optimization.
    """
    print("\n" + "="*70)
    print("EXAMPLE 5: Iterative Solving")
    print("="*70)
    
    try:
        from pysat.solvers import Glucose3
    except ImportError:
        print("ERROR: python-sat not installed")
        return
    
    fact_collection = FoundationDBFactCollection(
        foundationdb_cluster_file='/pycypher-nmetl/fdb.cluster'
    )
    
    solver = CypherQuerySolver(fact_collection)
    
    # Start with base query
    query1 = "MATCH (n:Person) RETURN n"
    cnf1 = solver.solve_query(query1)
    clauses1, reverse_map1, _ = solver.get_clauses(cnf1)
    
    print(f"\nBase query: {query1}")
    print(f"Initial clauses: {len(clauses1)}")
    
    with Glucose3() as sat:
        for clause in clauses1:
            sat.add_clause(clause)
        
        if sat.solve():
            print("✓ Base query is satisfiable")
            solution_count = 1
            
            # Find multiple solutions
            print("\nFinding alternative solutions...")
            model = sat.get_model()
            
            # Block this solution and find another
            blocking_clause = [-lit for lit in model if lit > 0]
            sat.add_clause(blocking_clause)
            
            if sat.solve():
                print("✓ Alternative solution exists")
                solution_count += 1
            
            print(f"\nFound {solution_count} solution(s)")


def example_6_solution_interpretation():
    """
    Example 6: Detailed solution interpretation.
    
    This shows how to map SAT solver solutions back to the original
    Cypher query variables and their assignments.
    """
    print("\n" + "="*70)
    print("EXAMPLE 6: Solution Interpretation")
    print("="*70)
    
    try:
        from pysat.solvers import Glucose3
    except ImportError:
        print("ERROR: python-sat not installed")
        return
    
    from pycypher.fact_collection.solver import VariableAssignedToNode, VariableAssignedToRelationship
    
    fact_collection = FoundationDBFactCollection(
        foundationdb_cluster_file='/pycypher-nmetl/fdb.cluster'
    )
    
    solver = CypherQuerySolver(fact_collection)
    query = "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n, m"
    
    cnf = solver.solve_query(query)
    clauses, reverse_map, _ = solver.get_clauses(cnf)
    
    print(f"\nQuery: {query}")
    
    with Glucose3() as sat:
        for clause in clauses:
            sat.add_clause(clause)
        
        if sat.solve():
            print("✓ Solution found\n")
            model = sat.get_model()
            
            # Organize by variable
            node_assignments = {}
            relationship_assignments = {}
            
            for var_id in model:
                if var_id > 0:
                    constraint = reverse_map[var_id]
                    
                    if isinstance(constraint, VariableAssignedToNode):
                        node_assignments[constraint.variable] = constraint.node_id
                    elif isinstance(constraint, VariableAssignedToRelationship):
                        relationship_assignments[constraint.variable] = constraint.relationship_id
            
            print("Node Assignments:")
            for var, node_id in node_assignments.items():
                print(f"  {var} = {node_id}")
            
            print("\nRelationship Assignments:")
            for var, rel_id in relationship_assignments.items():
                print(f"  {var} = {rel_id}")
            
            print("\nQuery Result:")
            print(f"  Pattern (n:Person)-[r:KNOWS]->(m:Person)")
            print(f"  n = {node_assignments.get('n', 'N/A')}")
            print(f"  r = {relationship_assignments.get('r', 'N/A')}")
            print(f"  m = {node_assignments.get('m', 'N/A')}")


if __name__ == '__main__':
    """Run all examples."""
    
    print("\n" + "="*70)
    print("CypherQuerySolver - Complete Usage Examples")
    print("="*70)
    
    examples = [
        ("Basic CNF Generation", example_1_basic_cnf),
        ("DIMACS File Export", example_2_dimacs_file),
        ("python-sat Integration", example_3_python_sat),
        ("pycosat Integration", example_4_pycosat),
        ("Iterative Solving", example_5_iterative_solving),
        ("Solution Interpretation", example_6_solution_interpretation),
    ]
    
    for name, example_func in examples:
        try:
            example_func()
        except Exception as e:
            print(f"\n✗ Example '{name}' failed: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "="*70)
    print("Examples completed")
    print("="*70)
