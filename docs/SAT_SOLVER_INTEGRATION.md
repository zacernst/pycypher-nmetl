# SAT Solver Integration Guide

This guide explains how to use the `CypherQuerySolver` to convert Cypher graph queries into boolean constraint satisfaction problems and solve them using SAT solvers.

## Overview

The `CypherQuerySolver` converts Cypher graph pattern matching queries into Conjunctive Normal Form (CNF) boolean constraints that can be solved using standard SAT solvers. This enables:

- Finding valid variable assignments for graph patterns
- Verifying query satisfiability
- Discovering all possible solutions
- Integrating with constraint-based optimization

## Quick Start

```python
from pycypher.fact_collection.solver import CypherQuerySolver
from pycypher.fact_collection.foundationdb import FoundationDBFactCollection

# Initialize
fact_collection = FoundationDBFactCollection(
    foundationdb_cluster_file='/path/to/fdb.cluster'
)
solver = CypherQuerySolver(fact_collection)

# Solve a query
cnf = solver.solve_query("MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n, m")

# Get clauses for SAT solver
clauses, reverse_map, forward_map = solver.get_clauses(cnf)
```

## Integration Methods

### Method 1: python-sat (Recommended)

**Best for**: Python applications, multiple solver backends, ease of use

```python
from pysat.solvers import Glucose3

# Get clauses
cnf = solver.solve_query(query)
clauses, reverse_map, forward_map = solver.get_clauses(cnf)

# Solve
with Glucose3() as sat:
    for clause in clauses:
        sat.add_clause(clause)
    
    if sat.solve():
        model = sat.get_model()
        # Interpret solution
        for var_id in model:
            if var_id > 0:
                constraint = reverse_map[var_id]
                print(f"True: {constraint}")
```

**Installation**: `pip install python-sat`

**Available Solvers**:
- `Glucose3` - Fast, good for most problems
- `Minisat22` - Classic, reliable
- `Cadical` - Modern, very fast
- `Lingeling` - Excellent for large problems
- `Maplesat` - Portfolio solver

### Method 2: pycosat (Fastest)

**Best for**: Maximum performance, C-based speed

```python
import pycosat

cnf = solver.solve_query(query)
clauses, reverse_map, _ = solver.get_clauses(cnf)

solution = pycosat.solve(clauses)
if solution != "UNSAT":
    for var_id in solution:
        if var_id > 0:
            print(f"True: {reverse_map[var_id]}")
```

**Installation**: `pip install pycosat`

**Performance**: Typically 2-5x faster than pure Python solvers for large problems.

### Method 3: DIMACS File Export

**Best for**: External tools, integration with other systems, debugging

```python
cnf = solver.solve_query(query)
dimacs = solver.to_dimacs(cnf)

# Save to file
with open('problem.cnf', 'w') as f:
    f.write(dimacs)

# Solve with external solver
import subprocess
result = subprocess.run(['minisat', 'problem.cnf', 'solution.txt'])

if result.returncode == 10:  # SAT
    print("Solution found in solution.txt")
elif result.returncode == 20:  # UNSAT
    print("No solution exists")
```

**Compatible Solvers**:
- MiniSat
- CryptoMiniSat
- Glucose
- Z3 (also supports SMT)
- Kissat
- CaDiCaL

## API Reference

### CypherQuerySolver

Main class for query solving.

#### `__init__(fact_collection: FoundationDBFactCollection)`

Initialize the solver with a fact collection.

#### `solve_query(query: str) -> Conjunction`

Convert a Cypher query to CNF constraints.

**Parameters**:
- `query`: Cypher query string

**Returns**: `Conjunction` object in CNF form

**Example**:
```python
cnf = solver.solve_query("MATCH (n:Person) RETURN n")
```

#### `get_clauses(cnf: Conjunction) -> tuple[list[list[int]], dict, dict]`

Extract clauses and mappings for SAT solver input.

**Parameters**:
- `cnf`: Conjunction from `solve_query()`

**Returns**: Tuple of:
- `clauses`: List of integer lists (SAT clauses)
- `reverse_map`: Dict mapping variable IDs to constraints
- `forward_map`: Dict mapping constraints to variable IDs

**Example**:
```python
clauses, reverse_map, forward_map = solver.get_clauses(cnf)
```

#### `to_dimacs(cnf: Conjunction) -> str`

Convert CNF to DIMACS format.

**Parameters**:
- `cnf`: Conjunction from `solve_query()`

**Returns**: String in DIMACS CNF format

**Example**:
```python
dimacs = solver.to_dimacs(cnf)
```

### Helper Function

#### `cypher_query_to_cnf(query: str, fact_collection) -> Conjunction`

Convenience function for one-step conversion.

```python
from pycypher.fact_collection.solver import cypher_query_to_cnf

cnf = cypher_query_to_cnf(query, fact_collection)
```

## Advanced Usage

### Finding Multiple Solutions

```python
from pysat.solvers import Glucose3

cnf = solver.solve_query(query)
clauses, reverse_map, _ = solver.get_clauses(cnf)

solutions = []
with Glucose3() as sat:
    for clause in clauses:
        sat.add_clause(clause)
    
    while sat.solve():
        model = sat.get_model()
        solutions.append(model)
        
        # Block this solution
        blocking_clause = [-lit for lit in model if lit > 0]
        sat.add_clause(blocking_clause)
    
print(f"Found {len(solutions)} solutions")
```

### Interpreting Solutions

```python
from pycypher.fact_collection.solver import (
    VariableAssignedToNode, 
    VariableAssignedToRelationship
)

# After solving...
node_assignments = {}
relationship_assignments = {}

for var_id in model:
    if var_id > 0:
        constraint = reverse_map[var_id]
        
        if isinstance(constraint, VariableAssignedToNode):
            node_assignments[constraint.variable] = constraint.node_id
        elif isinstance(constraint, VariableAssignedToRelationship):
            relationship_assignments[constraint.variable] = constraint.relationship_id

print("Nodes:", node_assignments)
print("Relationships:", relationship_assignments)
```

### Incremental Solving

```python
from pysat.solvers import Glucose3

cnf = solver.solve_query(base_query)
clauses, _, _ = solver.get_clauses(cnf)

with Glucose3() as sat:
    # Add base constraints
    for clause in clauses:
        sat.add_clause(clause)
    
    # Add additional constraints incrementally
    sat.add_clause([1, 2])  # Add custom constraint
    
    if sat.solve():
        print("Still satisfiable")
```

### Performance Optimization

For large queries:

1. **Use pycosat** for maximum speed
2. **Enable solver assumptions** (python-sat only):
   ```python
   with Glucose3() as sat:
       for clause in clauses:
           sat.add_clause(clause)
       
       # Test with assumptions
       if sat.solve(assumptions=[1, -2, 3]):
           print("Satisfiable under assumptions")
   ```

3. **Use iterative solving** to reuse solver state:
   ```python
   with Glucose3() as sat:
       for clause in clauses:
           sat.add_clause(clause)
       
       # Solve multiple times with different assumptions
       for assumption_set in assumption_sets:
           if sat.solve(assumptions=assumption_set):
               print(f"SAT: {assumption_set}")
   ```

## Troubleshooting

### UNSAT Results

If a query returns UNSAT:

1. **Verify the query**: Ensure the graph pattern is valid
2. **Check labels**: Make sure node/relationship labels exist in the database
3. **Inspect constraints**: Use `cnf.constraints` to see what was generated
4. **Test subqueries**: Break down complex queries into smaller parts

### Performance Issues

For slow solving:

1. **Simplify the query**: Reduce the number of nodes/relationships
2. **Use faster solvers**: Try pycosat or CaDiCaL
3. **Check problem size**: Use `len(clauses)` and `len(forward_map)`
4. **Enable solver options**: Some solvers have tuning parameters

### Memory Issues

For large problems:

1. **Use streaming**: Don't load all solutions into memory
2. **Limit search**: Add constraints to reduce solution space
3. **Use external solvers**: Write DIMACS file and use external solver

## Examples

See `/examples/solver_usage.py` for comprehensive examples including:

- Basic CNF generation
- DIMACS file export
- python-sat integration
- pycosat integration
- Iterative solving
- Solution interpretation

## References

- [python-sat Documentation](https://pysathq.github.io/)
- [pycosat Documentation](https://pypi.org/project/pycosat/)
- [DIMACS CNF Format](http://www.satcompetition.org/2009/format-benchmarks2009.html)
- [SAT Competition](http://www.satcompetition.org/)
