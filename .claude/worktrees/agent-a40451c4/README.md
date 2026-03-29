[![Install and run tests](https://github.com/zacernst/pycypher-nmetl/actions/workflows/makefile.yml/badge.svg)](https://github.com/zacernst/pycypher-nmetl/actions/workflows/makefile.yml)

# PyCypher-NMETL

A declarative ETL framework with graph database capabilities, featuring a Cypher query engine and SAT-based constraint solving.

## Overview

NMETL (New Methods for ETL) is a Python framework that takes a fundamentally different approach to ETL. Instead of pipelines and transformations, NMETL uses a **declarative, model-first approach** where you define your data model and transformations as triggers, and the framework handles execution, ordering, and storage automatically.

### Why NMETL?

Traditional ETL frameworks treat complexity as an orchestration problem. NMETL recognizes that **complexity itself is the enemy**, not scale or computational resources. By using:

- **Atomic fact-based storage**: Data stored as immutable facts (nodes, attributes, relationships)
- **Declarative triggers**: Python functions with Cypher queries and type hints
- **Automatic dependency resolution**: Framework determines execution order
- **Multiple storage backends**: FoundationDB, RocksDB, etcd3, or in-memory
- **SAT-based query solving**: Constraint satisfaction for complex graph patterns

You get ETL that stays **simple**, **maintainable**, and **testable** as your data grows.

## Key Features

### 🔍 Cypher Query Engine
- Full Cypher query parser with lexer and AST
- Support for MATCH, WHERE, WITH, RETURN, COLLECT, SIZE
- Graph pattern matching on fact collections
- **SAT-based constraint solver** for complex queries
- Query result projections and aggregations

### 🧮 SAT Solver Integration
- Convert Cypher queries to CNF (Conjunctive Normal Form)
- Integration with python-sat and pycosat
- DIMACS format export for external solvers
- Constraint types: Conjunction, Disjunction, Negation, Implication, Cardinality
- Find all solutions or verify satisfiability

### ⚡ Reactive Triggers
- Declarative trigger definitions using decorators
- Cypher-based pattern matching for trigger conditions
- Automatic dependency tracking and execution ordering
- Support for aggregations (COLLECT, SIZE) in triggers
- Type-safe attribute and relationship creation

### 🚀 Distributed Processing
- Multi-threaded queue-based processing
- Worker context for thread-local state
- Backpressure handling and error isolation
- Prometheus metrics integration
- Configurable batch processing

### 💾 Multiple Storage Backends
- **FoundationDB**: Distributed ACID transactions, fault-tolerant
- **RocksDB**: High-performance local key-value store
- **etcd3**: Distributed configuration and coordination
- **SimpleFactCollection**: In-memory for testing
- Abstract interface for custom backends

## Architecture

The project consists of four packages:

- **`pycypher`**: Cypher parsing, query execution, fact collections, and SAT solver
- **`nmetl`**: Core ETL framework with sessions, triggers, and queue processors
- **`shared`**: Common utilities, logging, and telemetry
- **`fastopendata`**: Integrations for open data sources (Census, OpenStreetMap, etc.)

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/zacernst/pycypher-nmetl.git
cd pycypher-nmetl

# Install all packages
pip install -e packages/shared
pip install -e packages/pycypher
pip install -e packages/nmetl
pip install -e packages/fastopendata

# Or install specific packages as needed
pip install -e packages/pycypher  # Just Cypher and SAT solver
```

### Basic Session Example

```python
from nmetl.session import Session
from pycypher.fact_collection.foundationdb import FoundationDBFactCollection
from nmetl.trigger import VariableAttribute

# Create and configure session
session = Session(session_config_file="config.toml")
fact_collection = FoundationDBFactCollection()
session.attach_fact_collection(fact_collection)

# Define a reactive trigger
@session.trigger("MATCH (c:City) WITH c.population AS pop RETURN pop")
def classify_city(pop) -> VariableAttribute["c", "size_class"]:
    """Automatically classify cities by population."""
    if pop > 1000000:
        return "large"
    elif pop > 100000:
        return "medium"
    else:
        return "small"

# Load data and start processing
from nmetl.data_source import DataSource
data_source = DataSource.from_uri("file://cities.csv")
session.attach_data_source(data_source)

session.start_threads()
session.block_until_finished()
```

### Cypher Query Example

```python
from pycypher.cypher_parser import CypherParser
from pycypher.fact_collection.simple import SimpleFactCollection

# Create fact collection
fc = SimpleFactCollection()

# Parse and execute Cypher query
query = """
    MATCH (n:Person)-[r:KNOWS]->(m:Person)
    WHERE n.age > 18
    WITH n.name AS person, COLLECT(m.name) AS friends
    RETURN person, friends
"""
parser = CypherParser(query)
results = parser.parse_tree._evaluate(fc)

# Results are ProjectionList objects
for projection in results:
    print(projection.pythonify())
```

### SAT Solver Example

```python
from pycypher.fact_collection.solver import CypherQuerySolver
from pycypher.cypher_parser import CypherParser
from pysat.solvers import Glucose3

# Initialize solver
solver = CypherQuerySolver(fact_collection)

# Parse query and convert to CNF
query = "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n, m"
parser = CypherParser(query)
cnf = solver.solve_query(parser.parse_tree)

# Get SAT clauses
clauses, reverse_map, forward_map = solver.get_clauses(cnf)

# Solve with SAT solver
with Glucose3() as sat:
    for clause in clauses:
        sat.add_clause(clause)
    
    if sat.solve():
        model = sat.get_model()
        # Convert solution to projection
        solution = [reverse_map[v] for v in model if v > 0]
        projection = solver.solution_to_projection(solution)
        print(projection.pythonify())
```

## Core Concepts

### Fact-Based Data Model

The system represents all data as atomic, immutable facts:

```python
from pycypher.fact import (
    FactNodeHasLabel,
    FactNodeHasAttributeWithValue,
    FactRelationshipHasSourceNode,
    FactRelationshipHasTargetNode,
    FactRelationshipHasLabel,
)

# Node facts
FactNodeHasLabel(node_id="person1", label="Person")
FactNodeHasAttributeWithValue(node_id="person1", attribute="name", value="Alice")
FactNodeHasAttributeWithValue(node_id="person1", attribute="age", value=30)

# Relationship facts
FactRelationshipHasLabel(relationship_id="rel1", relationship_label="KNOWS")
FactRelationshipHasSourceNode(relationship_id="rel1", source_node_id="person1")
FactRelationshipHasTargetNode(relationship_id="rel1", target_node_id="person2")
```

**Benefits:**
- **Immutability**: Facts never change, only new facts are added
- **Provenance**: Track where each fact came from
- **Atomicity**: Small blast radius - errors affect only related facts
- **Flexibility**: Easy to add new fact types

### Declarative Triggers

Triggers automatically compute derived data when patterns match:

```python
@session.trigger("""
    MATCH (c:City)-[r:In]->(s:State)
    WITH SIZE(COLLECT(c.has_beach)) AS beach_count
    RETURN beach_count
""")
def count_beaches(beach_count) -> VariableAttribute["s", "total_beaches"]:
    """Automatically count beaches in each state."""
    return beach_count

@session.trigger("""
    MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
    WHERE p.salary > 100000
    RETURN p.id, c.id
""")
def high_earner_relationship(p_id, c_id) -> NodeRelationship["p", "HIGH_EARNER_AT", "c"]:
    """Create relationship for high earners."""
    return True
```

**Features:**
- Type-safe using Python type hints
- Automatic dependency resolution
- Supports aggregations (COLLECT, SIZE)
- Can create attributes or relationships

### SAT-Based Query Solving

Complex graph queries are converted to boolean constraints:

```python
from pycypher.fact_collection.solver import (
    ConstraintBag,
    Conjunction,
    Disjunction,
    ExactlyOne,
    IfThen,
    Negation,
)

# Constraints are built automatically from Cypher queries
# Example: "variable x must be exactly one of these nodes"
bag = ConstraintBag()
options = Disjunction([
    VariableAssignedToNode("x", "node1"),
    VariableAssignedToNode("x", "node2"),
    VariableAssignedToNode("x", "node3"),
])
bag.add_constraint(ExactlyOne(options))

# Convert to CNF for SAT solver
cnf = bag.cnf()

# Export to DIMACS format
dimacs = cnf.to_dimacs(bag.atomic_constraint_mapping)
```

**Constraint Types:**
- `Conjunction` - AND (P ∧ Q)
- `Disjunction` - OR (P ∨ Q)
- `Negation` - NOT (¬P)
- `IfThen` - Implication (P → Q)
- `ExactlyOne` - Cardinality constraint
- `AtMostOne` - Cardinality constraint

### Queue Processors

Distributed pipeline components that handle different stages:

```python
from nmetl.queue_processor import (
    RawDataProcessor,                        # Convert raw data to facts
    FactGeneratedQueueProcessor,             # Persist facts to storage
    CheckFactAgainstTriggersQueueProcessor,  # Evaluate triggers
    TriggeredLookupProcessor,                # Execute triggered computations
)
```

**Processing Flow:**
```
Raw Data → RawDataProcessor → Facts
Facts → FactGeneratedQueueProcessor → Storage
Facts → CheckFactAgainstTriggersQueueProcessor → Trigger Matches
Matches → TriggeredLookupProcessor → New Facts
```

## CLI Tools

### PyCypher CLI

```bash
# Parse and validate Cypher queries
pycypher parse "MATCH (n:Person) RETURN n.name"

# Analyze query structure
pycypher analyze "MATCH (n)-[r]->(m) WHERE n.age > 18 RETURN n, m"
```

### NMETL CLI

```bash
# Run ETL session with configuration
nmetl run --config session_config.toml

# Validate configuration
nmetl validate --config session_config.toml

# Dump facts to tables
nmetl dump --output data_export/
```

## Configuration

### Session Configuration (TOML)

```toml
[session]
name = "my_etl_session"
run_monitor = true
logging_level = "INFO"

[foundationdb]
cluster_file = "/etc/foundationdb/fdb.cluster"
sync_writes = true

[threading]
num_workers = 4
queue_size = 1000

[prometheus]
enabled = true
port = 9090
```

### Data Source Mapping (YAML)

```yaml
data_sources:
  - name: cities
    uri: file://data/cities.csv
    format: csv
    mappings:
      # Node label mapping
      - identifier_key: city_id
        label: City
      
      # Attribute mappings
      - identifier_key: city_id
        attribute_key: name
        attribute: CityName
        label: City
      
      - identifier_key: city_id
        attribute_key: population
        attribute: Population
        label: City
      
      # Relationship mapping
      - source_identifier_key: city_id
        target_identifier_key: state_id
        relationship_label: IN_STATE
```

## Development

### Project Structure

```
pycypher-nmetl/
├── packages/
│   ├── pycypher/          # Cypher parser, SAT solver, fact collections
│   │   └── src/pycypher/
│   │       ├── cypher_parser.py
│   │       ├── cypher_lexer.py
│   │       ├── fact_collection/
│   │       │   ├── solver.py       # SAT-based constraint solving
│   │       │   ├── foundationdb.py
│   │       │   ├── rocksdb.py
│   │       │   └── simple.py
│   │       ├── fact.py
│   │       ├── node_classes.py
│   │       └── solutions.py
│   │
│   ├── nmetl/             # Core ETL framework
│   │   └── src/nmetl/
│   │       ├── session.py
│   │       ├── trigger.py
│   │       ├── queue_processor.py
│   │       ├── data_source.py
│   │       └── configuration.py
│   │
│   ├── shared/            # Common utilities
│   │   └── src/shared/
│   │       ├── logger.py
│   │       ├── telemetry.py
│   │       └── helpers.py
│   │
│   └── fastopendata/      # Open data integrations
│       └── src/fastopendata/
│           ├── ingest.py
│           └── filter_us_nodes.py
│
├── tests/                 # Comprehensive test suite
│   ├── test_eval.py
│   ├── test_pycypher.py
│   ├── test_solver.py
│   └── test_cypher_query_solver.py
│
├── docs/                  # Sphinx documentation
│   ├── index.rst
│   ├── api/              # Auto-generated API docs
│   ├── examples.rst
│   ├── architecture.rst
│   └── SAT_SOLVER_INTEGRATION.md
│
└── examples/             # Usage examples
    ├── solver_usage.py
    └── projection_conversion_example.py
```

### Running Tests

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_solver.py

# Run with coverage
pytest tests/ --cov=pycypher --cov=nmetl

# Run specific test class
pytest tests/test_cypher_query_solver.py::TestIfThen -v
```

### Test Coverage

The project includes comprehensive test suites:

- **`test_solver.py`**: 40+ tests for ConstraintBag and constraint classes
- **`test_cypher_query_solver.py`**: 100+ tests for CypherQuerySolver and IfThen
- **`test_eval.py`**: Integration tests for triggers and query evaluation
- **`test_pycypher.py`**: Cypher parser and fact collection tests

Key areas covered:
- Constraint creation and CNF conversion
- SAT solver integration
- Cypher query parsing and evaluation
- Trigger system functionality
- Fact collection operations
- Projection and solution handling

### Building Documentation

```bash
cd docs

# Build HTML documentation
make html

# Open in browser
open _build/html/index.html  # macOS
xdg-open _build/html/index.html  # Linux

# Build PDF
make latexpdf

# Clean build
make clean
```

The documentation includes:
- **API Reference**: Auto-generated from docstrings
- **Tutorials**: Getting started guides
- **Architecture**: System design overview
- **Examples**: Code samples for common tasks
- **SAT Solver Guide**: Detailed integration documentation

### Code Style

The project uses:
- **Ruff** for linting and formatting
- **Type hints** throughout the codebase
- **Docstrings** in Google/NumPy style

```bash
# Run ruff
ruff check packages/

# Format code
ruff format packages/
```

## Advanced Features

### Custom Constraint Types

Create your own constraint classes:

```python
from pycypher.fact_collection.solver import AtomicConstraint

class CustomConstraint(AtomicConstraint):
    def __init__(self, variable, value):
        self.variable = variable
        self.value = value
    
    def walk(self):
        yield self
```

### Multiple Fact Collection Backends

Switch backends based on your needs:

```python
# Development: In-memory
from pycypher.fact_collection.simple import SimpleFactCollection
fc = SimpleFactCollection()

# Production: FoundationDB (distributed, ACID)
from pycypher.fact_collection.foundationdb import FoundationDBFactCollection
fc = FoundationDBFactCollection(
    cluster_file="/etc/foundationdb/fdb.cluster",
    sync_writes=True
)

# High-performance local: RocksDB
from pycypher.fact_collection.rocksdb import RocksDBFactCollection
fc = RocksDBFactCollection(db_path="/data/rocks")

# Distributed coordination: etcd3
from pycypher.fact_collection.etcd3 import Etcd3FactCollection
fc = Etcd3FactCollection(host="localhost", port=2379)
```

### Prometheus Monitoring

Built-in metrics for monitoring:

```python
from nmetl.prometheus_metrics import (
    facts_processed_total,
    trigger_evaluations_total,
    queue_size_gauge,
)

# Metrics are automatically updated
# Access at http://localhost:9090/metrics
```

Available metrics:
- `facts_processed_total` - Counter of processed facts
- `trigger_evaluations_total` - Counter of trigger evaluations
- `queue_size_gauge` - Current queue sizes
- `processing_duration_seconds` - Histogram of processing times

### Integration with External SAT Solvers

Export to DIMACS format for any SAT solver:

```bash
# Generate DIMACS file
python -c "
from pycypher.fact_collection.solver import CypherQuerySolver
solver = CypherQuerySolver(fact_collection)
cnf = solver.solve_query(query)
dimacs = solver.to_dimacs(cnf)
with open('problem.cnf', 'w') as f:
    f.write(dimacs)
"

# Use with any DIMACS-compatible solver
minisat problem.cnf solution.txt
glucose problem.cnf solution.txt
```

## Use Cases

### Data Integration
- Merge data from multiple sources with automatic relationship creation
- Handle schema evolution with flexible fact model
- Track data lineage through fact provenance

### Feature Engineering
- Define derived features as triggers
- Automatic computation when dependencies change
- Type-safe transformations with Python type hints

### Graph Analysis
- Query graph patterns with Cypher
- Find complex relationships
- Compute graph metrics (centrality, clustering, etc.)

### Data Quality
- Define quality rules as triggers
- Automatic validation on data ingestion
- Error isolation with atomic facts

## Performance Characteristics

### Fact Collection Performance

| Backend | Reads/sec | Writes/sec | Transactions | Scalability |
|---------|-----------|------------|--------------|-------------|
| SimpleFactCollection | 1M+ | 500K+ | No | Single-process |
| RocksDB | 100K+ | 50K+ | No | Single-machine |
| FoundationDB | 10K+ | 5K+ | ACID | Distributed |
| etcd3 | 5K+ | 2K+ | ACID | Distributed |

### SAT Solver Performance

- Small queries (<100 variables): < 1ms
- Medium queries (100-1000 variables): 10-100ms
- Large queries (1000+ variables): seconds to minutes

**Optimization tips:**
- Use specific labels to reduce search space
- Add WHERE clauses to filter early
- Break complex queries into smaller parts
- Cache CNF for repeated queries

## Contributing

We welcome contributions! Here's how to get started:

1. **Fork the repository**
   ```bash
   git clone https://github.com/your-username/pycypher-nmetl.git
   ```

2. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Install development dependencies**
   ```bash
   pip install -e packages/pycypher[dev]
   pip install -e packages/nmetl[dev]
   pip install pytest ruff sphinx
   ```

4. **Make your changes**
   - Add tests for new functionality
   - Update documentation
   - Follow code style guidelines

5. **Run tests and linting**
   ```bash
   pytest tests/
   ruff check packages/
   ```

6. **Submit a pull request**
   - Describe your changes
   - Link related issues
   - Ensure CI passes

### Areas for Contribution

- Additional storage backends (PostgreSQL, MongoDB, etc.)
- Cypher query optimizations
- More SAT solver integrations
- Data source connectors
- Documentation improvements
- Performance optimizations
- Bug fixes

## Resources

- **Documentation**: [Full docs in `docs/`](docs/index.rst)
- **SAT Solver Guide**: [SAT_SOLVER_INTEGRATION.md](docs/SAT_SOLVER_INTEGRATION.md)
- **Examples**: [examples/](examples/)
- **Tests**: [tests/](tests/)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by Neo4j's Cypher query language
- SAT solving powered by python-sat and pycosat
- Storage backends: FoundationDB, RocksDB, etcd3
- Testing with pytest
- Documentation with Sphinx

## Citation

If you use NMETL in your research, please cite:

```bibtex
@software{nmetl2025,
  title = {NMETL: New Methods for ETL},
  author = {Ernst, Zachary},
  year = {2025},
  url = {https://github.com/zacernst/pycypher-nmetl}
}
```
