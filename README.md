# PyCypher-NMETL

A distributed graph data processing framework that combines Cypher query language capabilities with reactive ETL pipelines.

## Overview

PyCypher-NMETL is a Python framework for building scalable, reactive data processing pipelines using graph-based concepts. It enables you to:

- Parse and execute Cypher queries on streaming data
- Define reactive triggers that automatically compute derived facts
- Process data through distributed queue-based pipelines
- Store facts in multiple backend systems (FoundationDB, RocksDB, etc.)
- Build complex ETL workflows with graph semantics

## Key Features

### 🔍 Cypher Query Engine
- Full Cypher query parser and evaluator
- Support for MATCH, WHERE, WITH, and RETURN clauses
- Graph pattern matching on fact collections
- Constraint solving and variable binding

### ⚡ Reactive Triggers
- Define Python functions triggered by Cypher patterns
- Automatic computation of derived attributes and relationships
- Event-driven processing of new facts

### 🚀 Distributed Processing
- Dask-based distributed computation
- ZMQ message queues for inter-process communication
- Scalable queue processors for different pipeline stages

### 💾 Multiple Storage Backends
- FoundationDB for distributed ACID transactions
- RocksDB for high-performance local storage
- In-memory collections for development and testing

## Architecture

The framework consists of three main packages:

- **`pycypher`**: Core Cypher parsing, query execution, and fact management
- **`nmetl`**: ETL pipeline components, queue processors, and session management  
- **`fastopendata`**: Example data processing applications

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/pycypher-nmetl.git
cd pycypher-nmetl

# Install dependencies
pip install -e packages/pycypher
pip install -e packages/nmetl
```

### Basic Usage

```python
from nmetl.session import Session
from nmetl.data_source import DataSource
from nmetl.trigger import VariableAttribute

# Create a session
session = Session()

# Load data from CSV
data_source = DataSource.from_uri("file://data.csv")
session.attach_data_source(data_source)

# Define a reactive trigger
@session.trigger("MATCH (p:Person) WHERE p.age > 18 RETURN p.name")
def compute_adult_status(name) -> VariableAttribute["p", "is_adult"]:
    return True

# Start processing
session.start_threads()
session.block_until_finished()
```

### Cypher Query Examples

```python
from pycypher.cypher_parser import CypherParser

# Parse a Cypher query
parser = CypherParser("MATCH (n:Person)-[:KNOWS]->(m:Person) RETURN n.name, m.name")

# Execute against a fact collection
results = parser.parse_tree.cypher.match_clause.solutions(fact_collection)
```

## Core Concepts

### Facts
The system represents data as atomic facts:
- `FactNodeHasLabel(node_id, label)`
- `FactNodeHasAttributeWithValue(node_id, attribute, value)`
- `FactRelationshipHasSourceNode(relationship_id, source_node_id)`

### Triggers
Reactive functions that execute when Cypher patterns match:
```python
@session.trigger("MATCH (u:User) WHERE u.login_count > 10 RETURN u.id")
def mark_active_user(id) -> VariableAttribute["u", "active"]:
    return True
```

### Queue Processors
Distributed pipeline components:
- `RawDataProcessor`: Converts raw data to facts
- `FactGeneratedQueueProcessor`: Persists facts to storage
- `CheckFactAgainstTriggersQueueProcessor`: Evaluates triggers
- `TriggeredLookupProcessor`: Executes triggered computations

## CLI Tools

```bash
# Parse and validate Cypher queries
pycypher parse "MATCH (n:Person) RETURN n.name"
pycypher validate "MATCH (n:Person) RETURN n.name"

# Run ETL sessions
nmetl run config.yaml
```

## Configuration

Sessions can be configured via YAML:

```yaml
fact_collection: FoundationDBFactCollection
run_monitor: true
logging_level: INFO

data_sources:
  - name: users
    uri: file://users.csv
    mappings:
      - identifier_key: id
        label: User
      - attribute_key: name
        identifier_key: id
        attribute: name
        label: User
```

## Development

### Running Tests

```bash
pytest tests/
```

### Building Documentation

```bash
cd docs
make html
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
