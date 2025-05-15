# PyCypher-NMETL: Declarative ETL Using Cypher

[![Install and run tests](https://github.com/zacernst/pycypher-nmetl/actions/workflows/makefile.yml/badge.svg)](https://github.com/zacernst/pycypher-nmetl/actions/workflows/makefile.yml)

A monorepo containing three complementary packages for declarative ETL using Cypher queries:

- **PyCypher**: Parses Cypher queries into Python objects
- **NMETL**: Declarative ETL framework using PyCypher
- **FastOpenData**: Utilities for working with open data sources

## Quick Start

### Prerequisites

- Python 3.10 or higher
- `uv` package manager (recommended)

### Installation

Using `uv` (recommended):

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone the repository
git clone https://github.com/zacernst/pycypher-nmetl.git
cd pycypher-nmetl

# Install all packages
make install
```

Using pip:

```bash
pip install pycypher-nmetl
```

### Basic Usage

```python
from nmetl.configuration import load_session_config
from nmetl.trigger import VariableAttribute

# Load session from configuration
session = load_session_config("config/ingest.yaml")

# Define a trigger for data transformation
@session.trigger(
    """
    MATCH (c:Customer)-[:PURCHASED]->(p:Product)
    WITH c.id AS customer_id, SUM(p.price) AS total_spent
    RETURN customer_id, total_spent
    """
)
def calculate_total_spending(results):
    return results

# Run the ETL pipeline
session.start_threads()
session.block_until_finished()
```

## Documentation

Full documentation is available at [https://zacernst.github.io/pycypher-nmetl/](https://zacernst.github.io/pycypher-nmetl/)

The documentation is automatically built and deployed to GitHub Pages when changes are pushed to the main branch. You can also build the documentation locally:

```bash
cd docs
make html
```

The built documentation will be available in the `docs/build/html` directory.

## Development

### Setting Up Development Environment

```bash
# Clone the repository
git clone https://github.com/zacernst/pycypher-nmetl.git
cd pycypher-nmetl

# Install development dependencies
make install

# Run tests
make tests

# Build documentation
make docs
```

### Project Structure

```
pycypher-nmetl/
├── packages/
│   ├── pycypher/      # Cypher query parser
│   ├── nmetl/         # ETL framework
│   └── fastopendata/  # Open data utilities
├── docs/              # Documentation
└── tests/             # Test suite
```

## License

This project is licensed under the MIT License - see the LICENSE.txt file for details.
