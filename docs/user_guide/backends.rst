Backend Integration
===================

Working with different fact collection database backends in PyCypher.

Overview
--------

PyCypher supports multiple storage backends for fact collections through the ``FactCollection`` abstract interface. This allows you to choose the best backend for your use case while maintaining a consistent API.

Available Backends
------------------

FoundationDB Backend
~~~~~~~~~~~~~~~~~~~~

High-performance, distributed key-value store with ACID transactions.

.. code-block:: python

   from pycypher.fact_collection.foundationdb import FoundationDBFactCollection
   
   # Create a FoundationDB backend
   fc = FoundationDBFactCollection()
   
   # Add facts
   from pycypher.fact import FactNodeHasLabel
   fc.add_fact(FactNodeHasLabel(node_id="n1", label="Person"))
   
   # Query facts
   person_facts = fc.get_facts(label="Person")

**Features:**
- ACID transactions
- Distributed architecture
- High availability
- Excellent for production workloads
- Requires FoundationDB server running

RocksDB Backend
~~~~~~~~~~~~~~~

Embedded key-value store based on LevelDB.

.. code-block:: python

   from pycypher.fact_collection.rocksdb import RocksDBFactCollection
   
   # Create a RocksDB backend
   fc = RocksDBFactCollection(db_path="/path/to/db")
   
   # Use same API as other backends
   fc.add_fact(fact)

**Features:**
- Embedded (no separate server needed)
- Fast read/write performance
- Good for single-machine deployments
- Persistent storage

Simple In-Memory Backend
~~~~~~~~~~~~~~~~~~~~~~~~~

Dictionary-based in-memory storage for testing and development.

.. code-block:: python

   from pycypher.fact_collection.simple import SimpleFactCollection
   
   # Create an in-memory backend
   fc = SimpleFactCollection()
   
   # Fast for testing, not persistent
   fc.add_fact(fact)

**Features:**
- No dependencies
- Very fast
- No persistence (data lost on restart)
- Ideal for testing and prototyping

FactCollection Interface
------------------------

All backends implement the ``FactCollection`` abstract interface:

.. code-block:: python

   from pycypher.fact_collection import FactCollection
   from pycypher.fact import Fact
   
   class MyCustomBackend(FactCollection):
       """Custom fact collection backend."""
       
       def add_fact(self, fact: Fact) -> None:
           """Add a single fact to the collection."""
           # Implementation
           pass
       
       def add_facts(self, facts: List[Fact]) -> None:
           """Add multiple facts to the collection."""
           # Implementation
           pass
       
       def get_facts(self, **constraints) -> List[Fact]:
           """Query facts with constraints."""
           # Implementation
           pass
       
       def nodes(self) -> Set[str]:
           """Get all node IDs."""
           # Implementation
           pass
       
       def relationships(self) -> Set[str]:
           """Get all relationship IDs."""
           # Implementation
           pass

Working with Facts
------------------

Facts are immutable, atomic data points:

.. code-block:: python

   from pycypher.fact import (
       FactNodeHasLabel,
       FactNodeHasAttributeWithValue,
       FactRelationshipHasSourceNode,
       FactRelationshipHasTargetNode,
       FactRelationshipHasType
   )
   
   # Node facts
   fc.add_fact(FactNodeHasLabel(node_id="person1", label="Person"))
   fc.add_fact(FactNodeHasAttributeWithValue(
       node_id="person1",
       attribute="name",
       value="Alice"
   ))
   
   # Relationship facts
   fc.add_fact(FactRelationshipHasSourceNode(
       relationship_id="rel1",
       node_id="person1"
   ))
   fc.add_fact(FactRelationshipHasTargetNode(
       relationship_id="rel1",
       node_id="person2"
   ))
   fc.add_fact(FactRelationshipHasType(
       relationship_id="rel1",
       rel_type="KNOWS"
   ))

Querying Facts
--------------

Query facts using constraints:

.. code-block:: python

   # Get all Person nodes
   person_facts = fc.get_facts(label="Person")
   
   # Get facts for specific node
   node_facts = fc.get_facts(node_id="person1")
   
   # Get relationship facts
   rel_facts = fc.get_facts(rel_type="KNOWS")
   
   # Get all nodes
   all_nodes = fc.nodes()
   
   # Get all relationships
   all_rels = fc.relationships()

SAT Solver Integration
-----------------------

The SAT solver backend translates fact queries into SAT problems:

.. code-block:: python

   from pycypher.fact_collection.solver import SATSolver
   
   # Create a SAT solver
   solver = SATSolver(fact_collection=fc)
   
   # Solve queries using SAT
   solutions = solver.solve(constraints)

Performance Considerations
--------------------------

**FoundationDB**
- Best for: Distributed systems, high availability
- Latency: Low (network overhead)
- Throughput: Very high
- Scalability: Excellent horizontal scaling

**RocksDB**
- Best for: Single-machine, embedded use cases
- Latency: Very low (no network)
- Throughput: High
- Scalability: Vertical only

**Simple**
- Best for: Testing, development, small datasets
- Latency: Minimal (in-memory)
- Throughput: Very high
- Scalability: Limited by memory

Choosing a Backend
------------------

Choose based on your requirements:

1. **Production distributed system**: FoundationDB
2. **Embedded application**: RocksDB
3. **Testing/development**: Simple
4. **Custom requirements**: Implement ``FactCollection``

For More Information
--------------------

* See :doc:`../api/pycypher` for API reference
* See fact collection module documentation for implementation details
* See FoundationDB/RocksDB documentation for setup instructions
