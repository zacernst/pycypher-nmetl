Architecture Overview
=====================

This document provides an overview of the NMETL architecture and how the different packages work together.

System Architecture
-------------------

NMETL is composed of four main packages:

1. **nmetl** - Core ETL framework
2. **pycypher** - Cypher query engine and graph database
3. **shared** - Common utilities
4. **fastopendata** - Open data integrations

Package Relationships
---------------------

.. code-block:: text

   ┌─────────────────┐
   │   fastopendata  │  (Data source integrations)
   └────────┬────────┘
            │ uses
            ↓
   ┌─────────────────┐
   │      nmetl      │  (Core ETL framework)
   └────────┬────────┘
            │ uses
            ↓
   ┌─────────────────┐
   │    pycypher     │  (Cypher query engine)
   └────────┬────────┘
            │ uses
            ↓
   ┌─────────────────┐
   │     shared      │  (Common utilities)
   └─────────────────┘

NMETL Package
-------------

Core Components
~~~~~~~~~~~~~~~

Session Management
^^^^^^^^^^^^^^^^^^

- ``Session`` - Main entry point, manages triggers and fact collection
- ``WorkerContext`` - Thread-local context for workers
- ``ThreadManager`` - Manages worker threads

Data Flow
^^^^^^^^^

- ``DataSource`` - Abstraction for data sources (CSV, JSON, etc.)
- ``DataAsset`` - Represents a data asset with schema
- ``QueueGenerator`` - Generates items for processing
- ``QueueProcessor`` - Processes items from queue

Triggers
^^^^^^^^

- ``Trigger`` - Base trigger class
- ``VariableAttributeTrigger`` - Triggered on attribute changes
- ``NodeRelationshipTrigger`` - Triggered on relationship creation

Storage
^^^^^^^

NMETL delegates storage to PyCypher's fact collections.

PyCypher Package
----------------

Core Components
~~~~~~~~~~~~~~~

Query Engine
^^^^^^^^^^^^

- ``CypherParser`` - Parses Cypher queries to AST
- ``CypherLexer`` - Tokenizes Cypher syntax
- ``NodeClasses`` - AST node types

Fact Model
^^^^^^^^^^

- ``Fact`` - Base fact class
- ``FactNodeHasLabel`` - Node label facts
- ``FactNodeHasAttributeWithValue`` - Node attribute facts
- ``FactRelationshipHasLabel`` - Relationship label facts
- ``FactRelationshipHasSourceNode`` - Relationship source facts
- ``FactRelationshipHasTargetNode`` - Relationship target facts

Fact Collections
^^^^^^^^^^^^^^^^

Abstract interface with multiple backends:

- ``SimpleFactCollection`` - In-memory (for testing)
- ``FoundationDBFactCollection`` - FoundationDB backend
- ``RocksDBFactCollection`` - RocksDB backend
- ``Etcd3FactCollection`` - Etcd3 backend

SAT Solver
^^^^^^^^^^

- ``CypherQuerySolver`` - Converts queries to SAT problems
- ``ConstraintBag`` - Manages boolean constraints
- Constraint types: Conjunction, Disjunction, Negation, etc.

Solutions
^^^^^^^^^

- ``Projection`` - Single query result (dict-like)
- ``ProjectionList`` - Collection of projections

Data Flow
---------

Ingestion Flow
~~~~~~~~~~~~~~

.. code-block:: text

   Raw Data → DataSource → QueueGenerator → QueueProcessor
                                                  ↓
                                            Fact Creation
                                                  ↓
                                          FactCollection
                                                  ↓
                                         Trigger Evaluation
                                                  ↓
                                        New Facts Generated

Query Execution Flow
~~~~~~~~~~~~~~~~~~~~

.. code-block:: text

   Cypher Query → CypherParser → AST
                                  ↓
                          CypherQuerySolver
                                  ↓
                           CNF Constraints
                                  ↓
                            SAT Solver
                                  ↓
                         Variable Assignments
                                  ↓
                           ProjectionList

Fact Storage Architecture
--------------------------

Atomic Facts
~~~~~~~~~~~~

All data is stored as atomic facts:

.. code-block:: python

   FactNodeHasLabel("person1", "Person")
   FactNodeHasAttributeWithValue("person1", "name", "Alice")
   FactRelationshipHasSourceNode("rel1", "person1")
   FactRelationshipHasTargetNode("rel1", "person2")
   FactRelationshipHasLabel("rel1", "KNOWS")

This design provides:

- **Atomicity** - Each fact is independent
- **Flexibility** - Easy to add new fact types
- **Queryability** - Cypher queries work over facts
- **Provenance** - Each fact can track its origin

Backend Storage
~~~~~~~~~~~~~~~

FoundationDB Implementation:

.. code-block:: text

   Key Structure:
   - node_labels/{label}/{node_id}
   - node_attributes/{node_id}/{attribute}
   - relationship_labels/{label}/{relationship_id}
   - relationship_sources/{relationship_id}
   - relationship_targets/{relationship_id}

Trigger System
--------------

Declarative Triggers
~~~~~~~~~~~~~~~~~~~~

Triggers are declared using decorators:

.. code-block:: python

   @session.trigger("MATCH (c:City) WITH c.population AS pop RETURN pop")
   def classify_city(pop) -> VariableAttribute["c", "size_class"]:
       if pop > 1000000:
           return "large"
       return "small"

Trigger Evaluation
~~~~~~~~~~~~~~~~~~

1. Fact is added to FactCollection
2. CheckFactAgainstTriggersQueueProcessor evaluates fact against all triggers
3. Trigger's Cypher query is evaluated with the new fact
4. If query matches, trigger function is called
5. Result is stored as a new fact
6. Process repeats (with cycle detection)

Concurrency Model
-----------------

Threading
~~~~~~~~~

- Multiple worker threads process queue items
- Each worker has its own WorkerContext
- Thread-safe fact collection operations
- Lock-free where possible (FoundationDB transactions)

Queue-Based Processing
~~~~~~~~~~~~~~~~~~~~~~

- Producer-consumer pattern
- Backpressure handling
- Error isolation (per-item failures)

SAT Solver Architecture
------------------------

Constraint Generation
~~~~~~~~~~~~~~~~~~~~~

1. Parse Cypher query to AST
2. Extract nodes and relationships
3. Generate variable assignments (disjunctions)
4. Generate exactlyOne constraints
5. Generate relationship endpoint constraints (implications)
6. Convert to CNF

CNF Conversion
~~~~~~~~~~~~~~

Uses standard boolean logic transformations:

- P → Q ≡ ¬P ∨ Q (implication)
- ¬(P ∨ Q) ≡ ¬P ∧ ¬Q (De Morgan's)
- ¬(P ∧ Q) ≡ ¬P ∨ ¬Q (De Morgan's)
- Flatten nested conjunctions/disjunctions

SAT Encoding
~~~~~~~~~~~~

- Atomic constraints mapped to integers
- Clauses represented as lists of integers
- Positive integer = variable true
- Negative integer = variable negated

Extensibility
-------------

Adding New Fact Types
~~~~~~~~~~~~~~~~~~~~~

1. Define fact class inheriting from ``Fact``
2. Implement storage in fact collection backends
3. Add query methods to fact collections
4. Update Cypher parser if needed

Adding New Storage Backends
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Inherit from ``FactCollection`` abstract class
2. Implement all abstract methods
3. Handle transactions appropriately
4. Ensure thread safety

Adding New Trigger Types
~~~~~~~~~~~~~~~~~~~~~~~~~

1. Inherit from ``Trigger`` base class
2. Implement trigger matching logic
3. Add decorator support in ``Session``
4. Update queue processor

Performance Considerations
---------------------------

Fact Collection Performance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- FoundationDB: Distributed, ACID transactions
- RocksDB: Fast local key-value store
- Batching for bulk inserts
- Indexing for common query patterns

SAT Solver Performance
~~~~~~~~~~~~~~~~~~~~~~

- Constraint minimization
- Efficient CNF conversion
- Choice of solver (Glucose, MiniSat, etc.)
- Incremental solving for multiple queries

Queue Processing Performance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Multiple worker threads
- Lock-free queues where possible
- Batch processing for efficiency
- Backpressure to prevent memory issues

See Also
--------

- :doc:`api/index` - Complete API reference
- :doc:`examples` - Code examples
- :doc:`SAT_SOLVER_INTEGRATION` - SAT solver details
