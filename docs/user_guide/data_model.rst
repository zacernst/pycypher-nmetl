Data Model Requirements
=======================

This guide explains **why** PyCypher requires specific columns in your data,
how to use your existing column names without renaming, and how to
troubleshoot common data issues.

.. contents:: In this guide
   :local:
   :depth: 2

Why Does PyCypher Need ``__ID__`` Columns?
------------------------------------------

If you've tried loading data and been confused by ``__ID__``, ``__SOURCE__``,
or ``__TARGET__`` — you're not alone.  Here's what they are and why they exist.

The Short Answer
~~~~~~~~~~~~~~~~

PyCypher executes **graph queries** against **tabular data**.  Graph queries
traverse connections between things (``MATCH (a)-[:KNOWS]->(b)``).  For
that to work, every row needs a unique identity so relationships can
reference it.

- ``__ID__`` — uniquely identifies each entity (node) within its type
- ``__SOURCE__`` — which entity a relationship starts from
- ``__TARGET__`` — which entity a relationship points to

These are the structural glue that turns flat tables into a queryable graph.

.. note::

   **You do not need to rename your columns.**  PyCypher accepts your
   existing column names via the ``id_col``, ``source_col``, and
   ``target_col`` parameters.  The ``__ID__``/``__SOURCE__``/``__TARGET__``
   names are internal — PyCypher renames them for you.

Why Not Just Use Row Position?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You might wonder: "Why can't PyCypher just use row 0, row 1, row 2?"

1. **Relationships need stable references.**  If you say "Person 42 KNOWS
   Person 17", you need those IDs to look up the actual rows.  Row position
   can change if the data is filtered, sorted, or reloaded.

2. **Cross-type joins.**  When a relationship connects two different entity
   types (e.g., ``Person -[:WORKS_AT]-> Company``), the source and target
   IDs come from different tables.  Row positions would be meaningless
   across tables.

3. **Mutation correctness.**  ``SET``, ``CREATE``, ``MERGE``, and ``DELETE``
   operations must track which exact row to update.  A stable identity
   column guarantees this.

4. **Performance.**  During query execution, PyCypher stores only ID columns
   in intermediate results (BindingFrames).  Full row data is fetched
   on-demand only when needed — typically at ``RETURN`` or ``WHERE``.  This
   dramatically reduces memory usage and speeds up multi-hop traversals.

Using Your Existing Column Names
---------------------------------

The Most Common Pattern
~~~~~~~~~~~~~~~~~~~~~~~

You already have a ``customer_id`` column.  Don't rename it — tell
PyCypher which column to use:

.. code-block:: python

   import pandas as pd
   from pycypher.ingestion import ContextBuilder
   from pycypher import Star

   # Your existing data — no __ID__ column needed
   customers = pd.DataFrame({
       "customer_id": [101, 102, 103],
       "name": ["Alice", "Bob", "Carol"],
       "email": ["alice@example.com", "bob@example.com", "carol@example.com"],
   })

   orders = pd.DataFrame({
       "order_id": [1001, 1002, 1003, 1004],
       "customer_id": [101, 102, 101, 103],
       "product": ["Widget", "Gadget", "Gizmo", "Widget"],
       "amount": [29.99, 49.99, 19.99, 29.99],
   })

   context = (
       ContextBuilder()
       .add_entity("Customer", customers, id_col="customer_id")
       .add_entity("Order", orders, id_col="order_id")
       .add_relationship(
           "PLACED",
           orders,
           source_col="customer_id",    # which customer placed it
           target_col="order_id",       # which order was placed
       )
       .build()
   )

   star = Star(context=context)
   result = star.execute_query(
       "MATCH (c:Customer)-[:PLACED]->(o:Order) "
       "RETURN c.name, o.product, o.amount"
   )

Behind the scenes, PyCypher renames ``customer_id`` → ``__ID__`` in the
Customer table, ``order_id`` → ``__ID__`` in the Order table, and
``customer_id`` → ``__SOURCE__`` / ``order_id`` → ``__TARGET__`` in the
relationship table.  **Your original column names remain accessible as
properties** (e.g., ``c.customer_id`` still works in queries).

From the CLI
~~~~~~~~~~~~

The ``nmetl query`` command accepts the same column mapping:

.. code-block:: bash

   # Entity: Label=path[:id_col]
   nmetl query "MATCH (c:Customer) RETURN c.name" \
       --entity "Customer=customers.csv:customer_id"

   # Relationship: Type=path:source_col:target_col
   nmetl query "MATCH (c:Customer)-[:PLACED]->(o:Order) RETURN c.name, o.product" \
       --entity "Customer=customers.csv:customer_id" \
       --entity "Order=orders.csv:order_id" \
       --rel "PLACED=orders.csv:customer_id:order_id"

In YAML Pipeline Config
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   sources:
     entities:
       - id: customers
         uri: data/customers.csv
         entity_type: Customer
         id_col: customer_id       # your column name

       - id: orders
         uri: data/orders.csv
         entity_type: Order
         id_col: order_id

     relationships:
       - id: placed
         uri: data/orders.csv
         relationship_type: PLACED
         source_col: customer_id   # your column name
         target_col: order_id

When No ID Column Exists
~~~~~~~~~~~~~~~~~~~~~~~~~

If your data has no natural identifier, **omit** ``id_col`` and PyCypher
auto-generates sequential integer IDs (0, 1, 2, ...):

.. code-block:: python

   # No id_col — PyCypher assigns __ID__ = 0, 1, 2, ...
   products = pd.DataFrame({
       "name": ["Widget", "Gadget", "Gizmo"],
       "price": [9.99, 49.99, 19.99],
   })
   context = ContextBuilder().add_entity("Product", products).build()

This works for entities that are only queried by properties (not
referenced by relationships).  If you need to connect them via
relationships, auto-generated IDs are positional and fragile — prefer
explicit IDs.

Real-World Examples
-------------------

Example 1: E-Commerce (Customer → Order → Product)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Starting data — three CSVs as they might come from a database export:

``customers.csv``:

.. code-block:: text

   customer_id,name,email,city
   C001,Alice,alice@co.com,NYC
   C002,Bob,bob@co.com,LA
   C003,Carol,carol@co.com,NYC

``products.csv``:

.. code-block:: text

   sku,title,price,category
   SKU-100,Widget,29.99,Hardware
   SKU-200,Gadget,49.99,Electronics
   SKU-300,Gizmo,19.99,Hardware

``orders.csv``:

.. code-block:: text

   order_id,customer_id,sku,quantity,order_date
   ORD-1,C001,SKU-100,2,2024-01-15
   ORD-2,C002,SKU-200,1,2024-01-16
   ORD-3,C001,SKU-300,3,2024-01-17
   ORD-4,C003,SKU-100,1,2024-01-18

Loading into PyCypher:

.. code-block:: python

   context = (
       ContextBuilder()
       .add_entity("Customer", "customers.csv", id_col="customer_id")
       .add_entity("Product", "products.csv", id_col="sku")
       .add_relationship(
           "ORDERED",
           "orders.csv",
           source_col="customer_id",
           target_col="sku",
       )
       .build()
   )

   star = Star(context=context)

   # "What hardware did NYC customers order?"
   result = star.execute_query("""
       MATCH (c:Customer)-[:ORDERED]->(p:Product)
       WHERE c.city = 'NYC' AND p.category = 'Hardware'
       RETURN c.name AS customer, p.title AS product
   """)

Example 2: Social Network (User follows User)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Same entity type on both sides of the relationship:

``users.csv``:

.. code-block:: text

   user_id,username,joined
   1,alice,2023-01-01
   2,bob,2023-02-15
   3,carol,2023-03-10
   4,dave,2023-04-20

``follows.csv``:

.. code-block:: text

   follower_id,followee_id,since
   1,2,2023-03-01
   1,3,2023-04-01
   2,3,2023-05-01
   3,4,2023-06-01

.. code-block:: python

   context = (
       ContextBuilder()
       .add_entity("User", "users.csv", id_col="user_id")
       .add_relationship(
           "FOLLOWS",
           "follows.csv",
           source_col="follower_id",
           target_col="followee_id",
       )
       .build()
   )

   star = Star(context=context)

   # "Who does Alice follow, and who do they follow?"
   result = star.execute_query("""
       MATCH (a:User {username: 'alice'})-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)
       RETURN a.username AS user, b.username AS follows, c.username AS follows_of_follows
   """)

Example 3: Multi-Entity (Person + Company + Employment)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import pandas as pd

   people = pd.DataFrame({
       "emp_id": ["E001", "E002", "E003"],
       "name": ["Alice", "Bob", "Carol"],
       "department": ["Engineering", "Marketing", "Engineering"],
   })

   companies = pd.DataFrame({
       "company_id": ["ACME", "GLOB"],
       "name": ["Acme Corp", "Globex Inc"],
       "industry": ["Tech", "Finance"],
   })

   employment = pd.DataFrame({
       "emp_id": ["E001", "E002", "E003"],
       "company_id": ["ACME", "GLOB", "ACME"],
       "role": ["Senior Engineer", "Marketing Lead", "Junior Engineer"],
       "start_year": [2020, 2021, 2023],
   })

   context = (
       ContextBuilder()
       .add_entity("Person", people, id_col="emp_id")
       .add_entity("Company", companies, id_col="company_id")
       .add_relationship(
           "WORKS_AT",
           employment,
           source_col="emp_id",
           target_col="company_id",
       )
       .build()
   )

   star = Star(context=context)

   # "Who works at Acme Corp, and what's their role?"
   result = star.execute_query("""
       MATCH (p:Person)-[r:WORKS_AT]->(c:Company {name: 'Acme Corp'})
       RETURN p.name AS employee, r.role AS role, r.start_year AS since
   """)

Data Preparation Guide
-----------------------

Converting Existing Tables
~~~~~~~~~~~~~~~~~~~~~~~~~~

Most real-world data needs minimal preparation.  The key question is:
**which column uniquely identifies each row?**

.. list-table::
   :widths: 30 30 40
   :header-rows: 1

   * - Source data
     - ID column
     - PyCypher setup
   * - Database table with primary key
     - The PK column
     - ``id_col="pk_column"``
   * - CSV with a unique name/code
     - The unique column
     - ``id_col="name"`` or ``id_col="code"``
   * - CSV with no unique column
     - Omit ``id_col``
     - Auto-generated sequential IDs
   * - Data with composite keys
     - Create a combined column
     - See below

Handling Composite Keys
~~~~~~~~~~~~~~~~~~~~~~~

If uniqueness comes from multiple columns (e.g., ``(year, department)``),
create a combined ID before loading:

.. code-block:: python

   df = pd.DataFrame({
       "year": [2023, 2023, 2024],
       "dept": ["Eng", "Sales", "Eng"],
       "budget": [100000, 80000, 120000],
   })

   # Create a composite ID
   df["budget_id"] = df["year"].astype(str) + "_" + df["dept"]

   context = ContextBuilder().add_entity("Budget", df, id_col="budget_id").build()

ID Generation Strategies
~~~~~~~~~~~~~~~~~~~~~~~~

When your data has no natural identifier:

.. code-block:: python

   import uuid

   df = pd.DataFrame({
       "event": ["login", "click", "purchase"],
       "timestamp": ["2024-01-01 10:00", "2024-01-01 10:05", "2024-01-01 10:12"],
   })

   # Option 1: Sequential integers (simplest)
   df["id"] = range(len(df))

   # Option 2: UUIDs (globally unique)
   df["id"] = [str(uuid.uuid4()) for _ in range(len(df))]

   # Option 3: Hash of content (deterministic)
   df["id"] = df.apply(lambda r: f"{r['event']}_{r['timestamp']}", axis=1)

   context = ContextBuilder().add_entity("Event", df, id_col="id").build()

Type Coercion
~~~~~~~~~~~~~

**ID types must match between entity and relationship tables.**  If your
entity has integer IDs but your relationship CSV loads them as strings,
the join will produce zero results.

.. code-block:: python

   # Entity: integer IDs
   people = pd.DataFrame({"person_id": [1, 2, 3], "name": ["A", "B", "C"]})

   # Relationship: string IDs (common after CSV loading)
   knows = pd.DataFrame({"from": ["1", "2"], "to": ["2", "3"]})

   # Fix: convert to matching types before loading
   knows["from"] = knows["from"].astype(int)
   knows["to"] = knows["to"].astype(int)

Troubleshooting Common Data Issues
------------------------------------

Empty Query Results
~~~~~~~~~~~~~~~~~~~

**Symptom**: ``MATCH (a)-[:REL]->(b)`` returns an empty DataFrame.

**Most likely cause**: ID type mismatch between entity and relationship.

.. code-block:: python

   # Diagnose: check the dtypes
   print(entity_df["customer_id"].dtype)      # int64
   print(relationship_df["customer_id"].dtype) # object (string!)

   # Fix: align the types
   relationship_df["customer_id"] = relationship_df["customer_id"].astype(int)

**Other causes**:

- Relationship ``source_col``/``target_col`` point to wrong columns
- IDs in the relationship don't exist in the entity table (dangling references)

KeyError: ``__ID__``
~~~~~~~~~~~~~~~~~~~~

**Symptom**: ``KeyError: '__ID__'`` when loading data.

**Cause**: Using ``from_dict()`` or ``from_dataframe()`` without providing
an ``__ID__`` column or ``id_col`` parameter.

.. code-block:: python

   # This fails — no __ID__ and no id_col
   df = pd.DataFrame({"name": ["Alice", "Bob"]})
   table = EntityTable.from_dataframe("Person", df)  # KeyError!

   # Fix option 1: add __ID__ column
   df["__ID__"] = [1, 2]

   # Fix option 2: use ContextBuilder with id_col
   context = ContextBuilder().add_entity("Person", df, id_col="name").build()

   # Fix option 3: let ContextBuilder auto-generate IDs (omit id_col)
   context = ContextBuilder().add_entity("Person", df).build()

Duplicate ID Warning
~~~~~~~~~~~~~~~~~~~~

**Symptom**: ``MATCH`` returns unexpected extra rows (cross-join behavior).

**Cause**: Multiple rows share the same ``__ID__`` within an entity type.
Each duplicate multiplies the result rows.

.. code-block:: python

   # Diagnose: check for duplicates
   dupes = df[df["customer_id"].duplicated(keep=False)]
   print(f"Found {len(dupes)} duplicate IDs")

   # Fix: deduplicate before loading
   df = df.drop_duplicates(subset=["customer_id"], keep="first")

Missing Relationship Columns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Symptom**: ``ValueError`` or ``KeyError`` when adding a relationship.

**Cause**: The ``source_col`` or ``target_col`` name doesn't match any
column in the data.

.. code-block:: python

   # Check actual column names
   print(df.columns.tolist())
   # ['cust_id', 'prod_id', 'qty']  — not "customer_id"!

   # Fix: use the actual column names
   context = ContextBuilder().add_relationship(
       "BOUGHT", df,
       source_col="cust_id",      # matches the actual column
       target_col="prod_id",
   )

Quick Reference
---------------

.. list-table::
   :widths: 25 25 50
   :header-rows: 1

   * - Internal column
     - Your parameter
     - Purpose
   * - ``__ID__``
     - ``id_col="your_column"``
     - Unique identifier for each entity row
   * - ``__SOURCE__``
     - ``source_col="your_column"``
     - Which entity a relationship starts from
   * - ``__TARGET__``
     - ``target_col="your_column"``
     - Which entity a relationship points to

**Rules:**

- Entity IDs must be unique within each entity type
- Relationship source/target values must match entity IDs (same type)
- If ``id_col`` is omitted, sequential integers are auto-generated
- ``source_col`` and ``target_col`` are required for relationships

Next Steps
----------

* :doc:`../tutorials/graph_modeling` — graph design patterns and best practices
* :doc:`../tutorials/data_etl_pipeline` — building full ETL pipelines
* :doc:`error_handling` — understanding PyCypher error messages
