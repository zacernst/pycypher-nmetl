====================================================
Tutorial: Building ETL Pipelines with pycypher-nmetl
====================================================

This tutorial will guide you through the process of creating ETL (Extract, Transform, Load) pipelines using the pycypher-nmetl packages. We'll cover both basic and advanced usage patterns, with clear examples at each step.

Introduction
============

The pycypher-nmetl project consists of three main packages:

1. **PyCypher**: Parses Cypher queries into Python objects
2. **NMETL**: Declarative ETL framework using PyCypher
3. **FastOpenData**: Utilities for working with open data sources

To build an ETL pipeline, you'll primarily use the NMETL package, which imports PyCypher for parsing Cypher queries.

In this tutorial, you'll learn how to build a very simple ETL pipeline that uses NMETL's core features. We'll mention some of the more advanced features as we go, but detailed explanations of those will be left for later.

Before starting this tutorial, make sure you have the `pycypher-nmetl` repository and you've install the packages. For instructions, see :doc:`installation`.

Overview of the ETL Process
===========================

Using NMETL comes down to four steps:

1. Defining your data source(s)
2. Defining your triggers (transformations)
3. Running the pipeline
4. Exporting the results

Defining Your Data Sources and Model
------------------------------------  

For this tutorial, we'll use a couple of very simple CSV files, which you can find in the ``/tutorial`` directory of this repository. They are ``customers.csv`` and ``sales.csv``.

We'll assume we work for a WidgetCo, a company that sells widgets directly to customers at any of its three locations throughout the United States. Our data warehouse contains two CSV files, whose first few lines are:

.. code-block:: `
   :caption: customers.csv

   customer_id,customer_name,home_state,vip_status
   1,Alice,Alabama,True
   2,Bob,Texas,False
   3,Carol,Wisconsin,True

and:

.. code-block:: `
   :caption: sales.csv

   customer_id,widgets_purchased,transaction_date,transaction_id
   1,5,2023-01-01,123
   2,3,2023-01-02,456
   3,2,2023-01-03,789

In NMETL, it's a good idea to think about your data model right away. This comes down to making three sets of decisions:

1. What are the kinds of things we're dealing with?
2. What are the possible attributes of each kind of thing?
3. What relationships might we have between things?

We'll refer to the things in our data model as **entities** and their "types" as **labels**. For example, looking at the first CSV file, we'd say that Alice is an "entity" and that she has the label "Customer".

Each entity must have its own unique identifier that refers to a specific (e.g.) customer. In this case, each customer has an identifier in the ``customer_id`` column. Although we might find two customers named "Alice", there will only ever be one customer with the ``customer_id`` of 1.

Entities have "attributes". For example, Alice has the attribute "name" with the value "Alice". She is also enrolled into WidgetCo's VIP program, which means she is a "VIP" customer, which is an attribute with the value "True".

Coming up with those attributes for the "Customer" entity is pretty straightforward. But there are more difficult decisions to make about the data model. For example, should we consider "State" to be an attribute of the "Customer", or should it be its own entity, which Customers are related to? There's no definitive answer to this kind of question -- it just depends on the situation. As a rule, if you anticipate requiring attributes of things, then it makes sense to define them as entities in your data model. So for example, if we anticipate being interested in state-by-state data about total sales, then we probably want to have each State as its own entity. Let's do that.

With all of that having been decided, we're ready to define the first data source.

All the configuration for your project will go into a single YAML file. So we'll come back to this file periodically and add to it. For now, we'll create a file called ``tutorial.yaml`` and add the following:

.. code-block:: yaml
   :caption: tutorial.yaml

   data_sources:
     - name: customer_data
       uri: file:///path/to/the/file/customers.csv
       mappings:
         - identifier_key: customer_id
           label: Customer

As you can see, the ``data_sources`` section contains information about the first CSV file. The first two lines are fairly self-explanatory:

1. The name of the data source is ``customer_data``. Each data source must have a unique name.
2. The URI of the data source is ``file:///path/to/the/file/customers.csv``. NMETL always uses URIs to specify the location of a data source because it uses the URI to figure out how to access it and what format to expect.

The rest of that part of the YAML file relates to the data model we discussed above. The ``mappings`` section tells NMETL how to map the data in the CSV file to the entities and attributes in the data model. The ``data_types`` section tells NMETL what data types to expect for each column in the CSV file. So far, the YAML file only says that customers are uniquely identified by the value under the ``customer_id`` column and that they have the label "Customer".

As we mentioned above, we also know that each Customer has a name, so we'll add that information to the YAML file:

.. code-block:: yaml
   :caption: tutorial.yaml

   data_sources:
     - name: customer_data
       uri: file:///path/to/the/file/customers.csv
       mappings:
         - identifier_key: customer_id
           label: Customer
         - identifier_key: customer_id
           attribute_key: customer_name
           attribute: name
           label: Customer

Let's look at this last block because it follows an important pattern. In order to know that there's a Customer named "Alice", NMETL needs to infer a few facts from the first row of the table:

1. There is a Customer who is uniquely identified by the ID 1, which is listed under the ``customer_id`` column;
2. The column ``customer_name`` contains the name of that Customer;

In other words, the first row of that CSV file expresses the fact that "The Customer whose ID is 1 has the name 'Alice'". Accordingly, the four keys in that block have the following meanings:

1. ``identifier_key``: The column that contains the unique identifier for the entity. In this case, it's the ``customer_id`` column.
2. ``attribute_key``: The column that contains the attribute value. In this case, it's the ``customer_name`` column.
3. ``attribute``: The name of the attribute. In this case, it's "name".
4. ``label``: The label of the entity. In this case, the label is "Customer".

Finally, we have to tell NMETL what data types to expect for each column in the CSV file. We'll add that information to the YAML like so:

.. code-block:: yaml
   :caption: tutorial.yaml

   data_sources:
     - name: customer_data
       uri: file:///path/to/the/file/customers.csv
       mappings:
         - identifier_key: customer_id
           label: Customer
         - identifier_key: customer_id
           attribute_key: customer_name
           attribute: name
           label: Customer
       data_types:
         customer_id: PositiveInteger
         customer_name: NonEmptyString

In the ``data_types`` block, you'll have one key per column in the CSV file. The value of each key is the data type that NMETL should expect for that column. In this case, we're expecting the ``customer_id`` column to contain positive integers and the ``customer_name`` column to contain non-empty strings. NMETL uses Pydantic-style data types, including several custom types that are defined by NMETL. As the data source is read, NMETL will try to coerce the data into the types specified in this file. If it can't, then it will log an error and try to continue reading the rest of the data.

Let's add some more attributes of Customers to the YAML file:

.. code-block:: yaml
   :caption: tutorial.yaml

   data_sources:
     - name: customer_data
       uri: file:///path/to/the/file/customers.csv
       mappings:
         - identifier_key: customer_id
           label: Customer
         - identifier_key: customer_id
           attribute_key: customer_name
           attribute: name
           label: Customer
         - identifier_key: customer_id
           attribute_key: customer_id
           attribute: customer_id
           label: Customer
         - identifier_key: customer_id
           attribute_key: vip_status
           attribute: vip_status
           label: Customer
       data_types:
         customer_id: PositiveInteger
         customer_name: NonEmptyString
         vip_status: Boolean

Let's start by looking at the blocks we've added to the ``mappings`` section. The first block is the same as the one we added earlier, but the second block is new. It says that the ``customer_id`` column contains the value of the ``customer_id`` attribute of the Customer. So the ``identifier_key``, ``attribute_key``, and ``attribute`` are all the same. This redunandancy is a common pattern because we need to tell NMETL that not only do we use the ``customer_id`` column to identify Customers, but also that it is also an attribute of each Customer.

The third block is also new. It says that the ``vip_status`` column contains the value of the ``vip_status`` attribute of the Customer. It might seem counterintuitive to have the same value for ``attribute_key`` and ``attribute``, but this just means that the creators of the CSV file have sensibly named the column to reflect what it means.

And of course, we need to tell NMETL what data types those columns contain, so we've added ``vip_status: Boolean`` and ``customer_id: PositiveInteger`` to the ``data_types`` section.

Technically, we could stop here and run the pipeline, but we'd just end up with a table that's more or less just a part of the data source. So let's skip ahead to how we calculate new fetures based on the data we have.

Defining Your Triggers (Transformations)
----------------------------------------

First, we need a few important terminological distinctions.



Advanced Features
-----------------

Let's explore some advanced features of pycypher-nmetl:

1. **Derived Columns**: Creating new columns based on existing data
2. **Data Type Validation**: Ensuring data quality through type validation
3. **Complex Transformations**: Using Cypher for complex data transformations

.. code-block:: python
   :caption: advanced_etl.py

   from nmetl.configuration import load_session_config
   from nmetl.data_source import NewColumn
   from nmetl.trigger import VariableAttribute
   
   # Load session from configuration
   session = load_session_config("multi_source_config.yaml")
   
   # Define a new derived column
   @session.new_column("people_data")
   def full_details(name, age) -> NewColumn["description"]:
       return f"{name} (Age: {age})"
   
   # Define a trigger for complex transformation
   @session.trigger(
       """
       MATCH (p:Person)-[:ENJOYS]->(h:Hobby)
       WITH p, COLLECT(h.name) AS hobbies
       RETURN p.person_id, p.name, p.age, hobbies
       """
   )
   def create_profile(person_id, name, age, hobbies) -> VariableAttribute["p", "profile"]:
       hobby_list = ", ".join(hobbies)
       return {
           "name": name,
           "age": age,
           "hobbies": hobby_list,
           "is_senior": age >= 40,
           "hobby_count": len(hobbies)
       }
   
   # Run the ETL pipeline
   session.start_threads()
   session.block_until_finished()
   
   # Export the results
   from nmetl.writer import CSVWriter
   
   writer = CSVWriter("profiles.csv")
   writer.write_rows(session.rows_by_node_label("Person"))

This advanced ETL pipeline:
1. Creates a derived column that combines existing columns
2. Uses a complex Cypher query to collect related data
3. Creates a structured profile for each person
4. Exports the results to a CSV file

Conclusion
---------

In this tutorial, we've explored how to use the pycypher-nmetl packages to create ETL pipelines, from simple to complex. We've covered:

1. Basic ETL pipelines with source, transformation, and sink
2. Declarative ETL using YAML configuration files
3. Working with multiple data sources
4. Advanced features like derived columns and complex transformations

The pycypher-nmetl packages provide a powerful and flexible framework for building ETL pipelines that leverage the expressive power of Cypher queries for data transformation and loading.

Next Steps
---------

- Explore the API documentation for more details on the available classes and methods
- Check out the examples in the repository for more complex use cases
- Try integrating with Neo4j for graph database operations
