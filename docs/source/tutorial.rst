Tutorial: Building ETL Pipelines with pycypher-nmetl
==============================================

This tutorial will guide you through the process of creating ETL (Extract, Transform, Load) pipelines using the pycypher-nmetl packages. We'll cover both basic and advanced usage patterns, with clear examples at each step.

Introduction
-----------

The pycypher-nmetl project consists of three main packages:

1. **PyCypher**: Parses Cypher queries into Python objects
2. **NMETL**: Declarative ETL framework using PyCypher
3. **FastOpenData**: Utilities for working with open data sources

Together, these packages provide a powerful framework for building declarative ETL pipelines that leverage Cypher queries for data transformation and loading.

Prerequisites
------------

Before starting this tutorial, make sure you have:

* Python 3.12 or higher
* pycypher-nmetl installed (see :doc:`installation`)
* Basic understanding of Cypher query language
* Basic understanding of ETL concepts

Basic ETL Pipeline
-----------------

Let's start with a simple ETL pipeline that:

1. Extracts data from a CSV file
2. Transforms the data by adding a new column
3. Loads the data to another CSV file

First, let's create a sample CSV file with some data:

.. code-block:: python
   :caption: create_sample_data.py

   import pandas as pd
   
   # Create a sample DataFrame
   data = {
       'person_id': [1, 2, 3, 4, 5],
       'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
       'age': [25, 32, 45, 28, 36]
   }
   
   df = pd.DataFrame(data)
   
   # Save to CSV
   df.to_csv('people.csv', index=False)

Now, let's create a simple ETL pipeline using the pycypher-nmetl packages:

.. code-block:: python
   :caption: simple_etl.py

   from pycypher import CypherQuery
   from nmetl import ETLPipeline
   
   # Create a Cypher query to extract data
   query = CypherQuery("""
       MATCH (p:Person)
       WHERE p.age > 30
       RETURN p.person_id, p.name, p.age
   """)
   
   # Create an ETL pipeline
   pipeline = ETLPipeline()
   
   # Add the source (extract)
   pipeline.add_source(query)
   
   # Add a transformation
   pipeline.add_transformation(lambda data: data.assign(
       age_group=data.age // 10 * 10,
       is_senior=data.age >= 40
   ))
   
   # Add a sink (load)
   pipeline.add_sink("people_transformed.csv")
   
   # Execute the pipeline
   pipeline.execute()

This simple pipeline:
1. Extracts data using a Cypher query that matches Person nodes with age > 30
2. Transforms the data by adding two new columns: age_group and is_senior
3. Loads the transformed data to a CSV file

Declarative ETL with Configuration Files
----------------------------------------

For more complex ETL pipelines, pycypher-nmetl supports a declarative approach using YAML configuration files. This approach is more maintainable and allows for separation of concerns.

Let's create a configuration file for our ETL pipeline:

.. code-block:: yaml
   :caption: etl_config.yaml

   fact_collection: my_fact_collection
   run_monitor: true
   logging_level: DEBUG
   data_sources:
     - name: people_data
       uri: file://{CWD}/people.csv
       data_types:
         name: String
         age: Integer
       mappings:
         - identifier_key: person_id
           label: Person
         - identifier_key: person_id
           attribute_key: name
           attribute: name
         - identifier_key: person_id
           attribute_key: age
           attribute: age

Now, let's use this configuration file to create our ETL pipeline:

.. code-block:: python
   :caption: declarative_etl.py

   from nmetl.configuration import load_session_config
   from nmetl.trigger import VariableAttribute
   
   # Load session from configuration
   session = load_session_config("etl_config.yaml")
   
   # Define a trigger for data transformation
   @session.trigger(
       """
       MATCH (p:Person)
       WHERE p.age > 30
       RETURN p.person_id, p.name, p.age
       """
   )
   def process_senior_people(results):
       # Process the results
       print(f"Found {len(results)} people over 30")
       return results
   
   # Define a trigger to add a new attribute
   @session.trigger(
       """
       MATCH (p:Person)
       RETURN p.age
       """
   )
   def calculate_age_group(age) -> VariableAttribute["p", "age_group"]:
       return age // 10 * 10
   
   # Run the ETL pipeline
   session.start_threads()
   session.block_until_finished()

This declarative approach:
1. Loads the configuration from a YAML file
2. Defines triggers using Cypher queries and Python functions
3. Automatically processes the data based on the configuration and triggers

Working with Multiple Data Sources
----------------------------------

In real-world scenarios, ETL pipelines often need to integrate data from multiple sources. Let's extend our example to include multiple data sources:

.. code-block:: yaml
   :caption: multi_source_config.yaml

   fact_collection: my_fact_collection
   run_monitor: true
   logging_level: DEBUG
   data_sources:
     - name: people_data
       uri: file://{CWD}/people.csv
       data_types:
         name: String
         age: Integer
       mappings:
         - identifier_key: person_id
           label: Person
         - identifier_key: person_id
           attribute_key: name
           attribute: name
         - identifier_key: person_id
           attribute_key: age
           attribute: age
     - name: hobbies_data
       uri: file://{CWD}/hobbies.csv
       data_types:
         hobby: String
       mappings:
         - identifier_key: hobby_id
           label: Hobby
         - identifier_key: hobby_id
           attribute_key: hobby
           attribute: name
     - name: person_hobby_data
       uri: file://{CWD}/person_hobbies.csv
       mappings:
         - source_key: person_id
           target_key: hobby_id
           relationship: ENJOYS
           source_label: Person
           target_label: Hobby

Let's create the additional sample data files:

.. code-block:: python
   :caption: create_multi_source_data.py

   import pandas as pd
   
   # Create hobbies data
   hobbies_data = {
       'hobby_id': [1, 2, 3, 4],
       'hobby': ['Reading', 'Hiking', 'Cooking', 'Gaming']
   }
   
   hobbies_df = pd.DataFrame(hobbies_data)
   hobbies_df.to_csv('hobbies.csv', index=False)
   
   # Create person-hobby relationships
   person_hobbies_data = {
       'person_id': [1, 1, 2, 3, 4, 5, 5],
       'hobby_id': [1, 4, 2, 3, 1, 2, 3]
   }
   
   person_hobbies_df = pd.DataFrame(person_hobbies_data)
   person_hobbies_df.to_csv('person_hobbies.csv', index=False)

Now, let's use this configuration to create a more complex ETL pipeline:

.. code-block:: python
   :caption: multi_source_etl.py

   from nmetl.configuration import load_session_config
   from nmetl.trigger import VariableAttribute, NodeRelationship
   
   # Load session from configuration
   session = load_session_config("multi_source_config.yaml")
   
   # Define a trigger to find people and their hobbies
   @session.trigger(
       """
       MATCH (p:Person)-[:ENJOYS]->(h:Hobby)
       RETURN p.name AS person_name, h.name AS hobby_name
       """
   )
   def process_person_hobbies(results):
       for result in results:
           print(f"{result['person_name']} enjoys {result['hobby_name']}")
       return results
   
   # Define a trigger to count hobbies per person
   @session.trigger(
       """
       MATCH (p:Person)-[:ENJOYS]->(h:Hobby)
       WITH p, COUNT(h) AS hobby_count
       RETURN p.person_id, hobby_count
       """
   )
   def calculate_hobby_count(person_id, hobby_count) -> VariableAttribute["p", "hobby_count"]:
       return hobby_count
   
   # Run the ETL pipeline
   session.start_threads()
   session.block_until_finished()

This multi-source ETL pipeline:
1. Loads data from three different CSV files
2. Establishes relationships between people and their hobbies
3. Processes the data using Cypher queries and Python functions
4. Adds derived attributes based on the relationships

Advanced Features
----------------

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
