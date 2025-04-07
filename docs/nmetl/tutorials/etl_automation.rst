Automating ETL Processes with NMETL
================================

Introduction
-----------

In this tutorial, you'll learn how to use the NMETL (New Methods for ETL) framework to automate Extract, Transform, Load (ETL) processes in a declarative way. Unlike traditional ETL tools that require you to define complex procedural pipelines, NMETL allows you to define what should happen when specific conditions are met, making your ETL processes more maintainable and easier to understand.

By the end of this tutorial, you'll understand how to:

1. Configure data sources using YAML
2. Define triggers for data transformations
3. Create custom data processing functions
4. Run and monitor your ETL pipeline

Prerequisites
------------

- Python 3.8 or higher
- Basic understanding of ETL concepts
- Familiarity with YAML syntax

Our Example: Customer Analytics Pipeline
---------------------------------------

For this tutorial, we'll build an ETL pipeline for a customer analytics system. We'll work with three datasets:

1. **Customers**: Basic customer information (ID, name, age, etc.)
2. **Purchases**: Customer purchase records (customer ID, product ID, amount, date)
3. **Products**: Product information (ID, name, category, price)

Our goal is to transform this raw data into useful analytics, such as:

- Total spending per customer
- Preferred product categories
- Customer segmentation based on spending patterns

Step 1: Setting Up Your Project
------------------------------

First, let's install the necessary packages:

.. code-block:: bash

    pip install pycypher-nmetl

Next, create a project directory structure:

.. code-block:: text

    customer_analytics/
    ├── data/
    │   ├── customers.csv
    │   ├── purchases.csv
    │   └── products.csv
    ├── config/
    │   └── ingest.yaml
    └── pipeline.py

Let's create our sample data files:

**customers.csv**:

.. code-block:: text

    customer_id,name,age,email,signup_date
    1001,John Smith,34,john.smith@example.com,2022-01-15
    1002,Jane Doe,28,jane.doe@example.com,2022-02-20
    1003,Robert Johnson,45,robert.j@example.com,2021-11-05
    1004,Emily Wilson,31,emily.w@example.com,2022-03-10
    1005,Michael Brown,39,michael.b@example.com,2021-12-18

**purchases.csv**:

.. code-block:: text

    purchase_id,customer_id,product_id,amount,purchase_date
    2001,1001,3001,129.99,2022-02-01
    2002,1002,3002,49.95,2022-02-25
    2003,1001,3003,89.50,2022-03-15
    2004,1003,3001,129.99,2022-01-20
    2005,1002,3004,199.99,2022-03-05
    2006,1004,3002,49.95,2022-03-20
    2007,1005,3005,299.99,2022-02-10
    2008,1003,3003,89.50,2022-02-28
    2009,1001,3004,199.99,2022-04-05
    2010,1005,3001,129.99,2022-03-25

**products.csv**:

.. code-block:: text

    product_id,name,category,price
    3001,Smartphone X,Electronics,129.99
    3002,Running Shoes,Apparel,49.95
    3003,Coffee Maker,Home,89.50
    3004,Wireless Headphones,Electronics,199.99
    3005,Smart TV,Electronics,299.99

Step 2: Creating the Configuration File
-------------------------------------

NMETL uses YAML configuration files to define data sources and their mappings. Let's create our ``ingest.yaml`` file:

.. code-block:: yaml

    fact_collection: null
    fact_collection_class: MemcacheFactCollection
    run_monitor: true
    logging_level: INFO

    data_sources:
    - name: customers
      uri: file://{CWD}/data/customers.csv
      mappings:
      - identifier_key: customer_id
        label: Customer
      - attribute_key: customer_id
        identifier_key: customer_id
        attribute: id
        label: Customer
      - attribute_key: name
        identifier_key: customer_id
        attribute: name
        label: Customer
      - attribute_key: age
        identifier_key: customer_id
        attribute: age
        label: Customer
      - attribute_key: email
        identifier_key: customer_id
        attribute: email
        label: Customer
      - attribute_key: signup_date
        identifier_key: customer_id
        attribute: signup_date
        label: Customer
      data_types:
        customer_id: NonEmptyString
        name: NonEmptyString
        age: Integer
        email: NonEmptyString
        signup_date: String

    - name: products
      uri: file://{CWD}/data/products.csv
      mappings:
      - identifier_key: product_id
        label: Product
      - attribute_key: product_id
        identifier_key: product_id
        attribute: id
        label: Product
      - attribute_key: name
        identifier_key: product_id
        attribute: name
        label: Product
      - attribute_key: category
        identifier_key: product_id
        attribute: category
        label: Product
      - attribute_key: price
        identifier_key: product_id
        attribute: price
        label: Product
      data_types:
        product_id: NonEmptyString
        name: NonEmptyString
        category: NonEmptyString
        price: Float

    - name: purchases
      uri: file://{CWD}/data/purchases.csv
      mappings:
      - identifier_key: purchase_id
        label: Purchase
      - attribute_key: purchase_id
        identifier_key: purchase_id
        attribute: id
        label: Purchase
      - attribute_key: amount
        identifier_key: purchase_id
        attribute: amount
        label: Purchase
      - attribute_key: purchase_date
        identifier_key: purchase_id
        attribute: date
        label: Purchase
      - source_key: customer_id
        target_key: purchase_id
        source_label: Customer
        target_label: Purchase
        relationship: MADE
      - source_key: purchase_id
        target_key: product_id
        source_label: Purchase
        target_label: Product
        relationship: FOR
      data_types:
        purchase_id: NonEmptyString
        customer_id: NonEmptyString
        product_id: NonEmptyString
        amount: Float
        purchase_date: String

This configuration defines:

1. Three data sources (customers, products, purchases)
2. The mappings between CSV columns and graph entities/attributes
3. Relationships between entities (Customer MADE Purchase, Purchase FOR Product)
4. Data types for each column

Step 3: Creating the ETL Pipeline
-------------------------------

Now, let's create our ``pipeline.py`` file to define our ETL logic:

.. code-block:: python

    from nmetl.configuration import load_session_config
    from nmetl.trigger import VariableAttribute
    from datetime import datetime
    from typing import Dict

    # Load the session from our configuration file
    session = load_session_config("config/ingest.yaml")

    # Define a trigger to calculate total spending per customer
    @session.trigger(
        """
        MATCH (c:Customer)-[:MADE]->(p:Purchase)
        WITH c.id AS customer_id, SUM(p.amount) AS total_spent
        RETURN customer_id, total_spent
        """
    )
    def calculate_total_spending(customer_id, total_spent) -> VariableAttribute["c", "total_spent"]:
        """Calculate the total amount spent by a customer."""
        return float(total_spent)

    # Define a trigger to determine customer spending level
    @session.trigger(
        """
        MATCH (c:Customer)
        WHERE EXISTS(c.total_spent)
        WITH c.id AS customer_id, c.total_spent AS total_spent
        RETURN customer_id, total_spent
        """
    )
    def determine_spending_level(customer_id, total_spent) -> VariableAttribute["c", "spending_level"]:
        """Categorize customers based on their spending level."""
        if total_spent < 100:
            return "Low"
        elif total_spent < 300:
            return "Medium"
        else:
            return "High"

    # Define a trigger to find preferred product category
    @session.trigger(
        """
        MATCH (c:Customer)-[:MADE]->(p:Purchase)-[:FOR]->(prod:Product)
        WITH c.id AS customer_id, prod.category AS category, COUNT(*) AS purchase_count
        ORDER BY purchase_count DESC
        RETURN customer_id, category, purchase_count
        """
    )
    def find_preferred_category(customer_id, category, purchase_count) -> VariableAttribute["c", "preferred_category"]:
        """Determine a customer's preferred product category based on purchase history."""
        return category

    # Define a trigger to calculate days since signup
    @session.trigger(
        """
        MATCH (c:Customer)
        WHERE EXISTS(c.signup_date)
        WITH c.id AS customer_id, c.signup_date AS signup_date
        RETURN customer_id, signup_date
        """
    )
    def calculate_days_since_signup(customer_id, signup_date) -> VariableAttribute["c", "days_since_signup"]:
        """Calculate the number of days since a customer signed up."""
        signup_date = datetime.strptime(signup_date, "%Y-%m-%d")
        current_date = datetime.now()
        return (current_date - signup_date).days

    # Define a trigger to create customer segments
    @session.trigger(
        """
        MATCH (c:Customer)
        WHERE EXISTS(c.spending_level) AND EXISTS(c.days_since_signup)
        WITH c.id AS customer_id, c.spending_level AS spending_level, c.days_since_signup AS days_since_signup
        RETURN customer_id, spending_level, days_since_signup
        """
    )
    def create_customer_segment(customer_id, spending_level, days_since_signup) -> VariableAttribute["c", "segment"]:
        """Create customer segments based on spending level and signup recency."""
        loyalty = "New" if days_since_signup < 90 else "Established"
        return f"{loyalty} {spending_level} Spender"

    # Run the ETL pipeline
    if __name__ == "__main__":
        print("Starting ETL pipeline...")
        session.start_threads()
        session.block_until_finished()
        print("ETL pipeline completed!")
        
        # Print some results
        print("\nCustomer Analytics Results:")
        print("--------------------------")
        
        # Get all customers with their calculated attributes
        customers = session.fact_collection.node_label_attribute_inventory()
        
        for customer_id, attributes in customers.get("Customer", {}).items():
            print(f"\nCustomer ID: {customer_id}")
            for attr_name, attr_value in attributes.items():
                print(f"  {attr_name}: {attr_value}")

Step 4: Running the Pipeline
--------------------------

To run the ETL pipeline, simply execute the Python script:

.. code-block:: bash

    python pipeline.py

The output will show the results of our ETL process, including:

- Total spending per customer
- Spending level categorization
- Preferred product categories
- Days since signup
- Customer segments

Understanding How It Works
------------------------

Let's break down the key components of our ETL pipeline:

Session Configuration
~~~~~~~~~~~~~~~~~~~

The ``load_session_config`` function loads our YAML configuration and creates a ``Session`` object that orchestrates the entire ETL process.

Triggers
~~~~~~~

Triggers are the heart of NMETL's declarative approach. Each trigger consists of:

- A Cypher query that defines when the trigger should fire
- A Python function that performs the transformation
- A return type annotation that specifies what attribute to set with the result

For example, in our ``calculate_total_spending`` trigger:

.. code-block:: python

    @session.trigger(
        """
        MATCH (c:Customer)-[:MADE]->(p:Purchase)
        WITH c.id AS customer_id, SUM(p.amount) AS total_spent
        RETURN customer_id, total_spent
        """
    )
    def calculate_total_spending(customer_id, total_spent) -> VariableAttribute["c", "total_spent"]:
        return float(total_spent)

This trigger:

1. Matches all customers and their purchases
2. Calculates the sum of purchase amounts for each customer
3. Sets the ``total_spent`` attribute on the customer node

Dependency Chain
~~~~~~~~~~~~~~

Triggers can depend on the results of other triggers. For example, ``determine_spending_level`` depends on ``calculate_total_spending`` having run first. NMETL automatically handles these dependencies.

Fact Collection
~~~~~~~~~~~~~

All the data is stored in a fact collection, which is a graph-like data structure. The ``node_label_attribute_inventory`` method gives us access to all the nodes and their attributes.

Advanced Features
---------------

Custom Column Transformations
~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can define custom transformations for columns using the ``new_column`` decorator:

.. code-block:: python

    @session.new_column("purchases")
    def calculate_discount(amount, product_id) -> NewColumn["discounted_amount"]:
        """Calculate discounted amount based on product ID."""
        discount_rates = {"3001": 0.1, "3002": 0.05, "3003": 0.15, "3004": 0.2, "3005": 0.1}
        discount = discount_rates.get(product_id, 0)
        return float(amount) * (1 - discount)

Relationship Triggers
~~~~~~~~~~~~~~~~~~~

You can also create triggers that establish relationships between entities:

.. code-block:: python

    @session.trigger(
        """
        MATCH (c:Customer), (p:Product)
        WHERE c.preferred_category = p.category
        RETURN c.id AS customer_id, p.id AS product_id
        """
    )
    def create_interested_in_relationship(customer_id, product_id) -> NodeRelationship["c", "INTERESTED_IN", "p"]:
        """Create an INTERESTED_IN relationship between customers and products in their preferred category."""
        return True

Monitoring
~~~~~~~~~

NMETL provides built-in monitoring capabilities. You can enable monitoring by setting ``run_monitor: true`` in your configuration file.

Best Practices
------------

1. **Start Simple**: Begin with basic transformations and gradually add complexity.
2. **Use Meaningful Names**: Choose descriptive names for triggers and attributes.
3. **Document Your Triggers**: Add docstrings to explain what each trigger does.
4. **Test Incrementally**: Test each trigger individually before running the full pipeline.
5. **Monitor Performance**: Use the built-in monitoring to identify bottlenecks.

Conclusion
---------

In this tutorial, you've learned how to use NMETL to create a declarative ETL pipeline for customer analytics. The key advantages of this approach include:

1. **Declarative**: You define what should happen, not how it should happen.
2. **Maintainable**: Each transformation is a self-contained function with clear inputs and outputs.
3. **Flexible**: You can easily add, remove, or modify transformations without rewriting the entire pipeline.
4. **Dependency Management**: The system automatically handles dependencies between transformations.

By leveraging NMETL's declarative approach, you can create ETL pipelines that are easier to understand, maintain, and extend.

Next Steps
---------

To further explore NMETL's capabilities, you might want to:

1. Connect to different data sources (databases, APIs, etc.)
2. Implement more complex transformations
3. Integrate with visualization tools to display the results
4. Set up scheduled runs for continuous data processing
