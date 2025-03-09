[![Install and run tests](https://github.com/zacernst/pycypher/actions/workflows/makefile.yml/badge.svg)](https://github.com/zacernst/pycypher/actions/workflows/makefile.yml)

[![Build Sphinx documentation](https://github.com/zacernst/pycypher/actions/workflows/docs.yml/badge.svg)](https://github.com/zacernst/pycypher/actions/workflows/docs.yml)

[![Deploy documentation to Github Pages](https://github.com/zacernst/pycypher/actions/workflows/pages/pages-build-deployment/badge.svg)](https://github.com/zacernst/pycypher/actions/workflows/pages/pages-build-deployment)

# Cypher and Declarative ETL with `pycypher`

Modern ETL pipelines are overly complex, brittle, inflexible, and error-prone. These problems are the result of having to specify complex data pipelines that procedurally transform data step-by-step into the specific format you need.

`pycypher` takes a completely different approach. It lets you design ETL processes **declaratively**. Instead of designing a long, complex pipeline, you simply define your data sources, the derived data features that you want, and the conditions that have to be met in order for each of those features to be computed. Then you let `pycypher` figure out the rest. There is literally no data pipeline, DAG, job scheduling, or other complexity; you get to focus on what your data *means*, not on how it happens to be formatted.

Best of all, this declarative approach is done using familiar Python constructs and simple YAML configurations.

* Data sources are defined in YAML.
* Data transformations are defined in simple type-hinted Python functions in which
  * new features are defined with ordinary Python functions;
  * the type hints of the function define the dependencies;
  * the return type defines how the result will be stored and referenced;
  * decorators containing Cypher queries tell the system when the function is supposed to be applied.

Another difference between `pycypher` and traditional ETL processes is that you never think about the format of your output. Instead, `pycypher` is completely agnostic as to format, storing all the data in simple key-value pairs called `Facts`. Whenever you want, a simple command will transform and export those `Facts` into tables, graphs, or what-have-you. The result is that you don't need to make commitments about the format of your output before you've built your ETL system.

## Declarative data modeling with `Facts`

Rather than thinking of your data as tables, columns, key-value pairs, graphs, or documents, `pycypher` lets you focus on the **meaning** of your data. Data is stored as individual `Facts`, which represent a simple fact about your domain. For example, instead of having a table like this:


**What, Not How:** Instead of directly manipulating data structures (rows, columns, vertices, edges, documents...) imperatively, you declare "facts" about your data.

**Atomic Units:** Facts are atomic, self-contained units of information. For example:
* `FactNodeHasLabel("person_1", "Person")` declares that an entity referred to as "person_1" is of the type "Person."
* `FactNodeHasAttributeWithValue("person_1", "name", "Alice")` declares that "person_1" has an attribute "name" with the value "Alice."
* `FactRelationshipHasLabel("rel_1", "KNOWS")` states that the relationship with id rel_1 has the label "KNOWS."

**System Manages State:** All your facts are stored in a `FactCollection` which acts like an embedded database. It is responsible for maintaining the consistency of your facts, looking up facts when necessary, and transforming your data into tables or other output formats. As a data engineer, your only jobs are to define your data sources and define the facts that are derived from those sources.

**Customizable Data Store:** Depending on your use-case, you can use a simple in-memory `FactCollection` or a distributed, highly available key-value store, or anything else, just by changing a configuration value. No code changes are required, and it's not difficult to customize your own `FactCollection` if your requirements aren't met by the built-in options. 

## Data sources

**YAML Configuration:** You declare how to ingest data from external sources (like CSV files or databases) using YAML configuration files.

**Streaming and Batch are Identical:** Facts are streamed from data sources by the same process, regardless of whether they happen to come from a CSV file or a Kafka stream.

**Mappings Define Relationships:** The mappings specify the connections between the external data structure and the facts that should be created.

Example:
```
    - attribute_key: city
      identifier_key: city_state
      attribute: city_name
      label: City
```
This declaration says: 
* Take the `city` column from the data source.
* Create a `city_name` attribute for nodes identified by the `city_state` key.
* All these entities should have the label `City`.

**No Data Wrangling:** The logic for extracting data, handling different data types, and generating facts is managed by the `DataSource` class and its associated mappings. You just declare the relationships, not the parsing and conversion details.

**Extend to New Data Sources:** A `DataSource` is simply a class that yields dictionaries. As such, it is usually trivial to write a custom `DataSource` class if your use-case isn't handled already.

## Define Reactions, Not Procedures

**Triggers:** Instead of writing complex pipelines for transforming tables or documents, `pycypher` uses a "trigger" concept that simplifies your ETL significantly. A "trigger" is a simple function that says, "Whenever a condition `C` is met, perform this function and store the result as a new `Fact`." 

**What Happens, Not How:** You define what should happen when specific conditions are met (e.g., a new fact is added) rather than how to detect these conditions or how to update the graph.

**Recursive Updates:** Triggers generate facts, which in turn may cause other triggers to fire. In this way, you can generate new data that has multiple levels of dependencies without having to explicitly define an entire pipeline. 

# Using `pycypher`

There are a few required steps to set up a minimal `pycypher` ETL process:

1. Get some raw data.
1. Define your data source(s) in a configuration file, along with a few global configuration options.
1. Write a simple python script that:
   - Imports `pycypher`
   - Loads the configuration file
   - Starts the `pycypher` processes
   - Exports the data

This is good enough to ingest some data, validate it, and write a couple of tables. We'll build on the example to handle additional requirements.

## Get some data

In this example, we'll suppose that a data science team needs to ingest some information about a few squares and circles. Squares have a side length and a color; circles have radii; and squares can contain circles.

There are a few CSV tables with the necessary information:

```csv
squares.csv

name,length,color
squarename1,1,blue
squarename2,5,red
squarename3,3,blue
squarename4,10,orange
```

```csv
circles.csv

circle_name,center_x,center_y,radius
circle_a,0,0,2
circle_b,1,1,3
circle_c,2,2,5
circle_d,3,3,7
circle_e,4,4,11
circle_f,5,5,13
circle_g,6,6,17
```

```csv
contains.csv

square,circle
squarename1,circle_a
squarename1,circle_b
squarename2,circle_c
```

## Figure out your model

The most important decision that you have to make when modeling your data is determining what the entities, attributes, and relationships should be.

This example is very simple, and the model is pretty obvious. There are clearly two different "things" that this data is concerned with: squares and circles. So we'll have two types of `Entity`: `Square` and `Circle`. In a more realistic example, your entities might be customers, sales, products, cities, states, transactions, emails, or what-have-you.

Having settled on our entities, we now ask what attributes those entities can have. A quick look at the data shows that squares's lengths and colors are represented in one table, and the radius of each circle is represented in another file. We we'll have `side_length` and `color` as attributes of `Squares` and `radius` as an attribute of `Circles`. (Note that we can call these attributes whatever we like -- we don't need to stick to exactly what happens to be column headers in the CSV files.)

Two additional attributes of Squares and Circles are special -- they each have an identifier that uniquely picks out an entity. From our CSV files, we can see that there is another column called simply `square`, which looks like it unqiuely identifies each `Square`. In another table, there is another column called `circle_name` that does the same thing for `Circles`. We'll call these special attributes "identifiers".

Finally it looks like there is one relationship in the data that exists, connecting `Squares` to `Circles`. It appears that each `Square` may optionally `contain` one or more `Circles`. That's the content of the third CSV file -- each row says, in effect, that the `Square` named under the `square` column `contains` the `Circle` under the `circle` column.

In real-world examples, you'll often have to revise your data model; but `pycypher` makes this fairly easy. So it's best to just get started with something and iterate.

## Write the configuration file

Now we're ready to write a configuration file (in YAML format). There are a few global options that should go at the beginning of the file.
Here is a simple configuration file that we'll save as `ingest.yaml`. For now, we'll skip the explanation of these options and just set them to default values like so:

```yaml
fact_collection: null
fact_collection_class: null
run_monitor: true
logging_level: DEBUG
```

Now for the interesting part. We'll creste a section called `data_sources` that tells `pycypher` how to read each data source (i.e. CSV file).

In our example, there are three data sources -- one for each CSV file. Each data source has a few columns. We need to tell `pycypher` how to interpret each column. That requires a few pieces of information:

1. The name of the column
1. The column that contains the identifier for that entity
1. The name of the attribute represented by that column
1. The type of entity described by the column

For example, let's look at the first table, which I'll copy here for convenience:

```csv
squares.csv

name,length,color
squarename1,1,blue
squarename2,5,red
squarename3,3,blue
squarename4,10,orange
```

We interpret this table in the following way:

1. Each row contains information about a `Square`.
1. The identifier for each `Square` is listed under the `name` column.
1. The attribute `side_length` is given by the `length` column.
1. The `color` of the square is under the `color` column.

With that interpretation in hand, we begin writing the `data_source` configuration like so:

```yaml
data_sources:
- name: squares_table
  uri: file:///path/to/data/squares.csv
  mappings:
  - attribute_key: length
    identifier_key: name
    attribute: side_length
    label: Square
  - attribute_key: name
    identifier_key: name
    attribute: name
    label: Square
  - attribute_key: color
    identifier_key: name
    attribute: square_color
    label: Square
  - identifier_key: name
    label: Square
```

At the start, we give the data source a `name` and provide a `uri` which in the case is simply the pathname to the file. It's necessary to follow the appropriate standards for URIs; we've standardized on URIs so that we can handle remote data sources, APIs, and so on consistently.

Let's look at the first block under `mappings`. The name of the column is given by the `attribute_key`, which in the case is `length`. We're giving the attribute itself a slightly more descriptive name -- `side_length` -- which is under the `attribute` key. As we said above, the identifier for each `Square` is listed under the `name` column, which is why we have that as the `identifier_key`. Finally, we say explicitly that this entity is a `Square` with the `label` key.

The next two blocks work similarly. Then we separately have a short block that tells `pycypher` that the `name` column has the identifier for `Squares`.

> [!NOTE]
> Observant readers will notice that we could in principle simplify the configuration because there are some redundancies. This will be done in a future version of `pycypher`. For now, we're keeping everything very explicit at the cost of some redundancy.

There is one other type of mapping -- for relationships. Relationships link together two entities, but don't have attribute values such as `side_length` or `color`. The only relationship in this simple example is given by the `contains.csv` table, and its configuration is:

```
data_sources:
...
- name: contains_table
  uri: file:///path/to/data/contains.csv
  mappings:
  - attribute_key: square
    identifier_key: square
    attribute: name
    label: Square
  - attribute_key: circle
    identifier_key: circle
    attribute: name
    label: Circle
  - source_key: square
    target_key: circle
    source_label: Square
    target_label: Circle
    relationship: contains
```

The first two blocks tell `pycypher` that the `square` and `circle` columns contain the identifiers for each `Square` and `Circle`. The third block says that the *source* of each relationship is the entity named by the `square` column; the *target* of each relationship is named by the `target_key` column. Thus, each row of the table tells us that the `Square` listed under the `square` column `contains` the `Circle` listed under the `circle` column.

Each data source also has a `data_types` block, which tells `pycypher` the intended data type for each value. We follow Pydantic conventions here -- any built-in Pydantic data type is valid. `pycypher` will try to cast each value into the intended type, and will report an error if it's unable to do so. This is especially important for data structures such as CSVs, which have no type system built-in.

This leaves us with the following configuration, which is listed here in its entirety:

```yaml
data_sources:
- name: squares_table
  uri: file:///path/to/data/squares.csv
  mappings:
  - attribute_key: name
    identifier_key: name
    attribute: name
    label: Square
  - attribute_key: length
    identifier_key: name
    attribute: side_length
    label: Square
  - attribute_key: color
    identifier_key: name
    attribute: square_color
    label: Square
  - identifier_key: name
    label: Square
  data_types:
    name: NonEmptyString
    length: Float
    color: String
- name: circles_table
  uri: file:///path/to/data/circles.csv
  mappings:
  - attribute_key: circle_name
    identifier_key: circle_name
    attribute: identification_string
    label: Circle
  - attribute_key: center_x
    identifier_key: circle_name
    attribute: x_coordinate
    label: Circle
  - attribute_key: center_y
    identifier_key: circle_name
    attribute: y_coordinate
    label: Circle
  - identifier_key: circle_name
    label: Circle
- name: contains_table
  uri: file:///path/to/data/contains.csv
  mappings:
  - attribute_key: square
    identifier_key: square
    attribute: name
    label: Square
  - attribute_key: circle
    identifier_key: circle
    attribute: name
    label: Circle
  - source_key: square
    target_key: circle
    source_label: Square
    target_label: Circle
    relationship: contains
  data_types:
    square_name: NonEmptyString
    circle_name: NonEmptyString
```

## Execute a job with the `Session` class

To run the configured job, you just import the package, point it at the configuration file, and start it up. The complete code to do this is:

```python
from pycypher.util.session_loader import load_session_config

session = load_session_config(
    "/path/to/ingest.yaml"
)

session.start_threads()
session.block_until_finished()
```

This code creates a `Session` object, which coordinates our ETL jobs. Jobs are multithreaded, and run in the background by default. So we have to start the threads and then (in this case) we'll wait until it's finished (which is a second or two).

After the `session` has finished, it will contain a `FactCollection` with all the `Fact` objects that it's created in the course of parsing your data sources. You can see these facts by printing them:

```python
print(session.fact_collection.facts)
```

Of course, a collection of disorganized facts is not very useful. But `pycypher` creates this pile of facts as a flexible storage format so that the data can easily be shaped later into whichever form you like. We can create a table containing all the information about a specific type of entity with one command:

```pycypher
session.write_entity_table('file:///path/to/output/square_table.parquet', entity='Square')
```

> [!NOTE]
> The syntax of the `write_entity` table is under development and may not work as written quite yet.

And if you want to write a series of tables containing all the information about each entity, its attributes, and any relationships, you use:

```python
session.write_tables('file:///path/to/directory/for/output/tables'))
```

This will create a file for each entity type (e.g. `Square` and `Circle`), plus a file for each relationship (e.g. `contains`). CSV and Parquet formats are supported out of the box.

> [!NOTE]
> Same warning as above.


More documentation is [here](https://zacernst.github.io/pycypher/).

## Derived attributes

Of course, your ETL requirements aren't satisfied by simply ingesting data -- you want to create new attributes and relationships based on your specific requirements. This is where `pycypher` starts to show its value.

Normally, you'd think about loading your data into one or more tables (e.g. Pandas DataFrames) and performing table operations on them, such as joining tables together, creating new columns, dropping columns, and so on. In `pycypher`, there simply are no tables, so there are no such table-based operations. Instead, you write simple Python functions that can take one or more data points and compute a derived attribute. This is accomplished by decorating a function with a simple Cypher expression, and making sure that the return value is properly type-hinted.

For example, suppose your data scientists come to suspect that having the area of the squares calculated would be valuable. But as you will recall, we only have the length of each side of our squares recorded. Fortunately, a little research has revealed that there is a way to calculate the area of a square if you have the length of its sides. The function is this:

```python
def compute_area(side_length: float) -> float:
    return side_length ** 2
```

In `pycypher`, instead of performing a table-level operation to create a new column called `area`, you modify that simple `compute_area` function, and let `pycypher` apply it whenever it's needed. To do this, you have to tell `pycypher` a few things:

1. What types of new facts will trigger the function to run?
1. What data points are passed into the function as arguments?
1. What attribute is being set by the output of the function?

All three questions are answered by applying a combination of decorators and type hints. Let's modify the `compute_area` function to make all this happen:

```python
from pycypher.util.session_loader import load_session_config

session = load_session_config(
    "/path/to/ingest.yaml"
)

@session.cypher_trigger(
  'MATCH (s:Square) WHERE EXISTS(s.side_length) 
  WITH s.side_length AS side_length 
  RETURN side_length")
def compute_area(side_length: float) -> VariableAttribute['s', 'area']:
    return side_length ** 2

session.start_threads()
session.block_until_finished()
```

Most of the code block is identical to the earlier example. The only difference is the `compute_area` function and its decorator. Let's step through the function to understand how it all fits together.

### The decorator and function signature

After you've created your `Session` object (usually by loading a configuration file), you can call a method `cypher_trigger` on that object as a decorator.

The string inside the decorator is a vaid Cypher query. If you're not familiar with Cypher, don't worry -- a few patterns suffice for the vast majority of use-cases. For now, you only need to note that this query is matched whenever a `Square` object has the `side_length` attribute set. When that is matched, the Cypher query will pull out of the `side_length` attribute of the associated `Square` object, which we call `s`.

What this means is that whenever a `Square` acquires a value for `side_length`, this decorated function will be triggered.

You'll notice that we return the `side_length` attribute `AS side_length` in the `RETURN` clause. Because it's been named `side_length`, this value will be passed to the `compute_area` function as the variable `side_length`. In other words, the name(s) of attributes in the `RETURN` clause correspond to the function arguments, and the function will receive those values in its arguments accordingly.

Accordingly, whenever you apply the decorator, a check is automatically performed to ensure that the names in the `RETURN` clause match the variables in the function signature. An exception will be raised if there is a discrepency.

### Return type annotation

You'll also notice that the return type has been annotated as a `VariableAttribute`. This `VariableAttribute` is a special type that requires two string arguments. The first corresponds to the variable in the Cypher query that will have its attribute set by the value of the function, and the second is the name of that attribute.

For example, in our case, the return annotation is `VariableAttribute['s', 'area']`. You'll notice that `s` is used in the Cypher clause to refer to the `Square` object. So this annotation says, "Take the value returned by this function and set the `area` attribute on the `Square` that's referred to as `s`.

## Extending the example

Having added the `area` function,  we can define additional attributes that should be calculated using that value. Suppose that your data scientists have discovered that when a `Square` has an `area` greater than `100`, that's significant for their model. So we'd like to have a new attribute `big` which is `True` if the `area` is greater than `100` and `False` otherwise.

This is pretty easy to do:

```python
@session.cypher_trigger('MATCH (s:Square) WHERE EXISTS(s.area) WITH s.area AS square_area RETURN square_area')
def compute_bigness(square_area: float) -> VariableAttribute['s', 'big']:
    return square_area > 100)
```

As you can see, the decorator looks a lot like the earlier one, except that we're returning the value of `square_area`. Just as we'd expect, the function signature has `square_area` as its argument, and the return annotation says that we're setting the attribute `big` on the object designated by `s`. And of course, the function will return `True` just in case the area is greater than one-hundred, and `False` otherwise.

Adding this decorated function is all you need to do in order to have `pycypher` compute the `big` attribute. You do **not** need to tell the system that it depends on the first function, and you especially do not need to explicitly define a pipeline or DAG (directed acycylic graph) of jobs. The system will handle those dependencies itself. And of course, you can have functions that use the `big` attribute, or functions that require several different attributes. But you never need to keep track of the entire data processing pipeline; instead, you only provide functions that have simple inputs and outputs. So long as they are decorated and annotated appropriately, `pycypher` does the heavy lifting of coordinating all of the data processing.

If you want to see your data, you can export it to a file using the same process as above. If you export the table for `Square` entities, then you'll find new columns `area` and `big` that are populated correctly.

We'll get into some more complicated examples and optional bits of functionality later in the documentation.

## Derived columns

As we've seen, the concept of an entity with a *unique identifeier* or *name* is central to how `pycypher` works. Unfortunately, it is a common situation to discover that although there is a unique identifier for a type of entity, it's spread across more than one column. In this event, there is no single column you can use to refer to the identifier for an entity, as we did with the `name` column in our earlier example.

When this happens (and in other sorts of cases, too), it can be convenient to do a simple transformation on a column or set of columns to create a new key-value pair for your data source.

For example, the US Census Bureau assigns a unique number to each state and territory of the United States. They also assign a number to each county in the United States. Within each state, the county numbers are unique, but they often collide with the numbers in different states. Thus, you might see a CSV file that looks something like this:

```csv
state_id,county_id,state_name,county_name
1,1,Alabama,Autauga
1,2,Alabama,Baldwin
1,3,Alabama,Barbour
1,4,Alabama,Bibb
1,5,Alabama,Blount
2,1,Alaska,Anchorage
2,2,Alaska,Aleutians
2,3,Alaska,Bethel
2,4,Alaska,Bristol
```

If you need a unique identifier for each county, then a good strategy is to concatenate the id for the state and the id for the county. This is where "derived columns" come in.

Creating a derived column is a lot like creating new attributes using a function decorator and type annotations.

For example, suppose you've configured the CSV file with the name `states_counties`. All you need to do in order to create a new key-value pair that contains the concatenated identifiers is to add a decorator to a function and set the return type annotation:

```python
@session.new_column('states_counties')
def unique_county_id(state_id, county_id) -> NewColumn['unique_county_id']:
    return str(state_id) + '_' + str(county_id)
```

Now, when you stream the rows from the data source, in addition to the columns, you'll also see a new key called `unique_county_id` that contains the concatenated state and county identifiers, joined with an underscore.

A couple of quick things to note about this example. First the decorator calls the `new_column` method of the `Session` class. Instead of taking a Cypher query as an argument, it takes the name of the data source. This name is the same one you used to create the `ingest.yaml` file. Second, notice that the return type annotation has the `NewColumn` type, which takes a single string argument. That argument will be used as the name of the new column when the data source is streamed through the system. If the argument names don't correspond to keys in your data source, or the data source name isn't named in your configuration file, an exception will be thrown.

> [!NOTE]
> Although it is possible to use this `new_column` mechanism to apply complicated functions that perform arbitrary calculations, it is strongly suggested that you only use it to do trivial operations on your raw data. The `cypher_trigger` mechanism is assumed to be your first choice for anything more complicated, and the code is optimized accordingly.

