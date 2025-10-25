# NMETL Tutorial/Manifesto: Pythagoras and His Data Catalog

This section will be part tutorial, taking you through a complete example of using all of NMETL's basic features. But we will also include some rants about the design philosophy of NMETL and why its approach to data is superior to the traditional methods.

## Overview

A young scientist named "Pythagoras" is working on a project cataloging the properties of various geometric shapes that he and his students have measured. The data is getting to be quite complicated, so Pythagoras is using NMETL to create this catalog.

This tutorial takes you through all the steps that Pythagoras needs to perform in order to get NMETL to automatically ingest all his students' data, validate it, generate new features, and write the output.

## Pythagoras

His first data source is very simple. It is a CSV file containing some measurements of various circles that his students have come across. Specifically, it records an identifier for each circle, its radius, and its color. Here is what that table looks like:

```{table} circle_radii_color.csv
| id | radius | color |
|---|---|---|
| circle_a | 12 | red |
| circle_b | 30 | blue |
| circle_c | 17 | blue |
| circle_d | 1 | green |
| circle_e | 3 | yellow |
```

Using NMETL, Pythagoras will ingest this raw data for processing. To do this, he will configure a "data source" by writing a simple YAML file containing the information that NMETL needs in order to understand the data.

### Data philosophy

NMETL has strong opinions about how to think about data. Let's break down these opinions and see how they guide the design and use of NMETL.

The first tenet of NMETL's data philosophy is that **we should always separate syntax from meaning**. All too often, we focus too hard on all the syntactic idiosyncracies of our data: What type of file is it? Is the data typed or untyped? How do we handle null values? What's the right key to join two tables together? What flavor of SQL are we using?

Of course, these are important questions. But they distract us from what the data actually **means**. What types of entities does the data describe? What are the characteristics of those entities? How are the different entities related to each other? What can those relationships and characteristics teach us about _other_ relationships and characteristics?

When we do ETL, the more time we can spend thinking about the meaning of the data, the more insight we gain. But if our time is dominated by problems arising from a complicated syntax, then we struggle to get to the insight. After all, syntax is quite arbitrary and can vary from one data source to another, obscuring the meaning of the data. If NMETL is guided by one idea, it's that **we spend too much time on syntax and not enough time on meaning**.

Following this idea, NMETL packs all the syntactic considerations into one place and gets all of that stuff out of the way first, before any more processing happens. We can't ignore the syntax, but we can limit the amount of brainpower we have to devote to it. Having dealt with the syntax up-front, we are free to forget about it and devote our limited brainpower to high-value questions.

Thus, all of the syntactic considerations are dealt with immediately upon ingesting the raw data. During the data ingestion step, each data source is consumed and all of the data is transformed into a single, consistent data model. So let's get clear on how NMETL thinks about this data model.

A "model" is just a way of organizing and thinking about a domain. That domain could contain a set of financial transactions, customer interactions, surveys, weather patterns, or (in Pythagoras' case) a set of geometric shapes. Regardless of the domain, our data model will boil down to a few key concepts: Entities, Relationships, and Attributes.

```{note} Conventions
For clarity, we'll use a few conventions throughout the tutorial. When referring to
an Entity type, such as "Customer" or "Transaction", we'll capitalize the word. In contrast,
if we're just referring to an actual customer -- the human being, not the abstract entity --
we'll just use "customer" like usual. We'll do the same for Attributes and Relationships. Specific
Attributes such as a Customer's name, will be written as code, as in `name`.
```

Entities are nothing more or less than the things our data is about. In a business context, those entities could be products, customers, stores, transactions, or what-have-you. Typically, there are several different _types_ of entities. Customers are obviously different entities from Transactions, even though they are related. Customers also have different Attributes than Transactions. For example, a Customer has a first name, but a Transaction has a date.

Entities often have Relationships to each other. A specific Customer may have performed a specific Transaction, so the customer has a relationship -- "Performed" -- with that Transaction.

The data model used by NMETL boils down all your data, no matter what the domain happens to be, into Entities, Relationships, and Attributes. The first thing that happens when data is ingested is that it is transformed into Entities, Relationships, and Attributes, and those are what's stored and processed later, not the raw data.

To see how this is done, we have to make a few simple observations. The first observation is that every Entity has a type and an identifier. The type is the general category of the Entity, such as "Customer" or "Circle". But in order to be useful, we have to have a way of identifying _which_ Entity we're talking about, so that we can re-identify them later. NMETL therefore has the concept of an "Identifier", which is like a name that uniquely picks out a specific Customer, Transaction, or other Entity. An Identifier can be thought of as a specific Attribute of an Entity whose value is always unique.

When we look at common types of data sources, we often see identifiers in specific table columns. For example, in the `circle_radii_color.csv` table, there's a column called `id`, which has a unique identifier for each of the circles. Usually, the identifier's location in a data source is so obvious we don't even think about it. But NMETL requires us to notice these things in order to get past the syntax as quickly as possible.

Looking back to that table again, we can now make some observations using the concepts that NMETL deploys. These are:

1. The domain includes a type of Entity which is a `Circle`.
1. Each `Circle` has an Identifier which is provided in the `id` column.
1. Those identifiers happen to be strings.
1. Each `Circle` has at least two different attributes: `radius`, which is provided in the `radius` column, and `color`, which is provided in the `color` column.
1. Finally, the Identifier itself is a kind of Attribute. Let's call that the `name`.

With all of that in mind, let's look at the data ingestion configuration:

```yaml
data_sources:
- name: circle_radii_color
  uri: file://data/circle_radii_color.csv
  mappings:
  - attribute_key: radius
    identifier_key: id
    attribute: radius
    label: Circle
  - attribute_key: id
    attribute: id
    identifier_key: id
    label: Circle
  - attribute_key: color
    identifier_key: id
    attribute: color
    label: Circle
  data_types:
    id: NonEmptyString
    radius: Float
    color: NonEmptyString
```

As you can see, this file can be thought of as a mapping from a data source to a data model. Let's go through step by step:

Each data source has a name and a URI. The name is arbitrary, but it has to be unique. NMETL uses the URI format for all data sources, regardless of whether they're local files, remote databases, Kafka streams, or anything else. We do not have to specify any additional information about the data source because everything is already in the URI.

The `mappings` section is where things get more interesting. Consider the first block under `mappings`. It says essentially: "Each `Circle` has an attribute called `radius`. You can find the value of the `radius` by looking at the column `radius`, and you can tell which Circle has that `radius` by checking the `id` column." In general:

1. `attribute_key`: The name of the column which stores the Attribute's value
1. `identifier_key`: The name of the column that identifies which Entity has that Attribute
1. `attribute`: The name of the attribute in the data model
1. `label`: The type of entity which has the attribute

Note that in this case, the column name and the attribute name happen to be the same. This is often the case, but it doesn't have to be. If the column were named "foo", then we would have `attribute_key: foo`.

The second block under `mappings` is similar. It says that each `Circle` has an Attribute called `id`, which is stored in the column called `id`, and that the `id` of that Circle is (of course) given in the very same column. Again, it's a common pattern to see keys like `id` repeated several times in the configuration.

And finally, the third block follows exactly the same pattern as the first. It says that there is an Attribute of each Circle called `color`. The column name for this Attribute also happens to be `color`, and the Circle which has the given `color` value is identified by the `id` column.

The last bit of information concerns the data types. It is a mapping from columns to types, much like you'd specify in other ETL systems. One difference here is that NMETL uses Pydantic types and allows you to define new types with Pydantic. It will try to cast the data into the appropriate type if it can, or will return an error if it cannot.

## Relationships

Often, the real value of a data set is only apparent when it's combined with other data sets, and especially when there are different types of Entities that have relationships to each other.

It's ben observed by Pythagoras's students that occasionally, a shape is inside another shape. They've compiled a table that has information about a new shape -- squares -- and which circles they are in. It looks like this:

```{table} squares_and_circles.csv
| square| circle | side |
|---|---|---|
| square_1 | circle_a | 2.5 |
| square_2 | circle_g | 3.4 |
| square_3 | circle_f | 6.1 |
| square_4 | circle_b | 2.1 |
```

Like many real-world data sets, the `squares_and_circles.csv` file combines information about Attributes and information about Relationships. In this case, there are identifiers for both squares and circles, and the side length of each square. Relationships are represented by two identifiers occurring on the same row. For example, the first row tells us that `square_1` is inside `circle_a`.

We ingest this new table by adding entries to the same YAML file. 

```yaml
- name: squares_and_circles
  uri: file://data/squares_and_circles.csv
  mappings:
  - attribute_key: side
    identifier_key: square
    attribute: side_length
    label: Square
  - attribute_key: square
    attribute: id
    identifier_key: square
    label: Square
  - attribute_key: circle
    identifier_key: circle
    attribute: id
    label: Circle
  - source_key: square
    target_key: circle
    source_label: Square
    target_label: Circle
    relationship: Inside
  data_types:
    circle: NonEmptyString
    square: NonEmptyString
    length: Float
```

The first three blocks under the `mappings` key are similar to the previous example. But the fourth block is where the relationship `Inside` is described. It says that when a Circle (identified by the `circle` column) and a Square (identified by the `square`) column are in the same row, then that Square is "Inside" that specific Circle.

## Next steps

NMETL, as you can see, forces you to think about the data model immediately, even before any data has been loaded. This is a little bit of extra work up-front, but it has an important advantage: **having written this configuration, you are now free to forget about the format of the data source entirely**. From now on, we won't care in the slightest about where a particular bit of data happened to come from. Indeed, we will never even refer to these data sources or their formats again throughout the rest of the project. Instead, we will only refer to what the data **means**: the Entities, Relationships, and Attributes.

As data professionals, we spend a lot of time calculating the values of new attributes based upon the attributes that we've already ingested. This typically involves remembering the structure of various tables and other data sources, and then figuring out how to design a pipeline of steps that join tables together, compute the values of new columns, and so on.

NMETL does not require us to do any of those things. Instead, we specify two facts: (1) a function that calculates the value of the attribute for one instance of it; and (2) the prerequisites for calculating that attribute. Those having been specified, NMETL figures out when to perform those calculations. There is no concept of a data "pipeline" or "directed acyclic graph (DAG)". There are only functions and their prerequisites.

For example, suppose that Pythagoras wants to have an "area" attribute for each circle. He needs to specify:

1. The function that calculates the area of a circle, 
1. the prerequisites for that function -- namely, that we require a Circle with a specific radius -- and 
1. the name and type of the attribute being calculated

NMETL uses Python decorators and type hints to represent (1)--(3). For example, to calculate the area of each Circle, Pythagoras could write:

```python
@session.trigger("(c: Circle) RETURN c.radius")
def circle_area(r: radius) -> VariableAttribute[c, area]:
    return 3.14 * (r ** 2)
```

This combination of decorator and function is called a **trigger** in NMETL. Using NMETL to do ETL largely consists in writing simple triggers such as this one. So let's go through it in detail.

Recall that in our data ingestion configurations, we specified that one of the data sources had information about the radii of circles. For convenience, here is the relevant block from the YAML file:

```yaml
data_sources:
- name: circle_radii_color
  uri: file://data/circle_radii_color.csv
  mappings:
  ...
  - attribute_key: radius
    identifier_key: id
    attribute: radius
    label: Circle
```

This block told NMETL that in the `circle_radii_color` table, each row gave the `radius` of a Circle. So we know that there are entities of type Circle and that they can have an Attribute `radius`. And if you look at the bottom of that configuration, it also said that `radius` was always a float.

We know that the area of a circle depends only on its radius. So whenever NMETL comes across information about the `radius` of a `Circle`, it has enough information to calculate the value of a new attribute, `area`. NMETL breaks down these conditions into a few different parts, which are all represented in the trigger code:

1. The text in the decorator (`"(c: Circle) RETURN c.radius"`) expresses the fact that if we have a Circle with the label `c`, we are to extract its `radius` (if one has been defined, of course).
1. The value of the `radius` is passed into the function as the variable `r`. Note that we've used Python type-hinting to specify that the input to the function is of type `radius`.
1. The function returns the value for the area by applying the usual formula.
1. The return annotation means that the result of the function is to be assigned to the object `c` (the Circle), and the attribute is to be called `area`.

If this were another ETL system, the next step would probably be to insert this function in some kind of pipeline that operates on the data sources and creates intermediate tables using SQL, perhaps. But we don't do that. Instead, we just define this trigger and let NMETL figure out the rest.

Notice that the trigger code doesn't refer to any data source whatsoever. That's on purpose. The trigger doesn't care about the data sources; it only cares about whether there's a `radius` value for a Circle. If there is, the trigger calculates the `area` and records it. If we add a new data source that also has values for `radius`, we make no changes elsewhere in the code -- the trigger will fire whenever there's a `radius`, regardless of where that value came from.

The trigger also doesn't care about the lineage of any particular fact that it uses to make its calculations. That is, it doesn't have to know that `radius` came directly from a data source and wasn't calculated from other attributes. In fact, if another function calculated the value of the `radius` after a length series of complicated steps, the trigger would automatically fire and compute the `area` for them, too, without Pythagoras having to make any changes.

ETL pipelines get complicated when there are many dependencies, so the next thing we'll consider is how to handle cases when one attribute's value depends on another attribute's value.

Suppose Pythagoras wants to label some Circles as "`big`" if their `area` is greater than 10. In a typical ETL pipeline, we'd lay out three steps: (1) ingesting the data and recording the `radius`; (2) calculating the `area` from the `radius`; and (3) calculating the value of `big` from the area. Predictably, we do **not** do that here. Instead, we just make another trigger:


```python
@session.trigger("(c: Circle) RETURN c.area")
def is_it_big(a: area) -> VariableAttribute[c, big]:
    return a > 10
```

Now, whenever an `area` is calculated, NMETL will know that it can calculate the value of `big`, which will turn out to be either `True` or `False`.

How do we handle cases where the `area` isn't defined for a specific Circle? The answer is that we do nothing. If a Circle never gets assigned an `area`, then it never gets a `big` value, either. If the `area` were ever to show up, NMETL would immediately calculate whether the Circle is `big`.

Again, it doesn't matter **where** the value of `area` came from. If Pythagoras were to stumble across a data source that directly recorded the `area` of various Circles, this trigger would immediately start calculating whether the Circle is `big` or not; he wouldn't have to make any modifications to the code.

## Interlude

I hope that at this point, Pythagoras is starting to see the value of NMETL's data philosophy. The strength of NMETL is not that it handles huge amounts of data (although it can), or that it's especially fast (it isn't). Rather, the strength of this design is that it allows you to **forget everything except what the data means**. Once your data source is defined, you never think about it again. Once you know how to calculate an attribute's value, you don't have to know where its dependencies happen to come from. If you add a new attribute that happens to come directly from a data source, you don't have to modify any downstream calculations to prevent any intermediate tables from corrupting your pipeline (because there is no "downstream" and there are no intermediate tables).

It is an important assumption in NMETL's data philosophy that real-world ETL projects, especially for machine learning, data science, and artificial intelligence applications, are **not** usually difficult because of the quantity or diversity of data. Much fuss is made about high-performance data processing, or the challenges of integrating data from different types of databases, for example. These used to be major obstacles, but in modern cloud infrastructure, they are much less of an issue. Even a consumer-grade laptop can easily handle very large amounts of data which used to require specialized infrastructure. And for truly enormous data sets, there are well-understood techniques and tools available.

The same applies to transformation of data and computation of new attributes for data science. Limits on computational power are not nearly as important as they used to be. For the vast majority of machine learning models and real-world data sets, an ordinary laptop or an inexpensive machine instance will do just fine. Of course, there are exceptions -- if you're doing genomics research on massive data sets, or building the next generation of LLM foundation model, then you've still got to reach for highly specialized hardware and data pipelines. But for the other 99% of us who are dealing with real-world business problems, our limitation isn't computational resources or data storage.

But we are still facing severe limits in our data engineering work. But those limits come from the *complexity* of the data, not its volume. Data pipelines quickly get out of control due to their complexity. We still need to hold in our minds a complete picture of all the steps and transformations of our data pipelines, including all the syntactic idiosyncratic differences between the various data sources. And if we use a standard tool such as Airflow, which requires us to build complex, multi-stage pipelines, we **increase** the complexity of the data rather than decrease it, since these processes require us to create intermediate tables and other data structures that have their own quirks and inconsistencies.

The result is overly complex, brittle, error-prone ETL processes that become less stable over time. Too often, the tools make ETL worse rather than better by increasing complexity instead of decreasing it. And ironically, we use those tools to handle massive volumes of data and highly expensive computations, despite the fact that we don't actually suffer from those problems nearly as much as we used to.

NMETL aims to reduce complexity, period. Once you've made the mental shift to think about your data's meaning -- that is, as a consistent data model -- and put aside pipelines in favor of simple annotated and decorated functions, you will find that your ETL processes have suddenly become simple. And better yet, when you add new computations and transformations to your data, you won't be making your project more complex or brittle.

## Attributes involving relationships and aggregations

So far, Pythagoras has ingested some data about specific shapes, including one relationship and a couple of attributes. But he'd like to calculate some attributes that depend on relationships and aggregations.

When Pythagoras was learning SQL, he learned that the `GROUP BY` operator was often used for these tasks. If you wanted to know the average population of the hamlets in each city-state, you'd select the average of the hamlet population `GROUP BY` city-state. But that's an operation done on tables, and there are no tables in NMETL. So what does Pythagoras do?

Let's take a specific example. Recall that sometimes a Square may be Inside a Circle; and sometimes, more than one Square can be inside a Circle.

