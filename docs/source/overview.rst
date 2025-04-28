.. _Overview:

========
Overview
========

**NMETL** (New Methods for ETL) is a Python library that provides a totally different approach to ETL. It is intended for data engineers, machine learning engineers, AI ops, or data scientists who have to manage their own data for experimentation or training.

If you're in this space, you've probably used other tools such as Airflow, Luigi, AWS Glue, MLFlow, and so on. For some environments, these tools work perfectly well. But there are cases in which they start to break down. If you've been involved in building or maintaining a very complex data pipeline, then you may have reached the point where your ETL tools start to get in the way.

You can see this dynamic as you continue to work on your data pipeline. At first, inserting a new transformation into your pipeline is easy, and doesn't take very much of your time. But a few weeks or months later, adding a new transformation has somehow become an arduous, time-consuming chore. You may have to back-engineer your entire data pipeline to find the critical places that need to be adjusted in order to fit the new calculations into the existing pipeline. As the pipeline gets more complex, this problem gets worse. Even with excellent tooling, the complexity eventually slows down development to a crawl. Whast's worse is that the data pipeline becomes brittle -- the smallest error or inconsistency can cause the entire pipeline to fail.

The purpose of NMETL is to flatten the complexity curve, so that your pipeline doesn't get more complex as you add more features. As you add new sources or features to your dataset, there is never any need to track all the other components of a data pipeline. Adding the hundredth feature is just as simple as adding the first.

The Big Difference
==================

The key difference between NMETL and other systems like Airflow is that whereas Airflow and its cousins are imperative, NMETL is declarative*. In Airflow, you specify a series of steps that the data must go through. In NMETL, you specify what the data means and how it's calculated; the machine figures out the steps.

Furthermore, NMETL doesn't force you to put the computational code in one place, the structure of the DAG in another, and so on. This becomes far too complex very quickly. Instead, all the information that a feature requires is contained in the very same function that calculates it. This includes all its dependencies, names of features, the data model, and even the documentation that will be picked up by the data catalog. That way, you never fall into the trap of discovering that your code and your configuration have gotten out of sync with each other. There is nothing to fall out of sync, because everything is in one place.

The ability to keep all your configuration, code, and documentation in one place has important benefits. For one, it reduces the number of places where an error can be introduced. Second, discoverability is far easier; indeed, you can onboard a new person to your project by pointing them at a single part of your codebase; they don't have to put together a jigsaw puzzle of configurations and code in order to grasp the "big picture" of what you're accomplishing. And most importantly, it does not separate the data *model* from the data *syntax*. In a traditional ETL pipeline, you might have a concept of a "Customer", but the customer is represented by different names, variables, columns, etc. throughout your code. In NMETL, the customer is always a customer; naming conventions are simple and consistent; the same concept always has the same name.

Streaming vs. Batch
-------------------

Other ETL systems force you to treat batch data differently from streaming data. For example, if you have a big table and a Kafka topic, and you need data from both, you'll probably find that you're forced to write one system for ingesting the table and a totally different system for reading from Kafka. Then you will face the challenge of somehow combining these different paradigms into a single format.

In NMETL, there is no difference between batch and streaming because literally everything is a stream. What you might think of as a "batch" is simply a stream that happens to stop eventually. But that's not an important difference for you or for NMETL. All data sources, regardless of whether they are CSV files stored in S3, a Kafka topic on a managed cluster in the cloud, or a Parquet file that you download from an FTP server, are configured *identically*. NMETL only requires that you specify the URI for the data, and then it will handle all the messy details itself. In fact, the first thing that NMETL does when it comes across a new data source is to create a queue for the data to flow through, and then it transforms each unit of data into a trivially simple Python dictionary or JSON blob. By the time you have to deal with the data in your declarative functions, you no longer have to remember what format the data is in. It's already been validated and transformed into Python objects. If you were to change a data source from, say, a CSV file to a Kafka topic, the necessary changes to your pipeline would be minimal, and you probably wouldn't have to change your code at all.

Validation
----------

We've all experienced the problem of discovering that one or two rows in an enormous table have somehow failed to follow the expected conventions. And because ETL pipelines typically work on entire tables at a time, this breaks everything. Indeed, tiny errors often have an enormous blast radius, which forces you to restart major portions of your pipeline. In NMETL, this doesn't happen. For example, if a row in a database table happens to have a bad value, NMETL skips it, logs the error for you to examine later, and gets on with the rest of the work. Small errors don't grow into large problems because NMETL's streaming paradigm naturally keeps the "blast radius" of any error as small as possible. And as soon as you've fixed the error, you can retroactively put the data into the pipeline without stopping and restarting anything.

Validation of your data happens automatically. When you write your transforms, you specify type hints, which NMETL introspects upon startup. If it sees, for example, that a function expects a piece of data to be a valid datetime, NMETL will ensure that the data can be coerced into a datetime object. Assuming it can, then by the time NMETL passes it to your function, it will be a Python datetime object. As you would expect, this willl work in exactly the same way regardless of whether the datetime originally came from a CSV file (which has no concept of a "datetime" object) or a database that has given us a "timestamp" column. By the time the data has reached your code, you no longer have to care, or even remember where it came from.

Storing and Extracting Your Data
--------------------------------

Eventually, your data has to be stored somewhere and extracted into a structure that's easy and convenient. NMETL does this in a somewhat novel way.

Typically, as your data moves through a pipeline, you create a series of temporary tables whose columns hold the results of your calculations. At the end of the pipeline, the tables that are remaining are the ones that are handed off to your customers.

This dependency on tables carries an underappreciated risk. If you discover that you'd actually like to deliver tables that are in a different format, or contain a different subset of your available data, you may discover that this involves rewriting large chunks of your pipeline. Or more realistically, it may involve appending many more steps onto the end of the pipeline to transform its output into the output you actually need. In other words, by having tables move through your pipeline, you're committing yourself to one-way decisions about the ultimate format of your data product.

NMETL bypasses this issue because its streaming design avoids tables completely. NMETL doesn't stream tables or other complex structures -- it streams isolated facts through your functions, where a "fact" represents a single datum such as "The customer whose ID is 12345 has an age of 25" or "The customer whose ID is 12345 has a name of 'John Doe'". This is a very simple structure that is easy to transform into any other format. But you don't have to worry about *how* the data is stored at all. Instead, you can focus on what the data *means*. NMETL will take care of storing it and export it into a set of tables at the end, in whatever format you choose.

Trade-offs
==========

Of course, no new system for doing anything complex is without trade-offs. NMETL is no exception. The design philosophy of this package is that we will happily trade off almost anything in order to maintain simplicity, maintainability, discoverability, and robustness.

The reason for this decision is a simple observation. Most of us don't live in the world of "big data", where we have to search through petabytes of data per second. And if we do, then there are already excellent tools for handling that. Instead, we live in a world of "complicated, *ad-hoc* data", by which I mean that the problem isn't the sheer volume of data, but the complexity of the relationships among the data, inconsistencies in conventions, and the fact that traditional databases and data warehouses don't satisfy the functional requirements that are demanded by modern machine learning and AI applications. We are not buried by the volume of the data, but the complexity of it.

With all that in mind, you won't be surprised that there's a learning curve to NMETL. For example, you specify your data requirements in a domain specific language (Cypher) that is typically used for graph databases (although there is no graph database involved). Cypher is not nearly as familiar to most people as SQL. But you'll find that not only is it fairly easy to learn, but that a few trivially simple patterns can handle the vast majority of ETL use-cases. Another potential source of friction when you're new to NMETL is that, if you're used to thinking about data pipelines imperatively, it could take a while to get used to the declarative style. As opposed to other tools, NMETL asks you to write functions that you never actually call. You simply have to follow some conventions and trust NMETL to figure out the actual process of calling them at the right times.

Second, as I mentioned above, NMETL doesn't use tables or other complex structures to store data. Although this design decision is very deliberate, there's a cost here, too. We have excellent tools for performing operations on gigantic tables, but NMETL can't use any of them. If you're a data engineer at a company that absolutely requires very fast operations on enormous tables, NMETL might not be the right tool for you. However, you might also find that you only *thought* that you needed tables and that in fact, you don't. In other words, I'd encourage you to keep an open mind about your technical requirements.

Third, there's another performance consideration. NMETL is written in pure Python, and so all the usual performance considerations with Python apply here, too. NMETL tries to get around issues of poor performance by utilizing a fast key-value store and Dask for parallelism. But there's still an unavoidable overhead to NMETL's design. We are also assuming that the excellent ongoing projects to improve Python performance (free-threaded Python, no-GIL, subinterpreters, JIT compilers) will continue to provide benefits to Python's overall performance.
