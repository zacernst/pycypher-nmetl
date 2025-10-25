.. NMETL documentation master file, created by
   sphinx-quickstart on Thu Oct 23 11:04:14 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

NMETL documentation
===================

This is the documentation for the NMETL library. NMETL stands for "New Methods for ETL". It is designed to make ETL simple, efficient, safe, and robust.

NMETL is an ETL (extraction, transformation, and loading) library for Python. Although there are many such libraries, NMETL takes a very different approach to the ETL problem. It is based on the belief that ETL is almost **never** challenging because of the volume of data, the computational demands, or the need to integrate incompatible formats. We know how to solve all those problems. Rather, ETL projects are difficult because of the **complexity** of the data, the focus on surface-level syntactic features of the data, and lack of an explicit data model. Because of this complexity, ETL projects typically become more complex at a faster rate as more data is added, resulting in an exponential blowup of complexity over time. NMETL aims to keep your ETL projects **simple**, easy to document, trivial to test, and a breeze to maintain.

To accomplish this, NMETL is designed to be:

- **Delarative:** There are no pipelines or transformations to your ETL process; instead, NMETL takes a totally declarative approach to data ingestion and feature engineering. In other words, NMETL figures out *how* to transform your data while you focus on what your data *means*.
- **Streaming/batch agnostic:** In NMETL, there is no difference between streaming and batch data. Both types of sources are treated in exactly the same way.
- **Safe:** Pydantic constantly validates all of your data at each step. It is impossible to insert data that has the wrong type.

What NMETL is not:

Almost (maybe even "all") existing frameworks for ETL are based on the same core principle that an ETL pipeline is a series of transformations performed (usually) on tables, in which the stages of the pipeline can be arranged as a directed, acyclic graph (or DAG). Airflow is a perfect example of this approach. It creates DAGs of tasks, and the framework ensures that the tasks are executed in the correct order. Retries are handled intelligently if a task fails, and tasks can be done efficiently by offloading them to specialized platforms such as PySpark or Dask. Airflow and similar tools are mainly *orchestrators* -- they solve problems that arise from dependency graphs, and they promise to do so at scale.

NMETL does not compete with Airflow or any other ETL framework. It is not an orchestrator; this is for the simple reason that task orchestration was never really the problem. Task orchestration is largely a solved problem, and has been at least since the development of `make`. Nor is NMETL a high-powered computational framework (although it is compatible with them). Again, the reason why NMETL is not a high-powered computational framework is that limits on computational resources are usually not salient in real-world ETL problems.

30,000 foot view
----------------

NMETL has a strong opinion about ETL, namely, that **complexity is the enemy**, not scale, orchestration, or computational resources. This system has a few unusual characteristics, which are all motivated by the need to constantly lower complexity.

- **Model primacy:** Raw data is immediately transformed into an intuitive model consisting of Entities, Attributes, and Relationships. All subsequent work on the data is done via the model. You never have to think about how the data happened to be loaded (e.g. as a database table, JSON blob, or CSV file).
- **Declarative approach:** To create new attributes or features from your data, you write simple Python functions that leverage decorators and type hinting. But you never have to call those functions or determine when (or in what order) they should be called. Instead, NMETL reads those decorators and type hints, and it figures out when to call the functions and how to record their results.
- **Format agnosticism:** NMETL does not store your data in tables, graphs, or any other complex structure. In fact, the user doesn't even have to know how the data is being stored. But you can easily export your data into those formats whenever you like, using a single command. The benefit is that you don't have to consider the problem of how your data will eventually land in exactly the right database, graph, or flat file. You just declare your data model, and NMETL will figure out how to export your data into arbitrary formats.

Other features of NMETL include:

- **Pure Python:** NMETL is a pure Python library. You won't suddenly discover that your system has the wrong Fortran compiler.
- **Speed:** NMETL is designed to be fast. It leverages advanced key/value stores for intermediate results, guaranteeing solid performance with concurrency and safety.
- **Safety:** Your ETL process is safe in two senses: (1) your data is validated at each step, and (2) your data won't be lost or corrupted because the underlying data store is highly fault-tolerant.
- **Incremental development:** NMETL supports incremental development. You can add new data sources, new attributes, or new features to your ETL process without having to make changes to your existing code. As you explore your data and discover new features to add, you can do so gradually without fear of breaking your work.
- **Iteration:** NMETL supports quick iteration in development. None of us are perfect, and no ETL project was ever done perfectly the first time. Because of its streaming design, you'll usually see examples of each computation almost immediately, even before any data sources have finished loading their raw data. This allows you to experiment with new ideas and iterate on your ETL process without having to wait for all your data to load or for previous computations to complete.
- **Small blast radius:** We've all seen examples where a single row of a big table happens to have an error, which causes a job involving the entire table to fail. As a result, the entire table has to be reloaded from scratch, wasting valuable time. NMETL doesn't create such a wide blast radius. Errors regarding a single data point affect only that data point and other data points that depend on it, not the entire data source.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   nmetl_tutorial
