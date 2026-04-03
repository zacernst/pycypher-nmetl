Data Preview Capability
=======================

The TUI data preview functionality allows you to inspect data sources before finalizing your pipeline configuration. This feature provides async loading, tabular display, and comprehensive metadata about your data sources.

Overview
--------

The data preview capability integrates with the existing DataSourceIntrospector infrastructure to provide:

- **Async data sampling** with loading indicators
- **Tabular data display** with scrolling support
- **Schema information** (column types, row count)
- **Column statistics** (null counts, unique values, min/max)
- **Error handling** for invalid or inaccessible sources

Usage
-----

Accessing Data Preview
~~~~~~~~~~~~~~~~~~~~~~

1. Navigate to the **Data Sources** screen in the TUI
2. Select any configured data source using ``j/k`` navigation
3. Press ``p`` to open the data preview dialog
4. Use ``Tab`` to switch between preview tabs
5. Press ``Escape`` to close the dialog

Preview Tabs
~~~~~~~~~~~~

Sample Data Tab
~~~~~~~~~~~~~~~
Displays the first 100 rows of your data source in a scrollable table format. This gives you a quick view of the actual data values and structure.

Schema Tab
~~~~~~~~~~
Shows comprehensive schema information:

- Total row count
- Total column count
- Column names and data types
- Detected Arrow/Parquet type information

Statistics Tab
~~~~~~~~~~~~~~
Provides per-column statistical analysis:

- Data type for each column
- Null value counts
- Unique value counts
- Min/max values (for ordered types)

Supported File Types
--------------------

The data preview supports all file types handled by the DataSourceIntrospector:

- **CSV files** (``.csv``) - with automatic delimiter detection
- **Parquet files** (``.parquet``) - with native Arrow support
- **JSON files** (``.json``) - with automatic schema inference
- **In-memory sources** - pandas DataFrames and Arrow tables

Performance Considerations
--------------------------

Caching
~~~~~~~
The preview system uses an LRU cache (``PreviewCache``) to avoid re-loading data:

- Cache size: 16 entries by default
- Automatic cache key generation based on source URI and sampling parameters
- Cache hit/miss statistics available for debugging

Sampling Strategy
~~~~~~~~~~~~~~~~~
Data sampling uses configurable strategies:

- **HEAD** (default): First N rows - fastest for large files
- **TAIL**: Last N rows - useful for time-series data
- **RANDOM**: Random sampling - provides representative data distribution

Async Loading
~~~~~~~~~~~~~
All data loading operations are performed asynchronously to prevent UI blocking:

- Loading indicators show progress
- Cancellable operations when dialog is closed
- Error handling with user-friendly messages

Integration with Existing Components
------------------------------------

The data preview functionality builds on established infrastructure:

DataSourceIntrospector
~~~~~~~~~~~~~~~~~~~~~~
Leverages the existing ``DataSourceIntrospector`` class for:

- Schema detection via ``get_schema()``
- Data sampling via ``sample()``
- Column statistics via ``get_column_stats()``

VimNavigableScreen
~~~~~~~~~~~~~~~~~~
Extends the base screen functionality with:

- Additional ``p`` key binding for preview
- Screen override key registration
- Consistent VIM-style navigation

Dialog System
~~~~~~~~~~~~~
Uses the established modal dialog pattern:

- ``VimDialog`` base class inheritance
- Consistent keyboard shortcuts (Escape to close)
- Proper focus management and event handling

Error Handling
--------------

The preview system provides robust error handling:

File Access Errors
~~~~~~~~~~~~~~~~~~
- Missing files: "File not found" with full path
- Permission errors: "Access denied" with suggestions
- Network issues: Timeout and connection error messages

Data Format Errors
~~~~~~~~~~~~~~~~~~
- Invalid CSV: Parsing errors with line numbers
- Corrupted files: Detailed error descriptions
- Schema conflicts: Type inference issues

Performance Errors
~~~~~~~~~~~~~~~~~~
- Large file warnings: Automatic sampling recommendations
- Memory limits: Graceful degradation with smaller samples
- Timeout handling: Cancellable long-running operations

Developer Notes
---------------

Architecture
~~~~~~~~~~~~
The data preview system follows a layered architecture:

1. **UI Layer**: ``DataPreviewDialog`` - Textual widgets and layout
2. **Integration Layer**: ``DataSourcesScreen`` - VIM key handling and navigation
3. **Data Layer**: ``DataSampler`` - Core sampling and introspection
4. **Infrastructure Layer**: ``PreviewCache`` - Performance optimization

Extension Points
~~~~~~~~~~~~~~~~
The system is designed for extensibility:

- **Custom sampling strategies**: Implement ``SamplingStrategy`` enum values
- **Additional tabs**: Extend ``DataPreviewDialog`` with new ``TabPane`` widgets
- **Custom statistics**: Override ``DataSampler.column_stats()`` for domain-specific metrics
- **Preview formats**: Add support for new file types via ``DataSourceIntrospector``

Testing
~~~~~~~
Comprehensive test coverage includes:

- Unit tests for dialog components
- Integration tests with ``DataSourcesScreen``
- Error condition testing
- Performance benchmarks
- Mock-based testing to avoid UI dependencies

Example Usage
-------------

Basic Preview Workflow
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # In TUI application context
    from pycypher_tui.widgets import DataPreviewDialog

    # Create preview dialog
    dialog = DataPreviewDialog(
        source_uri="data/customers.csv",
        source_id="customers"
    )

    # Show dialog (handled by TUI framework)
    app.push_screen(dialog)

Programmatic Data Sampling
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from pycypher.ingestion.data_preview import DataSampler, SamplingStrategy

    # Create sampler with caching
    sampler = DataSampler("data/large_dataset.parquet")

    # Get schema information
    schema = sampler.schema()
    print(f"Columns: {len(schema.column_names)}")
    print(f"Rows: {schema.row_count:,}")

    # Sample data with different strategies
    head_sample = sampler.sample(n=100, strategy=SamplingStrategy.HEAD)
    random_sample = sampler.sample(n=100, strategy=SamplingStrategy.RANDOM)

    # Get column statistics
    stats = sampler.all_column_stats()
    for col_name, col_stats in stats.items():
        print(f"{col_name}: {col_stats.null_count} nulls, {col_stats.unique_count} unique")

See Also
--------

- :doc:`/user_guide/data_sources` - Data source configuration
- :doc:`/user_guide/performance_tuning` - Performance optimization
- :doc:`/developer_guide/tui_architecture` - TUI development guide