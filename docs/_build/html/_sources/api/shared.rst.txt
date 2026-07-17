Shared API
==========

The Shared package contains common utilities used across all PyCypher packages.

.. automodule:: shared
   :members:
   :undoc-members:
   :show-inheritance:

Core Modules
------------

Logger
~~~~~~

Structured logging configuration shared across all packages.

.. automodule:: shared.logger
   :members:
   :undoc-members:
   :show-inheritance:

Metrics
~~~~~~~

Query execution metrics collection, snapshots, and diagnostic summaries.

.. automodule:: shared.metrics
   :members:
   :undoc-members:
   :show-inheritance:

Helpers
~~~~~~~

Serialisation and utility functions.

.. automodule:: shared.helpers
   :members:
   :undoc-members:
   :show-inheritance:

Telemetry
~~~~~~~~~

Optional telemetry integration (Pyroscope profiling).

.. automodule:: shared.telemetry
   :members:
   :undoc-members:
   :show-inheritance:

Observability
-------------

OpenTelemetry
~~~~~~~~~~~~~

Tracing and span instrumentation for query execution phases.
Falls back to no-op stubs when ``opentelemetry`` is not installed.

.. automodule:: shared.otel
   :members:
   :undoc-members:
   :show-inheritance:

Exporters
~~~~~~~~~

Metrics export adapters: Prometheus, StatsD, and JSON file exporters
with a unified ``MetricsExporter`` protocol.

.. automodule:: shared.exporters
   :members:
   :undoc-members:
   :show-inheritance:

Compatibility
-------------

API Surface Compatibility
~~~~~~~~~~~~~~~~~~~~~~~~~

Snapshot and diff the public API surface for backward-compatibility
checking between releases.

.. automodule:: shared.compat
   :members:
   :undoc-members:
   :show-inheritance:

Deprecation Utilities
~~~~~~~~~~~~~~~~~~~~~

Emit structured deprecation warnings with configurable severity and
suggested replacements.

.. automodule:: shared.deprecation
   :members:
   :undoc-members:
   :show-inheritance:
