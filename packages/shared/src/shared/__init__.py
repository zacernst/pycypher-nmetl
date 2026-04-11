"""Shared utilities and common functionality for PyCypher-NMETL.

This package provides shared utilities, logging configuration, and helper
functions used across the PyCypher-NMETL ecosystem.

Submodules
----------

``shared.helpers``
    Common utility functions: null checks, string matching, URI handling,
    base64 encoding/decoding.

``shared.logger``
    Centralised logging configuration with Rich console formatting and
    optional JSON output.

``shared.metrics``
    Lightweight in-process query metrics collector for diagnostic access.

``shared.telemetry``
    Optional Pyroscope continuous profiling configuration.

``shared.otel``
    OpenTelemetry distributed tracing integration (optional dependency).

``shared.exporters``
    Pluggable metrics export adapters (Prometheus, StatsD, JSON).

``shared.regression_detector``
    Statistical performance regression detection engine using z-score
    analysis with configurable sensitivity thresholds.

``shared.alerting``
    Configurable alerting system with threshold rules, cooldown support,
    and pluggable notification handlers.

``shared.continuous_monitor``
    Continuous metrics monitoring orchestration that ties together the
    metrics collector, dashboard, alerting, regression detection, and
    exporters into a single periodic monitoring loop.

``shared.dashboard``
    Performance monitoring dashboard with web-based visualization,
    real-time metric updates, and JSON API.

``shared.deprecation``
    Standardised deprecation warnings with ``deprecated()`` decorator and
    ``emit_deprecation()`` helper for graceful API evolution.

``shared.compat``
    API surface snapshot/diff tools and Neo4j Cypher compatibility notes
    for detecting breaking changes and guiding migrations.

``shared.file_intent``
    File Intent Registration System for multi-agent coordination.
    Agents register intent before editing shared files; the system
    classifies risk levels and warns on conflicts.
"""

__all__ = [
    "alerting",
    "compat",
    "continuous_monitor",
    "dashboard",
    "deprecation",
    "file_intent",
    "exporters",
    "helpers",
    "logger",
    "metrics",
    "otel",
    "regression_detector",
    "telemetry",
]
