# shared

Shared utilities, logging, metrics, and observability for the PyCypher ecosystem.

## What is this?

The `shared` package provides cross-cutting infrastructure used by `pycypher` and `fastopendata`. It has minimal dependencies (only `rich`) and covers logging, metrics collection, telemetry, and common helpers.

## Installation

```bash
pip install shared
```

Installed automatically as a dependency of `pycypher`.

## Modules

### `shared.logger`

Centralized logging with Rich console formatting or JSON-lines output.

```python
from shared.logger import get_logger

logger = get_logger("my_module")
logger.info("Processing query", extra={"query_id": "abc-123"})
```

Configure via environment variables:

- `PYCYPHER_LOG_LEVEL` -- logging level (default: `WARNING`)
- `PYCYPHER_LOG_FORMAT` -- `rich` (default) or `json`

### `shared.metrics`

Lightweight, thread-safe query metrics collector.

```python
from shared.metrics import QUERY_METRICS

snapshot = QUERY_METRICS.snapshot()
print(snapshot.summary())
```

Tracks query counts, timing percentiles (p50/p90/p99), throughput, cache hit rates, memory usage, and error breakdowns.

### `shared.exporters`

Pluggable metrics export to Prometheus, StatsD, or JSON files.

```bash
export PYCYPHER_METRICS_EXPORT=prometheus,statsd
```

### `shared.otel`

Optional OpenTelemetry distributed tracing (graceful no-op if not installed).

```bash
export PYCYPHER_OTEL_ENABLED=1
```

### `shared.helpers`

Common utilities: null checks, string matching, URI handling, base64 encoding.

### `shared.deprecation`

Standardized deprecation warnings with a `@deprecated()` decorator.

### `shared.compat`

API surface snapshot and diff tools for detecting breaking changes.

## Documentation

Full docs: [https://zacernst.github.io/pycypher/](https://zacernst.github.io/pycypher/)

## License

MIT
