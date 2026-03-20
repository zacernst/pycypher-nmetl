"""Configuration for Pyroscope continuous profiling.

All settings are configurable via environment variables:

- ``PYROSCOPE_SERVER``  – Pyroscope endpoint (default ``http://localhost:4040``)
- ``PYROSCOPE_APP_NAME`` – application name tag (default ``nmetl``)
- ``PYROSCOPE_SAMPLE_RATE`` – samples per second (default ``100``)
- ``PYROSCOPE_ENABLED``  – set to ``0`` or ``false`` to disable (default enabled)

If Pyroscope is not installed or the server is unreachable the module
silently degrades — no import-time crash.
"""

import logging
import os

_logger = logging.getLogger(__name__)

_enabled = os.environ.get("PYROSCOPE_ENABLED", "1").lower() not in (
    "0",
    "false",
    "no",
)

if _enabled:
    try:
        import pyroscope

        pyroscope.configure(
            application_name=os.environ.get("PYROSCOPE_APP_NAME", "nmetl"),
            server_address=os.environ.get(
                "PYROSCOPE_SERVER",
                "http://localhost:4040",
            ),
            sample_rate=int(os.environ.get("PYROSCOPE_SAMPLE_RATE", "100")),
            detect_subprocesses=True,
            oncpu=True,
            gil_only=False,
            enable_logging=False,
        )
    except ImportError:
        _logger.debug(
            "pyroscope not installed — continuous profiling disabled",
        )
    except Exception:  # noqa: BLE001
        _logger.warning(
            "pyroscope configuration failed — profiling disabled",
            exc_info=True,
        )
