"""Shared logging configuration module.

This module provides a centralized logger configuration using Rich for
enhanced console output formatting. The logger is configured with a
default WARNING level and uses RichHandler for colored output.

Attributes:
    LOGGING_LEVEL: Default logging level as string.
    LOGGER: Configured logger instance with Rich formatting.
"""

import logging

from rich.logging import RichHandler

LOGGING_LEVEL = "WARNING"
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(getattr(logging, LOGGING_LEVEL))
LOGGER.addHandler(RichHandler())
