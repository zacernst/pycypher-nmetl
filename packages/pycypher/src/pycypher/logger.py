"""Just a logger."""

import logging

from rich.logging import RichHandler

LOGGING_LEVEL = "INFO"
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(getattr(logging, LOGGING_LEVEL))
LOGGER.addHandler(RichHandler())
