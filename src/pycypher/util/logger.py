"""Just a logger."""

import logging

from rich.logging import RichHandler

from pycypher.util.config import LOGGING_LEVEL  # pylint: disable=no-name-in-module

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(getattr(logging, LOGGING_LEVEL))
LOGGER.addHandler(RichHandler())
