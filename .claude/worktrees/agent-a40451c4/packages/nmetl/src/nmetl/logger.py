"""Just a logger."""

import logging

from rich.logging import RichHandler

from nmetl.config import LOGGING_LEVEL

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(getattr(logging, LOGGING_LEVEL))
LOGGER.addHandler(RichHandler())
