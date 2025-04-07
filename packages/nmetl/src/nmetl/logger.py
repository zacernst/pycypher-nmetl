"""Just a logger."""

import logging

from nmetl.config import LOGGING_LEVEL
from rich.logging import RichHandler

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(getattr(logging, LOGGING_LEVEL))
LOGGER.addHandler(RichHandler())
