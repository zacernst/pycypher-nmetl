import logging

from rich.logging import RichHandler

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
LOGGER.addHandler(RichHandler())
