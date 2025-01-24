"""Place for functions that might be used across the project."""

import queue
import time
from pathlib import Path
from typing import Any, Generator, Optional, Type
from urllib.parse import ParseResult, urlparse

from pycypher.etl.message_types import EndOfData
from pycypher.util.logger import LOGGER


def ensure_uri(uri: str | ParseResult | Path) -> ParseResult:
    """
    Ensure that the URI is parsed.

    Args:
        uri: The URI to ensure is parsed

    Returns:
        The URI as a ``ParseResult``
    """
    if isinstance(uri, ParseResult):
        pass
    elif isinstance(uri, str):
        uri = urlparse(uri)
    elif isinstance(uri, Path):
        uri = urlparse(uri.as_uri())
    else:
        raise ValueError(
            f"URI must be a string or ParseResult, not {type(uri)}"
        )
    LOGGER.debug("URI converted: %s", uri)
    return uri


class QueueGenerator:  # pylint: disable=too-few-public-methods
    """A queue that also generates items."""

    def __init__(
        self,
        *args,
        timeout: Optional[int] = 1,
        end_of_queue_cls: Optional[Type] = EndOfData,
        max_timeout: Optional[int] = 1,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.queue = queue.Queue()
        self.timeout = timeout
        self.end_of_queue_cls = end_of_queue_cls
        self.counter: int = 0
        self.max_timeout = max_timeout
        self.no_more_items = False  # ever
        self.exit_code = None

    def yield_items(self) -> Generator[Any, None, None]:
        """Generate items."""
        last_time = time.time()
        running = True
        exit_code = 0
        while running:
            while True:
                if (time.time() - last_time) > self.max_timeout:
                    running = False
                    exit_code = 1
                    break

                try:
                    item = self.get(timeout=self.timeout)
                except queue.Empty:
                    break
                if isinstance(item, self.end_of_queue_cls):
                    running = False
                    break
                self.counter += 1
                last_time = time.time()
                yield item
        self.no_more_items = True
        if exit_code == 1:
            LOGGER.warning("Exiting generator due to timeout")
        elif exit_code == 0:
            LOGGER.warning("Exiting generator normally")
        self.exit_code = exit_code

    @property
    def completed(self) -> bool:
        """Is the queue completed? Has ``EndOfData`` been received?"""
        return self.no_more_items

    def empty(self) -> bool:
        """Is the queue empty?"""
        return self.queue.empty

    def get(self, **kwargs) -> Any:
        """Get an item from the queue."""
        return self.queue.get(**kwargs)

    def put(self, item: Any) -> None:
        """Put an item on the queue."""
        self.queue.put(item)
