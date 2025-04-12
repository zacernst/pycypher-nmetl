"""Place for functions that might be used across the project."""

import base64
import datetime
import pickle
import queue
import uuid
from pathlib import Path
from typing import Any, Generator, Optional, Type
from urllib.parse import ParseResult, urlparse

from nmetl.config import (  # pylint: disable=no-name-in-module
    INNER_QUEUE_TIMEOUT,
    OUTER_QUEUE_TIMEOUT,
)
from nmetl.message_types import EndOfData
from pycypher.logger import LOGGER

from nmetl.config import DEFAULT_QUEUE_SIZE

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


class QueueGenerator:  # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """A queue that also generates items."""

    def __init__(
        self,
        *args,
        inner_queue_timeout: Optional[int] = INNER_QUEUE_TIMEOUT,
        end_of_queue_cls: Optional[Type] = EndOfData,
        outer_queue_timeout: Optional[int] = OUTER_QUEUE_TIMEOUT,
        name: Optional[str] = uuid.uuid4().hex,
        use_cache: Optional[bool] = False,
        session: Optional["Session"] = None,  # type: ignore
        max_queue_size: Optional[int] = DEFAULT_QUEUE_SIZE,
        **kwargs,
    ) -> None:
        """
        Initialize a QueueGenerator instance.

        Args:
            *args: Variable positional arguments passed to the parent class.
            inner_queue_timeout (Optional[int]): Timeout for the inner queue. Defaults to INNER_QUEUE_TIMEOUT.
            end_of_queue_cls (Optional[Type]): Class to use for end-of-queue markers. Defaults to EndOfData.
            outer_queue_timeout (Optional[int]): Timeout for the outer queue. Defaults to OUTER_QUEUE_TIMEOUT.
            name (Optional[str]): Name for this queue. Defaults to a random UUID.
            use_cache (Optional[bool]): Whether to use caching. Defaults to False.
            session (Optional[Session]): The session this queue belongs to. Defaults to None.
            **kwargs: Variable keyword arguments passed to the parent class.
        """
        super().__init__(*args, **kwargs)
        self.max_queue_size = max_queue_size
        self.queue = queue.Queue(maxsize=self.max_queue_size)
        self.inner_queue_timeout = inner_queue_timeout
        self.end_of_queue_cls = end_of_queue_cls
        self.counter: int = 0
        self.outer_queue_timeout = outer_queue_timeout
        self.no_more_items = False  # ever
        self.exit_code = None
        self.name = name
        self.session = session
        self.incoming_queue_processors = []
        self.timed_cache = {}
        self.use_cache = use_cache

        if self.session:
            self.session.queue_list.append(self)

    def yield_items(self) -> Generator[Any, None, None]:
        """Generate items."""
        last_time = datetime.datetime.now()
        running = True
        exit_code = 0
        finished_incoming_data_source_counter = 0
        while running:
            while True:
                if (
                    datetime.datetime.now() - last_time
                ).total_seconds() > self.outer_queue_timeout:
                    running = False
                    exit_code = 1
                    break

                try:
                    item = self.get(timeout=self.inner_queue_timeout)
                except queue.Empty:
                    break
                # Need to check ALL of the incoming data sources
                if isinstance(item, self.end_of_queue_cls):
                    finished_incoming_data_source_counter += 1
                    if finished_incoming_data_source_counter == len(
                        self.incoming_queue_processors
                    ):
                        running = False
                        break
                    else:
                        continue
                self.counter += 1
                last_time = datetime.datetime.now()
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
        return self.queue.empty()

    def get(self, **kwargs) -> Any:
        """Get an item from the queue."""
        return self.queue.get(**kwargs)

    def put(self, item: Any) -> None:
        """Put an item on the queue."""
        if self.session:
            item.session = self.session
        if not self.ignore_item(item):
            LOGGER.debug("QUEUE: %s: %s", self.name, item)
            self.queue.put(item)
            self.timed_cache[hash(item)] = datetime.datetime.now()

    def ignore_item(self, item: Any) -> bool:
        """Should the item be ignored?"""
        if self.use_cache and hash(item) in self.timed_cache:
            return True
        return False


def decode(encoded: str) -> Any:
    """Decode a base64 encoded string."""
    try:
        decoded = pickle.loads(base64.b64decode(encoded))
    except Exception as e:
        raise ValueError(f"Error decoding base64 string: {e}") from e
    return decoded


def encode(obj: Any) -> str:
    """Encode an object as a base64 string."""
    try:
        encoded = base64.b64encode(pickle.dumps(obj)).decode("utf-8")
    except Exception as e:
        LOGGER.error("Error encoding object to base64 string: %s", obj)
        raise ValueError(f"Error encoding object to base64 string: {e}") from e
    return encoded
