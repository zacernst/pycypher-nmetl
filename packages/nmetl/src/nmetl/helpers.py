"""Place for functions that might be used across the project."""

import base64
import datetime
import pickle
import queue
import time
import uuid
from pathlib import Path
from typing import Any, Generator, Optional, Type
from urllib.parse import ParseResult, urlparse

from nmetl.config import (  # pylint: disable=no-name-in-module
    DEFAULT_QUEUE_SIZE,
    INNER_QUEUE_TIMEOUT,
    OUTER_QUEUE_TIMEOUT,
)
from nmetl.message_types import EndOfData
from shared.logger import LOGGER


class Idle:  # pylint: disable=too-few-public-methods
    """Simply a message that is sent when a queue is idle."""


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
        *args,  # pylint: disable=unused-argument
        inner_queue_timeout: Optional[int] = INNER_QUEUE_TIMEOUT,
        end_of_queue_cls: Optional[Type] = EndOfData,
        outer_queue_timeout: Optional[int] = OUTER_QUEUE_TIMEOUT,
        name: Optional[str] = uuid.uuid4().hex,
        use_cache: Optional[bool] = False,
        session: Optional["Session"] = None,  # type: ignore
        max_queue_size: Optional[int] = DEFAULT_QUEUE_SIZE,
        queue_class: Optional[Type] = queue.Queue,
        **kwargs,  # pylint: disable=unused-argument
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
        # super().__init__(*args, **kwargs)
        # Look up the queue class from the compute class
        self.max_queue_size = max_queue_size
        self.inner_queue_timeout = inner_queue_timeout
        self.end_of_queue_cls = end_of_queue_cls
        self.counter: int = 0
        self.outer_queue_timeout = outer_queue_timeout
        self.no_more_items = False  # ever
        self.exit_code = None
        self.name = name
        self.idle = False
        self.session = session
        self.incoming_queue_processors = []
        self.timed_cache = {}
        self.queue_class = queue_class
        self.queue = queue.Queue(maxsize=self.max_queue_size)
        self.use_cache = use_cache

        if self.session:
            self.session.queue_list.append(self)

    def yield_items(
        self, quit_at_idle: bool = False
    ) -> Generator[Any, None, None]:
        """Generate items.

        ``quit_at_idle`` is for testing.
        """
        running = True
        finished_incoming_data_source_counter = 0
        while running:
            try:
                item = self.get(timeout=1.0)
            except queue.Empty:
                self.idle = True
                self.put(Idle())
                if quit_at_idle:
                    running = False
                    self.exit_code = 1
                    return
                continue
            self.idle = False
            if isinstance(item, self.end_of_queue_cls):
                finished_incoming_data_source_counter += 1
                if finished_incoming_data_source_counter >= len(
                    self.incoming_queue_processors
                ):
                    if finished_incoming_data_source_counter > len(
                        self.incoming_queue_processors
                    ):
                        LOGGER.warning(
                            "More EndOfData items than incoming data sources. Okay in unit tests."
                        )
                    running = False
                    self.no_more_items = True
                    self.exit_code = 0
                    return
                continue
            self.counter += 1
            yield item

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

    def put(self, item: Any, **kwargs) -> None:
        """Put an item on the queue."""
        if self.session:
            item.session = self.session
        if not self.ignore_item(item):
            self.queue.put(item, **kwargs)

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


def encode(obj: Any, to_bytes: bool = False) -> str:
    """Encode an object as a base64 string."""
    try:
        encoded = base64.b64encode(pickle.dumps(obj)).decode("utf-8")
    except Exception as e:
        LOGGER.error("Error encoding object to base64 string: %s", obj)
        raise ValueError(f"Error encoding object to base64 string: {e}") from e
    if to_bytes:
        return encoded.encode("utf-8")
    return encoded
