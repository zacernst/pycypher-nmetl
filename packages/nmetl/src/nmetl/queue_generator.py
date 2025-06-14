"""Place for functions that might be used across the project."""

from __future__ import annotations

import multiprocessing as mp
import queue
import uuid
from typing import TYPE_CHECKING, Any, Generator, Optional, Type

if TYPE_CHECKING:
    from nmetl.session import Session  # pylint: disable=cyclic-import

from nmetl.config import DEFAULT_QUEUE_SIZE  # pyrefly: ignore
from nmetl.config import INNER_QUEUE_TIMEOUT  # pyrefly: ignore
from nmetl.config import OUTER_QUEUE_TIMEOUT  # pyrefly: ignore
from nmetl.message_types import EndOfData
from shared.logger import LOGGER


class Idle:  # pylint: disable=too-few-public-methods
    """Simply a message that is sent when a queue is idle."""


class QueueGenerator:  # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """A queue that also generates items."""

    def __init__(
        self,
        *args,  # pylint: disable=unused-argument
        inner_queue_timeout: int = INNER_QUEUE_TIMEOUT,
        end_of_queue_cls: Type = EndOfData,
        outer_queue_timeout: int = OUTER_QUEUE_TIMEOUT,
        name: str = uuid.uuid4().hex,
        use_cache: bool = False,
        session: Optional["Session"] = None,  # type: ignore
        max_queue_size: int = DEFAULT_QUEUE_SIZE,
        queue_cls: Type[mp.Queue] | Type[queue.Queue] = queue.Queue,
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
        self.session = session
        self.end_of_queue_cls = end_of_queue_cls
        self.counter: int = 0
        self.outer_queue_timeout = outer_queue_timeout
        self.no_more_items = False  # ever
        self.exit_code = None
        self.name = name
        self.idle = False
        # self.session = session
        self.incoming_queue_processors = []
        self.queue_cls = queue_cls
        self.queue = queue_cls()  # TODO: Add max queue sizes later
        # self.queue = queue.Queue(maxsize=self.max_queue_size)
        self.use_cache = use_cache

        if self.session:  #  and isinstance(self.session, Session):
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
        # if self.session:
        #     item.session = self.session
        # if not self.ignore_item(item):
        self.queue.put(item, **kwargs)

    def ignore_item(self, item: Any) -> bool:
        """Should the item be ignored?"""
        return False
