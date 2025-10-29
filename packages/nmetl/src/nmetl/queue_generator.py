"""Just a test."""

from __future__ import annotations

import base64
import queue
import threading
import uuid
from typing import Any, Callable, Generator, List, Optional

# import pickle
import dill as pickle
from nmetl.logger import LOGGER
from pycypher.query import NullResult


class Shutdown:
    pass


class QueueGenerator:
    """Thread-safe queue with enhanced coordination."""

    def __init__(self, name: str, maxsize: int = 1):
        self.name = name
        self.queue = queue.Queue(maxsize=maxsize)
        self._shutdown_event = threading.Event()
        self._stats_lock = threading.Lock()
        self.items_processed = 0
        self.items_queued = 0

    def put(self, item: Any, timeout: Optional[float] = None):
        """Put item in queue with optional timeout."""
        if self._shutdown_event.is_set():
            return False

        try:
            self.queue.put(item, timeout=timeout)
            with self._stats_lock:
                self.items_queued += 1
            return True
        except queue.Full:
            return False

    def get(self, timeout: Optional[float] = None) -> Optional[Any]:
        """Get item from queue with optional timeout."""
        if self._shutdown_event.is_set() and self.queue.empty():
            return None

        try:
            item = self.queue.get(timeout=timeout)
            with self._stats_lock:
                self.items_processed += 1
            return item
        except queue.Empty:
            return None

    def yield_items(self) -> Generator[Any, None, None]:
        """Generate items."""
        LOGGER.info("YIELD ITEMS CALLED %s", self.name)
        while True:
            item: Any = self.get()
            if item is None:
                continue
            if isinstance(item, Shutdown):
                break
            yield item


if __name__ == "__main__":
    pass
