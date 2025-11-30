from __future__ import annotations

import queue
import threading
import time
from typing import Any, Generator, Optional

from nmetl.logger import LOGGER


class Shutdown:
    pass


class QueueGenerator:
    """Thread-safe queue with enhanced coordination."""

    def __init__(self, name: str, session: "Session"):
        self.name = name
        self.session = session
        self.queue = queue.Queue(
            maxsize=self.session.configuration.max_queue_size
        )
        self._shutdown_event = threading.Event()
        self._stats_lock = threading.Lock()
        self.items_processed = 0
        self.items_queued = 0

    def bak__getattr__(self, attr: str) -> Any:
        if hasattr(self.session, attr):
            return getattr(self.session, attr)
        raise ValueError(f"Unknown attr: {attr}")

    def monitor_thread(self) -> None:
        while 1:
            time.sleep(1)

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
        LOGGER.debug("YIELD ITEMS CALLED %s", self.name)
        while True:
            item: Any = self.get()
            if item is None:
                continue
            if isinstance(item, Shutdown):
                break
            yield item


if __name__ == "__main__":
    pass
