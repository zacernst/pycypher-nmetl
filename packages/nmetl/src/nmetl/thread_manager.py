import threading
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, Callable, Optional
import weakref

class ThreadManager:
    """Manages thread pools and worker coordination."""
    
    def __init__(self, max_workers: int = 10):
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.worker_data: Dict[int, Dict[str, Any]] = {}
        self.worker_callbacks: List[Callable] = []
        self._lock = threading.RLock()
    
    def submit_task(self, func: Callable, *args, priority: int = 0, **kwargs):
        """Submit a task to the thread pool."""
        return self.executor.submit(func, *args, **kwargs)
    
    def register_worker_callback(self, callback: Callable):
        """Register a callback to be called on worker initialization."""
        with self._lock:
            self.worker_callbacks.append(callback)
    
    def broadcast_data(self, key: str, data: Any):
        """Broadcast data to all worker threads."""
        with self._lock:
            for worker_id in self.worker_data:
                self.worker_data[worker_id][key] = data
    
    def shutdown(self):
        """Shutdown the thread pool."""
        self.executor.shutdown(wait=True)