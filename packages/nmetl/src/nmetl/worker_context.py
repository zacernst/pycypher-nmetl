import threading
from typing import Dict, Any

class WorkerContext:
    """Thread-local storage for worker data."""
    
    _local = threading.local()
    _global_data: Dict[str, Any] = {}
    _lock = threading.RLock()
    
    @classmethod
    def set_global_data(cls, key: str, value: Any):
        """Set data available to all workers."""
        with cls._lock:
            cls._global_data[key] = value
    
    @classmethod
    def get_data(cls, key: str, default=None):
        """Get data from thread-local or global storage."""
        # Check thread-local first
        if hasattr(cls._local, 'data') and key in cls._local.data:
            return cls._local.data[key]
        
        # Fall back to global data
        with cls._lock:
            return cls._global_data.get(key, default)
    
    @classmethod
    def set_local_data(cls, key: str, value: Any):
        """Set thread-local data."""
        if not hasattr(cls._local, 'data'):
            cls._local.data = {}
        cls._local.data[key] = value