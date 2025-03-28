"""Container for data that's used inside functions, but not contained in streams,
e.g. lookup tables, etc."""

import uuid
from typing import Any, Optional


class DataAsset:
    """Container -- not a very interesting class."""

    def __init__(self, name: Optional[str] = None, obj: Optional[Any] = None):
        self.name = name or uuid.uuid4().hex
        self.obj = obj
