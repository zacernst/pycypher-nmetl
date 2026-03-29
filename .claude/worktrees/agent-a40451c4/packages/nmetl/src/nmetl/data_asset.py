"""Container for data that's used inside functions, but not contained in streams,
e.g. lookup tables, etc.
"""

import uuid
from typing import Any, Optional


class DataAsset:
    """Container -- not a very interesting class."""

    def __init__(self, name: str | None = None, obj: Any | None = None):
        self.name = name or uuid.uuid4().hex
        self.obj = obj
