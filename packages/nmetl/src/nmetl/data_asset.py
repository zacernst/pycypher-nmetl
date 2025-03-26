'''Container for data that's used inside functions, but not contained in streams,
e.g. lookup tables, etc.'''
from typing import Optional, Any
import uuid


class DataAsset:
    '''Container -- not a very interesting class.'''
    def __init__(self, name: Optional[str] = None, obj: Optional[Any] = None):
        self.name = name or uuid.uuid4().hex
        self.obj = obj
