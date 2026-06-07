import base64
import json
import pickle
from typing import Any


def decode_serialized(serialized: str) -> Any:
    out = base64.b64decode(serialized)
    return out
