"""For recording the lineage of a ``Fact``."""

import datetime
from typing import Any


class Lineage:
    def __init__(self) -> None:
        self.time = datetime.datetime.now()

    pass


class FromMapping(Lineage):
    def __init__(self, mapping=None, row=None) -> None:
        self.mapping = mapping
        self.row = row
        super().__init__()


class FromRawData(Lineage):
    def __init__(self, data_source_name, mapping: dict[str, Any] = {}) -> None:
        self.data_source_name = data_source_name
        super().__init__()


class Appended(Lineage):
    def __init__(self, lineage=None) -> None:
        self.lineage = lineage
        super().__init__()
