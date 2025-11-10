from typing import Optional
from dataclasses import dataclass
import os


class PrepareData:
    data_path = None
    table: Optional[RawTable] = None

    def __init__(self):
        if self.table is None:
            raise ValueError("Table must be defined for PrepareData.")
        self.full_path = os.path.join(self.data_path, self.table.relative_path)


@dataclass
class RawTable:
    name: str
    relative_path: str
    partitioning: list[str]
