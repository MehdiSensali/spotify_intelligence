from dataclasses import dataclass
import abc


@dataclass
class RawTable:
    name: str
    relative_path: str
    format: str
    partitioning: list[str]
    
    def read_raw(self):
        pass


class RawSource(abc.ABC):
    source_name = None
    RawTable: dict[str, RawTable]
    source_path = None

    def __init__(self, source_name: str):
        self.source_name = source_name

    def get_table(self, table_name: str) -> RawTable:
        if table_name in self.RawTable:
            return self.RawTable[table_name]
        else:
            raise ValueError(
                f"Table {table_name} not found in source {self.source_name}."
            )
