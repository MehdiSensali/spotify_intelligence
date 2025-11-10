import abc
import polars as pl
from spotify_intelligence.silver_layer.SilverRawTable import SilverRawTable


class RawSource(abc.ABC):
    source_name = None
    RawTables: dict[str, SilverRawTable]
    source_path = None

    def __init__(self, source_name: str):
        self.source_name = source_name

    def get_table(self, table_name: str) -> SilverRawTable:
        if table_name in self.RawTables:
            return self.RawTables[table_name]
        else:
            raise ValueError(
                f"Table {table_name} not found in source {self.source_name}."
            )
