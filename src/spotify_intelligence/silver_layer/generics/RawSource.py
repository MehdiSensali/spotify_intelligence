import abc
import os
from spotify_intelligence.silver_layer.generics.SilverRawTable import SilverRawTable
from spotify_intelligence.silver_layer.generics.TableTracker import TableTracker


class RawSource(abc.ABC):
    source_name = None
    RawTables: dict[str, SilverRawTable]
    source_path = None

    def __init__(self, source_name: str):
        self.source_name = source_name
        for raw_table in self.RawTables.values():
            raw_table.read_path = str(
                os.path.join(
                    self.source_path,
                    source_name,
                    raw_table.table_name,
                    "raw",
                    f"{raw_table.table_name}.{raw_table.table_format}",
                ),
            )
            raw_table.write_path = str(
                os.path.join(
                    self.source_path,
                    source_name,
                    raw_table.table_name,
                    "data",
                    f"{raw_table.table_name}.PARQUET",
                )
            )
            raw_table.tracker = TableTracker(raw_table.read_path)

    def get_table(self, table_name: str) -> SilverRawTable:
        if table_name in self.RawTables:
            return self.RawTables[table_name]
        else:
            raise ValueError(
                f"Table {table_name} not found in source {self.source_name}."
            )
