import abc
import polars as pl
import spotify_intelligence.Utils as Utils
from spotify_intelligence.silver_layer.generics.TableTracker import TableTracker
from typing import Optional


class SilverRawTable(abc.ABC):
    tracker: Optional[TableTracker] = None

    def __init__(
        self,
        source_name: str,
        table_name: str,
        read_path: str = None,
        write_path: str = None,
        table_format: str = "PARQUET",
        partitioning: list[str] = [],
    ):
        self.source_name = source_name
        self.table_name = table_name
        self.read_path = read_path
        self.write_path = write_path
        self.table_format = table_format
        self.partitioning = partitioning
        self.logger = Utils.DataLogger().setup_logger(
            name=f"{source_name}.{table_name}"
        )

    def read_raw(self) -> pl.DataFrame:
        raise NotImplementedError("Subclasses must implement read_raw method.")

    def apply_rules(self, df: pl.DataFrame) -> pl.DataFrame:
        raise NotImplementedError("Subclasses must implement apply_rules method.")

    def save_silver(self, df: pl.DataFrame) -> None:
        raise NotImplementedError("Subclasses must implement save_silver method.")
